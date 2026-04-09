"""
fetch_ohlc.py - Async 並發拉取 OHLC 日線數據
============================================================================
用法（PowerShell）：
  cd E:\Aurum-Infinity\Aurum_Data_Fetcher
  python fetch_ohlc.py                          # 全部市場（首次全量 1 年）
  python fetch_ohlc.py --market US              # 只拉美股
  python fetch_ohlc.py --market HK              # 只拉港股
  python fetch_ohlc.py --market CN              # 只拉A股
  python fetch_ohlc.py --ticker AAPL            # 單支測試
  python fetch_ohlc.py --incremental            # 增量更新（只拉新資料）
  python fetch_ohlc.py --concurrency 100        # 調整並發數
  python fetch_ohlc.py --days 365               # 自訂天數（預設 365）

策略：
  - 首次全量：每支拉 1 年日線（--days 365）
  - 每日增量：--incremental 只拉上次更新後的新資料
============================================================================
"""
import argparse
import asyncio
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print("ERROR: aiohttp not installed. Run: pip install aiohttp")
    sys.exit(1)

from dotenv import load_dotenv
load_dotenv()

# ============================================================================
# Config
# ============================================================================
FMP_API_KEY = os.getenv("FMP_API_KEY")
if not FMP_API_KEY:
    print("ERROR: FMP_API_KEY not set in .env")
    sys.exit(1)

BASE_URL = "https://financialmodelingprep.com/stable"
DB_PATH = os.getenv("DB_PATH",
    str(Path(__file__).resolve().parent / ".." / "Aurum_Infinity_AI" / "aurum.db"))
STOCK_LIST_PATH = os.getenv("STOCK_LIST_PATH",
    str(Path(__file__).resolve().parent / ".." / "Get_stock" / "data" / "stock_code.json"))


# ============================================================================
# Logger
# ============================================================================
def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    try:
        print(f"[{ts}] {msg}", flush=True)
    except UnicodeEncodeError:
        print(f"[{ts}] {msg.encode('ascii', 'replace').decode()}", flush=True)


# ============================================================================
# Load stock list
# ============================================================================
def load_stocks(market_filter: str | None = None) -> list[dict]:
    with open(STOCK_LIST_PATH, encoding="utf-8") as f:
        data = json.load(f)
    stocks = data if isinstance(data, list) else [{"symbol": k, **v} for k, v in data.items()]

    if market_filter:
        mf = market_filter.upper()
        if mf == "CN":
            stocks = [s for s in stocks if s.get("market", "").startswith("CN")]
        elif mf == "HK":
            stocks = [s for s in stocks if s.get("market", "") == "HK"]
        elif mf == "US":
            stocks = [s for s in stocks if s.get("market", "") == "US"]
    return stocks


# ============================================================================
# DB
# ============================================================================
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    return conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_last_ohlc_date(conn: sqlite3.Connection, ticker: str) -> str | None:
    row = conn.execute(
        "SELECT MAX(date) FROM ohlc_daily WHERE ticker = ?", (ticker,)
    ).fetchone()
    return row[0] if row and row[0] else None


def upsert_ohlc_batch(conn: sqlite3.Connection, ticker: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    conn.executemany("""
        INSERT OR REPLACE INTO ohlc_daily
            (ticker, date, open, high, low, close, adj_close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (ticker, r.get('date'), r.get('open'), r.get('high'), r.get('low'),
         r.get('close'), r.get('adjClose') or r.get('close'), r.get('volume'))
        for r in rows if r.get('date')
    ])
    conn.execute(
        "UPDATE stocks_master SET ohlc_updated_at = ? WHERE ticker = ?",
        (_now_iso(), ticker))
    return len(rows)


# ============================================================================
# Async fetcher
# ============================================================================
class AsyncOHLCFetcher:
    def __init__(self, concurrency: int = 100):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.session: aiohttp.ClientSession | None = None
        self.success = 0
        self.failed = 0
        self.skipped = 0
        self.total_rows = 0

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30))
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def _fetch_json(self, symbol: str, from_date: str, to_date: str,
                          retries: int = 2) -> list | None:
        url = f"{BASE_URL}/historical-price-eod/full"
        params = {
            "symbol": symbol,
            "from": from_date,
            "to": to_date,
            "apikey": FMP_API_KEY,
        }

        for attempt in range(retries + 1):
            async with self.semaphore:
                try:
                    async with self.session.get(url, params=params) as resp:
                        if resp.status == 429:
                            wait = 2 ** attempt
                            log(f"  429 rate limit, wait {wait}s ...")
                            await asyncio.sleep(wait)
                            continue
                        if resp.status >= 500:
                            if attempt < retries:
                                await asyncio.sleep(2)
                                continue
                            return None
                        if resp.status != 200:
                            return None
                        data = await resp.json()
                        if isinstance(data, list):
                            return data
                        if isinstance(data, dict) and "Error Message" in data:
                            return None
                        return None
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    if attempt < retries:
                        await asyncio.sleep(2)
                        continue
                    return None
        return None

    async def fetch_batch(self, tickers: list[str], days: int = 365,
                          incremental: bool = False):
        conn = get_db()
        total = len(tickers)
        done = 0
        start = time.time()
        today = datetime.now().strftime("%Y-%m-%d")
        default_from = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        COMMIT_EVERY = 50

        async def _process(ticker: str):
            nonlocal done

            if incremental:
                last_date = get_last_ohlc_date(conn, ticker)
                if last_date:
                    from_dt = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
                    from_date = from_dt.strftime("%Y-%m-%d")
                    if from_date > today:
                        self.skipped += 1
                        done += 1
                        return
                else:
                    from_date = default_from
            else:
                from_date = default_from

            data = await self._fetch_json(ticker, from_date, today)

            if data is None:
                self.failed += 1
            elif len(data) == 0:
                self.skipped += 1
            else:
                count = upsert_ohlc_batch(conn, ticker, data)
                self.total_rows += count
                self.success += 1

            done += 1
            if done % COMMIT_EVERY == 0:
                conn.commit()
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                log(f"  Progress: {done}/{total} ({done/total*100:.1f}%) "
                    f"| {self.total_rows:,} rows | {rate:.0f} stocks/s | ETA {eta:.0f}s")

        await asyncio.gather(*[_process(t) for t in tickers])
        conn.commit()
        conn.close()


# ============================================================================
# Main
# ============================================================================
async def async_main(args):
    stocks = load_stocks(args.market)

    if args.ticker:
        stocks = [s for s in stocks if s["symbol"] == args.ticker.upper()]
        if not stocks:
            stocks = [{"symbol": args.ticker.upper(), "market": "US"}]

    if not stocks:
        log("No stocks to fetch")
        return

    by_market = {}
    for s in stocks:
        by_market.setdefault(s.get("market", "?"), []).append(s["symbol"])

    mode = "incremental" if args.incremental else f"full ({args.days} days)"
    log(f"=== OHLC Fetcher ({mode}) ===")
    log(f"Total: {len(stocks)} stocks | Concurrency: {args.concurrency}")
    for m, t in sorted(by_market.items()):
        log(f"  {m}: {len(t)}")

    if args.dry_run:
        log("DRY RUN - not fetching")
        return

    tickers = [s["symbol"] for s in stocks]
    start = time.time()

    async with AsyncOHLCFetcher(concurrency=args.concurrency) as fetcher:
        await fetcher.fetch_batch(tickers, days=args.days,
                                  incremental=args.incremental)
        elapsed = time.time() - start
        log(f"")
        log(f"=== DONE in {elapsed:.0f}s ===")
        log(f"  Success: {fetcher.success} | Failed: {fetcher.failed} | Skipped: {fetcher.skipped}")
        log(f"  DB rows written: {fetcher.total_rows:,}")

    # Quick stats
    conn = get_db()
    count = conn.execute("SELECT COUNT(*) FROM ohlc_daily").fetchone()[0]
    tickers_count = conn.execute("SELECT COUNT(DISTINCT ticker) FROM ohlc_daily").fetchone()[0]
    log(f"  ohlc_daily total: {count:,} rows / {tickers_count:,} tickers")
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Async fetch OHLC daily data")
    parser.add_argument("--market", choices=["US", "HK", "CN"],
                        help="Only fetch this market")
    parser.add_argument("--ticker", help="Single ticker to test")
    parser.add_argument("--incremental", action="store_true",
                        help="Only fetch new data since last update")
    parser.add_argument("--days", type=int, default=365,
                        help="Days of history for full fetch (default 365)")
    parser.add_argument("--concurrency", type=int, default=100,
                        help="Max concurrent requests (default 100)")
    parser.add_argument("--dry-run", action="store_true",
                        help="List stocks without fetching")
    args = parser.parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
