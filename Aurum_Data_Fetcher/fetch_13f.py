"""
fetch_13f.py - Async 並發拉取 FMP Form 13F 機構持倉數據
============================================================================
用法：
  cd Aurum_Data_Fetcher
  python fetch_13f.py                        # 全部美股（最近 4 季）
  python fetch_13f.py --market US            # 只拉美股（同上，HK/CN 自動跳過）
  python fetch_13f.py --ticker AAPL          # 單支測試
  python fetch_13f.py --concurrency 30       # 調整並發數
  python fetch_13f.py --quarters 2           # 只拉最近 2 季（預設 4）
  python fetch_13f.py --dry-run              # 只列出不拉取

端點：/stable/institutional-ownership/extract-analytics/holder
每支股票每季 1 次 API call（分頁取前 100 大機構）× N 季 → institutional_holdings 表
只保留最近 4 個季度的數據，舊數據自動清除。
注意：Form 13F 僅適用於美股，HK/CN 股票自動跳過。
============================================================================
"""
import argparse
import asyncio
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
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
# Quarter helpers
# ============================================================================
def get_recent_quarters(n: int = 4) -> list[tuple[int, int]]:
    """回傳最近 N 個季度的 (year, quarter) 列表，由新到舊。
    13F 有 45 天延遲，所以當前季度通常還沒出來，從上季開始算。
    """
    now = datetime.now()
    # 當前所在季度
    current_q = (now.month - 1) // 3 + 1
    current_y = now.year

    # 上一個已結束的季度（13F 要等 45 天）
    q = current_q - 1
    y = current_y
    if q == 0:
        q = 4
        y -= 1

    quarters = []
    for _ in range(n):
        quarters.append((y, q))
        q -= 1
        if q == 0:
            q = 4
            y -= 1
    return quarters


# ============================================================================
# Load stock list (US only — 13F is SEC-mandated)
# ============================================================================
def load_stocks(market_filter: str | None = None) -> list[dict]:
    with open(STOCK_LIST_PATH, encoding="utf-8") as f:
        data = json.load(f)
    stocks = data if isinstance(data, list) else [{"symbol": k, **v} for k, v in data.items()]

    # 13F only applies to US stocks
    if market_filter and market_filter.upper() != "US":
        log(f"WARNING: 13F only applies to US stocks, skipping market={market_filter}")
        return []

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


def ensure_table(conn: sqlite3.Connection):
    """Ensure institutional_holdings table exists"""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS institutional_holdings (
            ticker          TEXT    NOT NULL,
            holder          TEXT    NOT NULL,
            date_reported   TEXT    NOT NULL,
            shares          INTEGER,
            market_value    REAL,
            change          INTEGER,
            change_pct      REAL,
            ownership_pct   REAL,
            is_new          BOOLEAN DEFAULT 0,
            is_sold_out     BOOLEAN DEFAULT 0,
            filing_date     TEXT,
            fetched_at      TEXT,
            PRIMARY KEY (ticker, holder, date_reported)
        );
        CREATE INDEX IF NOT EXISTS idx_ih_ticker
            ON institutional_holdings(ticker);
        CREATE INDEX IF NOT EXISTS idx_ih_date
            ON institutional_holdings(ticker, date_reported);
    """)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def upsert_holdings(conn: sqlite3.Connection, ticker: str, holdings: list[dict]):
    """寫入機構持倉數據，每筆 INSERT OR REPLACE。
    欄位映射自 FMP extract-analytics/holder 端點。
    """
    now = _now_iso()
    for h in holdings:
        conn.execute("""
            INSERT OR REPLACE INTO institutional_holdings (
                ticker, holder, date_reported, shares, market_value,
                change, change_pct, ownership_pct,
                is_new, is_sold_out, filing_date, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ticker,
            h.get("investorName", "Unknown"),
            h.get("date", ""),
            h.get("sharesNumber"),
            h.get("marketValue"),
            h.get("changeInSharesNumber"),
            h.get("changeInSharesNumberPercentage"),
            h.get("ownership"),
            1 if h.get("isNew") else 0,
            1 if h.get("isSoldOut") else 0,
            h.get("filingDate", ""),
            now,
        ))


def cleanup_old_quarters(conn: sqlite3.Connection, ticker: str, keep: int = 4):
    """只保留最近 N 個季度的數據"""
    quarters = conn.execute(
        "SELECT DISTINCT date_reported FROM institutional_holdings "
        "WHERE ticker = ? ORDER BY date_reported DESC",
        (ticker,)
    ).fetchall()

    if len(quarters) > keep:
        old_dates = [q[0] for q in quarters[keep:]]
        placeholders = ",".join("?" * len(old_dates))
        conn.execute(
            f"DELETE FROM institutional_holdings "
            f"WHERE ticker = ? AND date_reported IN ({placeholders})",
            (ticker, *old_dates)
        )


# ============================================================================
# Async fetcher
# ============================================================================
class Async13FFetcher:
    def __init__(self, concurrency: int = 30):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.session: aiohttp.ClientSession | None = None
        self.success = 0
        self.failed = 0
        self.empty = 0

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30))
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def _fetch_quarter(self, symbol: str, year: int, quarter: int,
                             retries: int = 2) -> list | None:
        """拉取單支股票單季的機構持倉（前 100 大）"""
        url = f"{BASE_URL}/institutional-ownership/extract-analytics/holder"
        params = {
            "symbol": symbol,
            "year": year,
            "quarter": quarter,
            "page": 0,
            "limit": 100,
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
                        return None
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    if attempt < retries:
                        await asyncio.sleep(2)
                        continue
                    return None
        return None

    async def fetch_batch(self, tickers: list[str],
                          quarters: list[tuple[int, int]],
                          batch_size: int = 2000,
                          batch_pause: int = 60):
        """分批拉取，每 batch_size 次 API call 暫停 batch_pause 秒。"""
        conn = get_db()
        ensure_table(conn)

        # Build all (ticker, year, quarter) tasks
        all_tasks = []
        for ticker in tickers:
            for year, quarter in quarters:
                all_tasks.append((ticker, year, quarter))

        total_tasks = len(all_tasks)
        done = 0
        start = time.time()
        COMMIT_EVERY = 50

        async def _process(ticker: str, year: int, quarter: int):
            nonlocal done
            data = await self._fetch_quarter(ticker, year, quarter)
            if data is None:
                self.failed += 1
            elif len(data) == 0:
                self.empty += 1
            else:
                upsert_holdings(conn, ticker, data)
                self.success += 1

            done += 1
            if done % COMMIT_EVERY == 0:
                conn.commit()
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total_tasks - done) / rate if rate > 0 else 0
                log(f"  Progress: {done}/{total_tasks} ({done/total_tasks*100:.1f}%) "
                    f"| {self.success:,} ok | {rate:.0f} req/s | ETA {eta:.0f}s")

        # Process in batches of batch_size
        for batch_start in range(0, total_tasks, batch_size):
            batch = all_tasks[batch_start:batch_start + batch_size]
            batch_num = batch_start // batch_size + 1
            total_batches = (total_tasks + batch_size - 1) // batch_size

            if total_batches > 1:
                log(f"  ── Batch {batch_num}/{total_batches} "
                    f"({len(batch)} calls) ──")

            await asyncio.gather(*[
                _process(t, y, q) for t, y, q in batch
            ])
            conn.commit()

            # Pause between batches (skip after last batch)
            remaining = total_tasks - (batch_start + len(batch))
            if remaining > 0:
                log(f"  ⏸ Rate limit pause: waiting {batch_pause}s "
                    f"before next batch ({remaining} remaining)...")
                await asyncio.sleep(batch_pause)

        # Cleanup old quarters per ticker
        for ticker in tickers:
            cleanup_old_quarters(conn, ticker, keep=4)
        conn.commit()
        conn.close()


# ============================================================================
# Main
# ============================================================================
async def async_main(args):
    stocks = load_stocks(args.market)

    if args.ticker:
        # Allow single ticker even if not in stock list
        stocks = [s for s in stocks if s["symbol"] == args.ticker.upper()]
        if not stocks:
            stocks = [{"symbol": args.ticker.upper(), "market": "US"}]

    if not stocks:
        log("No US stocks to fetch (13F only applies to US-listed stocks)")
        return

    quarters = get_recent_quarters(args.quarters)

    log(f"=== Form 13F Institutional Holdings Fetcher ===")
    log(f"Stocks: {len(stocks)} | Quarters: {len(quarters)} | "
        f"Total API calls: {len(stocks) * len(quarters)} | "
        f"Concurrency: {args.concurrency}")
    for y, q in quarters:
        log(f"  → {y} Q{q}")

    if args.dry_run:
        log("DRY RUN - not fetching")
        for s in stocks[:20]:
            log(f"  {s['symbol']}")
        if len(stocks) > 20:
            log(f"  ... and {len(stocks) - 20} more")
        return

    tickers = [s["symbol"] for s in stocks]
    start = time.time()

    async with Async13FFetcher(concurrency=args.concurrency) as fetcher:
        await fetcher.fetch_batch(tickers, quarters)
        elapsed = time.time() - start
        log(f"")
        log(f"=== DONE in {elapsed:.0f}s ===")
        log(f"  Success: {fetcher.success} | Failed: {fetcher.failed} | Empty: {fetcher.empty}")

    # Quick stats
    conn = get_db()
    ensure_table(conn)
    count = conn.execute("SELECT COUNT(*) FROM institutional_holdings").fetchone()[0]
    tickers_count = conn.execute(
        "SELECT COUNT(DISTINCT ticker) FROM institutional_holdings").fetchone()[0]
    quarters_count = conn.execute(
        "SELECT COUNT(DISTINCT date_reported) FROM institutional_holdings").fetchone()[0]
    log(f"  institutional_holdings: {count:,} rows | {tickers_count} tickers | {quarters_count} quarters")
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Async fetch FMP Form 13F Institutional Holdings")
    parser.add_argument("--market", choices=["US", "HK", "CN"],
                        help="Market filter (only US is supported for 13F)")
    parser.add_argument("--ticker", help="Single ticker to test")
    parser.add_argument("--quarters", type=int, default=4,
                        help="Number of recent quarters to fetch (default 4)")
    parser.add_argument("--concurrency", type=int, default=30,
                        help="Max concurrent requests (default 30)")
    parser.add_argument("--dry-run", action="store_true",
                        help="List stocks without fetching")
    args = parser.parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
