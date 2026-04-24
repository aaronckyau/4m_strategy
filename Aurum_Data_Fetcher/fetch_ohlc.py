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
  python fetch_ohlc.py --backfill-from 2024-01-01  # 指定起始日補歷史資料
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
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print("ERROR: aiohttp not installed. Run: pip install aiohttp")
    sys.exit(1)

from dotenv import load_dotenv
load_dotenv()

from config import Config
from db import get_db, get_last_ohlc_date, upsert_ohlc_batch, ensure_stock_exists
from utils import log

# ============================================================================
# Config
# ============================================================================
FMP_API_KEY = os.getenv("FMP_API_KEY")
if not FMP_API_KEY:
    print("ERROR: FMP_API_KEY not set in .env")
    sys.exit(1)

BASE_URL = "https://financialmodelingprep.com/stable"


# ============================================================================
# Load stock list
# ============================================================================
def load_stocks(market_filter: str | None = None) -> list[dict]:
    with open(Config.STOCK_LIST_PATH, encoding="utf-8") as f:
        data = json.load(f)
    stocks = data if isinstance(data, list) else [{"symbol": k, **v} for k, v in data.items()]

    mf = (market_filter or "US").upper()
    if mf == "CN":
        stocks = []
    elif mf == "HK":
        stocks = []
    elif mf == "US":
        stocks = [s for s in stocks if s.get("market", "") == "US"]

    include_sector_etfs = mf == "US"
    if include_sector_etfs:
        existing = {s.get("symbol") for s in stocks}
        for etf in Config.SECTOR_ETFS:
            if etf["symbol"] in existing:
                continue
            stocks.append({
                "symbol": etf["symbol"],
                "market": "US",
                "exchange": "AMEX",
                "currency": "USD",
                "name_eng": etf["name"],
                "sector": etf["sector"],
                "industry": "Sector ETF",
            })
    return stocks


# ============================================================================
# Async fetcher
# ============================================================================
class RateLimiter:
    """Token bucket: max `rate` requests per `period` seconds.

    Enforces a global throughput ceiling across all concurrent tasks.
    Designed for asyncio — all waits are non-blocking.
    """

    def __init__(self, rate: int = 900, period: float = 60.0):
        self._rate = rate            # tokens per period
        self._period = period        # seconds
        self._tokens: float = rate   # start full
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            # refill proportionally
            self._tokens = min(
                float(self._rate),
                self._tokens + elapsed * (self._rate / self._period),
            )
            self._last_refill = now
            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) / (self._rate / self._period)
                await asyncio.sleep(wait)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0


class AsyncOHLCFetcher:
    def __init__(self, concurrency: int = 100, rate_limit: int = 900):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.rate_limiter = RateLimiter(rate=rate_limit)
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
            await self.rate_limiter.acquire()   # honour global req/min ceiling
            async with self.semaphore:
                try:
                    async with self.session.get(url, params=params) as resp:
                        if resp.status == 429:
                            wait = 2 ** (attempt + 2)   # longer back-off (4 / 8 / 16s)
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

    async def fetch_batch(self, tickers: list[str], stock_map: dict[str, dict],
                          days: int = 365, incremental: bool = False,
                          backfill_from: str | None = None):
        """並發抓網路資料，DB 讀寫由單一受控 writer 處理。"""
        total = len(tickers)
        done = 0
        start = time.time()
        today = datetime.now().strftime("%Y-%m-%d")
        default_from = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        db_lock = asyncio.Lock()
        conn = get_db()
        try:
            last_date_map = {}
            if incremental and tickers:
                placeholders = ",".join("?" * len(tickers))
                rows = conn.execute(
                    f"""
                    SELECT ticker, MAX(date) AS last_date
                    FROM ohlc_daily
                    WHERE ticker IN ({placeholders})
                    GROUP BY ticker
                    """,
                    tickers,
                ).fetchall()
                last_date_map = {
                    row["ticker"]: row["last_date"]
                    for row in rows
                    if row["last_date"]
                }

            async def _process(ticker: str):
                nonlocal done
                try:
                    if backfill_from:
                        from_date = backfill_from
                    elif incremental:
                        last_date = last_date_map.get(ticker)
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
                        try:
                            async with db_lock:
                                stock_meta = stock_map.get(ticker, {})
                                ensure_stock_exists(
                                    conn,
                                    ticker,
                                    market=stock_meta.get("market", "US"),
                                    exchange=stock_meta.get("exchange"),
                                    currency=stock_meta.get("currency"),
                                    name=stock_meta.get("name_eng"),
                                )
                                count = upsert_ohlc_batch(conn, ticker, data)
                            self.total_rows += count
                            self.success += 1
                        except Exception as exc:
                            self.failed += 1
                            log(f"  ✗ {ticker} write error: {exc}")
                except Exception as exc:
                    self.failed += 1
                    log(f"  ✗ {ticker} process error: {exc}")

                done += 1
                if done % 50 == 0 or done == total:
                    elapsed = time.time() - start
                    rate = done / elapsed if elapsed > 0 else 0
                    eta = (total - done) / rate if rate > 0 else 0
                    log(f"  Progress: {done}/{total} ({done/total*100:.1f}%) "
                        f"| {self.total_rows:,} rows | {rate:.0f} stocks/s | ETA {eta:.0f}s")

            await asyncio.gather(*[_process(t) for t in tickers])
        finally:
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

    if args.backfill_from:
        mode = f"backfill from {args.backfill_from}"
    else:
        mode = "incremental" if args.incremental else f"full ({args.days} days)"
    log(f"=== OHLC Fetcher ({mode}) ===")
    log(f"Total: {len(stocks)} stocks | Concurrency: {args.concurrency} | Rate limit: {args.rate_limit} req/min")
    for m, t in sorted(by_market.items()):
        log(f"  {m}: {len(t)}")

    if args.dry_run:
        log("DRY RUN - not fetching")
        return

    tickers = [s["symbol"] for s in stocks]
    stock_map = {s["symbol"]: s for s in stocks}
    start = time.time()

    async with AsyncOHLCFetcher(concurrency=args.concurrency, rate_limit=args.rate_limit) as fetcher:
        await fetcher.fetch_batch(tickers, stock_map, days=args.days,
                                  incremental=args.incremental,
                                  backfill_from=args.backfill_from)
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
    parser.add_argument("--backfill-from", dest="backfill_from",
                        help="Fetch historical data from this date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=365,
                        help="Days of history for full fetch (default 365)")
    parser.add_argument("--concurrency", type=int, default=100,
                        help="Max concurrent requests (default 100)")
    parser.add_argument("--rate-limit", dest="rate_limit", type=int, default=900,
                        help="Max requests per minute (default 900, API limit is 1000)")
    parser.add_argument("--dry-run", action="store_true",
                        help="List stocks without fetching")
    args = parser.parse_args()
    if args.backfill_from and args.incremental:
        parser.error("--backfill-from cannot be used with --incremental")
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
