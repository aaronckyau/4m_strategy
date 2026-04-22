"""
fetch_ratios_ttm.py - Async 並發拉取 FMP Financial Ratios TTM
============================================================================
用法（PowerShell）：
  cd E:\Aurum-Infinity\Aurum_Data_Fetcher
  python fetch_ratios_ttm.py                        # 全部市場
  python fetch_ratios_ttm.py --market US            # 只拉美股
  python fetch_ratios_ttm.py --market HK            # 只拉港股
  python fetch_ratios_ttm.py --market CN            # 只拉A股
  python fetch_ratios_ttm.py --ticker AAPL          # 單支測試
  python fetch_ratios_ttm.py --concurrency 100      # 調整並發數
  python fetch_ratios_ttm.py --dry-run              # 只列出不拉取

每支股票 1 次 API call → ratios_ttm 表（只保留最新一筆）
============================================================================
"""
import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print("ERROR: aiohttp not installed. Run: pip install aiohttp")
    sys.exit(1)

from dotenv import load_dotenv
load_dotenv()

from config import Config
from db import get_db, upsert_ratios_ttm, ensure_stock_exists
from utils import log

# ============================================================================
# Config
# ============================================================================
FMP_API_KEY = Config.FMP_API_KEY
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
    if mf in {"CN", "HK"}:
        stocks = []
    elif mf == "US":
        stocks = [s for s in stocks if s.get("market", "") == "US"]
    return stocks


# ============================================================================
# Rate limiter
# ============================================================================
class RateLimiter:
    """Token bucket: max `rate` requests per `period` seconds."""

    def __init__(self, rate: int = 900, period: float = 60.0):
        self._rate = rate
        self._period = period
        self._tokens: float = rate
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
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


# ============================================================================
# Async fetcher
# ============================================================================
class AsyncRatiosFetcher:
    def __init__(self, concurrency: int = 100, rate_limit: int = 900):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.rate_limiter = RateLimiter(rate=rate_limit)
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

    async def _fetch_json(self, symbol: str, retries: int = 2) -> dict | None:
        url = f"{BASE_URL}/ratios-ttm"
        params = {"symbol": symbol, "apikey": FMP_API_KEY}

        for attempt in range(retries + 1):
            await self.rate_limiter.acquire()
            async with self.semaphore:
                try:
                    async with self.session.get(url, params=params) as resp:
                        if resp.status == 429:
                            wait = 2 ** (attempt + 2)
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
                        if isinstance(data, list) and data:
                            return data[0]
                        if isinstance(data, dict) and "Error Message" not in data:
                            return data
                        return None
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    if attempt < retries:
                        await asyncio.sleep(2)
                        continue
                    return None
        return None

    async def fetch_batch(self, tickers: list[str]):
        """並發抓網路資料，DB 寫入由單一受控 writer 處理。"""
        total = len(tickers)
        done = 0
        start = time.time()
        db_lock = asyncio.Lock()
        conn = get_db()
        try:
            async def _process(ticker: str):
                nonlocal done
                try:
                    data = await self._fetch_json(ticker)
                    if data is None:
                        self.failed += 1
                    elif not any(v for k, v in data.items() if k != "symbol" and v):
                        self.empty += 1
                    else:
                        try:
                            async with db_lock:
                                ensure_stock_exists(conn, ticker)
                                upsert_ratios_ttm(conn, ticker, data)  # db.py
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
                        f"| {self.success:,} ok | {rate:.0f} stocks/s | ETA {eta:.0f}s")

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

    log(f"=== Ratios TTM Fetcher ===")
    log(f"Total: {len(stocks)} stocks | Concurrency: {args.concurrency} | Rate limit: {args.rate_limit} req/min")
    for m, t in sorted(by_market.items()):
        log(f"  {m}: {len(t)}")

    if args.dry_run:
        log("DRY RUN - not fetching")
        return

    tickers = [s["symbol"] for s in stocks]
    start = time.time()

    async with AsyncRatiosFetcher(concurrency=args.concurrency, rate_limit=args.rate_limit) as fetcher:
        await fetcher.fetch_batch(tickers)
        elapsed = time.time() - start
        log(f"")
        log(f"=== DONE in {elapsed:.0f}s ===")
        log(f"  Success: {fetcher.success} | Failed: {fetcher.failed} | Empty: {fetcher.empty}")

    # Quick stats
    conn = get_db()
    count = conn.execute("SELECT COUNT(*) FROM ratios_ttm").fetchone()[0]
    log(f"  ratios_ttm total rows: {count:,}")
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Async fetch FMP Financial Ratios TTM")
    parser.add_argument("--market", choices=["US", "HK", "CN"], help="Only fetch this market")
    parser.add_argument("--ticker", help="Single ticker to test")
    parser.add_argument("--concurrency", type=int, default=100, help="Max concurrent requests (default 100)")
    parser.add_argument("--rate-limit", dest="rate_limit", type=int, default=900,
                        help="Max requests per minute (default 900, API limit is 1000)")
    parser.add_argument("--dry-run", action="store_true", help="List stocks without fetching")
    args = parser.parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
