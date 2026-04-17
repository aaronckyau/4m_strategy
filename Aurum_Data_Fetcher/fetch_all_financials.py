"""
fetch_all_financials.py - Async 並發拉取全部股票財報
============================================================================
用法（PowerShell）：
  cd E:\Aurum-Infinity\Aurum_Data_Fetcher
  python fetch_all_financials.py                    # 全部市場
  python fetch_all_financials.py --market US        # 只拉美股
  python fetch_all_financials.py --market HK        # 只拉港股
  python fetch_all_financials.py --market CN        # 只拉A股
  python fetch_all_financials.py --ticker AAPL      # 單支測試
  python fetch_all_financials.py --concurrency 50   # 調整並發數（預設 100）
  python fetch_all_financials.py --dry-run          # 只列出會拉哪些股票，不實際拉取

拉取策略：
  - US / CN: period=quarter, limit=12（3 年季報）
  - HK:      period=quarter, limit=12（先嘗試季報，港股有些只有半年報）

完成後自動產出驗證 CSV：data/financial_coverage.csv
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

# ── 嘗試載入 aiohttp，若沒裝則提示 ──
try:
    import aiohttp
except ImportError:
    print("ERROR: aiohttp not installed. Run: pip install aiohttp")
    sys.exit(1)

from dotenv import load_dotenv
load_dotenv()

from config import Config
from db import get_db, upsert_financial_statement, ensure_stock_exists
from utils import log

# ============================================================================
# Config
# ============================================================================
FMP_API_KEY = os.getenv("FMP_API_KEY")
if not FMP_API_KEY:
    print("ERROR: FMP_API_KEY not set in .env")
    sys.exit(1)

BASE_URL = "https://financialmodelingprep.com/stable"

# 三表對照
STATEMENTS = [
    ("income-statement",         "income"),
    ("balance-sheet-statement",  "balance"),
    ("cash-flow-statement",      "cashflow"),
]

# ============================================================================
# Load stock list
# ============================================================================
def load_stocks(market_filter: str | None = None) -> list[dict]:
    with open(Config.STOCK_LIST_PATH, encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        stocks = data
    else:
        stocks = [{"symbol": k, **v} for k, v in data.items()]

    if market_filter:
        mf = market_filter.upper()
        if mf == "CN":
            stocks = [s for s in stocks if s.get("market", "").startswith("CN")]
        elif mf == "HK":
            stocks = [s for s in stocks if s.get("market", "") == "HK"]
        elif mf == "US":
            stocks = [s for s in stocks if s.get("market", "") == "US"]
        else:
            stocks = [s for s in stocks if s.get("market", "") == mf]

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
# Async FMP fetcher
# ============================================================================
class AsyncFMPFetcher:
    def __init__(self, concurrency: int = 80, rate_limit: int = 950):
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        self.rate_limiter = RateLimiter(rate=rate_limit)
        self.session: aiohttp.ClientSession | None = None
        # 全局 429 暫停事件：cleared = 暫停中，set = 可繼續
        self._throttle_event = asyncio.Event()
        self._throttle_event.set()  # 初始可繼續

        # Stats
        self.success = 0
        self.failed = 0
        self.empty = 0
        self.total_rows = 0
        self._429_count = 0

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def _global_pause(self, wait: int):
        """全局暫停所有請求 wait 秒，避免雪崩"""
        if self._throttle_event.is_set():
            self._throttle_event.clear()
            self._429_count += 1
            log(f"  ⚠ 429 觸發全局暫停 {wait}s（第 {self._429_count} 次）...")
            await asyncio.sleep(wait)
            self._throttle_event.set()
        else:
            # 其他 coroutine 已在暫停中，等它結束即可
            await self._throttle_event.wait()

    async def _fetch_json(self, endpoint: str, params: dict,
                          retries: int = 3) -> list | None:
        url = f"{BASE_URL}/{endpoint}"
        params["apikey"] = FMP_API_KEY

        for attempt in range(retries + 1):
            # 先等全局 throttle 解除
            await self._throttle_event.wait()
            await self.rate_limiter.acquire()
            async with self.semaphore:
                try:
                    async with self.session.get(url, params=params) as resp:
                        if resp.status == 429:
                            # 全局暫停 60s，指數退避
                            wait = min(60 * (attempt + 1), 180)
                            await self._global_pause(wait)
                            continue
                        if resp.status >= 500:
                            if attempt < retries:
                                await asyncio.sleep(3)
                                continue
                            return None
                        if resp.status != 200:
                            return None
                        data = await resp.json()
                        if isinstance(data, dict) and "Error Message" in data:
                            return None
                        return data if isinstance(data, list) else None
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    if attempt < retries:
                        await asyncio.sleep(3)
                        continue
                    return None
        return None

    async def fetch_one_stock(self, ticker: str, conn: sqlite3.Connection,
                              period: str = "quarter", limit: int = 12):
        """拉取單支股票的三表並寫入 DB"""
        stock_rows = 0

        for endpoint, stmt_type in STATEMENTS:
            data = await self._fetch_json(endpoint, {
                "symbol": ticker,
                "period": period,
                "limit": limit,
            })

            if data is None:
                self.failed += 1
                continue

            if len(data) == 0:
                self.empty += 1
                continue

            ensure_stock_exists(conn, ticker)
            upsert_financial_statement(conn, ticker, stmt_type, data)
            stock_rows += len(data)
            self.success += 1

        self.total_rows += stock_rows
        return stock_rows

    async def fetch_batch(self, tickers: list[str], period: str = "quarter",
                          limit: int = 12):
        """並發拉取一批股票（每個 task 自己的 DB 連線，避免共用連線爭用）"""
        total = len(tickers)
        done = 0
        batch_start = time.time()

        async def _process(ticker: str):
            nonlocal done
            conn = get_db()
            try:
                await self.fetch_one_stock(ticker, conn, period, limit)
            except Exception as exc:
                self.failed += 1
                print(f"  ✗ {ticker} write error: {exc}", flush=True)
            finally:
                conn.close()

            done += 1
            if done % 10 == 0 or done == total:
                elapsed = time.time() - batch_start
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                print(f"  [{done}/{total}] {done/total*100:.1f}% | "
                      f"{self.total_rows:,} rows | {rate:.0f} stocks/s | "
                      f"ETA {int(eta//60)}m{int(eta%60):02d}s | last: {ticker}",
                      flush=True)

        tasks = [_process(t) for t in tickers]
        await asyncio.gather(*tasks)


# ============================================================================
# 驗證 CSV 產出
# ============================================================================
def generate_coverage_csv(output_path: str = "data/financial_coverage.csv"):
    """產出 ticker x quarter 的覆蓋矩陣"""
    conn = get_db()

    # 取所有 income statement 的 period
    rows = conn.execute("""
        SELECT ticker, period, fiscal_year, fiscal_quarter, revenue, net_income, eps_diluted
        FROM financial_statements
        WHERE statement_type = 'income'
        ORDER BY ticker, period DESC
    """).fetchall()
    conn.close()

    if not rows:
        log("No data to generate CSV")
        return

    # 整理成 ticker -> {period: data}
    data = {}
    all_periods = set()
    for r in rows:
        t = r["ticker"]
        p = r["period"]
        fy = r["fiscal_year"]
        fq = r["fiscal_quarter"]
        label = f"{fy}Q{fq}" if fy and fq else p
        all_periods.add(label)
        if t not in data:
            data[t] = {}
        data[t][label] = {
            "revenue": r["revenue"],
            "net_income": r["net_income"],
            "eps": r["eps_diluted"],
        }

    # 按時間排序 periods
    sorted_periods = sorted(all_periods, reverse=True)

    # 寫 CSV
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        import csv
        # Header: ticker, count, 2026Q1_rev, 2026Q1_ni, 2025Q4_rev, ...
        header = ["ticker", "quarters"]
        for p in sorted_periods:
            header.extend([f"{p}_revenue", f"{p}_net_income", f"{p}_eps"])
        writer = csv.writer(f)
        writer.writerow(header)

        for ticker in sorted(data.keys()):
            row = [ticker, len(data[ticker])]
            for p in sorted_periods:
                d = data[ticker].get(p, {})
                row.extend([d.get("revenue", ""), d.get("net_income", ""), d.get("eps", "")])
            writer.writerow(row)

    log(f"Coverage CSV: {output_path} ({len(data)} tickers x {len(sorted_periods)} periods)")


# ============================================================================
# Main
# ============================================================================
async def async_main(args):
    stocks = load_stocks(args.market)

    if args.ticker:
        stocks = [s for s in stocks if s["symbol"] == args.ticker.upper()]
        if not stocks:
            # 直接用 ticker
            stocks = [{"symbol": args.ticker.upper(), "market": "US"}]

    if not stocks:
        log("No stocks to fetch")
        return

    # 分市場統計
    by_market = {}
    for s in stocks:
        m = s.get("market", "?")
        by_market.setdefault(m, []).append(s["symbol"])

    log(f"=== Async Financial Fetcher ===")
    log(f"Total: {len(stocks)} stocks | Concurrency: {args.concurrency} | Rate limit: {args.rate_limit} req/min")
    for m, tickers in sorted(by_market.items()):
        log(f"  {m}: {len(tickers)}")

    if args.dry_run:
        log("DRY RUN - not fetching")
        for m, tickers in sorted(by_market.items()):
            log(f"\n--- {m} ({len(tickers)}) ---")
            for t in tickers[:10]:
                log(f"  {t}")
            if len(tickers) > 10:
                log(f"  ... and {len(tickers) - 10} more")
        return

    tickers = [s["symbol"] for s in stocks]
    start = time.time()

    async with AsyncFMPFetcher(concurrency=args.concurrency, rate_limit=args.rate_limit) as fetcher:
        # US / CN: quarter limit=12, HK: quarter limit=12 (take what we can get)
        await fetcher.fetch_batch(tickers, period="quarter", limit=12)

        elapsed = time.time() - start
        log(f"")
        log(f"=== DONE in {elapsed:.0f}s ===")
        log(f"  API calls: {fetcher.success} ok / {fetcher.failed} fail / {fetcher.empty} empty")
        log(f"  DB rows written: {fetcher.total_rows:,}")

    # 產出驗證 CSV
    log("")
    generate_coverage_csv()


def main():
    parser = argparse.ArgumentParser(description="Async fetch all financial statements")
    parser.add_argument("--market", choices=["US", "HK", "CN"], help="Only fetch this market")
    parser.add_argument("--ticker", help="Single ticker to test")
    parser.add_argument("--concurrency", type=int, default=80, help="Max concurrent requests (default 80)")
    parser.add_argument("--rate-limit", dest="rate_limit", type=int, default=950,
                        help="Max requests per minute (default 950, FMP Ultimate limit is 1000)")
    parser.add_argument("--dry-run", action="store_true", help="List stocks without fetching")
    args = parser.parse_args()

    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
