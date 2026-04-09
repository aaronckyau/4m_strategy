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

# 三表對照
STATEMENTS = [
    ("income-statement",         "income"),
    ("balance-sheet-statement",  "balance"),
    ("cash-flow-statement",      "cashflow"),
]

# ============================================================================
# Logger (簡化版，UTF-8 safe)
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
# DB helpers (同步，但在 async 間隙呼叫)
# ============================================================================
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    return conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _num(d: dict, key: str):
    v = d.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _int_val(d: dict, key: str):
    v = d.get(key)
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _quarter(d: dict) -> int | None:
    p = d.get("period", "")
    if p == "Q1": return 1
    if p == "Q2": return 2
    if p == "Q3": return 3
    if p == "Q4": return 4
    return None


def upsert_financials(conn: sqlite3.Connection, ticker: str,
                      stmt_type: str, rows: list[dict]):
    if not rows:
        return 0
    now = _now_iso()
    count = 0
    for r in rows:
        period = r.get("date") or r.get("period")
        if not period:
            continue
        conn.execute("""
            INSERT OR REPLACE INTO financial_statements
                (ticker, period, statement_type, fiscal_year, fiscal_quarter,
                 revenue, cost_of_revenue, gross_profit, operating_income,
                 net_income, eps, eps_diluted, ebitda, operating_expenses,
                 total_assets, total_liabilities, total_equity, total_debt,
                 cash_and_equivalents, current_assets, current_liabilities,
                 operating_cash_flow, capex, free_cash_flow, dividends_paid,
                 raw_json, fetched_at)
            VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?, ?,?,?,?,?, ?,?, ?,?,?,?, ?,?)
        """, (
            ticker, period, stmt_type,
            _int_val(r, "calendarYear") or _int_val(r, "fiscalYear"),
            _quarter(r),
            # Income
            _num(r, "revenue"), _num(r, "costOfRevenue"), _num(r, "grossProfit"),
            _num(r, "operatingIncome"), _num(r, "netIncome"),
            _num(r, "eps"),
            _num(r, "epsDiluted") or _num(r, "epsdiluted"),
            _num(r, "ebitda"), _num(r, "operatingExpenses"),
            # Balance
            _num(r, "totalAssets"), _num(r, "totalLiabilities"),
            _num(r, "totalStockholdersEquity") or _num(r, "totalEquity"),
            _num(r, "totalDebt"), _num(r, "cashAndCashEquivalents"),
            _num(r, "totalCurrentAssets"), _num(r, "totalCurrentLiabilities"),
            # Cash Flow
            _num(r, "operatingCashFlow"),
            _num(r, "capitalExpenditure") or _num(r, "capex"),
            _num(r, "freeCashFlow"),
            _num(r, "netDividendsPaid") or _num(r, "dividendsPaid") or _num(r, "commonDividendsPaid"),
            # Raw
            json.dumps(r, ensure_ascii=False), now,
        ))
        count += 1
    return count


# ============================================================================
# Async FMP fetcher
# ============================================================================
class AsyncFMPFetcher:
    def __init__(self, concurrency: int = 100):
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        self.session: aiohttp.ClientSession | None = None

        # Stats
        self.success = 0
        self.failed = 0
        self.empty = 0
        self.total_rows = 0

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def _fetch_json(self, endpoint: str, params: dict,
                          retries: int = 2) -> list | None:
        url = f"{BASE_URL}/{endpoint}"
        params["apikey"] = FMP_API_KEY

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
                        if isinstance(data, dict) and "Error Message" in data:
                            return None
                        return data if isinstance(data, list) else None
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    if attempt < retries:
                        await asyncio.sleep(2)
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

            count = upsert_financials(conn, ticker, stmt_type, data)
            stock_rows += count
            self.success += 1

        if stock_rows > 0:
            conn.execute(
                "UPDATE stocks_master SET financials_updated_at = ? WHERE ticker = ?",
                (_now_iso(), ticker))

        self.total_rows += stock_rows
        return stock_rows

    async def fetch_batch(self, tickers: list[str], period: str = "quarter",
                          limit: int = 12):
        """並發拉取一批股票"""
        conn = get_db()
        total = len(tickers)
        done = 0
        batch_start = time.time()

        # 分批 commit（每 50 支 commit 一次）
        COMMIT_EVERY = 50

        async def _process(ticker: str):
            nonlocal done
            await self.fetch_one_stock(ticker, conn, period, limit)
            done += 1
            if done % COMMIT_EVERY == 0:
                conn.commit()
                elapsed = time.time() - batch_start
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                log(f"  Progress: {done}/{total} ({done/total*100:.1f}%) "
                    f"| {self.total_rows:,} rows | {rate:.0f} stocks/s "
                    f"| ETA {eta:.0f}s")

        tasks = [_process(t) for t in tickers]
        await asyncio.gather(*tasks)
        conn.commit()
        conn.close()


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
    log(f"Total: {len(stocks)} stocks | Concurrency: {args.concurrency}")
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

    async with AsyncFMPFetcher(concurrency=args.concurrency) as fetcher:
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
    parser.add_argument("--concurrency", type=int, default=100, help="Max concurrent requests (default 100)")
    parser.add_argument("--dry-run", action="store_true", help="List stocks without fetching")
    args = parser.parse_args()

    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
