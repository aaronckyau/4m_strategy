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


def _f(d: dict, key: str):
    """Safe float extract"""
    v = d.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def upsert_ratios_ttm(conn: sqlite3.Connection, ticker: str, r: dict):
    """寫入 ratios_ttm（REPLACE，每支只保留最新）"""
    now = _now_iso()
    conn.execute("""
        INSERT OR REPLACE INTO ratios_ttm (
            ticker,
            pe, pb, ps, peg, forward_peg, ev, ev_multiple,
            price_to_fcf, price_to_ocf, price_to_fair_value, debt_to_market_cap,
            gross_margin, operating_margin, pretax_margin, net_margin,
            ebitda_margin, effective_tax_rate,
            net_income_per_ebt, ebt_per_ebit,
            debt_to_equity, debt_to_assets, debt_to_capital, long_term_debt_to_capital,
            financial_leverage, current_ratio, quick_ratio, cash_ratio,
            solvency_ratio, interest_coverage, debt_service_coverage,
            asset_turnover, fixed_asset_turnover, receivables_turnover,
            inventory_turnover, working_capital_turnover,
            eps, revenue_per_share, book_value_per_share, tangible_bv_per_share,
            cash_per_share, ocf_per_share, fcf_per_share, interest_debt_per_share,
            dividend_yield, dividend_per_share, dividend_payout_ratio,
            ocf_ratio, ocf_sales_ratio, fcf_ocf_ratio,
            capex_coverage_ratio, dividend_capex_coverage,
            raw_json, fetched_at
        ) VALUES (
            ?,
            ?,?,?,?,?,?,?,
            ?,?,?,?,
            ?,?,?,?,
            ?,?,
            ?,?,
            ?,?,?,?,
            ?,?,?,?,
            ?,?,?,
            ?,?,?,
            ?,?,
            ?,?,?,?,
            ?,?,?,?,
            ?,?,?,
            ?,?,?,
            ?,?,
            ?,?
        )
    """, (
        ticker,
        # Valuation
        _f(r, "priceToEarningsRatioTTM"),
        _f(r, "priceToBookRatioTTM"),
        _f(r, "priceToSalesRatioTTM"),
        _f(r, "priceToEarningsGrowthRatioTTM"),
        _f(r, "forwardPriceToEarningsGrowthRatioTTM"),
        _f(r, "enterpriseValueTTM"),
        _f(r, "enterpriseValueMultipleTTM"),
        _f(r, "priceToFreeCashFlowRatioTTM"),
        _f(r, "priceToOperatingCashFlowRatioTTM"),
        _f(r, "priceToFairValueTTM"),
        _f(r, "debtToMarketCapTTM"),
        # Profitability
        _f(r, "grossProfitMarginTTM"),
        _f(r, "operatingProfitMarginTTM"),
        _f(r, "pretaxProfitMarginTTM"),
        _f(r, "netProfitMarginTTM"),
        _f(r, "ebitdaMarginTTM"),
        _f(r, "effectiveTaxRateTTM"),
        # Returns
        _f(r, "netIncomePerEBTTTM"),
        _f(r, "ebtPerEbitTTM"),
        # Leverage / Liquidity
        _f(r, "debtToEquityRatioTTM"),
        _f(r, "debtToAssetsRatioTTM"),
        _f(r, "debtToCapitalRatioTTM"),
        _f(r, "longTermDebtToCapitalRatioTTM"),
        _f(r, "financialLeverageRatioTTM"),
        _f(r, "currentRatioTTM"),
        _f(r, "quickRatioTTM"),
        _f(r, "cashRatioTTM"),
        _f(r, "solvencyRatioTTM"),
        _f(r, "interestCoverageRatioTTM"),
        _f(r, "debtServiceCoverageRatioTTM"),
        # Efficiency
        _f(r, "assetTurnoverTTM"),
        _f(r, "fixedAssetTurnoverTTM"),
        _f(r, "receivablesTurnoverTTM"),
        _f(r, "inventoryTurnoverTTM"),
        _f(r, "workingCapitalTurnoverRatioTTM"),
        # Per Share
        _f(r, "netIncomePerShareTTM"),
        _f(r, "revenuePerShareTTM"),
        _f(r, "bookValuePerShareTTM"),
        _f(r, "tangibleBookValuePerShareTTM"),
        _f(r, "cashPerShareTTM"),
        _f(r, "operatingCashFlowPerShareTTM"),
        _f(r, "freeCashFlowPerShareTTM"),
        _f(r, "interestDebtPerShareTTM"),
        # Dividend
        _f(r, "dividendYieldTTM"),
        _f(r, "dividendPerShareTTM"),
        _f(r, "dividendPayoutRatioTTM"),
        # Cash Flow
        _f(r, "operatingCashFlowRatioTTM"),
        _f(r, "operatingCashFlowSalesRatioTTM"),
        _f(r, "freeCashFlowOperatingCashFlowRatioTTM"),
        _f(r, "capitalExpenditureCoverageRatioTTM"),
        _f(r, "dividendPaidAndCapexCoverageRatioTTM"),
        # Raw
        json.dumps(r, ensure_ascii=False),
        now,
    ))


# ============================================================================
# Async fetcher
# ============================================================================
class AsyncRatiosFetcher:
    def __init__(self, concurrency: int = 100):
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

    async def _fetch_json(self, symbol: str, retries: int = 2) -> dict | None:
        url = f"{BASE_URL}/ratios-ttm"
        params = {"symbol": symbol, "apikey": FMP_API_KEY}

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
        conn = get_db()
        total = len(tickers)
        done = 0
        start = time.time()
        COMMIT_EVERY = 50

        async def _process(ticker: str):
            nonlocal done
            data = await self._fetch_json(ticker)
            if data is None:
                self.failed += 1
            elif not any(v for k, v in data.items() if k != "symbol" and v):
                self.empty += 1
            else:
                upsert_ratios_ttm(conn, ticker, data)
                self.success += 1

            done += 1
            if done % COMMIT_EVERY == 0:
                conn.commit()
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                log(f"  Progress: {done}/{total} ({done/total*100:.1f}%) "
                    f"| {self.success:,} ok | {rate:.0f} stocks/s | ETA {eta:.0f}s")

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

    log(f"=== Ratios TTM Fetcher ===")
    log(f"Total: {len(stocks)} stocks | Concurrency: {args.concurrency}")
    for m, t in sorted(by_market.items()):
        log(f"  {m}: {len(t)}")

    if args.dry_run:
        log("DRY RUN - not fetching")
        return

    tickers = [s["symbol"] for s in stocks]
    start = time.time()

    async with AsyncRatiosFetcher(concurrency=args.concurrency) as fetcher:
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
    parser.add_argument("--dry-run", action="store_true", help="List stocks without fetching")
    args = parser.parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
