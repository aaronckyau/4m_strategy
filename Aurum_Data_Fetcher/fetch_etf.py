"""
fetch_etf.py - 拉取美股 ETF 清單 + 各 ETF 持倉，存入 SQLite
============================================================================
流程：
  Step 1: GET /stable/etf-list         → 取得全部 ETF
  Step 2: GET /stable/batch-etf-quotes → 取得報價（含 volume）
  Step 3: filter_us_etfs()             → 過濾掉含 . 的非美國代碼，篩選 volume ≥ 100,000
  Step 4: GET /stable/etf/info         → 取得 AUM、expense_ratio、holdings_count（async）
  Step 5: GET /stable/etf/holdings     → 取得每隻 ETF 的持倉清單（async）

用法：
  cd Aurum_Data_Fetcher
  python fetch_etf.py                  # 完整流程
  python fetch_etf.py --skip-holdings  # 只拉清單，不拉持倉（快速）
  python fetch_etf.py --concurrency 20 # 調整並發數（預設 15）
  python fetch_etf.py --min-volume 500000  # 調整 volume 門檻
  python fetch_etf.py --dry-run        # 只列出符合條件的 ETF，不寫 DB
============================================================================
"""
import argparse
import asyncio
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

import requests
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
# DB
# ============================================================================
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    return conn


def ensure_tables(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS etf_list (
            symbol          TEXT    PRIMARY KEY,
            name            TEXT,
            exchange        TEXT,
            asset_class     TEXT,
            aum             REAL,
            avg_volume      REAL,
            expense_ratio   REAL,
            holdings_count  INTEGER,
            fetched_at      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_etf_volume
            ON etf_list(avg_volume DESC);

        CREATE TABLE IF NOT EXISTS etf_holdings (
            etf_symbol      TEXT    NOT NULL,
            asset           TEXT    NOT NULL,
            name            TEXT,
            weight_pct      REAL,
            shares          REAL,
            market_value    REAL,
            updated_at      TEXT,
            fetched_at      TEXT,
            PRIMARY KEY (etf_symbol, asset)
        );
        CREATE INDEX IF NOT EXISTS idx_etfh_symbol
            ON etf_holdings(etf_symbol);
        CREATE INDEX IF NOT EXISTS idx_etfh_asset
            ON etf_holdings(asset);
    """)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def upsert_etf(conn: sqlite3.Connection, etf: dict):
    conn.execute("""
        INSERT OR REPLACE INTO etf_list
            (symbol, name, exchange, asset_class, aum, avg_volume,
             expense_ratio, holdings_count, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        etf.get("symbol"),
        etf.get("name"),
        etf.get("exchange"),
        etf.get("assetClass") or etf.get("asset_class"),
        etf.get("aum"),
        etf.get("volume") or etf.get("avg_volume"),
        etf.get("expenseRatio") or etf.get("expense_ratio"),
        etf.get("holdingsCount") or etf.get("holdings_count"),
        _now_iso(),
    ))


def upsert_holdings(conn: sqlite3.Connection, etf_symbol: str, holdings: list[dict]):
    now = _now_iso()
    # Delete old holdings for this ETF first (full refresh)
    conn.execute("DELETE FROM etf_holdings WHERE etf_symbol = ?", (etf_symbol,))
    for h in holdings:
        asset = h.get("asset") or h.get("symbol") or ""
        if not asset:
            continue
        conn.execute("""
            INSERT OR REPLACE INTO etf_holdings
                (etf_symbol, asset, name, weight_pct, shares, market_value,
                 updated_at, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            etf_symbol,
            asset,
            h.get("name"),
            h.get("weightPercentage") or h.get("weight_pct"),
            h.get("sharesNumber") or h.get("shares"),
            h.get("marketValue") or h.get("market_value"),
            h.get("updated") or h.get("updatedAt"),
            now,
        ))


# ============================================================================
# Step 1 + 2: Fetch ETF list with volume filter (sync — one-time calls)
# ============================================================================
def fetch_etf_list() -> list[dict]:
    """GET /stable/etf-list → full list of ETFs"""
    url = f"{BASE_URL}/etf-list?apikey={FMP_API_KEY}"
    log("Fetching ETF list...")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        log(f"ERROR: unexpected response from etf-list: {type(data)}")
        return []
    log(f"  Total ETFs from API: {len(data)}")
    return data


def fetch_batch_quotes() -> dict[str, dict]:
    """GET /stable/batch-etf-quotes → volume + price data, keyed by symbol"""
    url = f"{BASE_URL}/batch-etf-quotes?apikey={FMP_API_KEY}"
    log("Fetching batch ETF quotes (volume data)...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        log(f"WARNING: unexpected response from batch-etf-quotes")
        return {}
    log(f"  Quotes received: {len(data)}")
    return {q["symbol"]: q for q in data if "symbol" in q}


def filter_us_etfs(etf_list: list[dict], quotes: dict[str, dict],
                   min_volume: int = 100_000) -> list[dict]:
    """
    過濾條件：
    1. symbol 不含 '.'（排除 BRK.B 等非標準代碼）
    2. volume（來自 batch-etf-quotes）≥ min_volume
    """
    result = []
    skipped_dot = 0
    skipped_volume = 0
    no_quote = 0

    for etf in etf_list:
        symbol = etf.get("symbol", "")
        if not symbol:
            continue

        # 排除含 . 的代碼
        if "." in symbol:
            skipped_dot += 1
            continue

        # 從 quotes 取 volume
        q = quotes.get(symbol, {})
        volume = q.get("volume") or q.get("avgVolume") or 0

        if not volume:
            no_quote += 1
            continue

        if volume < min_volume:
            skipped_volume += 1
            continue

        # 合併 quote 數據（volume）
        etf["volume"] = volume
        result.append(etf)

    log(f"  Filtered: {len(result)} US ETFs (volume ≥ {min_volume:,})")
    log(f"  Skipped: {skipped_dot} (has '.') | {skipped_volume} (low volume) | {no_quote} (no quote)")
    return result


# ============================================================================
# Async Step 4 + 5: ETF info + holdings
# ============================================================================
class AsyncETFFetcher:
    def __init__(self, concurrency: int = 15):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.session: aiohttp.ClientSession | None = None
        self.info_ok = 0
        self.holdings_ok = 0
        self.failed = 0

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30))
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def _get(self, path: str, params: dict, retries: int = 2) -> list | dict | None:
        url = f"{BASE_URL}/{path}"
        params["apikey"] = FMP_API_KEY

        for attempt in range(retries + 1):
            async with self.semaphore:
                try:
                    async with self.session.get(url, params=params) as resp:
                        if resp.status == 429:
                            wait = 2 ** (attempt + 1)
                            log(f"  429 rate limit on {path}, wait {wait}s...")
                            await asyncio.sleep(wait)
                            continue
                        if resp.status >= 500:
                            if attempt < retries:
                                await asyncio.sleep(2)
                                continue
                            return None
                        if resp.status != 200:
                            return None
                        return await resp.json()
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    if attempt < retries:
                        await asyncio.sleep(2)
                        continue
                    return None
        return None

    async def fetch_info(self, symbol: str) -> dict | None:
        """GET /stable/etf/info?symbol=X"""
        data = await self._get("etf/info", {"symbol": symbol})
        if isinstance(data, list) and data:
            return data[0]
        if isinstance(data, dict):
            return data
        return None

    async def fetch_holdings(self, symbol: str) -> list | None:
        """GET /stable/etf/holdings?symbol=X"""
        data = await self._get("etf/holdings", {"symbol": symbol})
        if isinstance(data, list):
            return data
        return None

    async def process_etf(self, etf: dict, conn: sqlite3.Connection,
                          skip_holdings: bool = False):
        symbol = etf["symbol"]

        # Step 4: ETF info (AUM, expense ratio, holdings count)
        info = await self.fetch_info(symbol)
        if info:
            etf["assetClass"]    = info.get("assetClass") or etf.get("assetClass", "")
            etf["aum"]           = info.get("aum")
            etf["expenseRatio"]  = info.get("expenseRatio")
            etf["holdingsCount"] = info.get("holdingsCount")
            self.info_ok += 1

        upsert_etf(conn, etf)

        if skip_holdings:
            return

        # Step 5: ETF holdings
        holdings = await self.fetch_holdings(symbol)
        if holdings is not None:
            upsert_holdings(conn, symbol, holdings)
            self.holdings_ok += 1
        else:
            self.failed += 1

    async def run(self, etfs: list[dict], skip_holdings: bool = False,
                  batch_size: int = 500, batch_pause: int = 30):
        conn = get_db()
        ensure_tables(conn)

        total = len(etfs)
        done = 0
        start = time.time()
        COMMIT_EVERY = 50

        async def _process(etf):
            nonlocal done
            await self.process_etf(etf, conn, skip_holdings=skip_holdings)
            done += 1
            if done % COMMIT_EVERY == 0:
                conn.commit()
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                log(f"  Progress: {done}/{total} ({done/total*100:.1f}%) "
                    f"| info: {self.info_ok} | holdings: {self.holdings_ok} "
                    f"| {rate:.1f} ETF/s | ETA {eta:.0f}s")

        # Process in batches (rate limit protection)
        for batch_start in range(0, total, batch_size):
            batch = etfs[batch_start:batch_start + batch_size]
            batch_num = batch_start // batch_size + 1
            total_batches = (total + batch_size - 1) // batch_size
            if total_batches > 1:
                log(f"  ── Batch {batch_num}/{total_batches} ({len(batch)} ETFs) ──")

            await asyncio.gather(*[_process(e) for e in batch])
            conn.commit()

            remaining = total - (batch_start + len(batch))
            if remaining > 0:
                log(f"  ⏸ Pause {batch_pause}s before next batch ({remaining} remaining)...")
                await asyncio.sleep(batch_pause)

        conn.commit()
        conn.close()


# ============================================================================
# Main
# ============================================================================
async def async_main(args):
    # Step 1: ETF list
    etf_list = fetch_etf_list()
    if not etf_list:
        log("ERROR: No ETF data returned")
        return

    # Step 2: Batch quotes for volume
    quotes = fetch_batch_quotes()

    # Step 3: Filter US ETFs
    etfs = filter_us_etfs(etf_list, quotes, min_volume=args.min_volume)
    if not etfs:
        log("No ETFs passed the filter")
        return

    log(f"")
    log(f"=== ETF Fetcher ===")
    log(f"ETFs to process : {len(etfs)}")
    log(f"Skip holdings   : {args.skip_holdings}")
    log(f"Concurrency     : {args.concurrency}")
    log(f"DB path         : {DB_PATH}")

    if args.dry_run:
        log(f"\nDRY RUN — top 30 ETFs:")
        for e in sorted(etfs, key=lambda x: x.get("volume", 0), reverse=True)[:30]:
            log(f"  {e['symbol']:<10} vol={e.get('volume', 0):>12,.0f}  {e.get('name', '')}")
        return

    start = time.time()
    async with AsyncETFFetcher(concurrency=args.concurrency) as fetcher:
        await fetcher.run(etfs, skip_holdings=args.skip_holdings)
        elapsed = time.time() - start
        log(f"")
        log(f"=== DONE in {elapsed:.0f}s ===")
        log(f"  Info fetched    : {fetcher.info_ok}")
        log(f"  Holdings fetched: {fetcher.holdings_ok}")
        log(f"  Failed          : {fetcher.failed}")

    # Quick DB stats
    conn = get_db()
    etf_count = conn.execute("SELECT COUNT(*) FROM etf_list").fetchone()[0]
    holding_count = conn.execute("SELECT COUNT(*) FROM etf_holdings").fetchone()[0]
    etfs_with_holdings = conn.execute(
        "SELECT COUNT(DISTINCT etf_symbol) FROM etf_holdings").fetchone()[0]
    conn.close()
    log(f"")
    log(f"DB stats:")
    log(f"  etf_list     : {etf_count:,} ETFs")
    log(f"  etf_holdings : {holding_count:,} rows across {etfs_with_holdings:,} ETFs")


def main():
    parser = argparse.ArgumentParser(description="Fetch US ETF list + holdings into SQLite")
    parser.add_argument("--min-volume",    type=int,   default=100_000,
                        help="Minimum average volume filter (default: 100,000)")
    parser.add_argument("--concurrency",   type=int,   default=15,
                        help="Async concurrency limit (default: 15)")
    parser.add_argument("--skip-holdings", action="store_true",
                        help="Only fetch ETF list/info, skip holdings")
    parser.add_argument("--dry-run",       action="store_true",
                        help="List ETFs that pass the filter without writing to DB")
    args = parser.parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
