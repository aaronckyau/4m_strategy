"""
fetch_analyst_forecast.py - Batch fetch FMP analyst forecast data into aurum.db.

Usage:
  cd Aurum_Data_Fetcher
  python fetch_analyst_forecast.py
  python fetch_analyst_forecast.py --ticker AAPL
  python fetch_analyst_forecast.py --dry-run

Writes one year of analyst grade events and historical rating distribution.
"""
import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timedelta, timezone

try:
    import aiohttp
except ImportError:
    print("ERROR: aiohttp not installed. Run: pip install aiohttp")
    sys.exit(1)

from dotenv import load_dotenv
load_dotenv()

from config import Config
from db import get_db, init_tables, ensure_stock_exists, upsert_analyst_forecast
from utils import log

FMP_API_KEY = Config.FMP_API_KEY
if not FMP_API_KEY:
    print("ERROR: FMP_API_KEY not set in .env")
    sys.exit(1)

BASE_URL = "https://financialmodelingprep.com/stable"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _cutoff_date(years: int = 1) -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=365 * years)).isoformat()


def load_db_tickers(market_filter: str = "US") -> list[str]:
    conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT ticker
            FROM stocks_master
            WHERE (
                market = ?
                OR (COALESCE(market, '') = '' AND ticker NOT LIKE '%.%')
            )
            AND ticker NOT LIKE '%.%'
            ORDER BY ticker
            """,
            (market_filter,),
        ).fetchall()
        return [row["ticker"] for row in rows if row["ticker"]]
    finally:
        conn.close()


class RateLimiter:
    def __init__(self, rate: int = 600, period: float = 60.0):
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


class AnalystForecastFetcher:
    def __init__(self, concurrency: int = 20, rate_limit: int = 600):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.rate_limiter = RateLimiter(rate=rate_limit)
        self.session: aiohttp.ClientSession | None = None
        self.success = 0
        self.failed = 0
        self.skipped = 0
        self.records_written = 0
        self.items: list[dict] = []

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def _fetch_json(self, endpoint: str, symbol: str, retries: int = 2):
        url = f"{BASE_URL}/{endpoint}"
        params = {"symbol": symbol, "apikey": FMP_API_KEY}

        for attempt in range(retries + 1):
            await self.rate_limiter.acquire()
            async with self.semaphore:
                try:
                    async with self.session.get(url, params=params) as resp:
                        if resp.status == 429:
                            await asyncio.sleep(2 ** (attempt + 1))
                            continue
                        if resp.status >= 500 and attempt < retries:
                            await asyncio.sleep(2)
                            continue
                        if resp.status != 200:
                            raise RuntimeError(f"{endpoint} HTTP {resp.status}")
                        return await resp.json()
                except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                    if attempt < retries:
                        await asyncio.sleep(2)
                        continue
                    raise RuntimeError(f"{endpoint} request failed: {exc}") from exc
        return None

    async def fetch_symbol(self, ticker: str) -> dict:
        endpoints = {
            "price_consensus": "price-target-consensus",
            "price_summary": "price-target-summary",
            "grades": "grades",
            "grades_historical": "grades-historical",
            "grades_consensus": "grades-consensus",
        }
        results = await asyncio.gather(
            *[self._fetch_json(endpoint, ticker) for endpoint in endpoints.values()],
            return_exceptions=True,
        )
        payload = {}
        for key, value in zip(endpoints.keys(), results):
            if isinstance(value, Exception):
                raise value
            payload[key] = value
        return payload

    async def fetch_batch(self, tickers: list[str], cutoff_date: str):
        total = len(tickers)
        done = 0
        started = time.time()
        db_lock = asyncio.Lock()
        conn = get_db()

        def first_row(value):
            if isinstance(value, list):
                return value[0] if value else {}
            return value if isinstance(value, dict) else {}

        try:
            async def process(ticker: str):
                nonlocal done
                item_started = _now_iso()
                records = 0
                status = "success"
                error = None

                try:
                    payload = await self.fetch_symbol(ticker)
                    price_consensus = first_row(payload.get("price_consensus"))
                    price_summary = first_row(payload.get("price_summary"))
                    grades_consensus = first_row(payload.get("grades_consensus"))
                    grades_historical = payload.get("grades_historical")
                    grade_events = payload.get("grades")

                    if not any((price_consensus, grades_consensus, grades_historical, grade_events)):
                        status = "skipped"
                        self.skipped += 1
                    else:
                        async with db_lock:
                            ensure_stock_exists(conn, ticker)
                            records = upsert_analyst_forecast(
                                conn,
                                ticker,
                                price_consensus=price_consensus,
                                price_summary=price_summary,
                                grades_consensus=grades_consensus,
                                grades_historical=grades_historical if isinstance(grades_historical, list) else [],
                                grade_events=grade_events if isinstance(grade_events, list) else [],
                                cutoff_date=cutoff_date,
                            )
                        self.success += 1
                        self.records_written += records
                except Exception as exc:
                    status = "failed"
                    error = str(exc)
                    self.failed += 1
                    log(f"  x {ticker}: {error}")

                done += 1
                self.items.append({
                    "item_key": ticker,
                    "item_type": "ticker",
                    "status": status,
                    "attempts": 1,
                    "records_written": records,
                    "error_message": error,
                    "started_at": item_started,
                    "finished_at": _now_iso(),
                })

                if done % 25 == 0 or done == total:
                    elapsed = time.time() - started
                    rate = done / elapsed if elapsed > 0 else 0
                    eta = (total - done) / rate if rate > 0 else 0
                    log(
                        f"  Progress: {done}/{total} ({done/total*100:.1f}%) "
                        f"| {self.success} ok | {self.failed} failed | "
                        f"{self.records_written} records | ETA {eta:.0f}s"
                    )

            await asyncio.gather(*[process(ticker) for ticker in tickers])
        finally:
            conn.close()


async def async_main(args) -> int:
    init_tables()
    tickers = load_db_tickers("US")
    if args.ticker:
        ticker = args.ticker.upper()
        tickers = [ticker] if ticker in tickers else [ticker]

    if not tickers:
        log("No US tickers found in stocks_master")
        summary = {
            "total_items": 0,
            "success_items": 0,
            "failed_items": 0,
            "skipped_items": 0,
            "records_written": 0,
            "summary_text": "No US tickers found",
            "items": [],
        }
        print("RUN_SUMMARY_JSON:" + json.dumps(summary, ensure_ascii=False), flush=True)
        return 0

    cutoff = _cutoff_date(1)
    log("=== Analyst Forecast Fetcher ===")
    log(f"Tickers: {len(tickers)} | Cutoff: {cutoff} | Concurrency: {args.concurrency}")

    if args.dry_run:
        for ticker in tickers[:50]:
            log(f"  {ticker}")
        if len(tickers) > 50:
            log(f"  ... and {len(tickers) - 50} more")
        summary = {
            "total_items": len(tickers),
            "success_items": 0,
            "failed_items": 0,
            "skipped_items": len(tickers),
            "records_written": 0,
            "summary_text": "Dry run",
            "items": [],
        }
        print("RUN_SUMMARY_JSON:" + json.dumps(summary, ensure_ascii=False), flush=True)
        return 0

    start = time.time()
    async with AnalystForecastFetcher(
        concurrency=args.concurrency,
        rate_limit=args.rate_limit,
    ) as fetcher:
        await fetcher.fetch_batch(tickers, cutoff)
        elapsed = time.time() - start
        log(f"=== DONE in {elapsed:.0f}s ===")
        log(
            f"  Success: {fetcher.success} | Failed: {fetcher.failed} | "
            f"Skipped: {fetcher.skipped} | Records: {fetcher.records_written}"
        )
        summary = {
            "total_items": len(tickers),
            "success_items": fetcher.success,
            "failed_items": fetcher.failed,
            "skipped_items": fetcher.skipped,
            "records_written": fetcher.records_written,
            "summary_text": (
                f"{fetcher.success} success, {fetcher.failed} failed, "
                f"{fetcher.skipped} skipped, {fetcher.records_written} records"
            ),
            "items": fetcher.items,
        }
        print("RUN_SUMMARY_JSON:" + json.dumps(summary, ensure_ascii=False), flush=True)
        return 1 if fetcher.failed else 0


def main():
    parser = argparse.ArgumentParser(description="Fetch FMP analyst forecast data into DB")
    parser.add_argument("--ticker", help="Single ticker to test")
    parser.add_argument("--concurrency", type=int, default=20, help="Max concurrent tickers")
    parser.add_argument("--rate-limit", dest="rate_limit", type=int, default=600, help="Max FMP requests per minute")
    parser.add_argument("--dry-run", action="store_true", help="List tickers without fetching")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(async_main(args)))


if __name__ == "__main__":
    main()
