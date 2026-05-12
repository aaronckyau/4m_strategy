from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone

from config import Config
from db import get_db
from fmp_client import FMPClient
from logger import log

PROXIES = {
    "SPY": "S&P 500 ETF proxy",
    "DIA": "Dow Jones ETF proxy",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ensure_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_proxy_ohlc (
            symbol     TEXT NOT NULL,
            date       TEXT NOT NULL,
            open       REAL,
            high       REAL,
            low        REAL,
            close      REAL,
            adj_close  REAL,
            volume     INTEGER,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (symbol, date)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_market_proxy_ohlc_symbol_date ON market_proxy_ohlc(symbol, date DESC)"
    )
    conn.commit()


def _last_date(conn, symbol: str) -> str | None:
    row = conn.execute(
        "SELECT MAX(date) FROM market_proxy_ohlc WHERE symbol = ?",
        (symbol,),
    ).fetchone()
    return row[0] if row and row[0] else None


def _upsert_rows(conn, symbol: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    fetched_at = _now_iso()
    conn.executemany(
        """
        INSERT OR REPLACE INTO market_proxy_ohlc
            (symbol, date, open, high, low, close, adj_close, volume, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                symbol,
                row.get("date"),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("adjClose") or row.get("close"),
                row.get("volume"),
                fetched_at,
            )
            for row in rows
            if row.get("date")
        ],
    )
    conn.commit()
    return len(rows)


def fetch_market_proxy_ohlc(
    *,
    symbols: list[str] | None = None,
    incremental: bool = False,
    backfill_from: str | None = None,
    days: int = 730,
) -> dict:
    client = FMPClient()
    symbols = [symbol.upper() for symbol in (symbols or list(PROXIES))]
    today = datetime.now().strftime("%Y-%m-%d")
    default_from = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    summary = {
        "total_items": len(symbols),
        "success_items": 0,
        "failed_items": 0,
        "skipped_items": 0,
        "records_written": 0,
        "items": [],
    }

    conn = get_db()
    try:
        _ensure_table(conn)
        for symbol in symbols:
            if backfill_from:
                from_date = backfill_from
            elif incremental:
                last_date = _last_date(conn, symbol)
                if last_date:
                    from_date = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                    if from_date > today:
                        summary["skipped_items"] += 1
                        summary["items"].append({"item_key": symbol, "item_type": "market_proxy", "status": "skipped"})
                        continue
                else:
                    from_date = default_from
            else:
                from_date = default_from

            log.info("[market_proxy_ohlc] %s %s -> %s", symbol, from_date, today)
            rows = client.get_historical_prices(symbol, from_date, today)
            if not rows:
                summary["failed_items"] += 1
                summary["items"].append({"item_key": symbol, "item_type": "market_proxy", "status": "failed"})
                continue
            written = _upsert_rows(conn, symbol, rows)
            summary["success_items"] += 1
            summary["records_written"] += written
            summary["items"].append({
                "item_key": symbol,
                "item_type": "market_proxy",
                "status": "done",
                "records_written": written,
            })
    finally:
        conn.close()

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch SPY/DIA ETF proxy OHLC into market_proxy_ohlc")
    parser.add_argument("--symbol", action="append", help="Proxy symbol to fetch. Repeatable. Defaults to SPY and DIA.")
    parser.add_argument("--incremental", action="store_true", help="Fetch only dates after the latest stored date.")
    parser.add_argument("--backfill-from", help="Fetch from YYYY-MM-DD.")
    parser.add_argument("--days", type=int, default=730, help="History length for full fetch.")
    args = parser.parse_args()
    if args.incremental and args.backfill_from:
        parser.error("--incremental cannot be used with --backfill-from")

    summary = fetch_market_proxy_ohlc(
        symbols=args.symbol,
        incremental=args.incremental,
        backfill_from=args.backfill_from,
        days=args.days,
    )
    print("RUN_SUMMARY_JSON:" + json.dumps(summary, ensure_ascii=False))
    if summary["failed_items"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
