"""
fetch_stocktwits.py - Stocktwits 同步工具
============================================================================
寫入主站 aurum.db 的 stocktwits_* 表，供 /trending 頁面讀取。

範例：
  python fetch_stocktwits.py --symbols AAPL,MSFT
  python fetch_stocktwits.py --batch-size 25 --batch-index 0
  python fetch_stocktwits.py --all-batches --batch-size 25
============================================================================
"""
from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests

from config import Config
from logger import log


STOCKTWITS_API_BASE = "https://api.stocktwits.com/api/2"
HEADERS = {"User-Agent": "Mozilla/5.0"}
TERMINAL_SOURCE_ERROR_PREFIXES = ("cloudflare_challenge", "non_json_response")


class TerminalSourceError(Exception):
    """來源端回覆不可重試內容，例如 Cloudflare challenge HTML。"""

    def __init__(self, status_code: int | None, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


@dataclass(frozen=True)
class SyncConfig:
    batch_size: int
    batch_index: int
    delay_min: float
    delay_max: float
    max_retries: int
    limit: int
    all_batches: bool
    symbols: tuple[str, ...]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def today_start_utc() -> datetime:
    now = datetime.now(timezone.utc)
    return datetime(now.year, now.month, now.day, tzinfo=timezone.utc)


def dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS stocktwits_symbols (
            symbol TEXT PRIMARY KEY,
            company_name TEXT,
            exchange TEXT,
            sector TEXT,
            sub_industry TEXT,
            headquarters TEXT,
            date_added TEXT,
            cik TEXT,
            founded TEXT,
            is_sp500 INTEGER NOT NULL DEFAULT 1,
            stocktwits_id INTEGER,
            stocktwits_title TEXT,
            stocktwits_exchange TEXT,
            stocktwits_region TEXT,
            logo_url TEXT,
            instrument_class TEXT,
            watchlist_count_latest INTEGER,
            raw_wikipedia_json TEXT,
            raw_stocktwits_symbol_json TEXT,
            last_stream_at TEXT,
            last_error TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_stocktwits_symbols_sp500
            ON stocktwits_symbols(is_sp500, symbol);
        CREATE INDEX IF NOT EXISTS idx_stocktwits_symbols_watchlist
            ON stocktwits_symbols(watchlist_count_latest DESC);

        CREATE TABLE IF NOT EXISTS stocktwits_daily_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            snapshot_date TEXT NOT NULL,
            captured_at TEXT NOT NULL,
            watchlist_count INTEGER,
            message_count_fetched INTEGER NOT NULL DEFAULT 0,
            message_count_saved INTEGER NOT NULL DEFAULT 0,
            extra_pages_fetched INTEGER NOT NULL DEFAULT 0,
            bullish_count INTEGER NOT NULL DEFAULT 0,
            bearish_count INTEGER NOT NULL DEFAULT 0,
            unlabeled_count INTEGER NOT NULL DEFAULT 0,
            newest_message_at TEXT,
            oldest_message_at TEXT,
            is_intraday_complete INTEGER NOT NULL DEFAULT 0,
            raw_symbol_json TEXT,
            raw_cursor_json TEXT,
            sync_run_id INTEGER,
            UNIQUE(symbol, snapshot_date),
            FOREIGN KEY(symbol) REFERENCES stocktwits_symbols(symbol)
        );
        CREATE INDEX IF NOT EXISTS idx_stocktwits_snapshots_symbol_date
            ON stocktwits_daily_snapshots(symbol, snapshot_date DESC);

        CREATE TABLE IF NOT EXISTS stocktwits_messages (
            stocktwits_message_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            body TEXT,
            created_at TEXT,
            captured_at TEXT NOT NULL,
            username TEXT,
            display_name TEXT,
            avatar_url TEXT,
            sentiment TEXT,
            likes_total INTEGER,
            source_title TEXT,
            source_url TEXT,
            discussion INTEGER NOT NULL DEFAULT 0,
            raw_message_json TEXT,
            PRIMARY KEY (stocktwits_message_id, symbol),
            FOREIGN KEY(symbol) REFERENCES stocktwits_symbols(symbol)
        );
        CREATE INDEX IF NOT EXISTS idx_stocktwits_messages_symbol_created
            ON stocktwits_messages(symbol, created_at DESC);

        CREATE TABLE IF NOT EXISTS stocktwits_sync_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            mode TEXT NOT NULL,
            batch_index INTEGER,
            batch_size INTEGER,
            symbols_total INTEGER NOT NULL DEFAULT 0,
            symbols_processed INTEGER NOT NULL DEFAULT 0,
            symbols_succeeded INTEGER NOT NULL DEFAULT 0,
            symbols_failed INTEGER NOT NULL DEFAULT 0,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS stocktwits_sync_run_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sync_run_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            status TEXT NOT NULL,
            http_status INTEGER,
            attempts INTEGER NOT NULL DEFAULT 0,
            message_count INTEGER NOT NULL DEFAULT 0,
            pages_fetched INTEGER NOT NULL DEFAULT 0,
            watchlist_count INTEGER,
            error_message TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            FOREIGN KEY(sync_run_id) REFERENCES stocktwits_sync_runs(id)
        );
        CREATE INDEX IF NOT EXISTS idx_stocktwits_sync_items_run
            ON stocktwits_sync_run_items(sync_run_id, symbol);
        """
    )
    conn.commit()


def normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace(".", "-")


def load_sp500_universe(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT ticker, company_name, sector, sub_sector, headquarters,
               date_first_added, cik, founded
        FROM sp500_constituents
        WHERE ticker IS NOT NULL AND TRIM(ticker) != ''
        ORDER BY ticker
        """
    ).fetchall()
    return [
        {
            "symbol": normalize_symbol(row["ticker"]),
            "company_name": row["company_name"],
            "exchange": "",
            "sector": row["sector"],
            "sub_industry": row["sub_sector"],
            "headquarters": row["headquarters"],
            "date_added": row["date_first_added"],
            "cik": row["cik"],
            "founded": row["founded"],
            "source": "sp500_constituents",
        }
        for row in rows
    ]


def upsert_symbol_universe(conn: sqlite3.Connection, config: SyncConfig) -> int:
    now = utc_now()
    if config.symbols:
        conn.executemany(
            """
            INSERT INTO stocktwits_symbols (symbol, company_name, is_sp500, updated_at)
            VALUES (?, ?, 0, ?)
            ON CONFLICT(symbol) DO UPDATE SET updated_at = excluded.updated_at
            """,
            [(symbol, symbol, now) for symbol in config.symbols],
        )
        conn.commit()
        return len(config.symbols)

    records = load_sp500_universe(conn)
    if not records:
        raise RuntimeError("sp500_constituents table is empty; run the S&P 500 sync first or pass --symbols.")

    conn.executemany(
        """
        INSERT INTO stocktwits_symbols (
            symbol, company_name, exchange, sector, sub_industry, headquarters,
            date_added, cik, founded, is_sp500, raw_wikipedia_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            company_name = excluded.company_name,
            sector = excluded.sector,
            sub_industry = excluded.sub_industry,
            headquarters = excluded.headquarters,
            date_added = excluded.date_added,
            cik = excluded.cik,
            founded = excluded.founded,
            is_sp500 = 1,
            raw_wikipedia_json = excluded.raw_wikipedia_json,
            updated_at = excluded.updated_at
        """,
        [
            (
                item["symbol"],
                item["company_name"],
                item["exchange"],
                item["sector"],
                item["sub_industry"],
                item["headquarters"],
                item["date_added"],
                item["cik"],
                item["founded"],
                dumps(item),
                now,
            )
            for item in records
        ],
    )
    conn.commit()
    return len(records)


def fetch_stream(symbol: str, limit: int, max_id: int | None = None) -> tuple[dict, int]:
    url = f"{STOCKTWITS_API_BASE}/streams/symbol/{quote(symbol)}.json"
    params = {"limit": limit}
    if max_id is not None:
        params["max"] = max_id
    response = requests.get(url, params=params, headers=HEADERS, timeout=30)
    content_type = response.headers.get("content-type", "").lower()
    if "application/json" not in content_type:
        body = " ".join(response.text[:500].split())
        lower_body = body.lower()
        if "just a moment" in lower_body or "cloudflare" in lower_body:
            raise TerminalSourceError(
                response.status_code,
                "cloudflare_challenge: Stocktwits returned Cloudflare HTML instead of JSON; "
                "direct server-side public API requests are currently blocked.",
            )
        raise TerminalSourceError(
            response.status_code,
            f"non_json_response: Stocktwits returned {content_type or 'unknown content-type'}; body={body}",
        )
    response.raise_for_status()
    return response.json(), response.status_code


def fetch_with_retries(
    symbol: str,
    config: SyncConfig,
    max_id: int | None = None,
) -> tuple[dict | None, int | None, int, str | None]:
    attempts = 0
    backoff = (30.0, 90.0, 300.0)
    while attempts <= config.max_retries:
        attempts += 1
        try:
            data, status = fetch_stream(symbol, config.limit, max_id=max_id)
            return data, status, attempts, None
        except TerminalSourceError as exc:
            return None, exc.status_code, attempts, exc.detail
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            detail = exc.response.text[:500] if exc.response is not None else str(exc)
        except requests.RequestException as exc:
            status = None
            detail = str(exc)

        if attempts > config.max_retries:
            return None, status, attempts, detail
        wait_for = backoff[min(attempts - 1, len(backoff) - 1)]
        log.warning("%s failed，%.0fs 後重試：%s", symbol, wait_for, detail)
        time.sleep(wait_for)

    return None, None, attempts, "unknown_error"


def is_terminal_source_error(error: str | None) -> bool:
    return bool(error and error.startswith(TERMINAL_SOURCE_ERROR_PREFIXES))


def parse_message_time(message: dict) -> datetime | None:
    created_at = message.get("created_at")
    if not created_at:
        return None
    try:
        return datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError:
        return None


def collect_today_stream(symbol: str, config: SyncConfig):
    today_start = today_start_utc()
    pages_fetched = 0
    total_attempts = 0
    total_fetched = 0
    current_max = None
    merged_data = None
    all_messages = []
    seen_ids = set()
    is_complete = False

    while True:
        data, status, attempts, error = fetch_with_retries(symbol, config, current_max)
        total_attempts += attempts
        if data is None:
            return None, status, total_attempts, error, pages_fetched, is_complete, total_fetched

        pages_fetched += 1
        merged_data = data if merged_data is None else {**merged_data, "cursor": data.get("cursor")}
        page_messages = data.get("messages") or []
        total_fetched += len(page_messages)

        for message in page_messages:
            message_id = message.get("id")
            if message_id in seen_ids:
                continue
            seen_ids.add(message_id)
            all_messages.append(message)

        if not page_messages:
            is_complete = True
            break

        oldest_time = parse_message_time(page_messages[-1])
        cursor = data.get("cursor") or {}
        if oldest_time is not None and oldest_time < today_start:
            is_complete = True
            break
        if not cursor.get("more"):
            is_complete = True
            break

        current_max = cursor.get("max")
        if current_max is None:
            break

        sleep_for = random.uniform(config.delay_min, config.delay_max)
        log.info("Paging %s：%.2fs 後讀下一頁", symbol, sleep_for)
        time.sleep(sleep_for)

    all_messages.sort(key=lambda item: item.get("id", 0), reverse=True)
    merged_data["messages"] = [
        message for message in all_messages
        if (parse_message_time(message) or today_start) >= today_start
    ]
    return merged_data, 200, total_attempts, None, pages_fetched, is_complete, total_fetched


def start_run(conn: sqlite3.Connection, config: SyncConfig, total: int) -> int:
    cursor = conn.execute(
        """
        INSERT INTO stocktwits_sync_runs (
            started_at, status, mode, batch_index, batch_size, symbols_total
        ) VALUES (?, 'running', 'daily_sp500', ?, ?, ?)
        """,
        (utc_now(), config.batch_index, config.batch_size, total),
    )
    conn.commit()
    return int(cursor.lastrowid)


def finish_run(conn: sqlite3.Connection, run_id: int, status: str, processed: int, succeeded: int, failed: int, notes: str | None = None) -> None:
    conn.execute(
        """
        UPDATE stocktwits_sync_runs
        SET finished_at = ?, status = ?, symbols_processed = ?, symbols_succeeded = ?,
            symbols_failed = ?, notes = ?
        WHERE id = ?
        """,
        (utc_now(), status, processed, succeeded, failed, notes, run_id),
    )
    conn.commit()


def start_item(conn: sqlite3.Connection, run_id: int, symbol: str) -> int:
    cursor = conn.execute(
        """
        INSERT INTO stocktwits_sync_run_items (sync_run_id, symbol, status, started_at)
        VALUES (?, ?, 'running', ?)
        """,
        (run_id, symbol, utc_now()),
    )
    conn.commit()
    return int(cursor.lastrowid)


def finish_item(
    conn: sqlite3.Connection,
    item_id: int,
    status: str,
    attempts: int,
    message_count: int,
    watchlist_count: int | None,
    pages_fetched: int,
    http_status: int | None = None,
    error_message: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE stocktwits_sync_run_items
        SET status = ?, attempts = ?, message_count = ?, pages_fetched = ?, watchlist_count = ?,
            http_status = ?, error_message = ?, finished_at = ?
        WHERE id = ?
        """,
        (status, attempts, message_count, pages_fetched, watchlist_count, http_status, error_message, utc_now(), item_id),
    )
    conn.commit()


def save_symbol(conn: sqlite3.Connection, symbol: str, stream_data: dict) -> None:
    info = stream_data.get("symbol") or {}
    conn.execute(
        """
        UPDATE stocktwits_symbols
        SET stocktwits_id = ?, stocktwits_title = ?, stocktwits_exchange = ?, stocktwits_region = ?,
            logo_url = ?, instrument_class = ?, watchlist_count_latest = ?, raw_stocktwits_symbol_json = ?,
            last_stream_at = ?, last_error = NULL, updated_at = ?
        WHERE symbol = ?
        """,
        (
            info.get("id"),
            info.get("title"),
            info.get("exchange"),
            info.get("region"),
            info.get("logo_url"),
            info.get("instrument_class"),
            info.get("watchlist_count"),
            dumps(info),
            utc_now(),
            utc_now(),
            symbol,
        ),
    )


def save_snapshot(conn: sqlite3.Connection, symbol: str, stream_data: dict, run_id: int, fetched: int, saved: int, pages: int, complete: bool) -> None:
    info = stream_data.get("symbol") or {}
    messages = stream_data.get("messages") or []
    summary = {"Bullish": 0, "Bearish": 0, "Unlabeled": 0}
    for message in messages:
        sentiment = ((message.get("entities") or {}).get("sentiment") or {}).get("basic")
        summary[sentiment if sentiment in {"Bullish", "Bearish"} else "Unlabeled"] += 1
    newest = messages[0].get("created_at") if messages else None
    oldest = messages[-1].get("created_at") if messages else None

    conn.execute(
        """
        INSERT INTO stocktwits_daily_snapshots (
            symbol, snapshot_date, captured_at, watchlist_count, message_count_fetched,
            message_count_saved, extra_pages_fetched, bullish_count, bearish_count, unlabeled_count,
            newest_message_at, oldest_message_at, is_intraday_complete,
            raw_symbol_json, raw_cursor_json, sync_run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, snapshot_date) DO UPDATE SET
            captured_at = excluded.captured_at,
            watchlist_count = excluded.watchlist_count,
            message_count_fetched = excluded.message_count_fetched,
            message_count_saved = excluded.message_count_saved,
            extra_pages_fetched = excluded.extra_pages_fetched,
            bullish_count = excluded.bullish_count,
            bearish_count = excluded.bearish_count,
            unlabeled_count = excluded.unlabeled_count,
            newest_message_at = excluded.newest_message_at,
            oldest_message_at = excluded.oldest_message_at,
            is_intraday_complete = excluded.is_intraday_complete,
            raw_symbol_json = excluded.raw_symbol_json,
            raw_cursor_json = excluded.raw_cursor_json,
            sync_run_id = excluded.sync_run_id
        """,
        (
            symbol,
            today_utc(),
            utc_now(),
            info.get("watchlist_count"),
            fetched,
            saved,
            max(0, pages - 1),
            summary["Bullish"],
            summary["Bearish"],
            summary["Unlabeled"],
            newest,
            oldest,
            1 if complete else 0,
            dumps(info),
            dumps(stream_data.get("cursor") or {}),
            run_id,
        ),
    )


def save_messages(conn: sqlite3.Connection, symbol: str, stream_data: dict) -> None:
    captured_at = utc_now()
    rows = []
    for message in stream_data.get("messages") or []:
        if message.get("id") is None:
            continue
        rows.append(
            (
                message.get("id"),
                symbol,
                message.get("body"),
                message.get("created_at"),
                captured_at,
                (message.get("user") or {}).get("username"),
                (message.get("user") or {}).get("name"),
                (message.get("user") or {}).get("avatar_url"),
                ((message.get("entities") or {}).get("sentiment") or {}).get("basic"),
                (message.get("likes") or {}).get("total"),
                (message.get("source") or {}).get("title"),
                (message.get("source") or {}).get("url"),
                1 if message.get("discussion") else 0,
                dumps(message),
            )
        )
    conn.executemany(
        """
        INSERT INTO stocktwits_messages (
            stocktwits_message_id, symbol, body, created_at, captured_at,
            username, display_name, avatar_url, sentiment, likes_total,
            source_title, source_url, discussion, raw_message_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stocktwits_message_id, symbol) DO UPDATE SET
            body = excluded.body,
            created_at = excluded.created_at,
            captured_at = excluded.captured_at,
            username = excluded.username,
            display_name = excluded.display_name,
            avatar_url = excluded.avatar_url,
            sentiment = excluded.sentiment,
            likes_total = excluded.likes_total,
            source_title = excluded.source_title,
            source_url = excluded.source_url,
            discussion = excluded.discussion,
            raw_message_json = excluded.raw_message_json
        """,
        rows,
    )


def mark_error(conn: sqlite3.Connection, symbol: str, error_message: str) -> None:
    conn.execute(
        "UPDATE stocktwits_symbols SET last_error = ?, updated_at = ? WHERE symbol = ?",
        (error_message, utc_now(), symbol),
    )
    conn.commit()


def symbols_for_batch(conn: sqlite3.Connection, config: SyncConfig) -> tuple[list[str], int]:
    if config.symbols:
        return list(config.symbols), len(config.symbols)
    rows = conn.execute("SELECT symbol FROM stocktwits_symbols WHERE is_sp500 = 1 ORDER BY symbol").fetchall()
    symbols = [row["symbol"] for row in rows]
    start = config.batch_index * config.batch_size
    return symbols[start:start + config.batch_size], len(symbols)


def total_batch_count(total: int, batch_size: int) -> int:
    return max(1, (total + batch_size - 1) // batch_size)


def run_batch(conn: sqlite3.Connection, config: SyncConfig, total_symbols: int) -> dict:
    batch_symbols, universe_total = symbols_for_batch(conn, config)
    run_id = start_run(conn, config, len(batch_symbols))
    processed = succeeded = failed = 0
    total_batches = total_batch_count(universe_total, config.batch_size)
    log.info("Stocktwits batch %s/%s started，symbols=%s", config.batch_index + 1, total_batches, len(batch_symbols))

    try:
        for index, symbol in enumerate(batch_symbols, start=1):
            item_id = start_item(conn, run_id, symbol)
            log.info("[%s/%s] Fetching %s", index, len(batch_symbols), symbol)
            data, status, attempts, error, pages, complete, fetched = collect_today_stream(symbol, config)
            processed += 1

            if data is None:
                failed += 1
                finish_item(conn, item_id, "failed", attempts, 0, None, pages, status, error)
                mark_error(conn, symbol, error or f"http_{status}")
                log.error("FAILED %s | attempts=%s | status=%s | error=%s", symbol, attempts, status or "n/a", error)
                if is_terminal_source_error(error):
                    finish_run(
                        conn,
                        run_id,
                        "blocked",
                        processed,
                        succeeded,
                        failed,
                        error,
                    )
                    log.error("Stocktwits source blocked; stopping this batch to avoid repeated failed requests.")
                    return {"processed": processed, "succeeded": succeeded, "failed": failed}
            else:
                messages = data.get("messages") or []
                save_symbol(conn, symbol, data)
                save_snapshot(conn, symbol, data, run_id, fetched, len(messages), pages, complete)
                save_messages(conn, symbol, data)
                conn.commit()
                watchlist_count = (data.get("symbol") or {}).get("watchlist_count")
                finish_item(conn, item_id, "succeeded", attempts, len(messages), watchlist_count, pages, status)
                succeeded += 1
                log.info("OK %s | fetched=%s | saved=%s | watchlists=%s", symbol, fetched, len(messages), watchlist_count)

            if index < len(batch_symbols):
                time.sleep(random.uniform(config.delay_min, config.delay_max))

        finish_run(conn, run_id, "completed", processed, succeeded, failed, f"total_symbols={total_symbols}")
        return {"processed": processed, "succeeded": succeeded, "failed": failed}
    except KeyboardInterrupt:
        finish_run(conn, run_id, "aborted", processed, succeeded, failed, "Interrupted by user")
        raise
    except Exception as exc:
        finish_run(conn, run_id, "failed", processed, succeeded, failed, str(exc))
        raise


def run_sync(db_path: str, config: SyncConfig) -> None:
    conn = connect_db(db_path)
    try:
        ensure_schema(conn)
        total_symbols = upsert_symbol_universe(conn, config)
        total_batches = total_batch_count(total_symbols, config.batch_size)
        log.info("Stocktwits universe ready：total_symbols=%s，batch_size=%s，total_batches=%s", total_symbols, config.batch_size, total_batches)

        if config.all_batches and not config.symbols:
            totals = {"processed": 0, "succeeded": 0, "failed": 0}
            for batch_index in range(total_batches):
                batch_config = SyncConfig(config.batch_size, batch_index, config.delay_min, config.delay_max, config.max_retries, config.limit, False, ())
                result = run_batch(conn, batch_config, total_symbols)
                for key in totals:
                    totals[key] += result[key]
                if batch_index < total_batches - 1:
                    time.sleep(max(config.delay_max * 10, 20.0))
            log.info("All Stocktwits batches completed：%s", totals)
            return

        run_batch(conn, config, total_symbols)
    finally:
        conn.close()


def parse_symbols(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(normalize_symbol(symbol) for symbol in value.split(",") if symbol.strip())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Stocktwits data into aurum.db.")
    parser.add_argument("--db-path", default=Config.DB_PATH)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--batch-index", type=int, default=0)
    parser.add_argument("--all-batches", action="store_true")
    parser.add_argument("--symbols", help="Comma-separated symbols for smoke test, e.g. AAPL,MSFT")
    parser.add_argument("--delay-min", type=float, default=1.5)
    parser.add_argument("--delay-max", type=float, default=3.5)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--limit", type=int, default=30)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    config = SyncConfig(
        batch_size=max(1, args.batch_size),
        batch_index=max(0, args.batch_index),
        delay_min=max(0.0, args.delay_min),
        delay_max=max(args.delay_min, args.delay_max),
        max_retries=max(0, args.max_retries),
        limit=max(1, min(args.limit, 30)),
        all_batches=bool(args.all_batches),
        symbols=parse_symbols(args.symbols),
    )
    log.info("Stocktwits sync target DB：%s", db_path)
    run_sync(str(db_path), config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
