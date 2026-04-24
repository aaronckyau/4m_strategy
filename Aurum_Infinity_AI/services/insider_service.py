from __future__ import annotations

import math
import os
import re
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from database import DB_PATH as APP_DB_PATH

INSIDER_WINDOWS = (7, 30, 60, 90)
INSIDER_MIN_VALUES = (0, 100_000, 500_000, 1_000_000, 5_000_000)
OPEN_MARKET_BUY = "P-Purchase"
OPEN_MARKET_SELL = "S-Sale"
MAX_TRANSACTION_VALUE = 5_000_000_000
TOP_SIGNAL_LIMIT = 24
ROLE_KEYWORDS = ("officer", "director", "ceo", "cfo", "coo", "president", "chair")
SEC_TABLE = "sec_stage_trades"
FMP_TABLE = "insider_trades"
INVALID_SYMBOLS = {"NONE", "N/A", "NA", "NULL", "UNKNOWN", "-"}
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]{1,6}(\.[A-Z]{1,3})?$")


def _app_dir() -> Path:
    return Path(__file__).resolve().parent.parent


def _project_dir() -> Path:
    return _app_dir().parent


def _candidate_db_paths() -> list[Path]:
    env_path = os.getenv("INSIDER_DB_PATH")
    candidates: list[Path] = []
    if env_path:
        candidates.append(Path(env_path))
    candidates.extend(
        [
            _project_dir() / "data" / "db" / "insider.db",
            Path(APP_DB_PATH),
            _app_dir() / "runtime" / "insider.db",
            _app_dir() / "insider.db",
        ]
    )
    seen: set[Path] = set()
    unique_candidates = []
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if resolved not in seen:
            unique_candidates.append(resolved)
            seen.add(resolved)
    return unique_candidates


def _connect_readonly(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _has_table(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _open_insider_db(db_path: str | Path | None = None) -> tuple[sqlite3.Connection | None, dict]:
    candidates = [Path(db_path).expanduser().resolve()] if db_path else _candidate_db_paths()
    checked_paths = []
    first_existing_without_table: Path | None = None

    for path in candidates:
        checked_paths.append(str(path))
        if not path.exists():
            continue
        try:
            conn = _connect_readonly(path)
        except sqlite3.Error:
            continue
        has_sec = _has_table(conn, SEC_TABLE)
        if has_sec:
            return conn, {
                "status": "ok",
                "path": str(path),
                "checked_paths": checked_paths,
                "has_sec_table": has_sec,
            }
        conn.close()
        first_existing_without_table = first_existing_without_table or path

    status = "missing_table" if first_existing_without_table else "missing_db"
    return None, {
        "status": status,
        "path": str(first_existing_without_table) if first_existing_without_table else None,
        "checked_paths": checked_paths,
    }


def _open_market_count(conn: sqlite3.Connection, table_name: str) -> int:
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS cnt
        FROM {table_name}
        WHERE transaction_type IN (?, ?)
          AND transaction_date GLOB '????-??-??'
          AND securities_transacted_value <= ?
        """,
        (OPEN_MARKET_BUY, OPEN_MARKET_SELL, MAX_TRANSACTION_VALUE),
    ).fetchone()
    return int(row["cnt"] if row else 0)


def _choose_trade_table(conn: sqlite3.Connection, source: dict) -> tuple[str | None, dict]:
    has_sec = source.get("has_sec_table", False)
    sec_count = _open_market_count(conn, SEC_TABLE) if has_sec else 0
    if not has_sec:
        return None, {
            "selected_table": None,
            "selected_label": None,
            "sec_count": sec_count,
        }
    return SEC_TABLE, {
        "selected_table": SEC_TABLE,
        "selected_label": "sec",
        "sec_count": sec_count,
    }


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _normalize_symbol(value: str | None) -> str:
    text = (value or "").strip().upper()
    if ":" in text:
        text = text.rsplit(":", 1)[-1]
    return text


def _is_valid_symbol(value: str) -> bool:
    return bool(value) and value not in INVALID_SYMBOLS and SYMBOL_PATTERN.match(value) is not None


def _format_number(value: float | int | None) -> str:
    if value is None:
        return "-"
    return f"{int(round(value)):,}"


def _format_money(value: float | int | None) -> str:
    if value is None:
        return "-"
    number = float(value)
    sign = "-" if number < 0 else ""
    number = abs(number)
    if number >= 1_000_000_000:
        return f"{sign}${number / 1_000_000_000:.2f}B"
    if number >= 1_000_000:
        return f"{sign}${number / 1_000_000:.2f}M"
    if number >= 1_000:
        return f"{sign}${number / 1_000:.1f}K"
    return f"{sign}${number:,.0f}"


def _format_price(value: float | int | None) -> str:
    if value in (None, 0):
        return "-"
    return f"${float(value):,.2f}"


def _is_officer_role(role: str | None) -> bool:
    normalized = (role or "").lower()
    if "10%" in normalized or "10 percent" in normalized:
        return False
    return any(keyword in normalized for keyword in ROLE_KEYWORDS)


def _score_amount(amount: float) -> float:
    if amount <= 0:
        return 0
    return min(30, math.log10((amount / 100_000) + 1) * 12)


def _score_cluster(insider_count: int) -> int:
    if insider_count >= 4:
        return 28
    if insider_count == 3:
        return 22
    if insider_count == 2:
        return 15
    return 0


def _score_recency(latest_date: str | None, anchor_date: date) -> int:
    parsed = _parse_iso_date(latest_date)
    if not parsed:
        return 0
    age = max((anchor_date - parsed).days, 0)
    if age <= 2:
        return 12
    if age <= 7:
        return 8
    if age <= 14:
        return 4
    return 0


def _build_reasons(signal: dict[str, Any]) -> list[dict[str, Any]]:
    reasons = []
    if signal["buy_insider_count"] >= 2:
        reasons.append({"kind": "buyers", "count": signal["buy_insider_count"]})
    if signal["buy_amount"] > 0:
        reasons.append({"kind": "buy_amount", "amount_display": _format_money(signal["buy_amount"])})
    if signal["large_trade_count"]:
        reasons.append({"kind": "large_trade", "count": signal["large_trade_count"]})
    if signal["officer_involved"]:
        reasons.append({"kind": "officer_involved"})
    if signal["sell_amount"] > 0:
        reasons.append({"kind": "sell_offset", "amount_display": _format_money(signal["sell_amount"])})
    return reasons[:4]


def _empty_dashboard(source: dict, *, window_days: int, min_value: int) -> dict:
    return {
        "source": source,
        "status": source["status"],
        "window_days": window_days,
        "min_value": min_value,
        "latest_transaction_date": None,
        "window_start": None,
        "signals": [],
        "stats": {
            "signal_count": 0,
            "buy_amount": 0,
            "sell_amount": 0,
            "net_amount": 0,
            "buy_count": 0,
            "sell_count": 0,
        },
        "stats_display": {
            "signal_count": "0",
            "buy_amount": "$0",
            "sell_amount": "$0",
            "net_amount": "$0",
            "buy_count": "0",
            "sell_count": "0",
        },
        "top_signal": None,
    }


def _latest_open_market_date(conn: sqlite3.Connection, table_name: str) -> date | None:
    row = conn.execute(
        f"""
        SELECT MAX(transaction_date) AS latest_date
        FROM {table_name}
        WHERE transaction_type IN (?, ?)
          AND transaction_date GLOB '????-??-??'
          AND securities_transacted_value <= ?
        """,
        (OPEN_MARKET_BUY, OPEN_MARKET_SELL, MAX_TRANSACTION_VALUE),
    ).fetchone()
    return _parse_iso_date(row["latest_date"] if row else None)


def _query_open_market_rows(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    window_start: date,
    latest_date: date,
) -> list[sqlite3.Row]:
    if table_name == SEC_TABLE:
        return conn.execute(
            """
            SELECT
                id,
                symbol,
                COALESCE(NULLIF(company_name, ''), symbol) AS company_name,
                reporting_name,
                type_of_owner,
                transaction_date,
                filling_date,
                acquisition_or_disposition,
                securities_transacted,
                price,
                securities_transacted_value,
                form_type,
                transaction_type,
                COALESCE(sec_index_url, sec_xml_url) AS link
            FROM sec_stage_trades
            WHERE transaction_type IN (?, ?)
              AND transaction_date >= ?
              AND transaction_date <= ?
              AND transaction_date GLOB '????-??-??'
              AND securities_transacted > 0
              AND securities_transacted_value >= 0
              AND securities_transacted_value <= ?
            ORDER BY transaction_date DESC, securities_transacted_value DESC, id DESC
            LIMIT 5000
            """,
            (
                OPEN_MARKET_BUY,
                OPEN_MARKET_SELL,
                window_start.isoformat(),
                latest_date.isoformat(),
                MAX_TRANSACTION_VALUE,
            ),
        ).fetchall()

    return conn.execute(
        """
        SELECT
            id,
            symbol,
            COALESCE(NULLIF(company_name, ''), symbol) AS company_name,
            reporting_name,
            type_of_owner,
            transaction_date,
            filling_date,
            acquisition_or_disposition,
            securities_transacted,
            price,
            securities_transacted_value,
            form_type,
            transaction_type,
            link
        FROM insider_trades
        WHERE transaction_type IN (?, ?)
          AND transaction_date >= ?
          AND transaction_date <= ?
          AND transaction_date GLOB '????-??-??'
          AND securities_transacted > 0
          AND securities_transacted_value >= 0
          AND securities_transacted_value <= ?
        ORDER BY transaction_date DESC, securities_transacted_value DESC, id DESC
        LIMIT 5000
        """,
        (
            OPEN_MARKET_BUY,
            OPEN_MARKET_SELL,
            window_start.isoformat(),
            latest_date.isoformat(),
            MAX_TRANSACTION_VALUE,
        ),
    ).fetchall()


def _detail_from_row(row: sqlite3.Row) -> dict:
    is_buy = (
        row["acquisition_or_disposition"] == "A"
        and row["transaction_type"] == OPEN_MARKET_BUY
    )
    signed_amount = row["securities_transacted_value"] if is_buy else -row["securities_transacted_value"]
    signed_shares = row["securities_transacted"] if is_buy else -row["securities_transacted"]
    return {
        "date": row["transaction_date"],
        "filing_date": row["filling_date"],
        "insider": row["reporting_name"] or "-",
        "role": row["type_of_owner"] or "-",
        "side_class": "buy" if is_buy else "sell",
        "shares": signed_shares,
        "amount": signed_amount,
        "price": row["price"],
        "form_type": row["form_type"] or "-",
        "link": row["link"],
        "shares_display": _format_number(signed_shares),
        "amount_display": _format_money(signed_amount),
        "price_display": _format_price(row["price"]),
    }


def _finalize_signal(signal: dict, *, anchor_date: date) -> dict:
    signal["buy_insider_count"] = len(signal.pop("_buyers"))
    signal["sell_insider_count"] = len(signal.pop("_sellers"))
    signal["officer_involved"] = bool(signal.pop("_officer_names"))
    signal["net_amount"] = signal["buy_amount"] - signal["sell_amount"]
    signal["net_shares"] = signal["buy_shares"] - signal["sell_shares"]
    signal["avg_buy_price"] = (
        signal["buy_amount"] / signal["buy_shares"] if signal["buy_shares"] else None
    )
    sell_drag = min(15, (signal["sell_amount"] / signal["buy_amount"]) * 10) if signal["buy_amount"] else 0
    score = (
        _score_cluster(signal["buy_insider_count"])
        + _score_amount(signal["buy_amount"])
        + (16 if signal["officer_involved"] else 0)
        + (8 if signal["large_trade_count"] else 0)
        + _score_recency(signal["latest_date"], anchor_date)
        - sell_drag
    )
    signal["score"] = round(max(0, min(100, score)))
    signal["score_width"] = f"{signal['score']}%"
    signal["reasons"] = _build_reasons(signal)
    signal["buy_amount_display"] = _format_money(signal["buy_amount"])
    signal["sell_amount_display"] = _format_money(signal["sell_amount"])
    signal["net_amount_display"] = _format_money(signal["net_amount"])
    signal["buy_shares_display"] = _format_number(signal["buy_shares"])
    signal["avg_buy_price_display"] = _format_price(signal["avg_buy_price"])
    signal["details"] = signal["details"][:8]
    return signal


def _build_signals(rows: list[sqlite3.Row], *, min_value: int, anchor_date: date) -> list[dict]:
    grouped: dict[str, dict] = {}
    for row in rows:
        symbol = _normalize_symbol(row["symbol"])
        if not _is_valid_symbol(symbol):
            continue
        signal = grouped.setdefault(
            symbol,
            {
                "symbol": symbol,
                "company_name": row["company_name"] or symbol,
                "latest_date": row["transaction_date"],
                "buy_amount": 0.0,
                "sell_amount": 0.0,
                "buy_shares": 0.0,
                "sell_shares": 0.0,
                "buy_count": 0,
                "sell_count": 0,
                "large_trade_count": 0,
                "details": [],
                "_buyers": set(),
                "_sellers": set(),
                "_officer_names": set(),
            },
        )
        signal["latest_date"] = max(signal["latest_date"], row["transaction_date"])
        signal["details"].append(_detail_from_row(row))

        amount = float(row["securities_transacted_value"] or 0)
        shares = float(row["securities_transacted"] or 0)
        insider = row["reporting_name"] or ""
        is_buy = (
            row["acquisition_or_disposition"] == "A"
            and row["transaction_type"] == OPEN_MARKET_BUY
        )
        if is_buy:
            signal["buy_amount"] += amount
            signal["buy_shares"] += shares
            signal["buy_count"] += 1
            signal["_buyers"].add(insider)
            if amount >= 500_000:
                signal["large_trade_count"] += 1
            if _is_officer_role(row["type_of_owner"]):
                signal["_officer_names"].add(insider)
        else:
            signal["sell_amount"] += amount
            signal["sell_shares"] += shares
            signal["sell_count"] += 1
            signal["_sellers"].add(insider)

    signals = []
    for signal in grouped.values():
        if signal["buy_amount"] < min_value or signal["buy_count"] == 0:
            continue
        signals.append(_finalize_signal(signal, anchor_date=anchor_date))

    signals.sort(
        key=lambda item: (
            item["score"],
            item["buy_insider_count"],
            item["buy_amount"],
            item["latest_date"],
        ),
        reverse=True,
    )
    return signals[:TOP_SIGNAL_LIMIT]


def _build_stats(rows: list[sqlite3.Row], signals: list[dict]) -> tuple[dict, dict]:
    buy_rows = [
        row
        for row in rows
        if row["acquisition_or_disposition"] == "A" and row["transaction_type"] == OPEN_MARKET_BUY
    ]
    sell_rows = [
        row
        for row in rows
        if row["acquisition_or_disposition"] == "D" and row["transaction_type"] == OPEN_MARKET_SELL
    ]
    buy_amount = sum(float(row["securities_transacted_value"] or 0) for row in buy_rows)
    sell_amount = sum(float(row["securities_transacted_value"] or 0) for row in sell_rows)
    stats = {
        "signal_count": len(signals),
        "buy_amount": buy_amount,
        "sell_amount": sell_amount,
        "net_amount": buy_amount - sell_amount,
        "buy_count": len(buy_rows),
        "sell_count": len(sell_rows),
    }
    stats_display = {
        "signal_count": _format_number(stats["signal_count"]),
        "buy_amount": _format_money(stats["buy_amount"]),
        "sell_amount": _format_money(stats["sell_amount"]),
        "net_amount": _format_money(stats["net_amount"]),
        "buy_count": _format_number(stats["buy_count"]),
        "sell_count": _format_number(stats["sell_count"]),
    }
    return stats, stats_display


def load_dashboard(
    *,
    window_days: int = 30,
    min_value: int = 100_000,
    db_path: str | Path | None = None,
) -> dict:
    window_days = window_days if window_days in INSIDER_WINDOWS else 30
    min_value = min_value if min_value in INSIDER_MIN_VALUES else 100_000
    conn, source = _open_insider_db(db_path)
    if conn is None:
        return _empty_dashboard(source, window_days=window_days, min_value=min_value)

    try:
        table_name, selection = _choose_trade_table(conn, source)
        if table_name is None:
            return _empty_dashboard(source, window_days=window_days, min_value=min_value)
        latest_date = _latest_open_market_date(conn, table_name)
        if latest_date is None:
            return _empty_dashboard(
                {
                    "status": "empty",
                    "path": source.get("path"),
                    "checked_paths": source["checked_paths"],
                    **selection,
                },
                window_days=window_days,
                min_value=min_value,
            )
        window_start = latest_date - timedelta(days=window_days - 1)
        rows = _query_open_market_rows(
            conn,
            table_name=table_name,
            window_start=window_start,
            latest_date=latest_date,
        )
    finally:
        conn.close()

    signals = _build_signals(rows, min_value=min_value, anchor_date=latest_date)
    stats, stats_display = _build_stats(rows, signals)
    status = "ok" if signals else "empty"
    return {
        "source": {**source, **selection},
        "status": status,
        "window_days": window_days,
        "min_value": min_value,
        "latest_transaction_date": latest_date.isoformat(),
        "window_start": window_start.isoformat(),
        "signals": signals,
        "stats": stats,
        "stats_display": stats_display,
        "top_signal": signals[0] if signals else None,
    }
