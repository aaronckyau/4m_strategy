from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from database import DB_PATH, get_db

DEFAULT_WINDOW_DAYS = 180
PROXY_SYMBOLS = {
    "SPY": "S&P 500 ETF",
    "DIA": "Dow ETF",
}


def _connect(db_path: str | Path | None = None) -> sqlite3.Connection:
    if db_path is None:
        return get_db()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _has_table(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _safe_pct(numerator: int | float, denominator: int | float) -> float | None:
    if not denominator:
        return None
    return round(float(numerator) / float(denominator) * 100.0, 1)


def _format_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.1f}%"


def _format_number(value: int | float | None) -> str:
    if value is None:
        return "-"
    return f"{int(round(float(value))):,}"


def _build_breadth_rows(conn: sqlite3.Connection, window_days: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        WITH lookback_dates AS (
            SELECT date
            FROM (
                SELECT DISTINCT date
                FROM ohlc_daily
                ORDER BY date DESC
                LIMIT ?
            )
        ),
        prepared AS (
            SELECT
                o.ticker,
                o.date,
                o.close,
                LAG(o.close) OVER (
                    PARTITION BY o.ticker
                    ORDER BY o.date
                ) AS previous_close,
                AVG(o.close) OVER (
                    PARTITION BY o.ticker
                    ORDER BY o.date
                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                ) AS ma50,
                COUNT(o.close) OVER (
                    PARTITION BY o.ticker
                    ORDER BY o.date
                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                ) AS count50,
                AVG(o.close) OVER (
                    PARTITION BY o.ticker
                    ORDER BY o.date
                    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                ) AS ma200,
                COUNT(o.close) OVER (
                    PARTITION BY o.ticker
                    ORDER BY o.date
                    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                ) AS count200
            FROM ohlc_daily o
            JOIN stocks_master s
                ON s.ticker = o.ticker
            WHERE s.market = 'US'
              AND o.close IS NOT NULL
              AND o.date IN (SELECT date FROM lookback_dates)
        ),
        recent_dates AS (
            SELECT date
            FROM (
                SELECT DISTINCT date
                FROM ohlc_daily
                ORDER BY date DESC
                LIMIT ?
            )
        )
        SELECT
            p.date,
            COUNT(*) AS traded_count,
            SUM(CASE WHEN p.previous_close IS NOT NULL AND p.close > p.previous_close THEN 1 ELSE 0 END) AS advancers,
            SUM(CASE WHEN p.previous_close IS NOT NULL AND p.close < p.previous_close THEN 1 ELSE 0 END) AS decliners,
            SUM(CASE WHEN p.previous_close IS NOT NULL AND p.close = p.previous_close THEN 1 ELSE 0 END) AS unchanged,
            SUM(CASE WHEN p.count50 >= 50 AND p.close > p.ma50 THEN 1 ELSE 0 END) AS above50,
            SUM(CASE WHEN p.count50 >= 50 THEN 1 ELSE 0 END) AS above50_eligible,
            SUM(CASE WHEN p.count200 >= 200 AND p.close > p.ma200 THEN 1 ELSE 0 END) AS above200,
            SUM(CASE WHEN p.count200 >= 200 THEN 1 ELSE 0 END) AS above200_eligible,
            AVG(
                CASE
                    WHEN p.previous_close IS NOT NULL AND p.previous_close != 0
                    THEN (p.close - p.previous_close) / p.previous_close
                END
            ) AS avg_return
        FROM prepared p
        WHERE p.date IN (SELECT date FROM recent_dates)
        GROUP BY p.date
        ORDER BY p.date ASC
        """,
        (window_days + 220, window_days),
    ).fetchall()

    ad_line = 0
    equal_weight_index = 100.0
    series: list[dict[str, Any]] = []
    for row in rows:
        advancers = int(row["advancers"] or 0)
        decliners = int(row["decliners"] or 0)
        unchanged = int(row["unchanged"] or 0)
        eligible = advancers + decliners + unchanged
        above50_eligible = int(row["above50_eligible"] or 0)
        above200_eligible = int(row["above200_eligible"] or 0)
        net_advances = advancers - decliners
        ad_line += net_advances
        avg_return = row["avg_return"]
        if avg_return is not None:
            equal_weight_index *= 1 + float(avg_return)
        series.append(
            {
                "date": row["date"],
                "traded_count": int(row["traded_count"] or 0),
                "eligible_count": eligible,
                "advancers": advancers,
                "decliners": decliners,
                "unchanged": unchanged,
                "advancers_pct": _safe_pct(advancers, eligible),
                "decliners_pct": _safe_pct(decliners, eligible),
                "above50_pct": _safe_pct(int(row["above50"] or 0), above50_eligible),
                "above200_pct": _safe_pct(int(row["above200"] or 0), above200_eligible),
                "net_advances": net_advances,
                "advance_decline_line": ad_line,
                "equal_weight_index": round(equal_weight_index, 2),
            }
        )
    return series


def _load_proxy_series(conn: sqlite3.Connection, dates: list[str]) -> dict[str, list[dict[str, Any]]]:
    if not dates or not _has_table(conn, "market_proxy_ohlc"):
        return {symbol: [] for symbol in PROXY_SYMBOLS}

    placeholders = ",".join("?" for _ in dates)
    rows = conn.execute(
        f"""
        SELECT symbol, date, close
        FROM market_proxy_ohlc
        WHERE symbol IN ({",".join("?" for _ in PROXY_SYMBOLS)})
          AND date IN ({placeholders})
          AND close IS NOT NULL
        ORDER BY symbol, date ASC
        """,
        (*PROXY_SYMBOLS.keys(), *dates),
    ).fetchall()

    grouped: dict[str, list[sqlite3.Row]] = {symbol: [] for symbol in PROXY_SYMBOLS}
    for row in rows:
        grouped.setdefault(row["symbol"], []).append(row)

    normalized: dict[str, list[dict[str, Any]]] = {}
    for symbol, symbol_rows in grouped.items():
        first_close = None
        points = []
        for row in symbol_rows:
            close = float(row["close"])
            if first_close is None:
                first_close = close
            if first_close:
                points.append(
                    {
                        "date": row["date"],
                        "close": round(close, 4),
                        "normalized": round(close / first_close * 100.0, 2),
                    }
                )
        normalized[symbol] = points
    return normalized


def _build_summary(series: list[dict[str, Any]], universe_count: int, proxy_series: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    if not series:
        return {
            "latest_date": None,
            "universe_count": universe_count,
            "traded_count": 0,
            "status": "empty",
            "message": "OHLC data is not available.",
        }

    latest = series[-1]
    advancers_pct = latest["advancers_pct"]
    above50_pct = latest["above50_pct"]
    above200_pct = latest["above200_pct"]
    if advancers_pct is None:
        tone = "neutral"
        conclusion = "資料不足"
    elif advancers_pct >= 60 and (above50_pct is None or above50_pct >= 55):
        tone = "healthy"
        conclusion = "市場廣度健康"
    elif advancers_pct <= 40 or (above200_pct is not None and above200_pct <= 40):
        tone = "weak"
        conclusion = "市場廣度轉弱"
    else:
        tone = "mixed"
        conclusion = "市場分化"

    return {
        "latest_date": latest["date"],
        "universe_count": universe_count,
        "traded_count": latest["traded_count"],
        "eligible_count": latest["eligible_count"],
        "advancers": latest["advancers"],
        "decliners": latest["decliners"],
        "advancers_pct": advancers_pct,
        "decliners_pct": latest["decliners_pct"],
        "above50_pct": above50_pct,
        "above200_pct": above200_pct,
        "advance_decline_line": latest["advance_decline_line"],
        "equal_weight_index": latest["equal_weight_index"],
        "conclusion": conclusion,
        "tone": tone,
        "proxy_available": {
            symbol: bool(points) for symbol, points in proxy_series.items()
        },
        "display": {
            "universe_count": _format_number(universe_count),
            "traded_count": _format_number(latest["traded_count"]),
            "advancers_pct": _format_pct(advancers_pct),
            "above50_pct": _format_pct(above50_pct),
            "above200_pct": _format_pct(above200_pct),
            "advance_decline_line": _format_number(latest["advance_decline_line"]),
        },
    }


def load_market_breadth(*, window_days: int = DEFAULT_WINDOW_DAYS, db_path: str | Path | None = None) -> dict[str, Any]:
    window_days = max(30, min(int(window_days or DEFAULT_WINDOW_DAYS), 260))
    conn = _connect(db_path)
    try:
        universe_count = conn.execute(
            "SELECT COUNT(*) FROM stocks_master WHERE market = 'US'"
        ).fetchone()[0]
        series = _build_breadth_rows(conn, window_days)
        dates = [item["date"] for item in series]
        proxies = _load_proxy_series(conn, dates)
    finally:
        conn.close()

    return {
        "db_path": str(db_path or DB_PATH),
        "window_days": window_days,
        "summary": _build_summary(series, int(universe_count or 0), proxies),
        "series": series,
        "proxies": {
            symbol: {
                "label": label,
                "points": proxies.get(symbol, []),
            }
            for symbol, label in PROXY_SYMBOLS.items()
        },
    }
