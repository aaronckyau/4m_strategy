from __future__ import annotations

import sqlite3

from services.market_breadth_service import load_market_breadth


def _build_db(path):
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE stocks_master (
            ticker TEXT PRIMARY KEY,
            name TEXT,
            market TEXT
        );
        CREATE TABLE ohlc_daily (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            close REAL,
            PRIMARY KEY (ticker, date)
        );
        CREATE TABLE market_proxy_ohlc (
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            close REAL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (symbol, date)
        );
        """
    )
    conn.executemany(
        "INSERT INTO stocks_master (ticker, name, market) VALUES (?, ?, 'US')",
        [("AAA", "AAA Corp"), ("BBB", "BBB Corp")],
    )
    conn.executemany(
        "INSERT INTO ohlc_daily (ticker, date, close) VALUES (?, ?, ?)",
        [
            ("AAA", "2026-01-01", 100),
            ("AAA", "2026-01-02", 110),
            ("AAA", "2026-01-03", 120),
            ("BBB", "2026-01-01", 100),
            ("BBB", "2026-01-02", 90),
            ("BBB", "2026-01-03", 100),
        ],
    )
    conn.executemany(
        "INSERT INTO market_proxy_ohlc (symbol, date, close, fetched_at) VALUES (?, ?, ?, ?)",
        [
            ("SPY", "2026-01-01", 100, "2026-01-03T00:00:00Z"),
            ("SPY", "2026-01-02", 102, "2026-01-03T00:00:00Z"),
            ("SPY", "2026-01-03", 105, "2026-01-03T00:00:00Z"),
            ("DIA", "2026-01-01", 200, "2026-01-03T00:00:00Z"),
            ("DIA", "2026-01-03", 210, "2026-01-03T00:00:00Z"),
        ],
    )
    conn.commit()
    conn.close()


def test_load_market_breadth_builds_universe_series_and_proxy_lines(tmp_path):
    db_path = tmp_path / "breadth.db"
    _build_db(db_path)

    dashboard = load_market_breadth(window_days=30, db_path=db_path)

    assert dashboard["summary"]["universe_count"] == 2
    assert dashboard["summary"]["latest_date"] == "2026-01-03"
    assert dashboard["summary"]["advancers"] == 2
    assert dashboard["summary"]["decliners"] == 0
    assert dashboard["summary"]["advancers_pct"] == 100.0
    assert dashboard["series"][-1]["advance_decline_line"] == 2
    assert dashboard["series"][-1]["equal_weight_index"] == 110.1
    assert dashboard["proxies"]["SPY"]["points"][-1]["normalized"] == 105.0
    assert dashboard["proxies"]["DIA"]["points"][-1]["normalized"] == 105.0
