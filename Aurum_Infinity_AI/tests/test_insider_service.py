from __future__ import annotations

import sqlite3

from services import insider_service
from services.insider_service import load_dashboard


def create_insider_db(path):
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE insider_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            filling_date TEXT,
            transaction_date TEXT,
            reporting_name TEXT,
            type_of_owner TEXT,
            acquisition_or_disposition TEXT,
            form_type TEXT,
            securities_transacted REAL,
            price REAL,
            securities_owned REAL,
            company_name TEXT,
            transaction_type TEXT,
            link TEXT,
            securities_transacted_value REAL
        )
        """
    )
    return conn


def create_sec_table(conn):
    conn.execute(
        """
        CREATE TABLE sec_stage_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            accession_no TEXT,
            source_record_id TEXT,
            source TEXT,
            source_priority INTEGER,
            symbol TEXT NOT NULL,
            issuer_cik TEXT,
            company_name TEXT,
            filling_date TEXT,
            transaction_date TEXT,
            reporting_name TEXT,
            reporting_owner_cik TEXT,
            type_of_owner TEXT,
            acquisition_or_disposition TEXT,
            form_type TEXT,
            transaction_code TEXT,
            transaction_type TEXT,
            securities_transacted REAL,
            price REAL,
            securities_transacted_value REAL,
            securities_owned REAL,
            shares_following_transaction REAL,
            sec_index_url TEXT,
            sec_xml_url TEXT,
            raw_payload_json TEXT,
            created_at TEXT
        )
        """
    )


def insert_trade(
    conn,
    *,
    symbol,
    tx_date,
    insider,
    role,
    side,
    transaction_type,
    shares,
    price,
    company="Example Co",
):
    conn.execute(
        """
        INSERT INTO insider_trades (
            symbol, filling_date, transaction_date, reporting_name, type_of_owner,
            acquisition_or_disposition, form_type, securities_transacted, price,
            securities_owned, company_name, transaction_type, link, securities_transacted_value
        ) VALUES (?, ?, ?, ?, ?, ?, '4', ?, ?, 0, ?, ?, ?, ?)
        """,
        (
            symbol,
            tx_date,
            tx_date,
            insider,
            role,
            side,
            shares,
            price,
            company,
            transaction_type,
            f"https://sec.example/{symbol}",
            shares * price,
        ),
    )


def insert_sec_trade(
    conn,
    *,
    symbol,
    tx_date,
    insider,
    role,
    side,
    transaction_type,
    shares,
    price,
    company="Example Co",
):
    conn.execute(
        """
        INSERT INTO sec_stage_trades (
            symbol, company_name, filling_date, transaction_date, reporting_name, type_of_owner,
            acquisition_or_disposition, form_type, transaction_type, securities_transacted, price,
            securities_transacted_value, sec_index_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, '4', ?, ?, ?, ?, ?)
        """,
        (
            symbol,
            company,
            tx_date,
            tx_date,
            insider,
            role,
            side,
            transaction_type,
            shares,
            price,
            shares * price,
            f"https://sec.example/{symbol}",
        ),
    )


def test_load_dashboard_returns_ranked_buy_signals(tmp_path):
    db_path = tmp_path / "insider.db"
    conn = create_insider_db(db_path)
    create_sec_table(conn)
    insert_sec_trade(
        conn,
        symbol="AAPL",
        tx_date="2026-04-20",
        insider="CEO One",
        role="CEO, director",
        side="A",
        transaction_type="P-Purchase",
        shares=10_000,
        price=100,
        company="Apple",
    )
    insert_sec_trade(
        conn,
        symbol="AAPL",
        tx_date="2026-04-19",
        insider="CFO Two",
        role="CFO",
        side="A",
        transaction_type="P-Purchase",
        shares=5_000,
        price=100,
        company="Apple",
    )
    insert_sec_trade(
        conn,
        symbol="MSFT",
        tx_date="2026-04-20",
        insider="Director Three",
        role="director",
        side="A",
        transaction_type="P-Purchase",
        shares=1_000,
        price=50,
        company="Microsoft",
    )
    insert_trade(
        conn,
        symbol="FMP1",
        tx_date="2026-04-20",
        insider="Ignore FMP",
        role="CEO",
        side="A",
        transaction_type="P-Purchase",
        shares=10_000,
        price=100,
        company="Ignored FMP",
    )
    insert_trade(
        conn,
        symbol="FMP2",
        tx_date="2026-04-20",
        insider="Ignore FMP 2",
        role="director",
        side="A",
        transaction_type="P-Purchase",
        shares=1_000,
        price=50,
        company="Microsoft",
    )
    conn.commit()
    conn.close()

    dashboard = load_dashboard(window_days=30, min_value=100_000, db_path=db_path)

    assert dashboard["status"] == "ok"
    assert dashboard["latest_transaction_date"] == "2026-04-20"
    assert dashboard["signals"][0]["symbol"] == "AAPL"
    assert dashboard["signals"][0]["buy_insider_count"] == 2
    assert dashboard["signals"][0]["officer_involved"] is True
    assert dashboard["stats"]["buy_count"] == 3
    assert dashboard["source"]["selected_label"] == "sec"


def test_load_dashboard_handles_missing_db(tmp_path):
    dashboard = load_dashboard(db_path=tmp_path / "missing.db")

    assert dashboard["status"] == "missing_db"
    assert dashboard["signals"] == []


def test_load_dashboard_finds_project_data_db_by_default(tmp_path, monkeypatch):
    app_dir = tmp_path / "Aurum_Infinity_AI"
    data_db_dir = tmp_path / "data" / "db"
    app_dir.mkdir()
    data_db_dir.mkdir(parents=True)
    db_path = data_db_dir / "insider.db"
    conn = create_insider_db(db_path)
    create_sec_table(conn)
    insert_sec_trade(
        conn,
        symbol="LW",
        tx_date="2026-04-17",
        insider="Director One",
        role="director",
        side="A",
        transaction_type="P-Purchase",
        shares=2_000,
        price=100,
        company="Lamb Weston",
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(insider_service, "_app_dir", lambda: app_dir)
    monkeypatch.setattr(insider_service, "APP_DB_PATH", str(app_dir / "aurum.db"))
    monkeypatch.delenv("INSIDER_DB_PATH", raising=False)

    dashboard = load_dashboard(window_days=30, min_value=100_000)

    assert dashboard["status"] == "ok"
    assert dashboard["source"]["path"] == str(db_path.resolve())
    assert dashboard["source"]["selected_label"] == "sec"
    assert dashboard["signals"][0]["symbol"] == "LW"


def test_load_dashboard_ignores_fmp_and_uses_sec_only(tmp_path):
    db_path = tmp_path / "insider.db"
    conn = create_insider_db(db_path)
    create_sec_table(conn)
    insert_trade(
        conn,
        symbol="AAPL",
        tx_date="2026-04-20",
        insider="CEO One",
        role="CEO",
        side="A",
        transaction_type="P-Purchase",
        shares=1_000,
        price=100,
        company="Apple",
    )
    insert_sec_trade(
        conn,
        symbol="MSFT",
        tx_date="2026-04-18",
        insider="CEO Two",
        role="CEO",
        side="A",
        transaction_type="P-Purchase",
        shares=2_000,
        price=120,
        company="Microsoft",
    )
    insert_sec_trade(
        conn,
        symbol="NVDA",
        tx_date="2026-04-17",
        insider="Director Three",
        role="director",
        side="A",
        transaction_type="P-Purchase",
        shares=1_500,
        price=110,
        company="NVIDIA",
    )
    conn.commit()
    conn.close()

    dashboard = load_dashboard(window_days=30, min_value=100_000, db_path=db_path)

    assert dashboard["source"]["selected_label"] == "sec"
    assert dashboard["source"]["sec_count"] == 2
    assert dashboard["signals"][0]["symbol"] == "MSFT"


def test_load_dashboard_returns_missing_table_when_only_fmp_exists(tmp_path):
    db_path = tmp_path / "insider.db"
    conn = create_insider_db(db_path)
    insert_trade(
        conn,
        symbol="AAPL",
        tx_date="2026-04-20",
        insider="CEO One",
        role="CEO",
        side="A",
        transaction_type="P-Purchase",
        shares=1_000,
        price=100,
        company="Apple",
    )
    conn.commit()
    conn.close()

    dashboard = load_dashboard(window_days=30, min_value=100_000, db_path=db_path)

    assert dashboard["status"] == "missing_table"
    assert dashboard["signals"] == []
