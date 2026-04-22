"""
database.py - SQLite 資料庫管理
============================================================
資料表：
  - admin_sessions          : Admin 登入 session 管理
  - institutional_holdings  : Form 13F 機構持倉（按季度快照）
  - etf_list                : 美股 ETF 清單（volume ≥ 100k）
  - etf_holdings            : 各 ETF 的持倉清單
============================================================
"""
import os
import sqlite3
from datetime import datetime

DB_PATH = (
    os.environ.get('DATABASE_PATH')
    or os.environ.get('DATABASE_URL')
    or os.path.join(os.path.dirname(os.path.abspath(__file__)), 'aurum.db')
)

DEFAULT_DATASETS = [
    ("stock_universe", "股票名單", "FMP", "weekly", 7 * 24 * 60, 90, "high", 1, 1, 10, "維護 US 股票池與名稱。"),
    ("ohlc", "OHLC 日線", "FMP", "daily", 24 * 60, 20, "high", 1, 1, 20, "股票與 11 檔 sector ETF 的日線。"),
    ("financials", "財報", "FMP", "weekly", 7 * 24 * 60, 45, "medium", 1, 1, 30, "三大報表與相關欄位。"),
    ("ratios", "TTM 比率", "FMP", "daily", 24 * 60, 20, "high", 1, 1, 40, "TTM ratios 與估值欄位。"),
    ("etf", "Sector ETF Master", "FMP", "manual", 7 * 24 * 60, 120, "low", 1, 1, 50, "只更新 11 檔 sector ETF master；不更新 holdings，不更新 OHLC。"),
    ("13f", "13F", "FMP", "weekly", 14 * 24 * 60, 90, "low", 1, 1, 60, "機構持股季度資料。"),
]


def _ensure_db_dir() -> None:
    db_dir = os.path.dirname(os.path.abspath(DB_PATH))
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)


def get_db() -> sqlite3.Connection:
    """取得資料庫連線，欄位可用名稱存取"""
    _ensure_db_dir()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _ensure_update_log_columns(conn: sqlite3.Connection) -> None:
    existing = {
        row["name"] for row in conn.execute("PRAGMA table_info(update_log)").fetchall()
    }
    if not existing:
        return
    if "triggered_by" not in existing:
        conn.execute(
            "ALTER TABLE update_log ADD COLUMN triggered_by TEXT NOT NULL DEFAULT 'scheduler'"
        )
    if "run_group_id" not in existing:
        conn.execute("ALTER TABLE update_log ADD COLUMN run_group_id TEXT")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_update_log_job ON update_log(job_name, started_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_update_log_status ON update_log(status, started_at DESC)"
    )
    if "run_group_id" in {
        row["name"] for row in conn.execute("PRAGMA table_info(update_log)").fetchall()
    }:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_update_log_group ON update_log(run_group_id, started_at DESC)"
        )


def _ensure_update_runs_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dataset_registry (
            dataset_key TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            source_key TEXT NOT NULL,
            frequency_type TEXT NOT NULL,
            freshness_sla_minutes INTEGER NOT NULL,
            running_timeout_minutes INTEGER NOT NULL,
            criticality TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            manual_run_allowed INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0,
            notes TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS update_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_key TEXT NOT NULL,
            trigger_source TEXT NOT NULL,
            run_group_id TEXT,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            duration_seconds INTEGER,
            total_items INTEGER NOT NULL DEFAULT 0,
            success_items INTEGER NOT NULL DEFAULT 0,
            failed_items INTEGER NOT NULL DEFAULT 0,
            skipped_items INTEGER NOT NULL DEFAULT 0,
            records_written INTEGER NOT NULL DEFAULT 0,
            error_summary TEXT,
            log_path TEXT,
            pid INTEGER,
            host TEXT,
            mode TEXT,
            FOREIGN KEY(dataset_key) REFERENCES dataset_registry(dataset_key)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS update_run_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            item_key TEXT NOT NULL,
            item_type TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            records_written INTEGER NOT NULL DEFAULT 0,
            error_message TEXT,
            started_at TEXT,
            finished_at TEXT,
            FOREIGN KEY(run_id) REFERENCES update_runs(id)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_update_runs_dataset ON update_runs(dataset_key, started_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_update_runs_status ON update_runs(status, started_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_update_runs_group ON update_runs(run_group_id, started_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_update_run_items_run ON update_run_items(run_id, item_type, item_key)"
    )
    existing = {
        row["name"] for row in conn.execute("PRAGMA table_info(update_runs)").fetchall()
    }
    if "log_path" not in existing:
        conn.execute("ALTER TABLE update_runs ADD COLUMN log_path TEXT")
    if "pid" not in existing:
        conn.execute("ALTER TABLE update_runs ADD COLUMN pid INTEGER")
    if "host" not in existing:
        conn.execute("ALTER TABLE update_runs ADD COLUMN host TEXT")


def _ensure_etf_list_columns(conn: sqlite3.Connection) -> None:
    existing = {
        row["name"] for row in conn.execute("PRAGMA table_info(etf_list)").fetchall()
    }
    if not existing:
        return
    alter_statements = {
        "etf_company": "ALTER TABLE etf_list ADD COLUMN etf_company TEXT",
        "inception_date": "ALTER TABLE etf_list ADD COLUMN inception_date TEXT",
        "website": "ALTER TABLE etf_list ADD COLUMN website TEXT",
    }
    for column, sql in alter_statements.items():
        if column not in existing:
            conn.execute(sql)


def _ensure_dataset_registry(conn: sqlite3.Connection) -> None:
    _ensure_update_runs_tables(conn)
    conn.executemany(
        """
        INSERT INTO dataset_registry (
            dataset_key, label, source_key, frequency_type,
            freshness_sla_minutes, running_timeout_minutes,
            criticality, enabled, manual_run_allowed, sort_order, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(dataset_key) DO UPDATE SET
            label = excluded.label,
            source_key = excluded.source_key,
            frequency_type = excluded.frequency_type,
            freshness_sla_minutes = excluded.freshness_sla_minutes,
            running_timeout_minutes = excluded.running_timeout_minutes,
            criticality = excluded.criticality,
            enabled = excluded.enabled,
            manual_run_allowed = excluded.manual_run_allowed,
            sort_order = excluded.sort_order,
            notes = excluded.notes
        """,
        DEFAULT_DATASETS,
    )


def _backfill_update_runs_from_log(conn: sqlite3.Connection) -> None:
    has_runs = conn.execute("SELECT COUNT(*) FROM update_runs").fetchone()[0]
    if has_runs:
        return
    existing = {
        row["name"] for row in conn.execute("PRAGMA table_info(update_log)").fetchall()
    }
    if not existing:
        return
    rows = conn.execute("""
        SELECT job_name, mode, started_at, finished_at, status,
               COALESCE(records_updated, 0) AS records_updated,
               error_message,
               COALESCE(triggered_by, 'scheduler') AS triggered_by,
               run_group_id
        FROM update_log
        ORDER BY started_at ASC
    """).fetchall()
    for row in rows:
        duration_seconds = None
        if row["started_at"] and row["finished_at"]:
            start_dt = datetime.strptime(row["started_at"], '%Y-%m-%dT%H:%M:%SZ')
            end_dt = datetime.strptime(row["finished_at"], '%Y-%m-%dT%H:%M:%SZ')
            duration_seconds = max(int((end_dt - start_dt).total_seconds()), 0)
        records_written = int(row["records_updated"] or 0)
        success_items = records_written if row["status"] == "done" else 0
        failed_items = 1 if row["status"] == "failed" else 0
        total_items = success_items + failed_items
        conn.execute(
            """
            INSERT INTO update_runs (
                dataset_key, trigger_source, run_group_id, status, started_at,
                finished_at, duration_seconds, total_items, success_items,
                failed_items, skipped_items, records_written, error_summary, mode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
            """,
            (
                row["job_name"],
                row["triggered_by"],
                row["run_group_id"],
                row["status"],
                row["started_at"],
                row["finished_at"],
                duration_seconds,
                total_items,
                success_items,
                failed_items,
                records_written,
                row["error_message"],
                row["mode"],
            ),
        )


def init_db():
    """初始化資料庫，app 啟動時執行一次"""
    conn = get_db()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS admin_sessions (
                token      TEXT PRIMARY KEY,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS institutional_holdings (
                ticker          TEXT    NOT NULL,
                holder          TEXT    NOT NULL,
                date_reported   TEXT    NOT NULL,
                shares          INTEGER,
                market_value    REAL,
                change          INTEGER,
                change_pct      REAL,
                ownership_pct   REAL,
                is_new          BOOLEAN DEFAULT 0,
                is_sold_out     BOOLEAN DEFAULT 0,
                filing_date     TEXT,
                fetched_at      TEXT,
                PRIMARY KEY (ticker, holder, date_reported)
            );
            CREATE INDEX IF NOT EXISTS idx_ih_ticker
                ON institutional_holdings(ticker);
            CREATE INDEX IF NOT EXISTS idx_ih_date
                ON institutional_holdings(ticker, date_reported);

            CREATE TABLE IF NOT EXISTS etf_list (
                symbol          TEXT    PRIMARY KEY,
                name            TEXT,
                exchange        TEXT,
                asset_class     TEXT,
                aum             REAL,
                avg_volume      REAL,
                expense_ratio   REAL,
                holdings_count  INTEGER,
                etf_company     TEXT,
                inception_date  TEXT,
                website         TEXT,
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

            CREATE TABLE IF NOT EXISTS sp500_constituents (
                ticker           TEXT    PRIMARY KEY,
                company_name     TEXT,
                sector           TEXT,
                sub_sector       TEXT,
                headquarters     TEXT,
                date_first_added TEXT,
                cik              TEXT,
                founded          TEXT,
                fetched_at       TEXT    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sp500_sector
                ON sp500_constituents(sector);
            CREATE INDEX IF NOT EXISTS idx_sp500_sub_sector
                ON sp500_constituents(sub_sector);

            CREATE TABLE IF NOT EXISTS stocktwits_symbols (
                symbol                   TEXT PRIMARY KEY,
                company_name             TEXT,
                exchange                 TEXT,
                sector                   TEXT,
                sub_industry             TEXT,
                headquarters             TEXT,
                date_added               TEXT,
                cik                      TEXT,
                founded                  TEXT,
                is_sp500                 INTEGER NOT NULL DEFAULT 1,
                stocktwits_id            INTEGER,
                stocktwits_title         TEXT,
                stocktwits_exchange      TEXT,
                stocktwits_region        TEXT,
                logo_url                 TEXT,
                instrument_class         TEXT,
                watchlist_count_latest   INTEGER,
                raw_wikipedia_json       TEXT,
                raw_stocktwits_symbol_json TEXT,
                last_stream_at           TEXT,
                last_error               TEXT,
                created_at               TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at               TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_stocktwits_symbols_sp500
                ON stocktwits_symbols(is_sp500, symbol);
            CREATE INDEX IF NOT EXISTS idx_stocktwits_symbols_watchlist
                ON stocktwits_symbols(watchlist_count_latest DESC);

            CREATE TABLE IF NOT EXISTS stocktwits_daily_snapshots (
                id                     INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol                 TEXT NOT NULL,
                snapshot_date          TEXT NOT NULL,
                captured_at            TEXT NOT NULL,
                watchlist_count        INTEGER,
                message_count_fetched  INTEGER NOT NULL DEFAULT 0,
                message_count_saved    INTEGER NOT NULL DEFAULT 0,
                extra_pages_fetched    INTEGER NOT NULL DEFAULT 0,
                bullish_count          INTEGER NOT NULL DEFAULT 0,
                bearish_count          INTEGER NOT NULL DEFAULT 0,
                unlabeled_count        INTEGER NOT NULL DEFAULT 0,
                newest_message_at      TEXT,
                oldest_message_at      TEXT,
                is_intraday_complete   INTEGER NOT NULL DEFAULT 0,
                raw_symbol_json        TEXT,
                raw_cursor_json        TEXT,
                sync_run_id            INTEGER,
                UNIQUE(symbol, snapshot_date),
                FOREIGN KEY(symbol) REFERENCES stocktwits_symbols(symbol)
            );
            CREATE INDEX IF NOT EXISTS idx_stocktwits_snapshots_symbol_date
                ON stocktwits_daily_snapshots(symbol, snapshot_date DESC);

            CREATE TABLE IF NOT EXISTS stocktwits_messages (
                stocktwits_message_id INTEGER NOT NULL,
                symbol                TEXT NOT NULL,
                body                  TEXT,
                created_at            TEXT,
                captured_at           TEXT NOT NULL,
                username              TEXT,
                display_name          TEXT,
                avatar_url            TEXT,
                sentiment             TEXT,
                likes_total           INTEGER,
                source_title          TEXT,
                source_url            TEXT,
                discussion            INTEGER NOT NULL DEFAULT 0,
                raw_message_json      TEXT,
                PRIMARY KEY (stocktwits_message_id, symbol),
                FOREIGN KEY(symbol) REFERENCES stocktwits_symbols(symbol)
            );
            CREATE INDEX IF NOT EXISTS idx_stocktwits_messages_symbol_created
                ON stocktwits_messages(symbol, created_at DESC);

            CREATE TABLE IF NOT EXISTS stocktwits_sync_runs (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at         TEXT NOT NULL,
                finished_at        TEXT,
                status             TEXT NOT NULL,
                mode               TEXT NOT NULL,
                batch_index        INTEGER,
                batch_size         INTEGER,
                symbols_total      INTEGER NOT NULL DEFAULT 0,
                symbols_processed  INTEGER NOT NULL DEFAULT 0,
                symbols_succeeded  INTEGER NOT NULL DEFAULT 0,
                symbols_failed     INTEGER NOT NULL DEFAULT 0,
                notes              TEXT
            );

            CREATE TABLE IF NOT EXISTS stocktwits_sync_run_items (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                sync_run_id      INTEGER NOT NULL,
                symbol           TEXT NOT NULL,
                status           TEXT NOT NULL,
                http_status      INTEGER,
                attempts         INTEGER NOT NULL DEFAULT 0,
                message_count    INTEGER NOT NULL DEFAULT 0,
                pages_fetched    INTEGER NOT NULL DEFAULT 0,
                watchlist_count  INTEGER,
                error_message    TEXT,
                started_at       TEXT NOT NULL,
                finished_at      TEXT,
                FOREIGN KEY(sync_run_id) REFERENCES stocktwits_sync_runs(id)
            );
            CREATE INDEX IF NOT EXISTS idx_stocktwits_sync_items_run
                ON stocktwits_sync_run_items(sync_run_id, symbol);

            CREATE TABLE IF NOT EXISTS update_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name        TEXT NOT NULL,
                mode            TEXT,
                started_at      TEXT NOT NULL,
                finished_at     TEXT,
                status          TEXT NOT NULL DEFAULT 'running',
                records_updated INTEGER DEFAULT 0,
                error_message   TEXT,
                triggered_by    TEXT NOT NULL DEFAULT 'scheduler',
                run_group_id    TEXT
            );
        """)
        _ensure_update_runs_tables(conn)
        _ensure_etf_list_columns(conn)
        _ensure_dataset_registry(conn)
        _ensure_update_log_columns(conn)
        _backfill_update_runs_from_log(conn)
        conn.commit()
    finally:
        conn.close()
