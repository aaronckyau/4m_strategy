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
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'aurum.db')


def get_db() -> sqlite3.Connection:
    """取得資料庫連線，欄位可用名稱存取"""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


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
        conn.commit()
    finally:
        conn.close()
