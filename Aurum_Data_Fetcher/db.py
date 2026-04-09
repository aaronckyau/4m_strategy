"""
db.py - 資料庫連線與操作
============================================================================
共用主站的 aurum.db，使用 WAL 模式避免鎖定衝突。
============================================================================
"""
import json
import os
import sqlite3
from datetime import datetime, timezone

from config import Config
from logger import log


def get_db() -> sqlite3.Connection:
    """取得資料庫連線（WAL + FK，與主站一致）"""
    conn = sqlite3.connect(Config.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_tables():
    """執行 schema.sql 建立所有表"""
    schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schema.sql')
    with open(schema_path, encoding='utf-8') as f:
        schema_sql = f.read()
    conn = get_db()
    try:
        conn.executescript(schema_sql)
        conn.commit()
        log.info("資料庫表已初始化")
    finally:
        conn.close()


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


# ============================================================================
# stocks_master
# ============================================================================

def upsert_stock(conn: sqlite3.Connection, data: dict):
    """新增或更新 stocks_master"""
    conn.execute("""
        INSERT INTO stocks_master (ticker, name, exchange, sector, industry,
                                   market_cap, currency, description, shares_outstanding,
                                   profile_updated_at)
        VALUES (:ticker, :name, :exchange, :sector, :industry,
                :market_cap, :currency, :description, :shares_outstanding,
                :profile_updated_at)
        ON CONFLICT(ticker) DO UPDATE SET
            name = excluded.name,
            exchange = excluded.exchange,
            sector = excluded.sector,
            industry = excluded.industry,
            market_cap = excluded.market_cap,
            currency = excluded.currency,
            description = excluded.description,
            shares_outstanding = excluded.shares_outstanding,
            profile_updated_at = excluded.profile_updated_at
    """, {
        'ticker': data['ticker'],
        'name': data.get('name'),
        'exchange': data.get('exchange'),
        'sector': data.get('sector'),
        'industry': data.get('industry'),
        'market_cap': data.get('market_cap'),
        'currency': data.get('currency'),
        'description': data.get('description'),
        'shares_outstanding': data.get('shares_outstanding'),
        'profile_updated_at': _now_iso(),
    })
    conn.commit()


def update_stock_timestamp(conn: sqlite3.Connection, ticker: str, field: str):
    """更新 stocks_master 的時間戳欄位"""
    valid_fields = {'ohlc_updated_at', 'financials_updated_at', 'metrics_updated_at'}
    if field not in valid_fields:
        raise ValueError(f"Invalid field: {field}")
    conn.execute(f"UPDATE stocks_master SET {field} = ? WHERE ticker = ?",
                 (_now_iso(), ticker))
    conn.commit()


def get_stock_timestamp(conn: sqlite3.Connection, ticker: str, field: str) -> str | None:
    """取得 stocks_master 的時間戳"""
    row = conn.execute(f"SELECT {field} FROM stocks_master WHERE ticker = ?",
                       (ticker,)).fetchone()
    return row[0] if row else None


# ============================================================================
# ohlc_daily
# ============================================================================

def get_last_ohlc_date(conn: sqlite3.Connection, ticker: str) -> str | None:
    """取得最新已存 OHLC 日期（增量用）"""
    row = conn.execute(
        "SELECT MAX(date) FROM ohlc_daily WHERE ticker = ?", (ticker,)
    ).fetchone()
    return row[0] if row and row[0] else None


def upsert_ohlc_batch(conn: sqlite3.Connection, ticker: str, rows: list[dict]):
    """批量寫入 OHLC 日線"""
    if not rows:
        return
    conn.executemany("""
        INSERT OR REPLACE INTO ohlc_daily
            (ticker, date, open, high, low, close, adj_close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (ticker, r.get('date'), r.get('open'), r.get('high'), r.get('low'),
         r.get('close'), r.get('adjClose') or r.get('close'), r.get('volume'))
        for r in rows
    ])
    conn.commit()
    update_stock_timestamp(conn, ticker, 'ohlc_updated_at')


# ============================================================================
# financial_statements
# ============================================================================

def upsert_financial_statement(conn: sqlite3.Connection, ticker: str,
                                stmt_type: str, rows: list[dict]):
    """批量寫入財務報表"""
    if not rows:
        return
    now = _now_iso()
    for r in rows:
        period = r.get('date') or r.get('period')
        if not period:
            continue
        conn.execute("""
            INSERT OR REPLACE INTO financial_statements
                (ticker, period, statement_type, fiscal_year, fiscal_quarter,
                 revenue, cost_of_revenue, gross_profit, operating_income,
                 net_income, eps, eps_diluted, ebitda, operating_expenses,
                 total_assets, total_liabilities, total_equity, total_debt,
                 cash_and_equivalents, current_assets, current_liabilities,
                 operating_cash_flow, capex, free_cash_flow, dividends_paid,
                 raw_json, fetched_at)
            VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?, ?,?,?,?,?, ?,?, ?,?,?,?, ?,?)
        """, (
            ticker, period, stmt_type,
            _int(r, 'calendarYear') or _int(r, 'fiscalYear'), _quarter(r),
            # Income
            _num(r, 'revenue'), _num(r, 'costOfRevenue'), _num(r, 'grossProfit'),
            _num(r, 'operatingIncome'), _num(r, 'netIncome'),
            _num(r, 'eps'), _num(r, 'epsDiluted') or _num(r, 'epsdiluted'), _num(r, 'ebitda'),
            _num(r, 'operatingExpenses'),
            # Balance
            _num(r, 'totalAssets'), _num(r, 'totalLiabilities'),
            _num(r, 'totalStockholdersEquity') or _num(r, 'totalEquity'), _num(r, 'totalDebt'),
            _num(r, 'cashAndCashEquivalents'),
            _num(r, 'totalCurrentAssets'), _num(r, 'totalCurrentLiabilities'),
            # Cash Flow
            _num(r, 'operatingCashFlow'), _num(r, 'capitalExpenditure') or _num(r, 'capex'),
            _num(r, 'freeCashFlow'), _num(r, 'netDividendsPaid') or _num(r, 'dividendsPaid') or _num(r, 'commonDividendsPaid'),
            # Raw
            json.dumps(r, ensure_ascii=False), now,
        ))
    conn.commit()
    update_stock_timestamp(conn, ticker, 'financials_updated_at')


# ============================================================================
# computed_metrics
# ============================================================================

def upsert_computed_metrics(conn: sqlite3.Connection, ticker: str, rows: list[dict]):
    """批量寫入計算指標"""
    if not rows:
        return
    now = _now_iso()
    for r in rows:
        conn.execute("""
            INSERT OR REPLACE INTO computed_metrics
                (ticker, period, metric_type,
                 pe_ratio, pb_ratio, ps_ratio, ev_to_ebitda,
                 gross_margin, operating_margin, net_margin,
                 roe, roa,
                 revenue_yoy, net_income_yoy, eps_yoy,
                 debt_to_equity, current_ratio, fcf_per_share,
                 computed_at)
            VALUES (?,?,?, ?,?,?,?, ?,?,?, ?,?, ?,?,?, ?,?,?, ?)
        """, (
            ticker, r['period'], r['metric_type'],
            r.get('pe_ratio'), r.get('pb_ratio'), r.get('ps_ratio'), r.get('ev_to_ebitda'),
            r.get('gross_margin'), r.get('operating_margin'), r.get('net_margin'),
            r.get('roe'), r.get('roa'),
            r.get('revenue_yoy'), r.get('net_income_yoy'), r.get('eps_yoy'),
            r.get('debt_to_equity'), r.get('current_ratio'), r.get('fcf_per_share'),
            now,
        ))
    conn.commit()
    update_stock_timestamp(conn, ticker, 'metrics_updated_at')


# ============================================================================
# Helpers
# ============================================================================

def _num(d: dict, key: str):
    """安全取數值，None / 非數值回傳 None"""
    v = d.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _int(d: dict, key: str):
    v = d.get(key)
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _quarter(d: dict) -> int | None:
    """從 FMP 的 period 欄位提取季度"""
    p = d.get('period', '')
    if p == 'Q1': return 1
    if p == 'Q2': return 2
    if p == 'Q3': return 3
    if p == 'Q4': return 4
    return None
