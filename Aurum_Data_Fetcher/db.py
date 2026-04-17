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
    """取得資料庫連線（WAL + FK，與主站一致）

    並發寫入保護：
    - timeout=30：Python sqlite3 層級的等待時間
    - busy_timeout=15000：SQLite 層級等待鎖釋放的時間（15 秒）
    - WAL 模式允許多個讀者 + 一個寫者共存
    """
    conn = sqlite3.connect(Config.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")  # WAL 模式下的建議值
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
    """新增或更新 stocks_master

    使用 coalesce 策略：各欄位只在 excluded 非 NULL 時才覆蓋，
    避免 generate_name.py 和 fetch_profile 互相清空對方負責的欄位。
    """
    conn.execute("""
        INSERT INTO stocks_master (
            ticker, name, name_zh_hk, name_zh_cn, market,
            exchange, sector, industry,
            market_cap, currency, description, shares_outstanding,
            profile_updated_at
        )
        VALUES (
            :ticker, :name, :name_zh_hk, :name_zh_cn, :market,
            :exchange, :sector, :industry,
            :market_cap, :currency, :description, :shares_outstanding,
            :profile_updated_at
        )
        ON CONFLICT(ticker) DO UPDATE SET
            name               = COALESCE(excluded.name,               name),
            name_zh_hk         = COALESCE(excluded.name_zh_hk,         name_zh_hk),
            name_zh_cn         = COALESCE(excluded.name_zh_cn,         name_zh_cn),
            market             = COALESCE(excluded.market,             market),
            exchange           = COALESCE(excluded.exchange,           exchange),
            sector             = COALESCE(excluded.sector,             sector),
            industry           = COALESCE(excluded.industry,           industry),
            market_cap         = COALESCE(excluded.market_cap,         market_cap),
            currency           = COALESCE(excluded.currency,           currency),
            description        = COALESCE(excluded.description,        description),
            shares_outstanding = COALESCE(excluded.shares_outstanding, shares_outstanding),
            profile_updated_at = COALESCE(excluded.profile_updated_at, profile_updated_at)
    """, {
        'ticker':             data['ticker'],
        'name':               data.get('name'),
        'name_zh_hk':         data.get('name_zh_hk'),
        'name_zh_cn':         data.get('name_zh_cn'),
        'market':             data.get('market'),
        'exchange':           data.get('exchange'),
        'sector':             data.get('sector'),
        'industry':           data.get('industry'),
        'market_cap':         data.get('market_cap'),
        'currency':           data.get('currency'),
        'description':        data.get('description'),
        'shares_outstanding': data.get('shares_outstanding'),
        'profile_updated_at': _now_iso(),
    })
    conn.commit()


def ensure_stock_exists(conn: sqlite3.Connection, ticker: str, market: str = "US",
                        exchange: str | None = None, currency: str | None = None,
                        name: str | None = None):
    """確保 stocks_master 至少存在一筆最小資料，供單 ticker 測試寫入子表。"""
    market = market or "US"
    exchange = exchange or {
        "US": "NASDAQ",
        "HK": "HKSE",
        "CN": "SHH",
        "CN_StockConnect": "SHH",
    }.get(market, "")
    currency = currency or {
        "US": "USD",
        "HK": "HKD",
        "CN": "CNY",
        "CN_StockConnect": "CNY",
    }.get(market, "USD")
    upsert_stock(conn, {
        "ticker": ticker,
        "name": name or ticker,
        "market": market,
        "exchange": exchange,
        "currency": currency,
    })


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


def upsert_ohlc_batch(conn: sqlite3.Connection, ticker: str, rows: list[dict]) -> int:
    """批量寫入 OHLC 日線，回傳寫入筆數"""
    if not rows:
        return 0
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
    return len(rows)


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
# ratios_ttm
# ============================================================================

def upsert_ratios_ttm(conn: sqlite3.Connection, ticker: str, r: dict):
    """寫入 TTM 財務比率（每支股票只保留最新一筆）"""
    def _f(*keys):
        v = None
        for key in keys:
            if key in r and r.get(key) is not None:
                v = r.get(key)
                break
        if v is None:
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    conn.execute("""
        INSERT OR REPLACE INTO ratios_ttm (
            ticker,
            pe, pb, ps, peg, forward_peg, ev, ev_multiple,
            price_to_fcf, price_to_ocf, price_to_fair_value, debt_to_market_cap,
            gross_margin, operating_margin, pretax_margin, net_margin,
            ebitda_margin, effective_tax_rate,
            net_income_per_ebt, ebt_per_ebit,
            debt_to_equity, debt_to_assets, debt_to_capital,
            long_term_debt_to_capital, financial_leverage,
            current_ratio, quick_ratio, cash_ratio, solvency_ratio,
            interest_coverage, debt_service_coverage,
            asset_turnover, fixed_asset_turnover, receivables_turnover,
            inventory_turnover, working_capital_turnover,
            eps, revenue_per_share, book_value_per_share, tangible_bv_per_share,
            cash_per_share, ocf_per_share, fcf_per_share, interest_debt_per_share,
            dividend_yield, dividend_per_share, dividend_payout_ratio,
            ocf_ratio, ocf_sales_ratio, fcf_ocf_ratio,
            capex_coverage_ratio, dividend_capex_coverage,
            raw_json, fetched_at
        ) VALUES (
            ?,
            ?,?,?,?,?,?,?,  ?,?,?,?,
            ?,?,?,?,  ?,?,
            ?,?,
            ?,?,?,  ?,?,
            ?,?,?,?,  ?,?,
            ?,?,?,  ?,?,
            ?,?,?,?,  ?,?,?,?,
            ?,?,?,
            ?,?,?,
            ?,?,
            ?,?
        )
    """, (
        ticker,
        _f("priceToEarningsRatioTTM"), _f("priceToBookRatioTTM"), _f("priceToSalesRatioTTM"),
        _f("priceToEarningsGrowthRatioTTM"), _f("forwardPriceToEarningsGrowthRatioTTM"),
        _f("enterpriseValueTTM"), _f("enterpriseValueMultipleTTM"),
        _f("priceToFreeCashFlowsRatioTTM", "priceToFreeCashFlowRatioTTM"),
        _f("priceToOperatingCashFlowsRatioTTM", "priceToOperatingCashFlowRatioTTM"),
        _f("priceToFairValueTTM"), _f("debtToMarketCapTTM"),
        _f("grossProfitMarginTTM"), _f("operatingProfitMarginTTM"), _f("pretaxProfitMarginTTM"),
        _f("netProfitMarginTTM"), _f("ebitdaMarginTTM"), _f("effectiveTaxRateTTM"),
        _f("netIncomePerEBTTTM"), _f("ebtPerEbitTTM"),
        _f("debtEquityRatioTTM", "debtToEquityRatioTTM"),
        _f("debtToAssetsTTM", "debtToAssetsRatioTTM"),
        _f("debtToCapitalRatioTTM"),
        _f("longTermDebtToCapitalizationTTM", "longTermDebtToCapitalRatioTTM"),
        _f("financialLeverageTTM", "financialLeverageRatioTTM"),
        _f("currentRatioTTM"), _f("quickRatioTTM"), _f("cashRatioTTM"),
        _f("solvencyRatioTTM"),
        _f("interestCoverageTTM", "interestCoverageRatioTTM"),
        _f("debtServiceCoverageRatioTTM"),
        _f("assetTurnoverTTM"), _f("fixedAssetTurnoverTTM"), _f("receivablesTurnoverTTM"),
        _f("inventoryTurnoverTTM"), _f("workingCapitalTurnoverRatioTTM", "operatingCycleTTM"),
        _f("epsTTM", "netIncomePerShareTTM"), _f("revenuePerShareTTM"), _f("bookValuePerShareTTM"),
        _f("tangibleBookValuePerShareTTM"), _f("cashPerShareTTM"),
        _f("operatingCashFlowPerShareTTM"), _f("freeCashFlowPerShareTTM"),
        _f("interestDebtPerShareTTM"),
        _f("dividendYieldTTM"), _f("dividendPerShareTTM"),
        _f("payoutRatioTTM", "dividendPayoutRatioTTM"),
        _f("operatingCashFlowRatioTTM"), _f("operatingCashFlowSalesRatioTTM"),
        _f("freeCashFlowOperatingCashFlowRatioTTM"),
        _f("capitalExpenditureCoverageRatioTTM"), _f("dividendPaidAndCapexCoverageRatioTTM"),
        json.dumps(r, ensure_ascii=False), _now_iso(),
    ))
    conn.commit()


# ============================================================================
# etf_list / etf_holdings
# ============================================================================

def upsert_etf(conn: sqlite3.Connection, etf: dict):
    """寫入 ETF 基本資料"""
    conn.execute("""
        INSERT OR REPLACE INTO etf_list
            (symbol, name, exchange, asset_class, aum, avg_volume,
             expense_ratio, holdings_count, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        etf.get("symbol"),
        etf.get("name"),
        etf.get("exchange"),
        etf.get("assetClass") or etf.get("asset_class"),
        etf.get("aum"),
        etf.get("volume") or etf.get("avg_volume"),
        etf.get("expenseRatio") or etf.get("expense_ratio"),
        etf.get("holdingsCount") or etf.get("holdings_count"),
        _now_iso(),
    ))
    conn.commit()


def upsert_etf_holdings(conn: sqlite3.Connection, etf_symbol: str, holdings: list[dict]):
    """全量替換某 ETF 的持倉清單"""
    now = _now_iso()
    conn.execute("DELETE FROM etf_holdings WHERE etf_symbol = ?", (etf_symbol,))
    for h in holdings:
        asset = h.get("asset") or h.get("symbol") or ""
        if not asset:
            continue
        conn.execute("""
            INSERT OR REPLACE INTO etf_holdings
                (etf_symbol, asset, name, weight_pct, shares, market_value,
                 updated_at, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            etf_symbol, asset, h.get("name"),
            h.get("weightPercentage") or h.get("weight_pct"),
            h.get("sharesNumber") or h.get("shares"),
            h.get("marketValue") or h.get("market_value"),
            h.get("updated") or h.get("updatedAt"),
            now,
        ))
    conn.commit()


# ============================================================================
# institutional_holdings
# ============================================================================

def upsert_institutional_holdings(conn: sqlite3.Connection, ticker: str, holdings: list[dict]):
    """寫入機構持倉（Form 13F）"""
    now = _now_iso()
    for h in holdings:
        conn.execute("""
            INSERT OR REPLACE INTO institutional_holdings (
                ticker, holder, date_reported, shares, market_value,
                change, change_pct, ownership_pct,
                is_new, is_sold_out, filing_date, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ticker,
            h.get("investorName", "Unknown"),
            h.get("date", ""),
            h.get("sharesNumber"),
            h.get("marketValue"),
            h.get("changeInSharesNumber"),
            h.get("changeInSharesNumberPercentage"),
            h.get("ownership"),
            1 if h.get("isNew") else 0,
            1 if h.get("isSoldOut") else 0,
            h.get("filingDate", ""),
            now,
        ))
    conn.commit()


def cleanup_old_institutional_quarters(conn: sqlite3.Connection, ticker: str, keep_quarters: int = 4):
    """清除舊季度的機構持倉，只保留最近 N 季"""
    quarters = conn.execute(
        "SELECT DISTINCT date_reported FROM institutional_holdings "
        "WHERE ticker = ? ORDER BY date_reported DESC",
        (ticker,)
    ).fetchall()
    if len(quarters) > keep_quarters:
        cutoff = quarters[keep_quarters - 1][0]
        conn.execute(
            "DELETE FROM institutional_holdings WHERE ticker = ? AND date_reported < ?",
            (ticker, cutoff)
        )
        conn.commit()


# ============================================================================
# sector_industry_i18n
# ============================================================================

def upsert_sector_i18n(conn: sqlite3.Connection, data: dict):
    """批量寫入行業中文翻譯 JSON（{sectors:{key:{zh_hk,zh_cn}}, industries:{...}}）"""
    count = 0
    for category in ("sectors", "industries"):
        type_val = "sector" if category == "sectors" else "industry"
        for key, val in data.get(category, {}).items():
            conn.execute("""
                INSERT INTO sector_industry_i18n (key, type, zh_hk, zh_cn)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    type  = excluded.type,
                    zh_hk = excluded.zh_hk,
                    zh_cn = excluded.zh_cn
            """, (key, type_val, val.get("zh_hk", ""), val.get("zh_cn", "")))
            count += 1
    conn.commit()
    return count


# ============================================================================
# stocks_master — 下市清除
# ============================================================================

def remove_delisted(conn: sqlite3.Connection, active_tickers: set[str]) -> int:
    """刪除 stocks_master 中不在 active_tickers 的股票，並同步清除子表資料。
    回傳刪除筆數。"""
    if not active_tickers:
        return 0
    db_tickers = {r[0] for r in conn.execute("SELECT ticker FROM stocks_master").fetchall()}
    to_remove = db_tickers - active_tickers
    if not to_remove:
        return 0
    placeholders = ",".join("?" * len(to_remove))
    args = list(to_remove)
    # 先刪子表（FK ON），再刪主表
    for table in ("ohlc_daily", "financial_statements", "computed_metrics",
                  "ratios_ttm", "institutional_holdings"):
        conn.execute(f"DELETE FROM {table} WHERE ticker IN ({placeholders})", args)
    conn.execute(f"DELETE FROM stocks_master WHERE ticker IN ({placeholders})", args)
    conn.commit()
    return len(to_remove)


# ============================================================================
# update_log
# ============================================================================

def start_update_log(conn: sqlite3.Connection, job_name: str, mode: str | None = None) -> int:
    """建立一筆 running 狀態的 update_log 記錄，回傳 log id"""
    cur = conn.execute(
        "INSERT INTO update_log (job_name, mode, started_at, status) VALUES (?, ?, ?, 'running')",
        (job_name, mode, _now_iso()),
    )
    conn.commit()
    return cur.lastrowid


def finish_update_log(conn: sqlite3.Connection, log_id: int,
                      status: str, records: int = 0, error: str | None = None):
    """更新 update_log 為 done / failed"""
    conn.execute(
        """UPDATE update_log
           SET finished_at = ?, status = ?, records_updated = ?, error_message = ?
           WHERE id = ?""",
        (_now_iso(), status, records, error, log_id),
    )
    conn.commit()


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
