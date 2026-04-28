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
        _ensure_etf_list_columns(conn)
        _ensure_update_runs_tables(conn)
        _ensure_dataset_registry(conn)
        _ensure_update_log_columns(conn)
        _backfill_update_runs_from_log(conn)
        conn.commit()
        log.info("資料庫表已初始化")
    finally:
        conn.close()


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


DEFAULT_DATASETS = [
    {
        "dataset_key": "stock_universe",
        "label": "股票名單",
        "source_key": "FMP",
        "frequency_type": "weekly",
        "freshness_sla_minutes": 7 * 24 * 60,
        "running_timeout_minutes": 90,
        "criticality": "high",
        "enabled": 1,
        "manual_run_allowed": 1,
        "sort_order": 10,
        "notes": "維護 US 股票池與名稱。",
    },
    {
        "dataset_key": "ohlc",
        "label": "OHLC 日線",
        "source_key": "FMP",
        "frequency_type": "daily",
        "freshness_sla_minutes": 24 * 60,
        "running_timeout_minutes": 20,
        "criticality": "high",
        "enabled": 1,
        "manual_run_allowed": 1,
        "sort_order": 20,
        "notes": "股票與 11 檔 sector ETF 的日線。",
    },
    {
        "dataset_key": "financials",
        "label": "財報",
        "source_key": "FMP",
        "frequency_type": "weekly",
        "freshness_sla_minutes": 7 * 24 * 60,
        "running_timeout_minutes": 45,
        "criticality": "medium",
        "enabled": 1,
        "manual_run_allowed": 1,
        "sort_order": 30,
        "notes": "三大報表與相關欄位。",
    },
    {
        "dataset_key": "ratios",
        "label": "TTM 比率",
        "source_key": "FMP",
        "frequency_type": "daily",
        "freshness_sla_minutes": 24 * 60,
        "running_timeout_minutes": 20,
        "criticality": "high",
        "enabled": 1,
        "manual_run_allowed": 1,
        "sort_order": 40,
        "notes": "TTM ratios 與估值欄位。",
    },
    {
        "dataset_key": "etf",
        "label": "Sector ETF Master",
        "source_key": "FMP",
        "frequency_type": "manual",
        "freshness_sla_minutes": 7 * 24 * 60,
        "running_timeout_minutes": 120,
        "criticality": "low",
        "enabled": 1,
        "manual_run_allowed": 1,
        "sort_order": 50,
        "notes": "只更新 11 檔 sector ETF master；不更新 holdings，不更新 OHLC。",
    },
    {
        "dataset_key": "13f",
        "label": "13F",
        "source_key": "FMP",
        "frequency_type": "weekly",
        "freshness_sla_minutes": 14 * 24 * 60,
        "running_timeout_minutes": 90,
        "criticality": "low",
        "enabled": 1,
        "manual_run_allowed": 1,
        "sort_order": 60,
        "notes": "機構持股季度資料。",
    },
    {
        "dataset_key": "insider_sec",
        "label": "Insider SEC Form 4",
        "source_key": "SEC EDGAR",
        "frequency_type": "daily",
        "freshness_sla_minutes": 24 * 60,
        "running_timeout_minutes": 120,
        "criticality": "medium",
        "enabled": 1,
        "manual_run_allowed": 1,
        "sort_order": 70,
        "notes": "SEC Form 4 / 4-A insider trades stored in data/db/insider.db.",
    },
    {
        "dataset_key": "analyst_forecast",
        "label": "Analyst Forecast",
        "source_key": "FMP",
        "frequency_type": "weekly",
        "freshness_sla_minutes": 7 * 24 * 60,
        "running_timeout_minutes": 90,
        "criticality": "medium",
        "enabled": 1,
        "manual_run_allowed": 1,
        "sort_order": 80,
        "notes": "FMP analyst price targets, consensus, historical ratings and grade events.",
    },
]


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
             expense_ratio, holdings_count, etf_company, inception_date,
             website, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        etf.get("symbol"),
        etf.get("name"),
        etf.get("exchange"),
        etf.get("assetClass") or etf.get("asset_class"),
        etf.get("assetsUnderManagement") or etf.get("aum"),
        etf.get("volume") or etf.get("avg_volume"),
        etf.get("expenseRatio") or etf.get("expense_ratio"),
        etf.get("holdingsCount") or etf.get("holdings_count"),
        etf.get("etfCompany") or etf.get("etf_company"),
        etf.get("inceptionDate") or etf.get("inception_date"),
        etf.get("website"),
        _now_iso(),
    ))
    conn.commit()


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
# analyst forecast
# ============================================================================

def _coerce_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value):
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def upsert_analyst_forecast(
    conn: sqlite3.Connection,
    ticker: str,
    *,
    price_consensus: dict | None = None,
    price_summary: dict | None = None,
    grades_consensus: dict | None = None,
    grades_historical: list[dict] | None = None,
    grade_events: list[dict] | None = None,
    cutoff_date: str | None = None,
) -> int:
    """Write FMP analyst forecast data for one ticker."""
    now = _now_iso()
    records = 0
    price_consensus = price_consensus or {}
    price_summary = price_summary or {}

    if price_consensus:
        analyst_count = None
        analyst_count_label = None
        for key, label in (
            ("lastYearCount", "last 12 months"),
            ("allTimeCount", "all time"),
            ("count", "count"),
        ):
            count = _coerce_int(price_summary.get(key))
            if count is not None:
                analyst_count = count
                analyst_count_label = label
                break

        publishers = price_summary.get("publishers")
        if isinstance(publishers, str):
            publishers_json = publishers
        else:
            publishers_json = json.dumps(publishers or [], ensure_ascii=False)

        conn.execute(
            """
            INSERT OR REPLACE INTO analyst_price_targets (
                ticker, target_high, target_low, target_avg, target_median,
                analyst_count, analyst_count_label, publishers_json,
                raw_consensus_json, raw_summary_json, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                _coerce_float(price_consensus.get("targetHigh")),
                _coerce_float(price_consensus.get("targetLow")),
                _coerce_float(price_consensus.get("targetConsensus")),
                _coerce_float(price_consensus.get("targetMedian")),
                analyst_count,
                analyst_count_label,
                publishers_json,
                json.dumps(price_consensus, ensure_ascii=False),
                json.dumps(price_summary, ensure_ascii=False),
                now,
            ),
        )
        records += 1

    if grades_consensus:
        conn.execute(
            """
            INSERT OR REPLACE INTO analyst_grades_consensus (
                ticker, consensus, strong_buy, buy, hold, sell, strong_sell,
                raw_json, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                grades_consensus.get("consensus"),
                _coerce_int(grades_consensus.get("strongBuy")),
                _coerce_int(grades_consensus.get("buy")),
                _coerce_int(grades_consensus.get("hold")),
                _coerce_int(grades_consensus.get("sell")),
                _coerce_int(grades_consensus.get("strongSell")),
                json.dumps(grades_consensus, ensure_ascii=False),
                now,
            ),
        )
        records += 1

    for row in grades_historical or []:
        date = row.get("date")
        if not date or (cutoff_date and date < cutoff_date):
            continue
        conn.execute(
            """
            INSERT OR REPLACE INTO analyst_grades_historical (
                ticker, date, strong_buy, buy, hold, sell, strong_sell,
                raw_json, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                date,
                _coerce_int(row.get("analystRatingsStrongBuy")) or 0,
                _coerce_int(row.get("analystRatingsBuy")) or 0,
                _coerce_int(row.get("analystRatingsHold")) or 0,
                _coerce_int(row.get("analystRatingsSell")) or 0,
                _coerce_int(row.get("analystRatingsStrongSell")) or 0,
                json.dumps(row, ensure_ascii=False),
                now,
            ),
        )
        records += 1

    for row in grade_events or []:
        date = row.get("date")
        grading_company = row.get("gradingCompany")
        if not date or not grading_company or (cutoff_date and date < cutoff_date):
            continue
        conn.execute(
            """
            INSERT OR REPLACE INTO analyst_grade_events (
                ticker, date, grading_company, previous_grade, new_grade,
                action, raw_json, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                date,
                grading_company,
                row.get("previousGrade"),
                row.get("newGrade"),
                row.get("action"),
                json.dumps(row, ensure_ascii=False),
                now,
            ),
        )
        records += 1

    if cutoff_date:
        conn.execute(
            "DELETE FROM analyst_grades_historical WHERE ticker = ? AND date < ?",
            (ticker, cutoff_date),
        )
        conn.execute(
            "DELETE FROM analyst_grade_events WHERE ticker = ? AND date < ?",
            (ticker, cutoff_date),
        )

    conn.commit()
    return records


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
                  "ratios_ttm", "institutional_holdings",
                  "analyst_price_targets", "analyst_grades_consensus",
                  "analyst_grades_historical", "analyst_grade_events"):
        conn.execute(f"DELETE FROM {table} WHERE ticker IN ({placeholders})", args)
    conn.execute(f"DELETE FROM stocks_master WHERE ticker IN ({placeholders})", args)
    conn.commit()
    return len(to_remove)


# ============================================================================
# update runs / dataset registry
# ============================================================================

def _ensure_update_runs_tables(conn: sqlite3.Connection):
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


def _ensure_dataset_registry(conn: sqlite3.Connection):
    _ensure_update_runs_tables(conn)
    conn.executemany(
        """
        INSERT INTO dataset_registry (
            dataset_key, label, source_key, frequency_type,
            freshness_sla_minutes, running_timeout_minutes,
            criticality, enabled, manual_run_allowed, sort_order, notes
        )
        VALUES (
            :dataset_key, :label, :source_key, :frequency_type,
            :freshness_sla_minutes, :running_timeout_minutes,
            :criticality, :enabled, :manual_run_allowed, :sort_order, :notes
        )
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


def _backfill_update_runs_from_log(conn: sqlite3.Connection):
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


def start_update_run(conn: sqlite3.Connection, dataset_key: str, mode: str | None = None,
                     trigger_source: str = "scheduler", run_group_id: str | None = None,
                     started_at: str | None = None, log_path: str | None = None,
                     pid: int | None = None, host: str | None = None) -> int:
    _ensure_update_runs_tables(conn)
    started_at = started_at or _now_iso()
    cur = conn.execute(
        """
        INSERT INTO update_runs (
            dataset_key, trigger_source, run_group_id, status, started_at,
            log_path, pid, host, mode
        ) VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?)
        """,
        (dataset_key, trigger_source, run_group_id, started_at, log_path, pid, host, mode),
    )
    conn.commit()
    return cur.lastrowid


def finish_update_run(conn: sqlite3.Connection, run_id: int, status: str,
                      total_items: int = 0, success_items: int = 0,
                      failed_items: int = 0, skipped_items: int = 0,
                      records_written: int = 0, error_summary: str | None = None):
    started = conn.execute(
        "SELECT started_at FROM update_runs WHERE id = ?", (run_id,)
    ).fetchone()
    finished_at = _now_iso()
    duration_seconds = 0
    if started and started["started_at"]:
        start_dt = datetime.strptime(started["started_at"], '%Y-%m-%dT%H:%M:%SZ')
        end_dt = datetime.strptime(finished_at, '%Y-%m-%dT%H:%M:%SZ')
        duration_seconds = max(int((end_dt - start_dt).total_seconds()), 0)
    conn.execute(
        """
        UPDATE update_runs
        SET finished_at = ?, status = ?, duration_seconds = ?,
            total_items = ?, success_items = ?, failed_items = ?,
            skipped_items = ?, records_written = ?, error_summary = ?
        WHERE id = ?
        """,
        (
            finished_at, status, duration_seconds,
            total_items, success_items, failed_items,
            skipped_items, records_written, error_summary, run_id,
        ),
    )
    conn.commit()


def replace_update_run_items(conn: sqlite3.Connection, run_id: int, items: list[dict]):
    conn.execute("DELETE FROM update_run_items WHERE run_id = ?", (run_id,))
    if not items:
        conn.commit()
        return
    conn.executemany(
        """
        INSERT INTO update_run_items (
            run_id, item_key, item_type, status, attempts, records_written,
            error_message, started_at, finished_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                run_id,
                item.get("item_key"),
                item.get("item_type", "item"),
                item.get("status", "success"),
                item.get("attempts", 1),
                item.get("records_written", 0),
                item.get("error_message"),
                item.get("started_at"),
                item.get("finished_at"),
            )
            for item in items
        ],
    )
    conn.commit()


# ============================================================================
# update_log
# ============================================================================

def _ensure_update_log_columns(conn: sqlite3.Connection):
    """為舊版資料庫補齊 update_log 欄位。"""
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


def start_update_log(conn: sqlite3.Connection, job_name: str, mode: str | None = None,
                     triggered_by: str = "scheduler", run_group_id: str | None = None,
                     started_at: str | None = None) -> int:
    """建立一筆 running 狀態的 update_log 記錄，回傳 log id"""
    _ensure_update_log_columns(conn)
    started_at = started_at or _now_iso()
    cur = conn.execute(
        """
        INSERT INTO update_log (
            job_name, mode, started_at, status, triggered_by, run_group_id
        ) VALUES (?, ?, ?, 'running', ?, ?)
        """,
        (job_name, mode, started_at, triggered_by, run_group_id),
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
