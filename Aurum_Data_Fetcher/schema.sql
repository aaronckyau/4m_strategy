-- ============================================================================
-- Aurum Data Fetcher — DB Schema
-- 寫入 aurum.db，與主站共用
-- ============================================================================

-- 股票主表
CREATE TABLE IF NOT EXISTS stocks_master (
    ticker                TEXT PRIMARY KEY,
    name                  TEXT,
    exchange              TEXT,
    sector                TEXT,
    industry              TEXT,
    market_cap            REAL,
    currency              TEXT,
    description           TEXT,
    shares_outstanding    REAL,
    profile_updated_at    TEXT,
    ohlc_updated_at       TEXT,
    financials_updated_at TEXT,
    metrics_updated_at    TEXT
);

-- 日線 OHLC
CREATE TABLE IF NOT EXISTS ohlc_daily (
    ticker    TEXT    NOT NULL,
    date      TEXT    NOT NULL,
    open      REAL,
    high      REAL,
    low       REAL,
    close     REAL,
    adj_close REAL,
    volume    INTEGER,
    PRIMARY KEY (ticker, date),
    FOREIGN KEY (ticker) REFERENCES stocks_master(ticker)
);
CREATE INDEX IF NOT EXISTS idx_ohlc_ticker_date ON ohlc_daily(ticker, date DESC);

-- 財務報表（三表合一，用 statement_type 區分）
CREATE TABLE IF NOT EXISTS financial_statements (
    ticker             TEXT NOT NULL,
    period             TEXT NOT NULL,
    statement_type     TEXT NOT NULL,
    fiscal_year        INTEGER,
    fiscal_quarter     INTEGER,
    -- Income Statement
    revenue            REAL,
    cost_of_revenue    REAL,
    gross_profit       REAL,
    operating_income   REAL,
    net_income         REAL,
    eps                REAL,
    eps_diluted        REAL,
    ebitda             REAL,
    operating_expenses REAL,
    -- Balance Sheet
    total_assets         REAL,
    total_liabilities    REAL,
    total_equity         REAL,
    total_debt           REAL,
    cash_and_equivalents REAL,
    current_assets       REAL,
    current_liabilities  REAL,
    -- Cash Flow
    operating_cash_flow REAL,
    capex               REAL,
    free_cash_flow      REAL,
    dividends_paid      REAL,
    --
    raw_json   TEXT,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (ticker, period, statement_type),
    FOREIGN KEY (ticker) REFERENCES stocks_master(ticker)
);
CREATE INDEX IF NOT EXISTS idx_fs_ticker ON financial_statements(ticker, statement_type, period DESC);

-- TTM 財務比率（FMP ratios-ttm，每支股票只存最新一筆）
CREATE TABLE IF NOT EXISTS ratios_ttm (
    ticker                  TEXT PRIMARY KEY,
    -- Valuation
    pe                      REAL,
    pb                      REAL,
    ps                      REAL,
    peg                     REAL,
    forward_peg             REAL,
    ev                      REAL,
    ev_multiple             REAL,
    price_to_fcf            REAL,
    price_to_ocf            REAL,
    price_to_fair_value     REAL,
    debt_to_market_cap      REAL,
    -- Profitability
    gross_margin            REAL,
    operating_margin        REAL,
    pretax_margin           REAL,
    net_margin              REAL,
    ebitda_margin           REAL,
    effective_tax_rate      REAL,
    -- Returns
    net_income_per_ebt      REAL,
    ebt_per_ebit            REAL,
    -- Leverage / Liquidity
    debt_to_equity          REAL,
    debt_to_assets          REAL,
    debt_to_capital         REAL,
    long_term_debt_to_capital REAL,
    financial_leverage      REAL,
    current_ratio           REAL,
    quick_ratio             REAL,
    cash_ratio              REAL,
    solvency_ratio          REAL,
    interest_coverage       REAL,
    debt_service_coverage   REAL,
    -- Efficiency
    asset_turnover          REAL,
    fixed_asset_turnover    REAL,
    receivables_turnover    REAL,
    inventory_turnover      REAL,
    working_capital_turnover REAL,
    -- Per Share
    eps                     REAL,
    revenue_per_share       REAL,
    book_value_per_share    REAL,
    tangible_bv_per_share   REAL,
    cash_per_share          REAL,
    ocf_per_share           REAL,
    fcf_per_share           REAL,
    interest_debt_per_share REAL,
    -- Dividend
    dividend_yield          REAL,
    dividend_per_share      REAL,
    dividend_payout_ratio   REAL,
    -- Cash Flow
    ocf_ratio               REAL,
    ocf_sales_ratio         REAL,
    fcf_ocf_ratio           REAL,
    capex_coverage_ratio    REAL,
    dividend_capex_coverage REAL,
    --
    raw_json                TEXT,
    fetched_at              TEXT NOT NULL,
    FOREIGN KEY (ticker) REFERENCES stocks_master(ticker)
);

-- 計算指標
CREATE TABLE IF NOT EXISTS computed_metrics (
    ticker           TEXT NOT NULL,
    period           TEXT NOT NULL,
    metric_type      TEXT NOT NULL,
    -- Valuation
    pe_ratio         REAL,
    pb_ratio         REAL,
    ps_ratio         REAL,
    ev_to_ebitda     REAL,
    -- Profitability
    gross_margin     REAL,
    operating_margin REAL,
    net_margin       REAL,
    roe              REAL,
    roa              REAL,
    -- Growth (YoY)
    revenue_yoy      REAL,
    net_income_yoy   REAL,
    eps_yoy          REAL,
    -- Leverage / Liquidity
    debt_to_equity   REAL,
    current_ratio    REAL,
    -- Cash
    fcf_per_share    REAL,
    --
    computed_at TEXT NOT NULL,
    PRIMARY KEY (ticker, period, metric_type),
    FOREIGN KEY (ticker) REFERENCES stocks_master(ticker)
);
