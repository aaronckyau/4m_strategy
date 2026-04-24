-- ============================================================================
-- Aurum Data Fetcher — DB Schema
-- 寫入 aurum.db，與主站共用
-- ============================================================================

-- 股票主表
CREATE TABLE IF NOT EXISTS stocks_master (
    ticker                TEXT PRIMARY KEY,
    name                  TEXT,
    name_zh_hk            TEXT,
    name_zh_cn            TEXT,
    market                TEXT,
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
CREATE INDEX IF NOT EXISTS idx_fs_unique_statement_period
ON financial_statements(
    COALESCE(ticker, ''),
    COALESCE(statement_type, ''),
    COALESCE(period, ''),
    COALESCE(fiscal_quarter, -1)
);

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

-- 行業中文翻譯
CREATE TABLE IF NOT EXISTS sector_industry_i18n (
    key    TEXT PRIMARY KEY,
    type   TEXT NOT NULL,   -- 'sector' | 'industry'
    zh_hk  TEXT,
    zh_cn  TEXT
);

-- ETF 清單
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

-- ETF 持倉
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

-- Form 13F 機構持倉
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

-- 資料集定義（更新頻率 / SLA / 重要性）
-- FMP analyst forecast and rating data
CREATE TABLE IF NOT EXISTS analyst_price_targets (
    ticker              TEXT PRIMARY KEY,
    target_high         REAL,
    target_low          REAL,
    target_avg          REAL,
    target_median       REAL,
    analyst_count       INTEGER,
    analyst_count_label TEXT,
    publishers_json     TEXT,
    raw_consensus_json  TEXT,
    raw_summary_json    TEXT,
    fetched_at          TEXT NOT NULL,
    FOREIGN KEY (ticker) REFERENCES stocks_master(ticker)
);
CREATE INDEX IF NOT EXISTS idx_apt_fetched
    ON analyst_price_targets(fetched_at DESC);

CREATE TABLE IF NOT EXISTS analyst_grades_consensus (
    ticker          TEXT PRIMARY KEY,
    consensus       TEXT,
    strong_buy      INTEGER,
    buy             INTEGER,
    hold            INTEGER,
    sell            INTEGER,
    strong_sell     INTEGER,
    raw_json        TEXT,
    fetched_at      TEXT NOT NULL,
    FOREIGN KEY (ticker) REFERENCES stocks_master(ticker)
);
CREATE INDEX IF NOT EXISTS idx_agc_fetched
    ON analyst_grades_consensus(fetched_at DESC);

CREATE TABLE IF NOT EXISTS analyst_grades_historical (
    ticker          TEXT NOT NULL,
    date            TEXT NOT NULL,
    strong_buy      INTEGER,
    buy             INTEGER,
    hold            INTEGER,
    sell            INTEGER,
    strong_sell     INTEGER,
    raw_json        TEXT,
    fetched_at      TEXT NOT NULL,
    PRIMARY KEY (ticker, date),
    FOREIGN KEY (ticker) REFERENCES stocks_master(ticker)
);
CREATE INDEX IF NOT EXISTS idx_agh_ticker_date
    ON analyst_grades_historical(ticker, date DESC);

CREATE TABLE IF NOT EXISTS analyst_grade_events (
    ticker          TEXT NOT NULL,
    date            TEXT NOT NULL,
    grading_company TEXT NOT NULL,
    previous_grade  TEXT,
    new_grade       TEXT,
    action          TEXT,
    raw_json        TEXT,
    fetched_at      TEXT NOT NULL,
    PRIMARY KEY (ticker, date, grading_company, previous_grade, new_grade, action),
    FOREIGN KEY (ticker) REFERENCES stocks_master(ticker)
);
CREATE INDEX IF NOT EXISTS idx_age_ticker_date
    ON analyst_grade_events(ticker, date DESC);

CREATE TABLE IF NOT EXISTS dataset_registry (
    dataset_key              TEXT PRIMARY KEY,
    label                    TEXT NOT NULL,
    source_key               TEXT NOT NULL,
    frequency_type           TEXT NOT NULL,
    freshness_sla_minutes    INTEGER NOT NULL,
    running_timeout_minutes  INTEGER NOT NULL,
    criticality              TEXT NOT NULL,
    enabled                  INTEGER NOT NULL DEFAULT 1,
    manual_run_allowed       INTEGER NOT NULL DEFAULT 1,
    sort_order               INTEGER NOT NULL DEFAULT 0,
    notes                    TEXT
);
CREATE INDEX IF NOT EXISTS idx_dataset_registry_enabled
    ON dataset_registry(enabled, sort_order);

-- 新版更新執行紀錄
CREATE TABLE IF NOT EXISTS update_runs (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_key       TEXT NOT NULL,
    trigger_source    TEXT NOT NULL,
    run_group_id      TEXT,
    status            TEXT NOT NULL,
    started_at        TEXT NOT NULL,
    finished_at       TEXT,
    duration_seconds  INTEGER,
    total_items       INTEGER DEFAULT 0,
    success_items     INTEGER DEFAULT 0,
    failed_items      INTEGER DEFAULT 0,
    skipped_items     INTEGER DEFAULT 0,
    records_written   INTEGER DEFAULT 0,
    error_summary     TEXT,
    log_path          TEXT,
    pid               INTEGER,
    host              TEXT,
    mode              TEXT,
    FOREIGN KEY (dataset_key) REFERENCES dataset_registry(dataset_key)
);
CREATE INDEX IF NOT EXISTS idx_update_runs_dataset
    ON update_runs(dataset_key, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_update_runs_status
    ON update_runs(status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_update_runs_group
    ON update_runs(run_group_id, started_at DESC);

-- 可選的 item-level 執行明細
CREATE TABLE IF NOT EXISTS update_run_items (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id           INTEGER NOT NULL,
    item_key         TEXT NOT NULL,
    item_type        TEXT NOT NULL,
    status           TEXT NOT NULL,
    attempts         INTEGER DEFAULT 0,
    records_written  INTEGER DEFAULT 0,
    error_message    TEXT,
    started_at       TEXT,
    finished_at      TEXT,
    FOREIGN KEY (run_id) REFERENCES update_runs(id)
);
CREATE INDEX IF NOT EXISTS idx_update_run_items_run
    ON update_run_items(run_id, status);

-- 更新任務日誌
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
CREATE INDEX IF NOT EXISTS idx_update_log_job ON update_log(job_name, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_update_log_status ON update_log(status, started_at DESC);
