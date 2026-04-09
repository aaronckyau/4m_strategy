"""
compute.py - 衍生指標計算
============================================================================
從 financial_statements + ohlc_daily 讀取原始數據，
計算 PE、PB、ROE、YoY 等指標，寫入 computed_metrics。
============================================================================
"""
from ticker import resolve_ticker
from db import get_db, upsert_computed_metrics
from logger import log


def compute_metrics(ticker: str):
    """計算並儲存衍生指標"""
    ticker = resolve_ticker(ticker)
    conn = get_db()
    try:
        # 取 stocks_master 的 shares_outstanding
        stock_row = conn.execute(
            "SELECT shares_outstanding FROM stocks_master WHERE ticker = ?", (ticker,)
        ).fetchone()
        shares = stock_row['shares_outstanding'] if stock_row and stock_row['shares_outstanding'] else None

        # 取所有季度損益表（按日期降序）
        income_rows = conn.execute("""
            SELECT * FROM financial_statements
            WHERE ticker = ? AND statement_type = 'income'
            ORDER BY period DESC
        """, (ticker,)).fetchall()

        # 取所有季度資產負債表
        balance_rows = conn.execute("""
            SELECT * FROM financial_statements
            WHERE ticker = ? AND statement_type = 'balance'
            ORDER BY period DESC
        """, (ticker,)).fetchall()

        # 取所有季度現金流量表
        cashflow_rows = conn.execute("""
            SELECT * FROM financial_statements
            WHERE ticker = ? AND statement_type = 'cashflow'
            ORDER BY period DESC
        """, (ticker,)).fetchall()

        if not income_rows:
            log.warning(f"[Compute] {ticker} — 無損益表資料，跳過")
            return False

        # 建立 period → row 的對照表
        balance_map = {r['period']: r for r in balance_rows}
        cashflow_map = {r['period']: r for r in cashflow_rows}

        metrics_list = []

        for i, inc in enumerate(income_rows):
            period = inc['period']
            bal = balance_map.get(period)
            cf = cashflow_map.get(period)

            m = {
                'period': period,
                'metric_type': 'quarterly',
            }

            # ── Margins ──
            revenue = inc['revenue']
            if revenue and revenue != 0:
                m['gross_margin'] = _pct(inc['gross_profit'], revenue)
                m['operating_margin'] = _pct(inc['operating_income'], revenue)
                m['net_margin'] = _pct(inc['net_income'], revenue)

            # ── ROE / ROA ──
            if bal:
                equity = bal['total_equity']
                assets = bal['total_assets']
                if equity and equity != 0 and inc['net_income'] is not None:
                    m['roe'] = _pct(inc['net_income'], equity)
                if assets and assets != 0 and inc['net_income'] is not None:
                    m['roa'] = _pct(inc['net_income'], assets)

                # ── Leverage ──
                if equity and equity != 0 and bal['total_debt'] is not None:
                    m['debt_to_equity'] = round(bal['total_debt'] / equity, 4)
                if bal['current_liabilities'] and bal['current_liabilities'] != 0:
                    m['current_ratio'] = round(
                        (bal['current_assets'] or 0) / bal['current_liabilities'], 4
                    )

            # ── FCF per share ──
            if cf and shares and shares > 0:
                fcf = cf['free_cash_flow']
                if fcf is not None:
                    m['fcf_per_share'] = round(fcf / shares, 4)

            # ── YoY Growth ──
            # 找同一季去年的數據（向後 4 個季度）
            yoy_inc = _find_yoy(income_rows, i)
            if yoy_inc:
                m['revenue_yoy'] = _growth(inc['revenue'], yoy_inc['revenue'])
                m['net_income_yoy'] = _growth(inc['net_income'], yoy_inc['net_income'])
                m['eps_yoy'] = _growth(inc['eps_diluted'], yoy_inc['eps_diluted'])

            # ── Valuation（用該期結束日的收盤價） ──
            close_price = _get_close_price(conn, ticker, period)
            if close_price and close_price > 0:
                # TTM EPS
                ttm_eps = _ttm_sum(income_rows, i, 'eps_diluted')
                if ttm_eps and ttm_eps != 0:
                    m['pe_ratio'] = round(close_price / ttm_eps, 2)

                # P/S（TTM revenue per share）
                ttm_revenue = _ttm_sum(income_rows, i, 'revenue')
                if ttm_revenue and shares and shares > 0:
                    rev_per_share = ttm_revenue / shares
                    if rev_per_share != 0:
                        m['ps_ratio'] = round(close_price / rev_per_share, 2)

                # P/B
                if bal and bal['total_equity'] and shares and shares > 0:
                    bv_per_share = bal['total_equity'] / shares
                    if bv_per_share != 0:
                        m['pb_ratio'] = round(close_price / bv_per_share, 2)

                # EV/EBITDA
                ttm_ebitda = _ttm_sum(income_rows, i, 'ebitda')
                if ttm_ebitda and ttm_ebitda != 0 and bal:
                    market_cap = close_price * shares if shares else None
                    if market_cap:
                        total_debt = bal['total_debt'] or 0
                        cash = bal['cash_and_equivalents'] or 0
                        ev = market_cap + total_debt - cash
                        m['ev_to_ebitda'] = round(ev / ttm_ebitda, 2)

            metrics_list.append(m)

        upsert_computed_metrics(conn, ticker, metrics_list)
        log.info(f"[Compute] {ticker} — 已計算 {len(metrics_list)} 期指標")
        return True

    finally:
        conn.close()


# ============================================================================
# Helpers
# ============================================================================

def _pct(numerator, denominator) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return round(numerator / denominator * 100, 2)


def _growth(current, previous) -> float | None:
    if current is None or previous is None or previous == 0:
        return None
    return round((current - previous) / abs(previous) * 100, 2)


def _find_yoy(rows: list, current_idx: int):
    """在季報列表中找到同一季度去年的數據"""
    if current_idx + 4 < len(rows):
        return rows[current_idx + 4]
    return None


def _ttm_sum(rows: list, start_idx: int, field: str) -> float | None:
    """計算 TTM（最近 4 季加總）"""
    if start_idx + 4 > len(rows):
        return None
    total = 0
    for i in range(start_idx, start_idx + 4):
        val = rows[i][field]
        if val is None:
            return None
        total += val
    return total


def _get_close_price(conn, ticker: str, period_date: str) -> float | None:
    """取得某個日期或之前最近交易日的收盤價"""
    row = conn.execute("""
        SELECT close FROM ohlc_daily
        WHERE ticker = ? AND date <= ?
        ORDER BY date DESC LIMIT 1
    """, (ticker, period_date)).fetchone()
    return row['close'] if row else None
