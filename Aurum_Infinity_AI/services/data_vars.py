"""
data_vars.py - 從 DB 讀取金融數據，格式化為 prompt 變數
============================================================================
提供 {stock_hist_price} 等資料庫驅動的變數，供 prompt 模板替換。
============================================================================
"""
from __future__ import annotations

from database import get_db


def get_stock_hist_price(ticker: str) -> str:
    """
    讀取 OHLC 日線，格式化為簡潔文字。
    - 最近 150 天：逐日
    - 150 天之前：週線摘要（每週 Mon open, week high/low, Fri close, total volume）
    """
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT date, open, high, low, close, adj_close, volume "
            "FROM ohlc_daily WHERE ticker = ? ORDER BY date DESC",
            (ticker,)
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return "(無歷史價格數據)"

    # rows are DESC, reverse to ASC for processing
    rows = list(reversed(rows))

    # Split: recent 150 days (daily) vs older (weekly)
    total = len(rows)
    split_idx = max(0, total - 150)
    older = rows[:split_idx]
    recent = rows[split_idx:]

    lines = []

    # --- Weekly summary for older data ---
    if older:
        lines.append("=== 週線摘要 ===")
        week_rows = []
        current_week = []
        for r in older:
            # Group by ISO week
            date_str = r['date']
            if current_week and _iso_week(date_str) != _iso_week(current_week[0]['date']):
                week_rows.append(_summarize_week(current_week))
                current_week = []
            current_week.append(dict(r))
        if current_week:
            week_rows.append(_summarize_week(current_week))
        lines.extend(week_rows)
        lines.append("")

    # --- Daily for recent 150 days ---
    if recent:
        lines.append("=== 近期日線 ===")
        for r in recent:
            v = _fmt_vol(r['volume'])
            lines.append(
                f"{r['date']}: O={_fmt(r['open'])} H={_fmt(r['high'])} "
                f"L={_fmt(r['low'])} C={_fmt(r['close'])} V={v}"
            )

    return "\n".join(lines)


def get_stock_financials(ticker: str) -> str:
    """
    讀取三表財務數據，每個季度清晰分段顯示。
    """
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT period, statement_type, fiscal_year, fiscal_quarter, "
            "revenue, cost_of_revenue, gross_profit, operating_income, "
            "net_income, eps, eps_diluted, ebitda, operating_expenses, "
            "total_assets, total_liabilities, total_equity, total_debt, "
            "cash_and_equivalents, current_assets, current_liabilities, "
            "operating_cash_flow, capex, free_cash_flow, dividends_paid "
            "FROM financial_statements WHERE ticker = ? "
            "ORDER BY period DESC, statement_type",
            (ticker,)
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return "(無財務報表數據)"

    from collections import OrderedDict
    periods = OrderedDict()
    for r in rows:
        p = r['period']
        if p not in periods:
            periods[p] = {}
        periods[p][r['statement_type']] = r

    lines = []
    for period, stmts in periods.items():
        sample = list(stmts.values())[0]
        fy = sample['fiscal_year'] or ''
        fq = f"Q{sample['fiscal_quarter']}" if sample['fiscal_quarter'] else 'Annual'
        lines.append(f"══ {period} (FY{fy} {fq}) ══")

        inc = stmts.get('income')
        if inc:
            lines.append("  損益表 (Income Statement):")
            lines.append(f"    Revenue (營收):          {_fmt_money(inc['revenue'])}")
            lines.append(f"    Gross Profit (毛利):     {_fmt_money(inc['gross_profit'])}")
            lines.append(f"    Operating Income (營業利潤): {_fmt_money(inc['operating_income'])}")
            lines.append(f"    Net Income (淨利潤):     {_fmt_money(inc['net_income'])}")
            lines.append(f"    EPS (Diluted):           {_fmt(inc['eps_diluted'])}")
            lines.append(f"    EBITDA:                  {_fmt_money(inc['ebitda'])}")

        bs = stmts.get('balance')
        if bs:
            lines.append("  資產負債表 (Balance Sheet):")
            lines.append(f"    Total Assets (總資產):    {_fmt_money(bs['total_assets'])}")
            lines.append(f"    Total Liabilities (總負債): {_fmt_money(bs['total_liabilities'])}")
            lines.append(f"    Total Equity (股東權益):  {_fmt_money(bs['total_equity'])}")
            lines.append(f"    Total Debt (總債務):      {_fmt_money(bs['total_debt'])}")
            lines.append(f"    Cash (現金及等價物):      {_fmt_money(bs['cash_and_equivalents'])}")
            lines.append(f"    Current Assets (流動資產): {_fmt_money(bs['current_assets'])}")
            lines.append(f"    Current Liabilities (流動負債): {_fmt_money(bs['current_liabilities'])}")

        cf = stmts.get('cashflow')
        if cf:
            lines.append("  現金流量表 (Cash Flow):")
            lines.append(f"    Operating Cash Flow (營業現金流): {_fmt_money(cf['operating_cash_flow'])}")
            lines.append(f"    CapEx (資本支出):                {_fmt_money(cf['capex'])}")
            lines.append(f"    Free Cash Flow (自由現金流):     {_fmt_money(cf['free_cash_flow'])}")
            lines.append(f"    Dividends Paid (已付股息):       {_fmt_money(cf['dividends_paid'])}")

        lines.append("")

    return "\n".join(lines).rstrip()


def get_stock_metrics(ticker: str) -> str:
    """
    讀取 TTM 財務比率（從 ratios_ttm 表），清晰分段顯示。
    """
    conn = get_db()
    try:
        r = conn.execute(
            "SELECT pe, pb, ps, peg, ev_multiple, "
            "gross_margin, operating_margin, net_margin, ebitda_margin, "
            "debt_to_equity, current_ratio, quick_ratio, interest_coverage, "
            "eps, revenue_per_share, book_value_per_share, fcf_per_share, "
            "dividend_yield, dividend_per_share, dividend_payout_ratio, "
            "asset_turnover, receivables_turnover, inventory_turnover, "
            "price_to_fcf, price_to_ocf, ocf_per_share "
            "FROM ratios_ttm WHERE ticker = ?",
            (ticker,)
        ).fetchone()
    finally:
        conn.close()

    if not r:
        return "(無 TTM 財務比率數據)"

    lines = ["── TTM 財務比率 ──"]

    # Valuation
    v = []
    if r['pe'] is not None: v.append(f"PE = {r['pe']:.1f}")
    if r['pb'] is not None: v.append(f"PB = {r['pb']:.1f}")
    if r['ps'] is not None: v.append(f"PS = {r['ps']:.1f}")
    if r['peg'] is not None: v.append(f"PEG = {r['peg']:.2f}")
    if r['ev_multiple'] is not None: v.append(f"EV/EBITDA = {r['ev_multiple']:.1f}")
    if r['price_to_fcf'] is not None: v.append(f"P/FCF = {r['price_to_fcf']:.1f}")
    if v: lines.append(f"  估值: {' | '.join(v)}")

    # Margins
    m = []
    if r['gross_margin'] is not None: m.append(f"毛利率 = {r['gross_margin']*100:.1f}%")
    if r['operating_margin'] is not None: m.append(f"營業利潤率 = {r['operating_margin']*100:.1f}%")
    if r['net_margin'] is not None: m.append(f"淨利率 = {r['net_margin']*100:.1f}%")
    if r['ebitda_margin'] is not None: m.append(f"EBITDA利潤率 = {r['ebitda_margin']*100:.1f}%")
    if m: lines.append(f"  利潤率: {' | '.join(m)}")

    # Per Share
    p = []
    if r['eps'] is not None: p.append(f"EPS = {r['eps']:.2f}")
    if r['revenue_per_share'] is not None: p.append(f"營收/股 = {r['revenue_per_share']:.2f}")
    if r['book_value_per_share'] is not None: p.append(f"帳面價值/股 = {r['book_value_per_share']:.2f}")
    if r['fcf_per_share'] is not None: p.append(f"FCF/股 = {r['fcf_per_share']:.2f}")
    if r['ocf_per_share'] is not None: p.append(f"OCF/股 = {r['ocf_per_share']:.2f}")
    if p: lines.append(f"  每股指標: {' | '.join(p)}")

    # Leverage
    l = []
    if r['debt_to_equity'] is not None: l.append(f"D/E = {r['debt_to_equity']:.2f}")
    if r['current_ratio'] is not None: l.append(f"流動比率 = {r['current_ratio']:.2f}")
    if r['quick_ratio'] is not None: l.append(f"速動比率 = {r['quick_ratio']:.2f}")
    if r['interest_coverage'] is not None: l.append(f"利息覆蓋率 = {r['interest_coverage']:.1f}")
    if l: lines.append(f"  槓桿/流動性: {' | '.join(l)}")

    # Dividend
    d = []
    if r['dividend_yield'] is not None: d.append(f"股息率 = {r['dividend_yield']*100:.2f}%")
    if r['dividend_per_share'] is not None: d.append(f"每股股息 = {r['dividend_per_share']:.2f}")
    if r['dividend_payout_ratio'] is not None: d.append(f"派息比率 = {r['dividend_payout_ratio']*100:.1f}%")
    if d: lines.append(f"  股息: {' | '.join(d)}")

    # Efficiency
    e = []
    if r['asset_turnover'] is not None: e.append(f"資產周轉率 = {r['asset_turnover']:.2f}")
    if r['receivables_turnover'] is not None: e.append(f"應收賬款周轉率 = {r['receivables_turnover']:.1f}")
    if r['inventory_turnover'] is not None: e.append(f"存貨周轉率 = {r['inventory_turnover']:.1f}")
    if e: lines.append(f"  效率: {' | '.join(e)}")

    return "\n".join(lines)


def _fmt_money(val) -> str:
    """Format large money values as human-readable."""
    if val is None:
        return "N/A"
    neg = val < 0
    v = abs(val)
    if v >= 1_000_000_000_000:
        s = f"{v / 1_000_000_000_000:.2f}T"
    elif v >= 1_000_000_000:
        s = f"{v / 1_000_000_000:.2f}B"
    elif v >= 1_000_000:
        s = f"{v / 1_000_000:.2f}M"
    else:
        s = f"{v:,.2f}"
    return f"-{s}" if neg else s


def _summarize_week(rows: list[dict]) -> str:
    """Summarize a week of daily rows into one line."""
    dates = [r['date'] for r in rows]
    o = rows[0]['open']
    c = rows[-1]['close']
    h = max(r['high'] for r in rows if r['high'] is not None)
    l = min(r['low'] for r in rows if r['low'] is not None)
    vol = sum(r['volume'] for r in rows if r['volume'] is not None)
    return (
        f"W {dates[0]}~{dates[-1]}: O={_fmt(o)} H={_fmt(h)} "
        f"L={_fmt(l)} C={_fmt(c)} V={_fmt_vol(vol)}"
    )


def _iso_week(date_str: str) -> str:
    """Return 'YYYY-WNN' for grouping."""
    from datetime import date
    parts = date_str.split('-')
    d = date(int(parts[0]), int(parts[1]), int(parts[2]))
    iso = d.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _fmt(val) -> str:
    """Format price value."""
    if val is None:
        return "N/A"
    return f"{val:.2f}"


def _fmt_vol(val) -> str:
    """Format volume as human-readable."""
    if val is None:
        return "N/A"
    if val >= 1_000_000_000:
        return f"{val / 1_000_000_000:.1f}B"
    if val >= 1_000_000:
        return f"{val / 1_000_000:.1f}M"
    if val >= 1_000:
        return f"{val / 1_000:.0f}K"
    return str(val)


# ============================================================================
# Registry: all DB-driven variables
# ============================================================================

def get_stock_ratios_ttm(ticker: str) -> str:
    """
    讀取 ratios_ttm 全部 54 個指標，以緊湊格式輸出供 AI prompt 使用。
    """
    conn = get_db()
    try:
        r = conn.execute(
            "SELECT * FROM ratios_ttm WHERE ticker = ?", (ticker,)
        ).fetchone()
    finally:
        conn.close()

    if not r:
        return "(無 Ratios TTM 數據)"

    # 跳過 ticker, raw_json, fetched_at
    skip = {'ticker', 'raw_json', 'fetched_at'}
    lines = [f"── {ticker} Ratios TTM ──"]
    for key in r.keys():
        if key in skip or r[key] is None:
            continue
        val = r[key]
        # 百分比類欄位用 % 顯示
        pct_fields = {
            'gross_margin', 'operating_margin', 'pretax_margin', 'net_margin',
            'ebitda_margin', 'effective_tax_rate', 'dividend_yield',
            'dividend_payout_ratio', 'ocf_ratio', 'ocf_sales_ratio',
            'fcf_ocf_ratio', 'solvency_ratio', 'net_income_per_ebt',
            'ebt_per_ebit',
        }
        if key in pct_fields:
            lines.append(f"  {key}: {val*100:.2f}%")
        elif isinstance(val, float):
            lines.append(f"  {key}: {val:.4f}")
        else:
            lines.append(f"  {key}: {val}")

    return "\n".join(lines)


def get_stock_master(ticker: str) -> str:
    """
    讀取 stocks_master 基本資料：名稱、市場、行業、市值等。
    """
    conn = get_db()
    try:
        r = conn.execute(
            "SELECT ticker, name, name_zh_hk, name_zh_cn, market, exchange, "
            "sector, industry, market_cap, currency, description, shares_outstanding "
            "FROM stocks_master WHERE ticker = ?",
            (ticker,)
        ).fetchone()
    finally:
        conn.close()

    if not r:
        return "(無 stocks_master 數據)"

    lines = [f"── {ticker} 基本資料 ──"]
    if r['name']: lines.append(f"  英文名稱: {r['name']}")
    if r['name_zh_hk']: lines.append(f"  繁體中文名: {r['name_zh_hk']}")
    if r['name_zh_cn']: lines.append(f"  簡體中文名: {r['name_zh_cn']}")
    if r['market']: lines.append(f"  市場: {r['market']}")
    if r['exchange']: lines.append(f"  交易所: {r['exchange']}")
    if r['sector']: lines.append(f"  板塊: {r['sector']}")
    if r['industry']: lines.append(f"  行業: {r['industry']}")
    if r['currency']: lines.append(f"  貨幣: {r['currency']}")
    if r['market_cap'] is not None: lines.append(f"  市值: {_fmt_money(r['market_cap'])}")
    if r['shares_outstanding'] is not None: lines.append(f"  流通股數: {_fmt_money(r['shares_outstanding'])}")
    if r['description']: lines.append(f"  公司簡介: {r['description']}")

    return "\n".join(lines)


def get_institutional_holders(ticker: str) -> str:
    """
    讀取最近 4 季 Form 13F 機構持倉，格式化為多季趨勢對比。
    """
    conn = get_db()
    try:
        # 取得最近 4 個不同的 date_reported
        quarters = conn.execute(
            "SELECT DISTINCT date_reported FROM institutional_holdings "
            "WHERE ticker = ? ORDER BY date_reported DESC LIMIT 4",
            (ticker,)
        ).fetchall()

        if not quarters:
            return "(該股票無 Form 13F 機構持倉數據，可能為非美股或尚未拉取)"

        quarter_dates = [q[0] for q in quarters]

        # 取每季全部機構數據
        placeholders = ",".join("?" * len(quarter_dates))
        rows = conn.execute(
            f"SELECT holder, date_reported, shares, market_value, "
            f"change, change_pct, ownership_pct, is_new, is_sold_out "
            f"FROM institutional_holdings "
            f"WHERE ticker = ? AND date_reported IN ({placeholders}) "
            f"ORDER BY date_reported DESC, market_value DESC",
            (ticker, *quarter_dates)
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return "(該股票無 Form 13F 機構持倉數據)"

    # Group by quarter
    from collections import OrderedDict
    by_quarter = OrderedDict()
    for r in rows:
        q = r['date_reported']
        if q not in by_quarter:
            by_quarter[q] = []
        by_quarter[q].append(r)

    lines = [f"══ {ticker} Form 13F 機構持倉 ({len(quarter_dates)} 季) ══"]
    lines.append(f"季度: {' | '.join(quarter_dates)}")

    latest_q = quarter_dates[0]

    # ──────────────────────────────────────────────────────
    # 4Q 大機構持倉對比表（前 15 大）+ 彙總
    # ──────────────────────────────────────────────────────
    if len(quarter_dates) >= 2:
        top_holders = by_quarter[latest_q][:15]
        lines.append(f"\n══ 大機構 {len(quarter_dates)}Q 持倉對比表（持股數 + 佔比%） ══")

        # Header
        header = f"{'機構名稱':<35}"
        for qd in quarter_dates:
            short_q = qd[2:]  # e.g. "25-12-31"
            header += f" {'Q'+short_q:>18}"
        header += f" {'4Q趨勢':>8}"
        lines.append(header)

        for lh in top_holders:
            name = lh['holder']
            row = f"  {name[:33]:<33}"
            shares_series = []

            for qd in quarter_dates:
                match = [h for h in by_quarter[qd] if h['holder'] == name]
                if match:
                    s = match[0]['shares']
                    own = match[0]['ownership_pct']
                    own_str = f"({own:.1f}%)" if own is not None else ""
                    row += f" {_fmt_money(s):>10}{own_str:>8}"
                    shares_series.append(s)
                else:
                    row += f" {'--':>18}"
                    shares_series.append(None)

            # Determine trend across available quarters
            valid = [s for s in shares_series if s is not None]
            if len(valid) >= 2:
                if all(valid[i] >= valid[i+1] for i in range(len(valid)-1)):
                    trend = "↑ 連增"
                elif all(valid[i] <= valid[i+1] for i in range(len(valid)-1)):
                    trend = "↓ 連減"
                elif valid[0] > valid[-1]:
                    trend = "↗ 增持"
                elif valid[0] < valid[-1]:
                    trend = "↘ 減持"
                else:
                    trend = "→ 持平"
            else:
                trend = "--"
            row += f" {trend:>8}"
            lines.append(row)

        # Summary rows at bottom of 4Q table
        lines.append(f"  {'─' * 33}" + "".join(f" {'─' * 18}" for _ in quarter_dates) + f" {'─' * 8}")

        row_top10 = f"  {'10大機構合計':<33}"
        for qd in quarter_dates:
            top10 = by_quarter[qd][:10]
            pct = sum((h['ownership_pct'] or 0) for h in top10)
            row_top10 += f" {pct:>17.2f}%"
        row_top10 += f" {'':>8}"
        lines.append(row_top10)

        row_all = f"  {'全部機構合計':<33}"
        for qd in quarter_dates:
            all_h = by_quarter[qd]
            pct = sum((h['ownership_pct'] or 0) for h in all_h)
            row_all += f" {pct:>17.2f}%"
        row_all += f" {'':>8}"
        lines.append(row_all)

    # ──────────────────────────────────────────────────────
    # Section 3: 新進 / 清倉 機構
    # ──────────────────────────────────────────────────────
    new_entries = [h for h in by_quarter[latest_q] if h['is_new']]
    sold_outs = [h for h in by_quarter[latest_q] if h['is_sold_out']]
    if new_entries or sold_outs:
        lines.append(f"\n── 本季異動：新進 {len(new_entries)} 家 / 清倉 {len(sold_outs)} 家 ──")
        if new_entries:
            lines.append("  新進場:")
            for h in new_entries[:10]:
                lines.append(f"    + {h['holder'][:40]} | {_fmt_money(h['shares'])} 股 | 市值 {_fmt_money(h['market_value'])}")
        if sold_outs:
            lines.append("  完全清倉:")
            for h in sold_outs[:10]:
                lines.append(f"    - {h['holder'][:40]}")

    return "\n".join(lines)


DATA_VAR_REGISTRY = {
    'stock_hist_price': {
        'label': '歷史股價 (OHLC)',
        'fn': get_stock_hist_price,
    },
    'stock_financials': {
        'label': '財務報表 (三表)',
        'fn': get_stock_financials,
    },
    'stock_metrics': {
        'label': '財務指標 (PE/ROE/YoY等)',
        'fn': get_stock_metrics,
    },
    'stock_ratios_ttm': {
        'label': 'Ratios TTM 完整版 (54指標)',
        'fn': get_stock_ratios_ttm,
    },
    'stock_master': {
        'label': '股票基本資料 (名稱/市場/行業/市值)',
        'fn': get_stock_master,
    },
    'stock_inst_holders': {
        'label': 'Form 13F 機構持倉 (4季趨勢)',
        'fn': get_institutional_holders,
    },
}


def resolve_data_vars(ticker: str) -> dict[str, str]:
    """Resolve all DB-driven variables for a given ticker."""
    import logging
    _dv_log = logging.getLogger(__name__)
    result = {}
    for key, entry in DATA_VAR_REGISTRY.items():
        try:
            result[key] = entry['fn'](ticker)
        except Exception as e:
            _dv_log.error("resolve_data_vars %s/%s: %s", ticker, key, e)
            result[key] = f"({key} 數據暫時無法載入)"
    return result
