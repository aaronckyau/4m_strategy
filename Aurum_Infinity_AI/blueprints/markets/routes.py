from flask import render_template, jsonify

from blueprints.markets import markets_bp
from blueprints.stock.routes import get_current_lang
from database import get_db
from services.feature_article_service import load_theme_articles
from services.market_overview_service import get_movers, get_most_active, get_pulse, get_sparklines, _sparkline_cache_safe
from translations import get_translations


@markets_bp.route("/markets")
def index():
    lang = get_current_lang()
    t = get_translations(lang)

    # pulse / movers / most_active 並行執行，不等 sparklines
    from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed
    tasks = {}
    with ThreadPoolExecutor(max_workers=3) as pool:
        tasks["pulse"]  = pool.submit(get_pulse)
        tasks["movers"] = pool.submit(get_movers)
        tasks["active"] = pool.submit(get_most_active)

    pulse,  pulse_updated  = tasks["pulse"].result()
    movers, movers_updated = tasks["movers"].result()
    active, active_updated = tasks["active"].result()

    # sparklines 不阻塞：如果已在 cache 就用，否則傳空 dict 讓前端 JS 非同步拿
    spark_cache = _sparkline_cache_safe()

    return render_template(
        "markets/index.html",
        pulse=pulse,
        pulse_updated=pulse_updated,
        sparklines=spark_cache,
        gainers=movers.get("gainers", []),
        losers=movers.get("losers", []),
        most_active=active,
        movers_updated=movers_updated,
        active_updated=active_updated,
        lang=lang,
        t=t,
    )


@markets_bp.route("/market-guide")
def guide():
    lang = get_current_lang()
    t = get_translations(lang)

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=3) as pool:
        pulse_task = pool.submit(get_pulse)
        movers_task = pool.submit(get_movers)
        active_task = pool.submit(get_most_active)

        pulse, pulse_updated = pulse_task.result()
        movers, movers_updated = movers_task.result()
        active, active_updated = active_task.result()

    guide_themes = load_theme_articles()[:3]

    return render_template(
        "markets/guide.html",
        pulse=pulse,
        gainers=movers.get("gainers", []),
        losers=movers.get("losers", []),
        most_active=active,
        pulse_updated=pulse_updated,
        movers_updated=movers_updated,
        active_updated=active_updated,
        guide_themes=guide_themes,
        lang=lang,
        t=t,
    )


@markets_bp.route("/api/markets/pulse")
def api_pulse():
    data, updated_at = get_pulse()
    sparklines = get_sparklines()
    # Attach sparkline to each pulse item
    for item in data:
        item["sparkline"] = sparklines.get(item["symbol"], [])
    return jsonify({"pulse": data, "updated_at": updated_at})


@markets_bp.route("/api/markets/movers")
def api_movers():
    movers, updated_at = get_movers()
    active, _ = get_most_active()
    return jsonify({
        "gainers":     movers.get("gainers", []),
        "losers":      movers.get("losers", []),
        "most_active": active,
        "updated_at":  updated_at,
    })


def _safe_pct(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator * 100.0, 1)


def _get_rsp_change(conn) -> dict:
    rows = conn.execute(
        """
        SELECT date, close
        FROM ohlc_daily
        WHERE ticker = 'RSP'
        ORDER BY date DESC
        LIMIT 2
        """
    ).fetchall()
    if len(rows) < 2 or rows[0]["close"] is None or rows[1]["close"] in (None, 0):
        return {"date": rows[0]["date"] if rows else None, "change_pct": None}
    change_pct = round((rows[0]["close"] - rows[1]["close"]) / rows[1]["close"] * 100.0, 2)
    return {"date": rows[0]["date"], "change_pct": change_pct}


def _breadth_conclusion(advancers_pct: float | None, above50_pct: float | None,
                        new_highs: int, new_lows: int, rsp_change: float | None) -> str:
    if advancers_pct is None:
        return "資料不足"
    if advancers_pct >= 60 and (above50_pct is None or above50_pct >= 55) and (rsp_change is None or rsp_change >= 0):
        return "廣度健康"
    if advancers_pct < 50 and rsp_change is not None and rsp_change <= 0:
        return "少數權值股撐市"
    if advancers_pct <= 40 or new_lows > new_highs:
        return "廣度轉弱"
    return "市場分化"


def _safe_change_pct(current: float | int | None, baseline: float | int | None) -> float | None:
    if current is None or baseline in (None, 0):
        return None
    return round((float(current) - float(baseline)) / float(baseline) * 100.0, 1)


def _market_flow_conclusion(value_vs_5d: float | None, value_vs_20d: float | None,
                            inflows: list[dict], outflows: list[dict]) -> str:
    lead_inflow = inflows[0]["sector"] if inflows else None
    lead_outflow = outflows[0]["sector"] if outflows else None

    active = (value_vs_20d is not None and value_vs_20d >= 8) or (
        value_vs_5d is not None and value_vs_5d >= 8
    )
    quiet = (value_vs_20d is not None and value_vs_20d <= -8) and (
        value_vs_5d is not None and value_vs_5d <= -5
    )

    if active and lead_inflow:
        return f"成交額升溫，偏向{lead_inflow}"
    if active and lead_outflow:
        return f"成交額放大下跌集中在{lead_outflow}"
    if quiet:
        return "整體成交偏淡，資金方向未明"
    if lead_inflow and lead_outflow:
        return f"板塊分化，{lead_inflow}較強、{lead_outflow}較弱"
    return "資料不足，暫未能判斷流向"


def _flow_state(change_pct: float | None, value_vs_5d: float | None) -> dict:
    if change_pct is None or value_vs_5d is None:
        return {"label": "資料不足", "tone": "neutral"}
    if change_pct >= 0 and value_vs_5d >= 0:
        return {"label": "資金流入跡象", "tone": "inflow"}
    if change_pct < 0 and value_vs_5d >= 0:
        return {"label": "資金流出跡象", "tone": "outflow"}
    if change_pct >= 0 and value_vs_5d < 0:
        return {"label": "縮量上升", "tone": "thin-up"}
    return {"label": "縮量下跌", "tone": "thin-down"}


@markets_bp.route("/api/markets/flow")
def api_market_flow():
    conn = get_db()
    try:
        rows = conn.execute(
            """
            WITH ranked AS (
                SELECT
                    s.ticker,
                    COALESCE(NULLIF(s.sector, ''), '未分類') AS sector,
                    o.date,
                    o.close,
                    o.volume,
                    sm.market_cap,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.ticker
                        ORDER BY o.date DESC
                    ) AS rn
                FROM sp500_constituents s
                JOIN ohlc_daily o
                    ON o.ticker = s.ticker
                LEFT JOIN stocks_master sm
                    ON sm.ticker = s.ticker
            ),
            stats AS (
                SELECT
                    ticker,
                    sector,
                    MAX(market_cap) AS market_cap,
                    MAX(CASE WHEN rn = 1 THEN date END) AS latest_date,
                    MAX(CASE WHEN rn = 1 THEN close END) AS latest_close,
                    MAX(CASE WHEN rn = 2 THEN close END) AS previous_close,
                    MAX(CASE WHEN rn = 1 THEN volume END) AS latest_volume,
                    MAX(CASE WHEN rn = 2 THEN volume END) AS previous_volume,
                    MAX(CASE WHEN rn = 1 THEN close * volume END) AS latest_traded_value,
                    MAX(CASE WHEN rn = 2 THEN close * volume END) AS previous_traded_value,
                    AVG(CASE WHEN rn <= 5 THEN close * volume END) AS avg5_traded_value,
                    SUM(CASE WHEN rn <= 5 AND close IS NOT NULL AND volume IS NOT NULL THEN 1 ELSE 0 END) AS count5,
                    AVG(CASE WHEN rn <= 20 THEN close * volume END) AS avg20_traded_value,
                    SUM(CASE WHEN rn <= 20 AND close IS NOT NULL AND volume IS NOT NULL THEN 1 ELSE 0 END) AS count20
                FROM ranked
                WHERE rn <= 20
                GROUP BY ticker, sector
            )
            SELECT *
            FROM stats
            WHERE latest_traded_value IS NOT NULL
            """
        ).fetchall()
    finally:
        conn.close()

    latest_dates = [row["latest_date"] for row in rows if row["latest_date"]]
    total_traded_value = sum(float(row["latest_traded_value"] or 0) for row in rows)
    previous_traded_value = sum(float(row["previous_traded_value"] or 0) for row in rows if row["previous_traded_value"] is not None)
    avg5_traded_value = sum(float(row["avg5_traded_value"] or 0) for row in rows if row["count5"] and row["count5"] >= 3)
    avg20_traded_value = sum(float(row["avg20_traded_value"] or 0) for row in rows if row["count20"] and row["count20"] >= 10)

    sectors: dict[str, dict] = {}
    for row in rows:
        sector = row["sector"] or "未分類"
        bucket = sectors.setdefault(
            sector,
            {
                "sector": sector,
                "ticker_count": 0,
                "total_traded_value": 0.0,
                "previous_traded_value": 0.0,
                "avg5_traded_value": 0.0,
                "avg20_traded_value": 0.0,
                "advancers": 0,
                "decliners": 0,
                "weighted_change_sum": 0.0,
                "weight_sum": 0.0,
                "change_sum": 0.0,
                "change_count": 0,
            },
        )
        bucket["ticker_count"] += 1
        bucket["total_traded_value"] += float(row["latest_traded_value"] or 0)
        bucket["previous_traded_value"] += float(row["previous_traded_value"] or 0)
        if row["count5"] and row["count5"] >= 3:
            bucket["avg5_traded_value"] += float(row["avg5_traded_value"] or 0)
        if row["count20"] and row["count20"] >= 10:
            bucket["avg20_traded_value"] += float(row["avg20_traded_value"] or 0)

        latest_close = row["latest_close"]
        previous_close = row["previous_close"]
        if latest_close is None or previous_close in (None, 0):
            continue
        change_pct = (float(latest_close) - float(previous_close)) / float(previous_close) * 100.0
        if change_pct > 0:
            bucket["advancers"] += 1
        elif change_pct < 0:
            bucket["decliners"] += 1
        market_cap = float(row["market_cap"] or 0)
        if market_cap > 0:
            bucket["weighted_change_sum"] += change_pct * market_cap
            bucket["weight_sum"] += market_cap
        bucket["change_sum"] += change_pct
        bucket["change_count"] += 1

    sector_items = []
    for bucket in sectors.values():
        change_pct = None
        if bucket["weight_sum"] > 0:
            change_pct = round(bucket["weighted_change_sum"] / bucket["weight_sum"], 2)
        elif bucket["change_count"]:
            change_pct = round(bucket["change_sum"] / bucket["change_count"], 2)
        value_vs_5d = _safe_change_pct(bucket["total_traded_value"], bucket["avg5_traded_value"])
        value_vs_20d = _safe_change_pct(bucket["total_traded_value"], bucket["avg20_traded_value"])
        advancers_pct = _safe_pct(bucket["advancers"], bucket["change_count"])
        flow_score = (
            (change_pct or 0) * 2
            + (value_vs_5d or 0) * 0.35
            + ((advancers_pct - 50) if advancers_pct is not None else 0) * 0.08
        )
        flow_state = _flow_state(change_pct, value_vs_5d)
        sector_items.append({
            "sector": bucket["sector"],
            "ticker_count": bucket["ticker_count"],
            "change_pct": change_pct,
            "total_traded_value": round(bucket["total_traded_value"]),
            "value_vs_yesterday_pct": _safe_change_pct(bucket["total_traded_value"], bucket["previous_traded_value"]),
            "value_vs_5d_pct": value_vs_5d,
            "value_vs_20d_pct": value_vs_20d,
            "advancers": bucket["advancers"],
            "decliners": bucket["decliners"],
            "advancers_pct": advancers_pct,
            "flow_score": round(flow_score, 2),
            "flow_state": flow_state["label"],
            "flow_state_tone": flow_state["tone"],
        })

    inflows = sorted(
        [item for item in sector_items if (item["change_pct"] or 0) > 0 and (item["value_vs_5d_pct"] or 0) >= 0],
        key=lambda item: (item["flow_score"], item["value_vs_5d_pct"] or 0),
        reverse=True,
    )[:5]
    outflows = sorted(
        [item for item in sector_items if (item["change_pct"] or 0) < 0 and (item["value_vs_5d_pct"] or 0) >= 0],
        key=lambda item: (abs(item["change_pct"] or 0), item["value_vs_5d_pct"] or 0),
        reverse=True,
    )[:5]

    market_value_vs_5d = _safe_change_pct(total_traded_value, avg5_traded_value)
    market_value_vs_20d = _safe_change_pct(total_traded_value, avg20_traded_value)

    return jsonify({
        "conclusion": _market_flow_conclusion(market_value_vs_5d, market_value_vs_20d, inflows, outflows),
        "latest_date": max(latest_dates) if latest_dates else None,
        "eligible_count": len(rows),
        "market": {
            "total_traded_value": round(total_traded_value),
            "previous_traded_value": round(previous_traded_value),
            "avg5_traded_value": round(avg5_traded_value),
            "avg20_traded_value": round(avg20_traded_value),
            "value_vs_yesterday_pct": _safe_change_pct(total_traded_value, previous_traded_value),
            "value_vs_5d_pct": market_value_vs_5d,
            "value_vs_20d_pct": market_value_vs_20d,
        },
        "inflows": inflows,
        "outflows": outflows,
        "sectors": sector_items,
    })


@markets_bp.route("/api/markets/breadth")
def api_market_breadth():
    conn = get_db()
    try:
        rows = conn.execute(
            """
            WITH ranked AS (
                SELECT
                    s.ticker,
                    o.date,
                    o.close,
                    o.high,
                    o.low,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.ticker
                        ORDER BY o.date DESC
                    ) AS rn
                FROM sp500_constituents s
                JOIN ohlc_daily o
                    ON o.ticker = s.ticker
            ),
            stats AS (
                SELECT
                    ticker,
                    MAX(CASE WHEN rn = 1 THEN date END) AS latest_date,
                    MAX(CASE WHEN rn = 1 THEN close END) AS latest_close,
                    MAX(CASE WHEN rn = 2 THEN close END) AS previous_close,
                    AVG(CASE WHEN rn <= 20 THEN close END) AS ma20,
                    SUM(CASE WHEN rn <= 20 THEN 1 ELSE 0 END) AS count20,
                    AVG(CASE WHEN rn <= 50 THEN close END) AS ma50,
                    SUM(CASE WHEN rn <= 50 THEN 1 ELSE 0 END) AS count50,
                    MAX(CASE WHEN rn <= 252 THEN high END) AS high252,
                    MIN(CASE WHEN rn <= 252 THEN low END) AS low252
                FROM ranked
                WHERE rn <= 252
                GROUP BY ticker
            )
            SELECT *
            FROM stats
            """
        ).fetchall()
        constituent_count = conn.execute("SELECT COUNT(*) FROM sp500_constituents").fetchone()[0]
        rsp = _get_rsp_change(conn)
    finally:
        conn.close()

    advancers = decliners = flat = eligible = 0
    above20 = above20_eligible = 0
    above50 = above50_eligible = 0
    new_highs = new_lows = 0
    latest_dates = []

    for row in rows:
        latest = row["latest_close"]
        previous = row["previous_close"]
        if row["latest_date"]:
            latest_dates.append(row["latest_date"])

        if latest is not None and previous is not None:
            eligible += 1
            if latest > previous:
                advancers += 1
            elif latest < previous:
                decliners += 1
            else:
                flat += 1

        if latest is not None and row["ma20"] is not None and row["count20"] >= 20:
            above20_eligible += 1
            if latest > row["ma20"]:
                above20 += 1

        if latest is not None and row["ma50"] is not None and row["count50"] >= 50:
            above50_eligible += 1
            if latest > row["ma50"]:
                above50 += 1

        if latest is not None and row["high252"] is not None and latest >= row["high252"]:
            new_highs += 1
        if latest is not None and row["low252"] is not None and latest <= row["low252"]:
            new_lows += 1

    advancers_pct = _safe_pct(advancers, eligible)
    above20_pct = _safe_pct(above20, above20_eligible)
    above50_pct = _safe_pct(above50, above50_eligible)
    adv_decl_ratio = round(advancers / decliners, 2) if decliners > 0 else None

    return jsonify({
        "conclusion": _breadth_conclusion(
            advancers_pct,
            above50_pct,
            new_highs,
            new_lows,
            rsp.get("change_pct"),
        ),
        "latest_date": max(latest_dates) if latest_dates else None,
        "constituent_count": constituent_count,
        "eligible_count": eligible,
        "advancers": advancers,
        "decliners": decliners,
        "flat": flat,
        "advancers_pct": advancers_pct,
        "decliners_pct": _safe_pct(decliners, eligible),
        "adv_decl_ratio": adv_decl_ratio,
        "above_20dma_pct": above20_pct,
        "above_20dma_count": above20,
        "above_20dma_eligible": above20_eligible,
        "above_50dma_pct": above50_pct,
        "above_50dma_count": above50,
        "above_50dma_eligible": above50_eligible,
        "new_highs": new_highs,
        "new_lows": new_lows,
        "rsp": rsp,
    })
