from flask import render_template, jsonify

from blueprints.markets import markets_bp
from blueprints.stock.routes import get_current_lang
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
