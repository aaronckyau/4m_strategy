from flask import render_template, request

from blueprints.insider import insider_bp
from blueprints.stock.routes import get_current_lang
from services.insider_service import INSIDER_MIN_VALUES, INSIDER_WINDOWS, load_dashboard
from translations import get_translations


def _parse_choice(raw_value: str | None, allowed: tuple[int, ...], default: int) -> int:
    try:
        value = int(raw_value or default)
    except (TypeError, ValueError):
        return default
    return value if value in allowed else default


@insider_bp.route("/insider")
def index():
    lang = get_current_lang()
    t = get_translations(lang)
    window_days = _parse_choice(request.args.get("window"), INSIDER_WINDOWS, 30)
    min_value = _parse_choice(request.args.get("min_value"), INSIDER_MIN_VALUES, 100_000)
    dashboard = load_dashboard(window_days=window_days, min_value=min_value)

    return render_template(
        "insider/index.html",
        dashboard=dashboard,
        window_days=window_days,
        min_value=min_value,
        window_options=INSIDER_WINDOWS,
        min_value_options=INSIDER_MIN_VALUES,
        lang=lang,
        t=t,
    )
