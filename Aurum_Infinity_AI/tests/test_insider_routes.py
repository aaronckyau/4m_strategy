from __future__ import annotations

import json

from flask import Flask

from blueprints.insider import insider_bp
from blueprints.insider import routes as insider_routes


def create_app():
    app = Flask(__name__)
    app.register_blueprint(insider_bp)
    return app


def test_insider_index_renders_dashboard_context(monkeypatch):
    app = create_app()
    monkeypatch.setattr(insider_routes, "get_current_lang", lambda: "zh_hk")
    monkeypatch.setattr(insider_routes, "get_translations", lambda lang: {"lang": lang})
    monkeypatch.setattr(
        insider_routes,
        "load_dashboard",
        lambda **kwargs: {
            "status": "empty",
            "stats": {"net_amount": 0},
            "stats_display": {
                "signal_count": "0",
                "buy_amount": "$0",
                "sell_amount": "$0",
                "net_amount": "$0",
                "buy_count": "0",
                "sell_count": "0",
            },
            "signals": [],
            "top_signal": None,
            "window_days": kwargs["window_days"],
            "min_value": kwargs["min_value"],
            "latest_transaction_date": None,
            "window_start": None,
        },
    )
    monkeypatch.setattr(
        insider_routes,
        "render_template",
        lambda template, **context: json.dumps({"template": template, **context}, ensure_ascii=False),
    )

    client = app.test_client()
    response = client.get("/insider?window=60&min_value=500000")

    payload = json.loads(response.get_data(as_text=True))
    assert response.status_code == 200
    assert payload["template"] == "insider/index.html"
    assert payload["window_days"] == 60
    assert payload["min_value"] == 500_000


def test_insider_index_falls_back_to_default_filters(monkeypatch):
    app = create_app()
    monkeypatch.setattr(insider_routes, "get_current_lang", lambda: "zh_hk")
    monkeypatch.setattr(insider_routes, "get_translations", lambda lang: {"lang": lang})
    monkeypatch.setattr(
        insider_routes,
        "load_dashboard",
        lambda **kwargs: {"window_days": kwargs["window_days"], "min_value": kwargs["min_value"]},
    )
    monkeypatch.setattr(
        insider_routes,
        "render_template",
        lambda template, **context: json.dumps(context, ensure_ascii=False),
    )

    client = app.test_client()
    response = client.get("/insider?window=13&min_value=123")

    payload = json.loads(response.get_data(as_text=True))
    assert payload["window_days"] == 30
    assert payload["min_value"] == 100_000
