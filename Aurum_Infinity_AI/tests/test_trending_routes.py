from __future__ import annotations

import json

from flask import Flask

from blueprints.trending import trending_bp
from blueprints.trending import routes as trending_routes


def create_app():
    app = Flask(__name__)
    app.register_blueprint(trending_bp)
    return app


class FakeQueryResult:
    def __init__(self, *, one=None, many=None):
        self._one = one
        self._many = many or []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class FakeDb:
    def __init__(self, handlers):
        self.handlers = handlers
        self.closed = False

    def execute(self, query, params=()):
        for key, result in self.handlers.items():
            if key in query:
                value = result(query, params) if callable(result) else result
                if isinstance(value, FakeQueryResult):
                    return value
                if isinstance(value, list):
                    return FakeQueryResult(many=value)
                return FakeQueryResult(one=value)
        raise AssertionError(f"Unexpected query: {query}")

    def close(self):
        self.closed = True


class TestTrendingHelpers:
    def test_load_json_returns_fallback_for_invalid_json(self):
        assert trending_routes.load_json("{bad", {"ok": False}) == {"ok": False}

    def test_parse_iso_date_handles_invalid_value(self):
        assert trending_routes.parse_iso_date("2026-04-21").isoformat() == "2026-04-21"
        assert trending_routes.parse_iso_date("not-a-date") is None

    def test_build_change_payload_computes_delta_and_percent(self):
        payload = trending_routes.build_change_payload(
            {"snapshot_date": "2026-04-21", "watchlist_count": 120},
            {"snapshot_date": "2026-04-20", "watchlist_count": 100},
        )

        assert payload["delta"] == 20
        assert payload["percent_change"] == 20
        assert payload["has_data"] is True

    def test_get_history_changes_returns_empty_when_no_latest_row(self):
        db = FakeDb(
            {
                "ORDER BY snapshot_date DESC": None,
            }
        )

        assert trending_routes.get_history_changes(db, "AAPL") == {}


class TestTrendingRoutes:
    def test_trending_home_renders_context(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(trending_routes, "get_current_lang", lambda: "zh_hk")
        monkeypatch.setattr(trending_routes, "get_translations", lambda lang: {"lang": lang})
        monkeypatch.setattr(
            trending_routes,
            "render_template",
            lambda template, **context: json.dumps({"template": template, **context}, ensure_ascii=False),
        )

        client = app.test_client()
        response = client.get("/trending")

        payload = json.loads(response.get_data(as_text=True))
        assert payload["template"] == "trending/index.html"
        assert payload["lang"] == "zh_hk"

    def test_api_trending_returns_symbols_and_meta(self, monkeypatch):
        app = create_app()
        fake_db = FakeDb(
            {
                "ORDER BY ds.watchlist_count DESC": [
                    {
                        "symbol": "AAPL",
                        "title": "Apple",
                        "exchange": "NASDAQ",
                        "logo_url": "logo",
                        "watchlist_count": 123,
                        "bullish_count": 10,
                        "bearish_count": 2,
                        "unlabeled_count": 1,
                        "snapshot_date": "2026-04-21",
                        "captured_at": "2026-04-21T10:00:00Z",
                    }
                ],
                "AS total_symbols": {
                    "total_symbols": 500,
                    "synced_symbols": 490,
                    "latest_snapshot_date": "2026-04-21",
                },
            }
        )
        monkeypatch.setattr(trending_routes, "get_db", lambda: fake_db)
        monkeypatch.setattr(trending_routes, "get_history_changes", lambda *args, **kwargs: {"1d": {"delta": 3}})

        client = app.test_client()
        response = client.get("/api/trending")

        payload = response.get_json()
        assert response.status_code == 200
        assert payload["symbols"][0]["symbol"] == "AAPL"
        assert payload["symbols"][0]["history"]["1d"]["delta"] == 3
        assert payload["meta"]["total_symbols"] == 500
        assert fake_db.closed is True

    def test_search_symbols_requires_query(self):
        app = create_app()
        client = app.test_client()

        response = client.get("/api/trending/search")

        assert response.status_code == 400
        assert response.get_json()["error"] == "missing_query"

    def test_search_symbols_returns_results(self, monkeypatch):
        app = create_app()
        fake_db = FakeDb(
            {
                "FROM stocktwits_symbols": [
                    {"symbol": "AAPL", "title": "Apple", "exchange": "NASDAQ"},
                    {"symbol": "AMD", "title": "AMD", "exchange": "NASDAQ"},
                ]
            }
        )
        monkeypatch.setattr(trending_routes, "get_db", lambda: fake_db)

        client = app.test_client()
        response = client.get("/api/trending/search?q=aa")

        payload = response.get_json()
        assert response.status_code == 200
        assert payload["results"][0]["symbol"] == "AAPL"

    def test_symbol_stream_returns_404_for_missing_symbol(self, monkeypatch):
        app = create_app()
        fake_db = FakeDb(
            {
                "WHERE s.symbol = ?": None,
            }
        )
        monkeypatch.setattr(trending_routes, "get_db", lambda: fake_db)

        client = app.test_client()
        response = client.get("/api/trending/stream/unknown")

        assert response.status_code == 404
        assert response.get_json()["error"] == "symbol_not_found"

    def test_symbol_stream_returns_symbol_and_messages(self, monkeypatch):
        app = create_app()
        fake_db = FakeDb(
            {
                "WHERE s.symbol = ?": {
                    "symbol": "AAPL",
                    "title": "Apple",
                    "exchange": "NASDAQ",
                    "id": 1,
                    "logo_url": "logo",
                    "region": "US",
                    "instrument_class": "Stock",
                    "watchlist_count": 100,
                    "snapshot_date": "2026-04-21",
                    "raw_symbol_json": "",
                },
                "FROM stocktwits_messages": [
                    {
                        "stocktwits_message_id": 11,
                        "body": "hello",
                        "created_at": "2026-04-21T09:00:00Z",
                        "captured_at": "2026-04-21T09:00:10Z",
                        "username": "user1",
                        "display_name": "User One",
                        "avatar_url": "avatar",
                        "sentiment": "Bullish",
                        "likes_total": 5,
                        "source_title": "Stocktwits",
                        "source_url": "https://example.com",
                        "discussion": 1,
                        "raw_message_json": "",
                    }
                ],
            }
        )
        monkeypatch.setattr(trending_routes, "get_db", lambda: fake_db)
        monkeypatch.setattr(trending_routes, "get_history_changes", lambda *args, **kwargs: {"7d": {"delta": 10}})

        client = app.test_client()
        response = client.get("/api/trending/stream/AAPL")

        payload = response.get_json()
        assert response.status_code == 200
        assert payload["symbol"]["symbol"] == "AAPL"
        assert payload["symbol"]["history"]["7d"]["delta"] == 10
        assert payload["messages"][0]["id"] == 11
