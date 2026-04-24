from __future__ import annotations

import json
from pathlib import Path

from flask import Flask

from blueprints.news_radar import news_radar_bp
from blueprints.news_radar import routes as news_radar_routes


def create_app():
    app = Flask(__name__)
    app.register_blueprint(news_radar_bp)
    return app


class TestNewsRadarHelpers:
    def test_extract_radar_data_returns_none_when_tag_missing(self):
        assert news_radar_routes.extract_radar_data("plain text") is None

    def test_extract_radar_data_parses_json_inside_tag(self):
        data = news_radar_routes.extract_radar_data(
            "<radar-data>{\"score\": 8, \"event_title\": \"Fed\"}</radar-data>"
        )

        assert data == {"score": 8, "event_title": "Fed"}

    def test_strip_radar_tags_removes_embedded_blocks(self):
        text = "<radar-data>{}</radar-data><card-summary>short</card-summary>report body"

        assert news_radar_routes.strip_radar_tags(text) == "report body"

    def test_cache_key_changes_when_prompt_changes(self):
        first = news_radar_routes._cache_key("prompt-a", "zh_hk")
        second = news_radar_routes._cache_key("prompt-b", "zh_hk")

        assert first != second


class TestNewsRadarRoutes:
    def test_index_renders_expected_context(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(news_radar_routes, "get_translations", lambda lang: {"lang": lang})
        monkeypatch.setattr(news_radar_routes, "get_today", lambda: "2026/04/21")
        monkeypatch.setattr(news_radar_routes, "load_seeking_alpha_report", lambda: {"items": []})
        monkeypatch.setattr(
            news_radar_routes,
            "render_template",
            lambda template, **context: json.dumps({"template": template, **context}, ensure_ascii=False),
        )

        client = app.test_client()
        response = client.get("/news-radar?lang=zh_cn")

        payload = json.loads(response.get_data(as_text=True))
        assert payload["template"] == "news_radar/index.html"
        assert payload["lang"] == "zh_cn"
        assert payload["date"] == "2026/04/21"

    def test_analyze_returns_empty_error_for_blank_event(self):
        app = create_app()
        client = app.test_client()

        response = client.post("/api/news-radar/analyze", json={"event": "   "})

        assert response.status_code == 200
        assert response.get_json() == {"success": False, "error": "empty"}

    def test_analyze_returns_cached_payload_when_cache_exists(self, monkeypatch, tmp_path):
        app = create_app()
        cache_path = tmp_path / "radar_cache.json"
        cache_path.write_text(
            json.dumps({"radar": {"score": 9}, "report_html": "<p>cached</p>", "summary": "cached"}),
            encoding="utf-8",
        )
        monkeypatch.setattr(news_radar_routes, "_cache_path", lambda *args: str(cache_path))

        client = app.test_client()
        response = client.post("/api/news-radar/analyze", json={"event": "Fed cuts rates", "lang": "zh_hk"})

        payload = response.get_json()
        assert payload["success"] is True
        assert payload["from_cache"] is True
        assert payload["report_html"] == "<p>cached</p>"

    def test_analyze_ignores_stale_cache_and_rebuilds(self, monkeypatch, tmp_path):
        app = create_app()
        cache_path = tmp_path / "radar_cache.json"
        cache_path.write_text(
            json.dumps({"radar": {"score": 3}, "report_html": "<p>stale</p>", "summary": "stale"}),
            encoding="utf-8",
        )
        raw = "<radar-data>{\"score\": 8}</radar-data><card-summary>fresh</card-summary>## Fresh"
        monkeypatch.setattr(news_radar_routes, "_cache_path", lambda *args: str(cache_path))
        monkeypatch.setattr(news_radar_routes, "_is_cache_fresh", lambda *args: False)
        monkeypatch.setattr(news_radar_routes, "call_gemini_api", lambda *args, **kwargs: raw)
        monkeypatch.setattr(news_radar_routes, "extract_card_summary", lambda text: "fresh")

        client = app.test_client()
        response = client.post("/api/news-radar/analyze", json={"event": "Fed cuts rates", "lang": "zh_hk"})

        payload = response.get_json()
        assert payload["success"] is True
        assert payload["from_cache"] is False
        assert payload["summary"] == "fresh"

    def test_analyze_returns_ai_error_without_processing(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(news_radar_routes, "_cache_path", lambda *args: str(Path("missing.json")))
        monkeypatch.setattr(news_radar_routes, "call_gemini_api", lambda *args, **kwargs: "⚠️ upstream failed")

        client = app.test_client()
        response = client.post("/api/news-radar/analyze", json={"event": "Inflation", "lang": "zh_cn"})

        assert response.get_json() == {"success": False, "error": "⚠️ upstream failed"}

    def test_analyze_builds_report_and_writes_cache(self, monkeypatch, tmp_path):
        app = create_app()
        cache_path = tmp_path / "radar_cache.json"
        raw = (
            "<radar-data>{\"score\": 7}</radar-data>"
            "<card-summary>brief</card-summary>"
            "## Heading"
        )
        monkeypatch.setattr(news_radar_routes, "_cache_path", lambda *args: str(cache_path))
        monkeypatch.setattr(news_radar_routes, "call_gemini_api", lambda *args, **kwargs: raw)
        monkeypatch.setattr(news_radar_routes, "extract_card_summary", lambda text: "brief")

        client = app.test_client()
        response = client.post("/api/news-radar/analyze", json={"event": "Oil spike", "lang": "zh_hk"})

        payload = response.get_json()
        assert payload["success"] is True
        assert payload["from_cache"] is False
        assert payload["radar"] == {"score": 7}
        assert "Heading" in payload["report_html"]
        cached = json.loads(cache_path.read_text(encoding="utf-8"))
        assert cached["summary"] == "brief"

    def test_get_current_lang_falls_back_to_default_for_english_browser(self):
        app = create_app()

        with app.test_request_context("/news-radar", headers={"Accept-Language": "en-US,en;q=0.9"}):
            assert news_radar_routes.get_current_lang() == "zh_hk"

    def test_get_current_lang_reads_cookie(self):
        app = create_app()

        with app.test_request_context("/news-radar", headers={"Cookie": "lang=zh_cn"}):
            assert news_radar_routes.get_current_lang() == "zh_cn"
