from __future__ import annotations

from flask import Flask

from blueprints.news import news_bp
from blueprints.news import routes as news_routes


def create_app():
    app = Flask(__name__)
    app.register_blueprint(news_bp)
    return app


class TestNewsRoutes:
    def test_feature_raw_returns_utf8_charset(self, monkeypatch, tmp_path):
        app = create_app()
        article_path = tmp_path / "feature.html"
        article_path.write_text(
            "<!doctype html><html><head><meta charset='utf-8'><title>美伊戰爭</title></head><body>返回專題文章</body></html>",
            encoding="utf-8",
        )
        monkeypatch.setattr(
            news_routes,
            "get_feature_article",
            lambda slug: {"path": article_path},
        )
        client = app.test_client()

        response = client.get("/news/features/test/raw")

        assert response.status_code == 200
        assert response.content_type == "text/html; charset=utf-8"
        body = response.get_data(as_text=True)
        assert "美伊戰爭" in body
        assert "返回專題文章" in body
