from __future__ import annotations

import json

from services import news_service


def reset_cache_state() -> None:
    news_service._cache_state["path"] = None
    news_service._cache_state["mtime"] = None
    news_service._cache_state["data"] = None


class TestNormalizeArticles:
    def test_normalize_articles_filters_invalid_items_and_cleans_fields(self):
        raw_articles = [
            None,
            {"id": ""},
            {
                "id": " article-1 ",
                "title": "  Example Title ",
                "summary": "  Summary  ",
                "source": " 富途 ",
                "source_url": " https://example.com/source ",
                "url": " https://example.com/article ",
                "time": " 09:30 ",
                "category": " 焦點新聞 ",
                "cover_image": " ",
                "paragraphs": [" First ", "", "Second "],
                "stock_tags": [" aapl ", "", "Tsla"],
                "country_tags": [" 美國 ", ""],
                "sector_tags": [" Technology ", ""],
            },
        ]

        articles = news_service._normalize_articles(raw_articles)

        assert articles == [
            {
                "id": "article-1",
                "title": "Example Title",
                "summary": "Summary",
                "source": "富途",
                "source_url": "https://example.com/source",
                "url": "https://example.com/article",
                "time": "09:30",
                "category": "焦點新聞",
                "cover_image": None,
                "paragraphs": ["First", "Second"],
                "stock_tags": ["AAPL", "TSLA"],
                "country_tags": ["美國"],
                "sector_tags": ["Technology"],
            }
        ]

    def test_normalize_articles_falls_back_when_category_and_source_missing(self):
        articles = news_service._normalize_articles([{"id": "a1"}])

        assert articles[0]["source"] == "富途"
        assert articles[0]["category"] == "富途"


class TestLoadNewsData:
    def test_returns_empty_payload_when_cache_missing(self, monkeypatch):
        reset_cache_state()
        monkeypatch.setattr(news_service, "resolve_news_cache_path", lambda: None)

        payload = news_service.load_news_data()

        assert payload["articles"] == []
        assert payload["cache_origin"] == "missing"
        assert "找不到新聞快取" in payload["message"]

    def test_returns_empty_payload_when_cache_json_is_invalid(self, tmp_path, monkeypatch):
        reset_cache_state()
        cache_path = tmp_path / "futunn_cache.json"
        cache_path.write_text("{bad json", encoding="utf-8")
        monkeypatch.setattr(news_service, "resolve_news_cache_path", lambda: cache_path)

        payload = news_service.load_news_data()

        assert payload["articles"] == []
        assert "格式錯誤" in payload["message"]

    def test_loads_and_indexes_articles_from_cache(self, tmp_path, monkeypatch):
        reset_cache_state()
        cache_path = tmp_path / "futunn_cache.json"
        cache_path.write_text(
            json.dumps(
                {
                    "fetched_at": "2026-04-21 10:00 HKT",
                    "categories": ["富途"],
                    "articles": [
                        {
                            "id": "a1",
                            "title": "  Title ",
                            "summary": " Summary ",
                            "source": "富途",
                            "source_url": "https://example.com/source",
                            "url": "https://example.com/article",
                            "time": "10:30",
                            "category": "富途",
                            "paragraphs": [" First ", ""],
                            "stock_tags": [" nvda "],
                            "country_tags": [" 美國 "],
                            "sector_tags": [" Technology "],
                        }
                    ],
                    "message": " ok ",
                    "meta": {"saved": 1},
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        monkeypatch.setattr(news_service, "resolve_news_cache_path", lambda: cache_path)

        payload = news_service.load_news_data()

        assert payload["cache_path"] == str(cache_path)
        assert payload["cache_origin"] == "external"
        assert payload["fetched_at"] == "2026-04-21 10:00 HKT"
        assert payload["message"] == "ok"
        assert payload["categories"] == ["富途"]
        assert payload["articles"][0]["source"] == "富途"
        assert payload["articles"][0]["category"] == "富途"
        assert payload["article_map"]["a1"]["stock_tags"] == ["NVDA"]
        assert payload["articles"][0]["paragraphs"] == ["First"]

    def test_loads_categories_from_article_values_when_payload_categories_missing(self, tmp_path, monkeypatch):
        reset_cache_state()
        cache_path = tmp_path / "futunn_cache.json"
        cache_path.write_text(
            json.dumps(
                {
                    "articles": [
                        {"id": "a1", "category": "宏觀"},
                        {"id": "a2", "category": "焦點"},
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        monkeypatch.setattr(news_service, "resolve_news_cache_path", lambda: cache_path)

        payload = news_service.load_news_data()

        assert payload["categories"] == ["宏觀", "焦點"]
