from __future__ import annotations

from types import SimpleNamespace

import pytest

from News_fetcher import refresh_futunn_cache as futunn_cache


def build_article(article_id: str, **overrides) -> dict:
    article = {
        "id": article_id,
        "title": "Article",
        "summary": "Summary",
        "summary_raw": "Summary",
        "paragraphs": ["Paragraph"],
        "paragraphs_raw": ["Paragraph"],
        "url": f"https://example.com/{article_id}",
    }
    article.update(overrides)
    return article


class DummyResponse:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class DummyClient:
    def __init__(self, response_text: str):
        self.response_text = response_text
        self.models = SimpleNamespace(generate_content=self.generate_content)

    def generate_content(self, **_kwargs):
        return SimpleNamespace(text=self.response_text)


class TestFetchHtml:
    def test_fetch_html_retries_when_waf_token_is_present(self, monkeypatch):
        responses = iter(
            [
                DummyResponse('<title>Document</title><script>wafToken="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.abc.def"</script>'),
                DummyResponse("<html>ok</html>"),
            ]
        )
        cookies = SimpleNamespace(set=lambda *args, **kwargs: None)
        session = SimpleNamespace(get=lambda *_args, **_kwargs: next(responses), cookies=cookies)
        monkeypatch.setattr(futunn_cache, "HTTP_SESSION", session)

        html = futunn_cache.fetch_html("https://example.com")

        assert html == "<html>ok</html>"

    def test_fetch_html_raises_when_waf_token_is_missing(self, monkeypatch):
        session = SimpleNamespace(
            get=lambda *_args, **_kwargs: DummyResponse("<title>Document</title>wafToken="),
            cookies=SimpleNamespace(set=lambda *args, **kwargs: None),
        )
        monkeypatch.setattr(futunn_cache, "HTTP_SESSION", session)

        with pytest.raises(RuntimeError, match="no wafToken"):
            futunn_cache.fetch_html("https://example.com")


class TestListingAndPublishRules:
    def test_parse_listing_keeps_unique_links_in_order(self):
        html = """
        https://news.futunn.com/hk/post/123/alpha
        https://news.futunn.com/hk/post/123/alpha
        https://news.futunn.com/hk/post/456/beta
        """

        links = futunn_cache.parse_listing(html)

        assert links == [
            "https://news.futunn.com/hk/post/123/alpha",
            "https://news.futunn.com/hk/post/456/beta",
        ]

    def test_ensure_publishable_rejects_too_few_articles(self):
        payload = {"articles": [build_article("a1")]}

        with pytest.raises(RuntimeError, match="minimum"):
            futunn_cache.ensure_publishable(payload)

    def test_ensure_publishable_rejects_document_title(self):
        payload = {
            "articles": [build_article(str(index)) for index in range(futunn_cache.MIN_SUCCESS_ARTICLES - 1)]
            + [build_article("bad", title="Document")]
        }

        with pytest.raises(RuntimeError, match="Document title"):
            futunn_cache.ensure_publishable(payload)

    def test_merge_with_existing_articles_deduplicates_and_counts_new(self, monkeypatch):
        monkeypatch.setattr(
            futunn_cache,
            "load_existing_articles",
            lambda: [build_article("old"), build_article("shared"), build_article("older")],
        )
        new_articles = [build_article("new"), build_article("shared"), build_article("fresh")]

        merged, new_count = futunn_cache.merge_with_existing_articles(new_articles)

        assert [article["id"] for article in merged] == ["new", "shared", "fresh", "old", "older"]
        assert new_count == 2


class TestAnalyzeAiBatch:
    def test_analyze_ai_batch_sanitizes_tags_and_keeps_items_by_id(self):
        client = DummyClient(
            """
            {"items":[
              {
                "id":"a1",
                "summary":" AI summary ",
                "paragraphs":[" First ", "", "Second "],
                "stock_tags":[" tsla ", "BRK.B", "nvda"],
                "country_tags":[" 美國 ", "", "美國"],
                "sector_tags":["科技", "Unknown"]
              }
            ]}
            """
        )

        _raw, result_map = futunn_cache.analyze_ai_batch(client, [build_article("a1")])

        assert result_map["a1"]["summary"] == "AI summary"
        assert result_map["a1"]["paragraphs"] == ["First", "Second"]
        assert result_map["a1"]["stock_tags"] == ["TSLA", "BRKB", "NVDA"]
        assert result_map["a1"]["country_tags"] == ["美國"]
        assert result_map["a1"]["sector_tags"] == ["科技"]
