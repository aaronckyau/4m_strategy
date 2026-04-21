from __future__ import annotations

import pytest

from News_fetcher.schema_validator import load_cache_schema, validate_cache_payload


def build_valid_payload() -> dict:
    return {
        "fetched_at": "2026-04-21 10:00 HKT",
        "categories": ["富途"],
        "articles": [
            {
                "id": "art-1",
                "title": "Title",
                "summary": "Summary",
                "summary_raw": "Summary raw",
                "paragraphs": ["Paragraph 1", "Paragraph 2"],
                "paragraphs_raw": ["Paragraph 1", "Paragraph 2"],
                "source": "富途",
                "source_url": "https://example.com/source",
                "url": "https://example.com/article",
                "time": "12:30",
                "category": "富途",
                "cover_image": "https://example.com/image.jpg",
                "stock_tags": ["AAPL", "TSLA"],
                "country_tags": ["美國"],
                "sector_tags": ["科技"],
                "ai_rewrite_raw": {"ok": True},
                "ai_rewrite_error": None,
            }
        ],
        "message": "",
        "meta": {
            "list_url": "https://example.com/list",
            "requested": 1,
            "saved": 1,
            "total_articles": 1,
            "new_articles": 1,
            "max_articles": 100,
            "skipped_flash": 0,
            "failures": [],
        },
    }


class TestValidateCachePayload:
    def test_valid_payload_passes(self):
        payload = build_valid_payload()

        validate_cache_payload(payload)

    def test_missing_required_field_raises(self):
        payload = build_valid_payload()
        del payload["meta"]

        with pytest.raises(ValueError, match="根層"):
            validate_cache_payload(payload)

    def test_meta_numeric_field_must_be_int(self):
        payload = build_valid_payload()
        payload["meta"]["requested"] = "1"

        with pytest.raises(ValueError, match="meta.requested"):
            validate_cache_payload(payload)

    def test_unknown_sector_is_rejected(self):
        payload = build_valid_payload()
        payload["articles"][0]["sector_tags"] = ["NotARealSector"]

        with pytest.raises(ValueError, match="sector_tags"):
            validate_cache_payload(payload)

    def test_stock_tags_limit_is_enforced(self):
        schema = load_cache_schema()
        max_items = int(schema["limits"]["stock_tags_max"])
        payload = build_valid_payload()
        payload["articles"][0]["stock_tags"] = [f"TICK{i}" for i in range(max_items + 1)]

        with pytest.raises(ValueError, match="stock_tags"):
            validate_cache_payload(payload, schema=schema)
