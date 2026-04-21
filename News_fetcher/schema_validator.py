from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_SCHEMA_PATH = Path(__file__).resolve().parent / "schema" / "futunn_cache_schema.json"


def load_cache_schema(path: Path | None = None) -> dict[str, Any]:
    schema_path = path or DEFAULT_SCHEMA_PATH
    return json.loads(schema_path.read_text(encoding="utf-8"))


def allowed_sectors(schema: dict[str, Any] | None = None) -> list[str]:
    active_schema = schema or load_cache_schema()
    return [str(item).strip() for item in active_schema.get("allowed_sectors", []) if str(item).strip()]


def _require_keys(payload: dict[str, Any], keys: list[str], scope: str) -> None:
    missing = [key for key in keys if key not in payload]
    if missing:
        raise ValueError(f"{scope} 缺少欄位：{', '.join(missing)}")


def _validate_string_list(values: Any, field_name: str, max_items: int) -> list[str]:
    if not isinstance(values, list):
        raise ValueError(f"{field_name} 必須是陣列")

    cleaned: list[str] = []
    for item in values:
        value = str(item).strip()
        if not value or value in cleaned:
            continue
        cleaned.append(value)

    if len(cleaned) > max_items:
        raise ValueError(f"{field_name} 超過上限 {max_items}")
    return cleaned


def validate_cache_payload(payload: dict[str, Any], schema: dict[str, Any] | None = None) -> None:
    active_schema = schema or load_cache_schema()

    if not isinstance(payload, dict):
        raise ValueError("快取內容必須是物件")

    _require_keys(payload, list(active_schema.get("required_top_level_fields", [])), "根層")

    categories = payload.get("categories")
    if not isinstance(categories, list):
        raise ValueError("categories 必須是陣列")

    allowed_categories = {
        str(item).strip() for item in active_schema.get("allowed_categories", []) if str(item).strip()
    }
    if any(str(item).strip() not in allowed_categories for item in categories if str(item).strip()):
        raise ValueError("categories 含未允許分類")

    meta = payload.get("meta")
    if not isinstance(meta, dict):
        raise ValueError("meta 必須是物件")
    _require_keys(meta, list(active_schema.get("required_meta_fields", [])), "meta")
    for field_name in ("requested", "saved", "total_articles", "new_articles", "max_articles", "skipped_flash"):
        if not isinstance(meta.get(field_name), int):
            raise ValueError(f"meta.{field_name} 必須是整數")
    if not isinstance(meta.get("failures"), list):
        raise ValueError("meta.failures 必須是陣列")

    articles = payload.get("articles")
    if not isinstance(articles, list):
        raise ValueError("articles 必須是陣列")

    limits = active_schema.get("limits", {})
    sector_allowlist = set(allowed_sectors(active_schema))
    required_article_fields = list(active_schema.get("required_article_fields", []))

    for index, article in enumerate(articles):
        if not isinstance(article, dict):
            raise ValueError(f"articles[{index}] 必須是物件")
        _require_keys(article, required_article_fields, f"articles[{index}]")

        for field_name in (
            "id",
            "title",
            "summary",
            "summary_raw",
            "source",
            "source_url",
            "url",
            "time",
            "category",
        ):
            if not isinstance(article.get(field_name), str):
                raise ValueError(f"articles[{index}].{field_name} 必須是字串")

        if article.get("category") not in allowed_categories:
            raise ValueError(f"articles[{index}].category 不在允許列表內")

        if article.get("cover_image") is not None and not isinstance(article.get("cover_image"), str):
            raise ValueError(f"articles[{index}].cover_image 必須是字串或 null")
        if article.get("ai_rewrite_raw") is not None and not isinstance(article.get("ai_rewrite_raw"), dict):
            raise ValueError(f"articles[{index}].ai_rewrite_raw 必須是物件或 null")
        if article.get("ai_rewrite_error") is not None and not isinstance(article.get("ai_rewrite_error"), str):
            raise ValueError(f"articles[{index}].ai_rewrite_error 必須是字串或 null")

        _validate_string_list(
            article.get("paragraphs"),
            f"articles[{index}].paragraphs",
            10_000,
        )
        _validate_string_list(
            article.get("paragraphs_raw"),
            f"articles[{index}].paragraphs_raw",
            10_000,
        )
        _validate_string_list(
            article.get("stock_tags"),
            f"articles[{index}].stock_tags",
            int(limits.get("stock_tags_max", 5)),
        )
        _validate_string_list(
            article.get("country_tags"),
            f"articles[{index}].country_tags",
            int(limits.get("country_tags_max", 5)),
        )
        sectors = _validate_string_list(
            article.get("sector_tags"),
            f"articles[{index}].sector_tags",
            int(limits.get("sector_tags_max", 11)),
        )
        if any(sector not in sector_allowlist for sector in sectors):
            raise ValueError(f"articles[{index}].sector_tags 含未允許板塊")
