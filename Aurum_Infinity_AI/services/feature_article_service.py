from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


_SERVICE_DIR = Path(__file__).resolve().parent
_APP_DIR = _SERVICE_DIR.parent
_WORKSPACE_DIR = _APP_DIR.parent
_FEATURE_DIR = _WORKSPACE_DIR / "News_features"
_ARTICLE_DIR = _FEATURE_DIR / "articles"
_MANIFEST_PATH = _FEATURE_DIR / "manifest.json"


def slugify_feature(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value).strip().lower())
    return slug.strip("-")


def parse_feature_tags(raw_tags: str | list[str]) -> list[str]:
    if isinstance(raw_tags, list):
        values = raw_tags
    else:
        values = re.split(r"[,，\n]+", str(raw_tags))
    tags: list[str] = []
    for item in values:
        tag = str(item).strip()
        if tag and tag not in tags:
            tags.append(tag)
    return tags


def _read_manifest_items() -> list[dict[str, Any]]:
    if not _MANIFEST_PATH.exists():
        return []

    try:
        payload = json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    return payload if isinstance(payload, list) else []


def _write_manifest_items(items: list[dict[str, Any]]) -> None:
    _FEATURE_DIR.mkdir(parents=True, exist_ok=True)
    _ARTICLE_DIR.mkdir(parents=True, exist_ok=True)
    _MANIFEST_PATH.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_feature(item: dict[str, Any]) -> dict[str, Any] | None:
    slug = slugify_feature(item.get("slug", ""))
    html_file = str(item.get("html_file", "")).strip()
    title = str(item.get("title", "")).strip()
    if not slug or not html_file or not title:
        return None

    article_path = (_ARTICLE_DIR / html_file).resolve()
    try:
        article_path.relative_to(_ARTICLE_DIR.resolve())
    except ValueError:
        return None
    if not article_path.exists():
        return None

    tags = item.get("tags", [])
    if not isinstance(tags, list):
        tags = []

    return {
        "slug": slug,
        "title": title,
        "summary": str(item.get("summary", "")).strip(),
        "date": str(item.get("date", "")).strip(),
        "tags": [str(tag).strip() for tag in tags if str(tag).strip()],
        "html_file": html_file,
        "source": str(item.get("source", "4M 專題")).strip() or "4M 專題",
        "path": article_path,
    }


def load_feature_articles() -> list[dict[str, Any]]:
    features: list[dict[str, Any]] = []
    for item in _read_manifest_items():
        if not isinstance(item, dict):
            continue
        feature = _normalize_feature(item)
        if feature:
            features.append(feature)
    return features


def get_feature_article(slug: str) -> dict[str, Any] | None:
    normalized_slug = slugify_feature(slug)
    for feature in load_feature_articles():
        if feature["slug"] == normalized_slug:
            return feature
    return None


def get_feature_manifest_item(slug: str) -> dict[str, Any] | None:
    normalized_slug = slugify_feature(slug)
    for item in _read_manifest_items():
        if not isinstance(item, dict):
            continue
        if slugify_feature(item.get("slug", "")) == normalized_slug:
            item = dict(item)
            item["slug"] = normalized_slug
            item["tags"] = parse_feature_tags(item.get("tags", []))
            return item
    return None


def save_feature_article(
    *,
    slug: str,
    title: str,
    summary: str,
    date: str,
    tags: list[str] | str,
    source: str,
    html_bytes: bytes | None,
    original_slug: str | None = None,
) -> dict[str, Any]:
    normalized_slug = slugify_feature(slug)
    normalized_original_slug = slugify_feature(original_slug or normalized_slug)
    if not normalized_slug:
        raise ValueError("slug 不可為空")
    if not str(title).strip():
        raise ValueError("title 不可為空")

    items = _read_manifest_items()
    target_index = -1
    existing_item: dict[str, Any] | None = None
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        item_slug = slugify_feature(item.get("slug", ""))
        if item_slug == normalized_original_slug:
            target_index = index
            existing_item = dict(item)
        if item_slug == normalized_slug and item_slug != normalized_original_slug:
            raise ValueError("slug 已存在")

    html_file = f"{normalized_slug}.html"
    if html_bytes is None:
        if existing_item is None:
            raise ValueError("新增專題時必須上傳 HTML 檔案")
        html_file = str(existing_item.get("html_file", "")).strip() or html_file
    else:
        _ARTICLE_DIR.mkdir(parents=True, exist_ok=True)
        (_ARTICLE_DIR / html_file).write_bytes(html_bytes)

    record = {
        "slug": normalized_slug,
        "title": str(title).strip(),
        "summary": str(summary).strip(),
        "date": str(date).strip(),
        "tags": parse_feature_tags(tags),
        "html_file": html_file,
        "source": str(source).strip() or "4M 專題",
    }

    if target_index >= 0:
        items[target_index] = record
        old_html_file = str(existing_item.get("html_file", "")).strip() if existing_item else ""
        if html_bytes is not None and old_html_file and old_html_file != html_file:
            old_path = _ARTICLE_DIR / old_html_file
            if old_path.exists():
                old_path.unlink()
    else:
        items.insert(0, record)

    _write_manifest_items(items)
    feature = get_feature_article(normalized_slug)
    if feature is None:
        raise RuntimeError("專題文章儲存後讀取失敗")
    return feature


def delete_feature_article(slug: str) -> bool:
    normalized_slug = slugify_feature(slug)
    items = _read_manifest_items()
    remaining: list[dict[str, Any]] = []
    target_html_file = ""

    for item in items:
        if not isinstance(item, dict):
            continue
        item_slug = slugify_feature(item.get("slug", ""))
        if item_slug == normalized_slug:
            target_html_file = str(item.get("html_file", "")).strip()
            continue
        remaining.append(item)

    if len(remaining) == len(items):
        return False

    _write_manifest_items(remaining)
    if target_html_file:
        target_path = _ARTICLE_DIR / target_html_file
        if target_path.exists():
            target_path.unlink()
    return True
