from __future__ import annotations

import json
from pathlib import Path
from typing import Any


_SERVICE_DIR = Path(__file__).resolve().parent
_APP_DIR = _SERVICE_DIR.parent
_WORKSPACE_DIR = _APP_DIR.parent
_FEATURE_DIR = _WORKSPACE_DIR / "News_features"
_ARTICLE_DIR = _FEATURE_DIR / "articles"
_MANIFEST_PATH = _FEATURE_DIR / "manifest.json"


def _normalize_feature(item: dict[str, Any]) -> dict[str, Any] | None:
    slug = str(item.get("slug", "")).strip()
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
    if not _MANIFEST_PATH.exists():
        return []

    try:
        payload = json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []

    features: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        feature = _normalize_feature(item)
        if feature:
            features.append(feature)
    return features


def get_feature_article(slug: str) -> dict[str, Any] | None:
    normalized_slug = str(slug).strip()
    for feature in load_feature_articles():
        if feature["slug"] == normalized_slug:
            return feature
    return None
