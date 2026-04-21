from __future__ import annotations

import json
import os
from pathlib import Path
from threading import Lock
from typing import Any

_SERVICE_DIR = Path(__file__).resolve().parent
_APP_DIR = _SERVICE_DIR.parent
_WORKSPACE_DIR = _APP_DIR.parent
_ENV_CACHE_PATH = os.getenv("FUTUNN_CACHE_PATH", "").strip()
_FETCHER_CACHE_PATH = _WORKSPACE_DIR / "News_fetcher" / "data" / "futunn_cache.json"
_LOCAL_CACHE_PATH = _WORKSPACE_DIR / "data" / "futunn_cache.json"
_SHARED_CACHE_PATH = _WORKSPACE_DIR.parent / "test" / "news" / "data" / "futunn_cache.json"
_MISSING_CACHE_MESSAGE = (
    "找不到新聞快取。請先準備 News_fetcher/data/futunn_cache.json，"
    "或保留 data/futunn_cache.json / test/news 現有的 futunn_cache.json。"
)

_cache_lock = Lock()
_cache_state: dict[str, Any] = {
    "path": None,
    "mtime": None,
    "data": None,
}
_DEFAULT_CATEGORY = "富途"
_DEFAULT_SOURCE = "富途"


def _candidate_paths() -> list[Path]:
    candidates: list[Path] = []
    if _ENV_CACHE_PATH:
        candidates.append(Path(_ENV_CACHE_PATH).expanduser())
    candidates.extend([_FETCHER_CACHE_PATH, _LOCAL_CACHE_PATH, _SHARED_CACHE_PATH])

    deduped: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path.resolve()) if path.exists() else str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def resolve_futunn_cache_path() -> Path | None:
    for path in _candidate_paths():
        if path.exists():
            return path
    return None


def _cache_origin(path: Path) -> str:
    try:
        path.resolve().relative_to(_WORKSPACE_DIR.resolve())
        return "workspace"
    except ValueError:
        return "external"


def _empty_payload(message: str) -> dict[str, Any]:
    return {
        "articles": [],
        "article_map": {},
        "categories": [],
        "fetched_at": "",
        "meta": {},
        "message": message,
        "cache_path": None,
        "cache_origin": "missing",
    }


def _normalize_articles(raw_articles: list[Any]) -> list[dict[str, Any]]:
    articles: list[dict[str, Any]] = []
    for item in raw_articles:
        if not isinstance(item, dict):
            continue

        article_id = str(item.get("id", "")).strip()
        if not article_id:
            continue

        article = dict(item)
        article["id"] = article_id
        article["title"] = str(article.get("title", "")).strip()
        article["summary"] = str(article.get("summary", "")).strip()
        article["source"] = str(article.get("source", "")).strip() or _DEFAULT_SOURCE
        article["source_url"] = str(article.get("source_url", "")).strip()
        article["url"] = str(article.get("url", "")).strip()
        article["time"] = str(article.get("time", "")).strip()
        article["category"] = str(article.get("category", "")).strip() or _DEFAULT_CATEGORY
        article["cover_image"] = str(article.get("cover_image", "")).strip() or None
        article["paragraphs"] = [
            str(paragraph).strip()
            for paragraph in article.get("paragraphs", [])
            if str(paragraph).strip()
        ]
        article["stock_tags"] = [
            str(ticker).strip().upper()
            for ticker in article.get("stock_tags", [])
            if str(ticker).strip()
        ]
        article["country_tags"] = [
            str(country).strip()
            for country in article.get("country_tags", [])
            if str(country).strip()
        ]
        article["sector_tags"] = [
            str(sector).strip()
            for sector in article.get("sector_tags", [])
            if str(sector).strip()
        ]
        articles.append(article)
    return articles


def load_futunn_data() -> dict[str, Any]:
    cache_path = resolve_futunn_cache_path()
    if cache_path is None:
        return _empty_payload(_MISSING_CACHE_MESSAGE)

    try:
        mtime = cache_path.stat().st_mtime
    except OSError as exc:
        return _empty_payload(f"無法讀取新聞快取：{exc}")

    with _cache_lock:
        if (
            _cache_state["data"] is not None
            and _cache_state["path"] == str(cache_path)
            and _cache_state["mtime"] == mtime
        ):
            return _cache_state["data"]

        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return _empty_payload(f"新聞快取格式錯誤：{exc}")

        articles = _normalize_articles(payload.get("articles", []))
        categories = payload.get("categories", [])
        if isinstance(categories, list):
            categories = [str(category).strip() for category in categories if str(category).strip()]
        else:
            categories = []
        if not categories:
            categories = sorted({article["category"] for article in articles})

        normalized = {
            **payload,
            "articles": articles,
            "article_map": {article["id"]: article for article in articles},
            "categories": categories,
            "fetched_at": str(payload.get("fetched_at", "")).strip(),
            "meta": payload.get("meta", {}) if isinstance(payload.get("meta"), dict) else {},
            "message": str(payload.get("message", "")).strip(),
            "cache_path": str(cache_path),
            "cache_origin": _cache_origin(cache_path),
        }

        _cache_state["path"] = str(cache_path)
        _cache_state["mtime"] = mtime
        _cache_state["data"] = normalized
        return normalized
