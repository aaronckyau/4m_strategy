from __future__ import annotations

import json
from pathlib import Path
from threading import Lock
from typing import Callable

_MARKET_TAXONOMY_PATH = Path(__file__).resolve().parents[1] / "localization" / "market_taxonomy.json"
_MARKET_TAXONOMY_CACHE_LOCK = Lock()
_MARKET_TAXONOMY_CACHE = {
    "mtime_ns": None,
    "labels": {},
}


def _normalize_label_key(value: str | None) -> str:
    return str(value or "").strip()


def _coerce_market_taxonomy_labels(payload: object) -> dict[str, dict[str, str]]:
    if not isinstance(payload, dict):
        return {}

    labels = payload.get("labels")
    if not isinstance(labels, dict):
        return {}

    normalized: dict[str, dict[str, str]] = {}
    for key, translations in labels.items():
        normalized_key = _normalize_label_key(key)
        if not normalized_key or not isinstance(translations, dict):
            continue
        normalized[normalized_key] = {
            "zh_hk": str(translations.get("zh_hk") or "").strip(),
            "zh_cn": str(translations.get("zh_cn") or "").strip(),
        }
    return normalized


def _load_market_taxonomy_labels() -> dict[str, dict[str, str]]:
    try:
        mtime_ns = _MARKET_TAXONOMY_PATH.stat().st_mtime_ns
    except OSError:
        return {}

    with _MARKET_TAXONOMY_CACHE_LOCK:
        if _MARKET_TAXONOMY_CACHE["mtime_ns"] == mtime_ns:
            return _MARKET_TAXONOMY_CACHE["labels"]

        try:
            payload = json.loads(_MARKET_TAXONOMY_PATH.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            labels: dict[str, dict[str, str]] = {}
        else:
            labels = _coerce_market_taxonomy_labels(payload)

        _MARKET_TAXONOMY_CACHE["mtime_ns"] = mtime_ns
        _MARKET_TAXONOMY_CACHE["labels"] = labels
        return labels


def _resolve_market_label(key: str, lang: str) -> str:
    normalized_key = _normalize_label_key(key)
    if not normalized_key:
        return ""

    labels = _load_market_taxonomy_labels()
    translations = labels.get(normalized_key)
    if not translations:
        return ""

    if lang == "zh_cn":
        return translations.get("zh_cn") or translations.get("zh_hk") or ""
    return translations.get("zh_hk") or translations.get("zh_cn") or ""


def resolve_display_name(db_info: dict, lang: str, *, ticker: str = "") -> str:
    """Resolve the stock display name for the requested language."""
    stock_name = db_info.get("name") or ticker
    if lang == "zh_cn":
        return db_info.get("name_zh_cn") or stock_name
    return db_info.get("name_zh_hk") or stock_name


def resolve_sector_industry_display(
    db_info: dict,
    lang: str,
    translator: Callable[[str, str, str], tuple[str, str]],
) -> tuple[str, str]:
    """Resolve localized sector/industry labels from JSON overrides, then fallback translator."""
    sector_eng = db_info.get("sector") or ""
    industry_eng = db_info.get("industry") or ""
    sector_localized = _resolve_market_label(sector_eng, lang)
    industry_localized = _resolve_market_label(industry_eng, lang)

    if sector_localized and industry_localized:
        return sector_localized, industry_localized

    fallback_sector = sector_eng
    fallback_industry = industry_eng
    if sector_eng or industry_eng:
        fallback_sector, fallback_industry = translator(sector_eng, industry_eng, lang)

    return (
        sector_localized or fallback_sector,
        industry_localized or fallback_industry,
    )
