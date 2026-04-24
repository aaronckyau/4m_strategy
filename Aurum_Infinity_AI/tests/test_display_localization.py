from __future__ import annotations

import json

from utils import display_localization
from utils.display_localization import (
    resolve_display_name,
    resolve_sector_industry_display,
)


def test_resolve_display_name_prefers_requested_chinese_variant():
    db_info = {
        "name": "Apple",
        "name_zh_hk": "蘋果",
        "name_zh_cn": "苹果",
    }

    assert resolve_display_name(db_info, "zh_hk", ticker="AAPL") == "蘋果"
    assert resolve_display_name(db_info, "zh_cn", ticker="AAPL") == "苹果"


def test_resolve_display_name_falls_back_to_english_name():
    db_info = {
        "name": "Apple",
        "name_zh_hk": "",
        "name_zh_cn": "",
    }

    assert resolve_display_name(db_info, "zh_hk", ticker="AAPL") == "Apple"
    assert resolve_display_name(db_info, "zh_cn", ticker="AAPL") == "Apple"


def test_resolve_sector_industry_display_uses_translator_callback():
    display_localization._MARKET_TAXONOMY_CACHE["mtime_ns"] = None
    display_localization._MARKET_TAXONOMY_CACHE["labels"] = {}
    db_info = {
        "sector": "Unmapped Sector",
        "industry": "Internet",
    }

    result = resolve_sector_industry_display(
        db_info,
        "zh_hk",
        lambda sector, industry, lang: (f"{sector}:{lang}", f"{industry}:{lang}"),
    )

    assert result == ("Unmapped Sector:zh_hk", "Internet:zh_hk")


def test_resolve_sector_industry_display_prefers_json_overrides(tmp_path, monkeypatch):
    taxonomy_path = tmp_path / "market_taxonomy.json"
    taxonomy_path.write_text(
        json.dumps(
            {
                "labels": {
                    "Technology": {"zh_hk": "資訊科技", "zh_cn": "信息技術"},
                    "Internet": {"zh_hk": "互聯網", "zh_cn": "互联网"},
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(display_localization, "_MARKET_TAXONOMY_PATH", taxonomy_path)
    display_localization._MARKET_TAXONOMY_CACHE["mtime_ns"] = None
    display_localization._MARKET_TAXONOMY_CACHE["labels"] = {}

    result = resolve_sector_industry_display(
        {"sector": "Technology", "industry": "Internet"},
        "zh_hk",
        lambda sector, industry, lang: ("fallback-sector", "fallback-industry"),
    )

    assert result == ("資訊科技", "互聯網")


def test_resolve_sector_industry_display_reload_json_after_file_change(tmp_path, monkeypatch):
    taxonomy_path = tmp_path / "market_taxonomy.json"
    taxonomy_path.write_text(
        json.dumps(
            {"labels": {"Technology": {"zh_hk": "科技", "zh_cn": "科技"}}},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(display_localization, "_MARKET_TAXONOMY_PATH", taxonomy_path)
    display_localization._MARKET_TAXONOMY_CACHE["mtime_ns"] = None
    display_localization._MARKET_TAXONOMY_CACHE["labels"] = {}

    first = resolve_sector_industry_display(
        {"sector": "Technology", "industry": ""},
        "zh_hk",
        lambda sector, industry, lang: (sector, industry),
    )

    taxonomy_path.write_text(
        json.dumps(
            {"labels": {"Technology": {"zh_hk": "資訊科技", "zh_cn": "信息技術"}}},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    display_localization._MARKET_TAXONOMY_CACHE["mtime_ns"] = None

    second = resolve_sector_industry_display(
        {"sector": "Technology", "industry": ""},
        "zh_hk",
        lambda sector, industry, lang: (sector, industry),
    )

    assert first == ("科技", "")
    assert second == ("資訊科技", "")
