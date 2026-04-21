from __future__ import annotations

from pathlib import Path

import translations


def test_translations_keep_utf8_chinese_strings():
    assert translations.TRANSLATIONS["zh_hk"]["nav_stock"] == "股票分析"
    assert translations.TRANSLATIONS["zh_hk"]["nav_lang_setting"] == "語言設定"
    assert translations.TRANSLATIONS["zh_cn"]["nav_stock"] == "股票分析"
    assert translations.TRANSLATIONS["zh_cn"]["nav_lang_setting"] == "语言设置"


def test_translations_file_has_no_english_block_or_mojibake():
    path = Path(__file__).resolve().parents[1] / "translations.py"
    text = path.read_text(encoding="utf-8")

    assert '"en": {' not in text
    assert "股票分析" in text
    assert "语言设置" in text
    assert "è‚¡" not in text
    assert "èªž" not in text
