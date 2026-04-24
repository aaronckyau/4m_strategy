from __future__ import annotations

from flask import Flask

from utils.request_helpers import detect_lang_from_request


def _make_app():
    return Flask(__name__)


def test_detect_lang_from_request_prefers_query_param():
    app = _make_app()

    with app.test_request_context("/?lang=zh_cn", headers={"Accept-Language": "zh-TW,zh;q=0.9"}):
        lang = detect_lang_from_request(
            supported_langs=["zh_hk", "zh_cn"],
            default_lang="zh_hk",
        )

    assert lang == "zh_cn"


def test_detect_lang_from_request_uses_cookie_before_header():
    app = _make_app()

    with app.test_request_context("/", headers={"Cookie": "lang=zh_cn", "Accept-Language": "zh-TW,zh;q=0.9"}):
        lang = detect_lang_from_request(
            supported_langs=["zh_hk", "zh_cn"],
            default_lang="zh_hk",
        )

    assert lang == "zh_cn"


def test_detect_lang_from_request_falls_back_to_default_for_english_only():
    app = _make_app()

    with app.test_request_context("/", headers={"Accept-Language": "en-US,en;q=0.9"}):
        lang = detect_lang_from_request(
            supported_langs=["zh_hk", "zh_cn"],
            default_lang="zh_hk",
        )

    assert lang == "zh_hk"
