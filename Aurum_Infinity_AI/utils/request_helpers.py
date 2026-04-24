from __future__ import annotations

from flask import request


def detect_lang_from_request(
    *,
    supported_langs: list[str] | tuple[str, ...],
    default_lang: str,
) -> str:
    """Resolve request language from query, cookie, then Accept-Language."""
    param_lang = request.args.get("lang", "").strip()
    if param_lang in supported_langs:
        return param_lang

    cookie_lang = request.cookies.get("lang", "").strip()
    if cookie_lang in supported_langs:
        return cookie_lang

    accept = request.headers.get("Accept-Language", "")
    for segment in accept.replace(" ", "").split(","):
        code = segment.split(";")[0].lower()
        if code in ("zh-tw", "zh-hk"):
            return "zh_hk"
        if code in ("zh-cn", "zh"):
            return "zh_cn"

    return default_lang
