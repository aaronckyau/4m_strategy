"""
services/radar_topics_service.py — 每日熱門話題服務
============================================================================
功能：
  - 每天 06:00 HKT 呼叫 Gemini 取得當日宏觀熱門話題
  - 結果存入 cache/radar_topics/YYYYMMDD.json
  - get_today_topics(lang) 供 routes 讀取，自動 fallback 到靜態清單
============================================================================
"""
import json
import os
import re
from datetime import datetime, timezone, timedelta

from logger import get_logger

_log = get_logger(__name__)

# HKT = UTC+8
_HKT = timezone(timedelta(hours=8))

_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "cache", "radar_topics"
)

# 靜態 fallback（Gemini 失敗時使用）
_FALLBACK_TOPICS = {
    "zh_hk": [
        "伊朗美國衝突", "AI晶片禁令", "美國關稅戰", "能源危機",
        "聯儲局政策", "供應鏈重組", "中美科技戰", "美元走強",
    ],
    "zh_cn": [
        "伊朗美国冲突", "AI芯片禁令", "美国关税战", "能源危机",
        "联储局政策", "供应链重组", "中美科技战", "美元走强",
    ],
    "en": [
        "Iran-US Conflict", "AI Chip Ban", "US Tariff War", "Energy Crisis",
        "Fed Policy", "Supply Chain Shift", "US-China Tech War", "USD Strength",
    ],
}


def _today_hkt() -> str:
    """取得 HKT 今日日期字串 YYYYMMDD"""
    return datetime.now(_HKT).strftime('%Y%m%d')


def _cache_file(date_str: str) -> str:
    os.makedirs(_CACHE_DIR, exist_ok=True)
    return os.path.join(_CACHE_DIR, f"{date_str}.json")


def _load_cache(date_str: str) -> dict | None:
    path = _cache_file(date_str)
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        _log.warning("radar_topics 快取讀取失敗: %s", e)
        return None


def _save_cache(date_str: str, data: dict) -> None:
    path = _cache_file(date_str)
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        _log.info("radar_topics 快取已儲存: %s", path)
    except Exception as e:
        _log.warning("radar_topics 快取寫入失敗: %s", e)


def _extract_topics(text: str) -> dict | None:
    """從 Gemini 回傳文字中解析 <topics-data>...</topics-data> JSON"""
    m = re.search(r'<topics-data>([\s\S]*?)</topics-data>', text, re.IGNORECASE)
    if not m:
        _log.warning("radar_topics: 找不到 <topics-data> 標籤")
        return None
    try:
        data = json.loads(m.group(1).strip())
        # 驗證結構
        for lang in ('zh_hk', 'zh_cn', 'en'):
            if lang not in data or not isinstance(data[lang], list) or len(data[lang]) < 4:
                _log.warning("radar_topics: %s 欄位格式不正確", lang)
                return None
        return data
    except (json.JSONDecodeError, ValueError) as e:
        _log.warning("radar_topics JSON 解析失敗: %s", e)
        return None


def fetch_and_cache_topics() -> dict:
    """
    呼叫 Gemini 取得今日熱門話題並快取。
    成功回傳話題 dict，失敗回傳 fallback。
    此函式由 APScheduler 每日 06:00 HKT 呼叫。
    """
    date_str = _today_hkt()
    today_label = datetime.now(_HKT).strftime('%Y-%m-%d')

    _log.info("radar_topics: 開始抓取 %s 熱門話題", date_str)

    try:
        from extensions import prompt_manager
        from services.gemini_service import call_gemini_api

        try:
            prompt = prompt_manager.get_prompt('radar_topics', 'prompt').format(today=today_label)
        except Exception:
            prompt = (
                f"今日日期：{today_label}\n\n"
                "請用 Google Search 搜尋今日全球最重要的宏觀投資話題，"
                "包含地緣政治、央行政策、科技監管、能源、貿易衝突等領域。\n\n"
                "在 <topics-data>...</topics-data> 標籤內輸出 JSON：\n"
                '{{"zh_hk":["話題1",...8個],"zh_cn":["话题1",...8个],"en":["Topic1",...8]}}\n'
                "每個話題 10 字以內，必須反映今日真實新聞，只輸出 JSON 標籤不輸出其他內容。"
            )

        raw = call_gemini_api(prompt, use_search=True)

        if raw.startswith('⚠️'):
            _log.error("radar_topics Gemini API 失敗: %s", raw)
            return _FALLBACK_TOPICS

        topics = _extract_topics(raw)
        if topics is None:
            return _FALLBACK_TOPICS

        _save_cache(date_str, topics)
        _log.info("radar_topics: 成功更新 %s", date_str)
        return topics

    except Exception as e:
        _log.error("radar_topics fetch 發生未預期錯誤: %s", e)
        return _FALLBACK_TOPICS


def get_today_topics(lang: str = 'zh_hk') -> list:
    """
    取得今日熱門話題清單（供 routes 調用）。
    優先讀快取，快取不存在則用 fallback（不在 request 期間呼叫 Gemini）。
    """
    date_str = _today_hkt()
    cached = _load_cache(date_str)

    if cached and lang in cached and isinstance(cached[lang], list) and len(cached[lang]) >= 4:
        return cached[lang]

    _log.info("radar_topics: 快取不存在或格式錯誤，使用 fallback [%s]", lang)
    return _FALLBACK_TOPICS.get(lang, _FALLBACK_TOPICS['zh_hk'])
