"""
blueprints/news_radar/routes.py - 新聞投資雷達路由
============================================================================
路由：
  GET  /news-radar                  → 頁面
  POST /api/news-radar/analyze      → AI 分析端點
============================================================================
"""
import hashlib
import json
import os
import re
from datetime import datetime

import markdown
from flask import render_template, request, jsonify

from blueprints.news_radar import news_radar_bp
from config import Config
from extensions import prompt_manager
from services.gemini_service import call_gemini_api, extract_card_summary, strip_card_summary
from services.seeking_alpha_report_service import load_seeking_alpha_report
from translations import get_translations, SUPPORTED_LANGS, DEFAULT_LANG
from logger import get_logger
from utils.request_helpers import detect_lang_from_request

_log = get_logger(__name__)

_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "cache", "news_radar"
)
_CACHE_MAX_AGE_SECONDS = 6 * 60 * 60


# ============================================================================
# 工具函式
# ============================================================================

def get_current_lang() -> str:
    return detect_lang_from_request(
        supported_langs=SUPPORTED_LANGS,
        default_lang=DEFAULT_LANG,
    )


def get_today() -> str:
    return datetime.now().strftime('%Y/%m/%d')


def _cache_path(event_key: str, today: str, lang: str) -> str:
    os.makedirs(_CACHE_DIR, exist_ok=True)
    return os.path.join(_CACHE_DIR, f"{event_key}_{today}_{lang}.json")


def _cache_key(prompt: str, lang: str) -> str:
    payload = json.dumps(
        {
            "prompt": prompt,
            "lang": lang,
            "search": True,
            "schema_version": 2,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def _is_cache_fresh(cache_file: str) -> bool:
    try:
        age_seconds = datetime.now().timestamp() - os.path.getmtime(cache_file)
    except OSError:
        return False
    return age_seconds <= _CACHE_MAX_AGE_SECONDS


def extract_radar_data(text: str) -> dict | None:
    """從 AI 回傳文字中提取 <radar-data>...</radar-data> JSON"""
    m = re.search(r'<radar-data>([\s\S]*?)</radar-data>', text, re.IGNORECASE)
    if not m:
        return None
    try:
        return json.loads(m.group(1).strip())
    except (json.JSONDecodeError, ValueError) as e:
        _log.warning("radar-data JSON 解析失敗: %s", e)
        return None


def strip_radar_tags(text: str) -> str:
    """移除 <radar-data>、<card-summary> 標籤，保留其他內容"""
    text = re.sub(r'<radar-data>[\s\S]*?</radar-data>\s*', '', text, flags=re.IGNORECASE)
    text = re.sub(r'<card-summary>[\s\S]*?</card-summary>\s*', '', text, flags=re.IGNORECASE)
    return text.strip()


def _build_prompt(event_text: str, lang: str, today: str) -> str:
    try:
        tpl = prompt_manager.get_prompt('news_radar', 'prompt')
        return tpl.format(today=today, event_input=event_text, lang=lang)
    except Exception:
        # Fallback inline prompt
        lang_label = {'zh_hk': '繁體中文', 'zh_cn': '简体中文'}.get(lang, '繁體中文')
        return f"""你是一位頂尖的宏觀投資策略師，擅長分析地緣政治、宏觀經濟事件對全球股市的影響。

今日日期：{today}
用戶輸入的事件/話題：{event_text}
回覆語言：{lang_label}

請用 Google Search 搜尋此事件的最新發展，然後進行深度投資分析。

【輸出格式要求】
第一部分：在 <radar-data>...</radar-data> 標籤內輸出 JSON（格式見下方）
第二部分：在 <card-summary>...</card-summary> 輸出 50 字內摘要
第三部分：完整的{lang_label}分析報告（Markdown 格式）

JSON 格式：
{{
  "event_title": "事件簡短標題（20字內）",
  "event_summary": "事件背景描述（100字內）",
  "scenario_a": {{
    "label": "情況好轉／風險下降的情境名",
    "sectors": ["板塊1", "板塊2", "板塊3"],
    "picks": [
      {{"ticker": "代碼", "name": "公司名", "reason": "推薦理由（30字內）", "direction": "up"}}
    ]
  }},
  "scenario_b": {{
    "label": "情況惡化／風險上升的情境名",
    "sectors": ["板塊1", "板塊2", "板塊3"],
    "picks": [
      {{"ticker": "代碼", "name": "公司名", "reason": "推薦理由（30字內）", "direction": "up"}}
    ]
  }},
  "timeline": "影響時間軸描述",
  "risk_note": "主要風險提示",
  "score": 影響力評分1到10的整數
}}

要求：
- 每個情境推薦 3-5 支股票/ETF，必須包含真實美股 ticker 代碼
- 推薦必須有明確邏輯聯繫，避免泛泛而談
- 分析報告包含：事件深度解析、行業鏈路影響分析、時間軸預測、風險提示
- score 必須是純數字整數，不要字串"""


# ============================================================================
# 路由
# ============================================================================

@news_radar_bp.route('/news-radar')
def index():
    lang = get_current_lang()
    t = get_translations(lang)

    return render_template('news_radar/index.html',
                           lang=lang, t=t,
                           date=get_today(),
                           seeking_alpha_report=load_seeking_alpha_report())


@news_radar_bp.route('/api/news-radar/analyze', methods=['POST'])
def analyze():
    data = request.get_json(silent=True) or {}
    event_text = str(data.get('event', '')).strip()[:300]
    lang = data.get('lang', '')
    if lang not in SUPPORTED_LANGS:
        lang = get_current_lang()
    force = bool(data.get('force_update', False))

    if not event_text:
        return jsonify({'success': False, 'error': 'empty'})

    # 快取鍵
    prompt = _build_prompt(event_text, lang, datetime.now().strftime('%Y-%m-%d'))
    cache_key = _cache_key(prompt, lang)
    today = datetime.now().strftime('%Y%m%d')
    cache_file = _cache_path(cache_key, today, lang)

    if not force and os.path.exists(cache_file) and _is_cache_fresh(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached = json.load(f)
            _log.info("news_radar 快取命中: %s", cache_key)
            return jsonify({'success': True, 'from_cache': True, **cached})
        except Exception as e:
            _log.warning("快取讀取失敗，重新分析: %s", e)

    # 呼叫 Gemini
    _log.info("news_radar 分析開始: %s [%s]", event_text[:50], lang)
    prompt = _build_prompt(event_text, lang, datetime.now().strftime('%Y-%m-%d'))
    raw = call_gemini_api(prompt, use_search=True)

    if raw.startswith('⚠️'):
        return jsonify({'success': False, 'error': raw})

    # 解析結果
    radar_data = extract_radar_data(raw)
    summary = extract_card_summary(raw)
    report_text = strip_radar_tags(raw)

    try:
        report_html = markdown.markdown(
            report_text,
            extensions=['tables', 'nl2br']
        )
    except Exception:
        report_html = f'<p>{report_text}</p>'

    result = {
        'radar': radar_data,
        'report_html': report_html,
        'summary': summary,
    }

    # 寫入快取
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False)
    except Exception as e:
        _log.warning("快取寫入失敗: %s", e)

    return jsonify({'success': True, 'from_cache': False, **result})
