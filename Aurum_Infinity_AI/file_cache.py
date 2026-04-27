"""
File Cache Manager - 靜態 HTML 快取管理
============================================================================
取代 SQLite 資料庫，改用靜態檔案儲存分析結果。

目錄結構（每個 ticker 一個資料夾）：
  cache/
    AAPL/
      info.json       ← 股票基本資料（名稱、交易所、時間戳記）
      biz.html        ← 商業模式分析（HTML）
      exec.html       ← 管理層分析（HTML）
      finance.html    ← 財務分析（HTML）
      call.html       ← 會議展望（HTML）
      ta_price.html   ← 技術面分析（HTML）
      ta_analyst.html ← 分析師預測（HTML）
      ta_social.html  ← 社群情緒（HTML）
    0700_HK/          ← 注意：點號換底線（避免檔案系統問題）
      info.json
      ...
============================================================================
"""

import json
import os
import tempfile
import hashlib
from datetime import datetime
from typing import Optional
from logger import get_logger

_log = get_logger(__name__)

# 合法的分析區塊白名單
VALID_SECTIONS = {
    'biz', 'exec', 'finance', 'call',
    'ta_price', 'ta_analyst', 'ta_social',
    'ai_bull', 'ai_watch', 'ai_risk',
}

# 快取根目錄（與 app.py 同層）
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cache')


# ============================================================================
# 內部工具函數
# ============================================================================

def _safe_name(ticker: str) -> str:
    """
    將 ticker 轉為安全的資料夾名稱
    規則：只允許英數、點、底線、連字號，點號換底線，全部大寫
    例：0700.HK → 0700_HK，601899.SS → 601899_SS，AAPL → AAPL
    """
    import re
    sanitized = ticker.upper().strip()
    if not re.match(r'^[A-Z0-9._-]+$', sanitized):
        raise ValueError(f"Invalid ticker format: {ticker!r}")
    return sanitized.replace('.', '_')


def _ticker_dir(ticker: str) -> str:
    """取得 ticker 對應的快取資料夾路徑"""
    return os.path.join(CACHE_DIR, _safe_name(ticker))


def _info_path(ticker: str) -> str:
    """取得 info.json 的完整路徑"""
    return os.path.join(_ticker_dir(ticker), 'info.json')


def _html_path(ticker: str, section: str, lang: str = "") -> str:
    """
    取得分析區塊 HTML 檔案的完整路徑
    lang 為空時使用舊格式 {section}.html（向下相容）
    lang 有值時使用新格式 {section}_{lang}.html
    """
    filename = f'{section}_{lang}.html' if lang else f'{section}.html'
    return os.path.join(_ticker_dir(ticker), filename)


def _safe_read_json(path: str) -> Optional[dict]:
    """安全讀取 JSON 檔案，損毀時回傳 None 而非 crash"""
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, ValueError, OSError):
        _log.warning("JSON 檔案損毀，將重建 → %s", path)
        return None


def _atomic_write_json(path: str, data: dict):
    """原子寫入 JSON，先寫暫存檔再 rename，避免寫入中途損毀"""
    dir_name = os.path.dirname(path)
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)
    except Exception:
        # 清理暫存檔
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ============================================================================
# 公開 API
# ============================================================================

def get_stock(ticker: str) -> Optional[dict]:
    """
    讀取股票基本資料（info.json）

    Returns:
        包含 ticker / stock_name / chinese_name / exchange /
        created_at / updated_at 的字典；不存在則回傳 None
    """
    return _safe_read_json(_info_path(ticker))


def get_section_date(ticker: str, section: str, lang: str = "zh_hk") -> Optional[str]:
    """取得快取檔案的最後更新日期（YYYY/MM/DD），不存在回傳 None"""
    path = _html_path(ticker, section, lang)
    if os.path.exists(path):
        mtime = os.path.getmtime(path)
        return datetime.fromtimestamp(mtime).strftime('%Y/%m/%d')
    if lang == "zh_hk":
        legacy_path = _html_path(ticker, section)
        if os.path.exists(legacy_path):
            mtime = os.path.getmtime(legacy_path)
            return datetime.fromtimestamp(mtime).strftime('%Y/%m/%d')
    return None


def get_section_html(ticker: str, section: str, lang: str = "zh_hk") -> Optional[str]:
    """
    讀取特定分析區塊的 HTML 內容

    Args:
        ticker:  股票代碼
        section: 分析區塊名稱
        lang:    語言代碼（zh-TW / zh-CN / en），預設 zh-TW

    向下相容邏輯：
      1. 優先讀取 {section}_{lang}.html（新格式）
      2. 若 lang == "zh_hk" 且新格式不存在，fallback 讀舊的 {section}.html

    Returns:
        HTML 字串；尚未分析則回傳 None
    """
    # 優先嘗試新格式
    path = _html_path(ticker, section, lang)
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()

    # 繁中 fallback 到舊格式（保護現有快取）
    if lang == "zh_hk":
        legacy_path = _html_path(ticker, section)
        if os.path.exists(legacy_path):
            with open(legacy_path, 'r', encoding='utf-8') as f:
                return f.read()

    return None


def save_stock(ticker: str, stock_name: str, chinese_name: str, exchange: str):
    """
    儲存（或更新）股票基本資料到 info.json
    若 info.json 已存在，保留原有的 created_at 時間戳記
    """
    ticker_dir = _ticker_dir(ticker)
    os.makedirs(ticker_dir, exist_ok=True)

    now = datetime.now().isoformat()
    path = _info_path(ticker)

    # 保留原有的 created_at
    created_at = now
    existing = _safe_read_json(path)
    if existing:
        created_at = existing.get('created_at', now)

    data = {
        'ticker':       ticker.upper(),
        'stock_name':   stock_name,
        'chinese_name': chinese_name,
        'exchange':     exchange,
        'created_at':   created_at,
        'updated_at':   now,
    }

    _atomic_write_json(path, data)


def _md_path(ticker: str, section: str, lang: str = "") -> str:
    """
    取得分析區塊 Markdown 檔案的完整路徑
    lang 為空時使用舊格式 {section}.md（向下相容）
    lang 有值時使用新格式 {section}_{lang}.md
    """
    filename = f'{section}_{lang}.md' if lang else f'{section}.md'
    return os.path.join(_ticker_dir(ticker), filename)


def save_section_md(ticker: str, section: str, md_content: str, lang: str = "zh_hk"):
    """
    儲存分析區塊的 Markdown 結果

    Args:
        ticker:       股票代碼（已標準化）
        section:      分析區塊名稱，需在 VALID_SECTIONS 內
        md_content:   Markdown 字串
        lang:         語言代碼（zh_hk / zh_cn / en），預設 zh_hk
    """
    if section not in VALID_SECTIONS:
        raise ValueError(f"非法的 section: {section}")

    ticker_dir = _ticker_dir(ticker)
    os.makedirs(ticker_dir, exist_ok=True)

    # 寫入新格式 Markdown 檔案
    with open(_md_path(ticker, section, lang), 'w', encoding='utf-8') as f:
        f.write(md_content)

    # zh_hk 同時寫入舊格式，確保向下相容
    if lang == "zh_hk":
        with open(_md_path(ticker, section), 'w', encoding='utf-8') as f:
            f.write(md_content)


def get_section_md(ticker: str, section: str, lang: str = "zh_hk") -> Optional[str]:
    """
    讀取特定分析區塊的 Markdown 內容

    Args:
        ticker:  股票代碼
        section: 分析區塊名稱
        lang:    語言代碼（zh_hk / zh_cn / en），預設 zh_hk

    向下相容邏輯：
      1. 優先讀取 {section}_{lang}.md（新格式）
      2. 若 lang == "zh_hk" 且新格式不存在，fallback 讀舊的 {section}.md

    Returns:
        Markdown 字串；尚未分析則回傳 None
    """
    # 優先嘗試新格式
    path = _md_path(ticker, section, lang)
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()

    # 繁中 fallback 到舊格式（保護現有快取）
    if lang == "zh_hk":
        legacy_path = _md_path(ticker, section)
        if os.path.exists(legacy_path):
            with open(legacy_path, 'r', encoding='utf-8') as f:
                return f.read()

    return None


def save_section_html(ticker: str, section: str, html_content: str, lang: str = "zh_hk"):
    """
    儲存分析區塊的 HTML 結果
    同時更新 info.json 的 updated_at 時間戳記

    Args:
        ticker:       股票代碼（已標準化）
        section:      分析區塊名稱，需在 VALID_SECTIONS 內
        html_content: 已轉換好的 HTML 字串
        lang:         語言代碼（zh-TW / zh-CN / en），預設 zh-TW
                      zh-TW 會同時寫入舊格式 {section}.html（向下相容）
    """
    if section not in VALID_SECTIONS:
        raise ValueError(f"非法的 section: {section}")

    ticker_dir = _ticker_dir(ticker)
    os.makedirs(ticker_dir, exist_ok=True)

    # 寫入新格式 HTML 檔案
    with open(_html_path(ticker, section, lang), 'w', encoding='utf-8') as f:
        f.write(html_content)

    # zh-TW 同時寫入舊格式，確保現有快取查詢不中斷
    if lang == "zh_hk":
        with open(_html_path(ticker, section), 'w', encoding='utf-8') as f:
            f.write(html_content)

    # 更新 info.json 的 updated_at
    info_path = _info_path(ticker)
    data = _safe_read_json(info_path)
    if data:
        data['updated_at'] = datetime.now().isoformat()
        _atomic_write_json(info_path, data)


# ============================================================================
# Verdict 快取（AI 綜合評語）
# ============================================================================

def _verdict_path(ticker: str, lang: str, cache_key: str = "") -> str:
    """取得 verdict 快取檔路徑。"""
    suffix = ""
    if cache_key:
        digest = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()[:16]
        suffix = f"_{digest}"
    return os.path.join(_ticker_dir(ticker), f'verdict_{lang}{suffix}.txt')


def get_verdict(ticker: str, lang: str = "zh_hk", cache_key: str = "") -> Optional[str]:
    """
    讀取快取的 AI 綜合評語

    Returns:
        verdict 文字；不存在則回傳 None
    """
    path = _verdict_path(ticker, lang, cache_key)
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read().strip()
    except OSError:
        return None


def save_verdict(ticker: str, verdict: str, lang: str = "zh_hk", cache_key: str = ""):
    """
    儲存 AI 綜合評語到快取

    Args:
        ticker:  股票代碼
        verdict: AI 生成的評語文字
        lang:    語言代碼
    """
    ticker_dir = _ticker_dir(ticker)
    os.makedirs(ticker_dir, exist_ok=True)

    path = _verdict_path(ticker, lang, cache_key)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(verdict)


def clear_verdict(ticker: str):
    """
    清除某 ticker 的所有語言 verdict 快取。
    當任何 section 重新分析時呼叫，確保下次 verdict 會重新生成。
    """
    ticker_dir = _ticker_dir(ticker)
    if not os.path.isdir(ticker_dir):
        return
    for f in os.listdir(ticker_dir):
        if f.startswith('verdict_') and f.endswith('.txt'):
            try:
                os.remove(os.path.join(ticker_dir, f))
            except OSError:
                pass


def is_translation_stale(ticker: str, section: str, lang: str) -> bool:
    """
    判斷非繁中快取是否需要重新翻譯。
    條件：(1) 翻譯 HTML 與繁中 HTML 完全相同 (2) 無對應語言的 MD 快取
    """
    if lang == "zh_hk":
        return False
    cached_html = get_section_html(ticker, section, lang)
    if not cached_html:
        return False
    zh_html = get_section_html(ticker, section, "zh_hk")
    if zh_html and cached_html.strip() == zh_html.strip():
        return True
    if not get_section_md(ticker, section, lang):
        return True
    return False
