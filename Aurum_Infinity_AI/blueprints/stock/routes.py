"""
blueprints/stock/routes.py - 股票分析路由
============================================================================
從 app.py 搬移的所有股票相關路由。
============================================================================
"""
import json
import os
import re
import sqlite3
import threading
import time
from datetime import datetime

import markdown
import requests
from flask import render_template, request, jsonify, redirect, abort, make_response

from blueprints.stock import stock_bp
from config import Config
from extensions import prompt_manager
from services.gemini_service import call_gemini_api, extract_card_summary, strip_card_summary
from file_cache import (
    get_stock, get_section_html, get_section_md,
    save_stock, save_section_html, save_section_md,
    get_verdict, save_verdict, clear_verdict, VALID_SECTIONS,
    is_translation_stale, get_section_date,
)
from read_stock_code import normalize_ticker, get_canonical_ticker, get_stock_info, search_stocks
from database import get_db
from translations import get_translations, SUPPORTED_LANGS, DEFAULT_LANG
from logger import get_logger

_log = get_logger(__name__)

# ── Sector / Industry i18n ──────────────────────────────────
_DB_PATH = os.environ.get("DATABASE_URL") or os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "aurum.db"
)

_I18N_COLUMNS = frozenset({"zh_hk", "zh_cn"})
_MARKET_INDEX_SYMBOLS = ("^DJI", "^IXIC", "^GSPC")
_MARKET_INDEX_LABELS = {
    "^DJI": "Dow Jones",
    "^IXIC": "NASDAQ",
    "^GSPC": "S&P 500",
}
_SECTOR_ETFS = (
    ("Communication Services", "XLC"),
    ("Consumer Discretionary", "XLY"),
    ("Consumer Staples", "XLP"),
    ("Energy", "XLE"),
    ("Financials", "XLF"),
    ("Health Care", "XLV"),
    ("Industrials", "XLI"),
    ("Materials", "XLB"),
    ("Real Estate", "XLRE"),
    ("Technology", "XLK"),
    ("Utilities", "XLU"),
)
_MARKET_INDEX_CACHE_TTL = 5
_market_indices_cache_lock = threading.Lock()
_market_indices_cache = {
    "data": None,
    "updated_at": None,
    "expires_at": 0.0,
}
_SP500_SYNC_MAX_AGE_SECONDS = 12 * 60 * 60
_sp500_sync_lock = threading.Lock()
_SP500_HEATMAP_CACHE_TTL = 900
_sp500_heatmap_cache_lock = threading.Lock()
_sp500_heatmap_cache = {
    "data": None,
    "updated_at": None,
    "expires_at": 0.0,
}
_SECTOR_PERFORMANCE_CACHE_TTL = 3600
_sector_performance_cache_lock = threading.Lock()
_sector_performance_cache = {
    "data": None,
    "updated_at": None,
    "expires_at": 0.0,
}


def _get_sector_industry_i18n(sector_eng: str, industry_eng: str, lang: str) -> tuple[str, str]:
    """根據語言回傳翻譯後的 sector / industry，找不到則回傳英文"""
    if lang == "en" or not (sector_eng or industry_eng):
        return sector_eng, industry_eng
    col = "zh_hk" if lang == "zh_hk" else "zh_cn"
    if col not in _I18N_COLUMNS:
        return sector_eng, industry_eng
    conn = sqlite3.connect(str(_DB_PATH))
    try:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        sector_out = sector_eng
        industry_out = industry_eng
        if sector_eng:
            c.execute(f"SELECT {col} FROM sector_industry_i18n WHERE key = ?", (sector_eng,))
            row = c.fetchone()
            if row and row[0]:
                sector_out = row[0]
        if industry_eng:
            c.execute(f"SELECT {col} FROM sector_industry_i18n WHERE key = ?", (industry_eng,))
            row = c.fetchone()
            if row and row[0]:
                industry_out = row[0]
        return sector_out, industry_out
    except (sqlite3.Error, OSError):
        return sector_eng, industry_eng
    finally:
        conn.close()

# 競態條件防護：per-ticker 鎖，防止同一股票同時觸發多次 Gemini API 呼叫
_analysis_locks_guard = threading.Lock()
_analysis_locks: dict[str, threading.Lock] = {}


def _get_analysis_lock(key: str) -> threading.Lock:
    """取得指定 key 的分析鎖（thread-safe）"""
    with _analysis_locks_guard:
        lock = _analysis_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _analysis_locks[key] = lock
        return lock


# ============================================================================
# 安全防護：Ticker 格式白名單驗證
# ============================================================================

_TICKER_PATTERN = re.compile(
    r'^(?:'
    r'[A-Z]{1,5}'                   # 純英文美股：AAPL、NVDA
    r'|[A-Z]{1,4}\.[A-Z]{1,2}'     # 英文含後綴：BRK.B
    r'|\d{1,6}\.[A-Z]{2,3}'        # 數字代碼（帶後綴）：0700.HK、601899.SS
    r'|\d{1,6}'                     # 純數字代碼（無後綴）：605196、00139、700
    r')$',
    re.IGNORECASE
)

_STATIC_BLACKLIST = frozenset([
    'favicon.ico', 'robots.txt', 'sitemap.xml',
    'apple-touch-icon.png', 'manifest.json',
])


def is_valid_ticker(raw: str) -> bool:
    """驗證輸入是否為合法股票代碼格式"""
    if not raw or len(raw) > 12:
        return False
    if not re.match(r'^[A-Za-z0-9.]+$', raw):
        return False
    if raw.startswith('.'):
        return False
    return bool(_TICKER_PATTERN.match(raw.upper()))


def resolve_ticker(raw: str) -> str:
    """將用戶輸入解析成 JSON 資料庫的官方代碼"""
    normalized = normalize_ticker(raw)
    canonical  = get_canonical_ticker(normalized) or get_canonical_ticker(raw)
    return canonical if canonical else normalized


def get_current_lang() -> str:
    """偵測當前請求的語言偏好"""
    param_lang = request.args.get('lang', '').strip()
    if param_lang in SUPPORTED_LANGS:
        return param_lang

    cookie_lang = request.cookies.get('lang', '').strip()
    if cookie_lang in SUPPORTED_LANGS:
        return cookie_lang

    accept = request.headers.get('Accept-Language', '')
    for segment in accept.replace(' ', '').split(','):
        code = segment.split(';')[0].lower()
        if code in ('zh-tw', 'zh-hk'):
            return 'zh_hk'
        if code in ('zh-cn', 'zh'):
            return 'zh_cn'
        if code.startswith('en'):
            return 'en'

    return DEFAULT_LANG


def get_today() -> str:
    return datetime.now().strftime('%Y/%m/%d')


def _fetch_market_indices() -> list[dict]:
    """從 FMP 取得 Dow / Nasdaq 即時資料。"""
    if not Config.FMP_API_KEY:
        raise RuntimeError("FMP_API_KEY not configured")

    headers = {"apikey": Config.FMP_API_KEY}
    rows_by_symbol: dict[str, dict] = {}

    batch_resp = requests.get(
        "https://financialmodelingprep.com/stable/batch-index-quotes",
        headers=headers,
        timeout=8,
    )
    batch_resp.raise_for_status()
    payload = batch_resp.json()

    if isinstance(payload, list):
        for row in payload:
            symbol = str(row.get("symbol", "")).upper()
            if symbol in _MARKET_INDEX_SYMBOLS:
                rows_by_symbol[symbol] = row

    for symbol in _MARKET_INDEX_SYMBOLS:
        if symbol in rows_by_symbol:
            continue
        quote_resp = requests.get(
            "https://financialmodelingprep.com/stable/quote",
            params={"symbol": symbol},
            headers=headers,
            timeout=8,
        )
        quote_resp.raise_for_status()
        quote_payload = quote_resp.json()
        if isinstance(quote_payload, list) and quote_payload:
            rows_by_symbol[symbol] = quote_payload[0]

    results = []
    for symbol in _MARKET_INDEX_SYMBOLS:
        row = rows_by_symbol.get(symbol)
        if not row:
            continue
        results.append({
            "symbol": symbol,
            "label": _MARKET_INDEX_LABELS[symbol],
            "name": row.get("name") or _MARKET_INDEX_LABELS[symbol],
            "price": row.get("price"),
            "change": row.get("change"),
            "changes_percentage": row.get("changesPercentage") or row.get("changePercentage"),
            "day_low": row.get("dayLow") or row.get("low"),
            "day_high": row.get("dayHigh") or row.get("high"),
            "year_low": row.get("yearLow"),
            "year_high": row.get("yearHigh"),
            "volume": row.get("volume"),
            "timestamp": row.get("timestamp"),
            "exchange": row.get("exchange"),
        })
    return results


def _get_cached_market_indices() -> tuple[list[dict], str]:
    """回傳共享快取中的市場資料，快取過期才向 FMP 取一次。"""
    now = time.time()
    with _market_indices_cache_lock:
        if _market_indices_cache["data"] and now < _market_indices_cache["expires_at"]:
            return _market_indices_cache["data"], _market_indices_cache["updated_at"]

        data = _fetch_market_indices()
        updated_at = datetime.utcnow().isoformat() + "Z"
        _market_indices_cache["data"] = data
        _market_indices_cache["updated_at"] = updated_at
        _market_indices_cache["expires_at"] = now + _MARKET_INDEX_CACHE_TTL
        return data, updated_at


def _fetch_sector_etf_history(symbol: str, timeseries: int = 40) -> list[dict]:
    """Fetch historical EOD prices for a sector ETF from local DB."""
    conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT date, close, adj_close, volume
            FROM ohlc_daily
            WHERE ticker = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (symbol, timeseries),
        ).fetchall()
    finally:
        conn.close()

    return [
        {
            "date": row["date"],
            "close": row["close"],
            "adjClose": row["adj_close"],
            "volume": row["volume"],
        }
        for row in rows
    ]


def _calc_period_return(history: list[dict], offset: int) -> float | None:
    """Calculate return versus N trading sessions ago."""
    if len(history) <= offset:
        return None

    latest = history[0]
    previous = history[offset]
    latest_close = latest.get("adjClose", latest.get("close"))
    previous_close = previous.get("adjClose", previous.get("close"))
    if latest_close in (None, 0) or previous_close in (None, 0):
        return None
    return round(((latest_close - previous_close) / previous_close) * 100.0, 2)


def _fetch_sector_performance() -> dict:
    """Build 1D / 1W / 1M sector performance from local OHLC data."""
    datasets = {
        "1d": {"label": "1 DAY PERFORMANCE", "offset": 1, "items": []},
        "1w": {"label": "1 WEEK PERFORMANCE", "offset": 5, "items": []},
        "1m": {"label": "1 MONTH PERFORMANCE", "offset": 21, "items": []},
    }
    latest_date = None

    for sector_name, symbol in _SECTOR_ETFS:
        history = _fetch_sector_etf_history(symbol)
        if not history:
            datasets["1d"]["items"].append({"sector": sector_name, "symbol": symbol, "performance": None})
            datasets["1w"]["items"].append({"sector": sector_name, "symbol": symbol, "performance": None})
            datasets["1m"]["items"].append({"sector": sector_name, "symbol": symbol, "performance": None})
            continue
        if latest_date is None:
            latest_date = history[0].get("date")

        for config in datasets.values():
            performance = _calc_period_return(history, config["offset"])
            config["items"].append({
                "sector": sector_name,
                "symbol": symbol,
                "performance": performance,
            })

    for config in datasets.values():
        config["items"].sort(
            key=lambda item: (
                item["performance"] is None,
                -(item["performance"] or 0),
                item["sector"],
            )
        )

    return {
        "periods": [datasets["1d"], datasets["1w"], datasets["1m"]],
        "latest_price_date": latest_date,
    }


def _get_cached_sector_performance() -> tuple[dict, str]:
    """Return cached sector ETF performance data."""
    now = time.time()
    with _sector_performance_cache_lock:
        if _sector_performance_cache["data"] and now < _sector_performance_cache["expires_at"]:
            return _sector_performance_cache["data"], _sector_performance_cache["updated_at"]

    data = _fetch_sector_performance()
    updated_at = datetime.utcnow().isoformat() + "Z"
    with _sector_performance_cache_lock:
        _sector_performance_cache["data"] = data
        _sector_performance_cache["updated_at"] = updated_at
        _sector_performance_cache["expires_at"] = now + _SECTOR_PERFORMANCE_CACHE_TTL
    return data, updated_at


# ============================================================================
# 路由
# ============================================================================

def _fetch_sp500_constituents() -> list[dict]:
    """Fetch the current S&P 500 constituent list from FMP."""
    if not Config.FMP_API_KEY:
        raise RuntimeError("FMP_API_KEY not configured")

    response = requests.get(
        "https://financialmodelingprep.com/stable/sp500-constituent",
        headers={"apikey": Config.FMP_API_KEY},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("Invalid S&P 500 constituents payload")
    return payload


def _sync_sp500_constituents(force: bool = False) -> str | None:
    """Refresh local S&P 500 constituents table when stale."""
    now = time.time()
    with _sp500_sync_lock:
        conn = get_db()
        try:
            latest_row = conn.execute(
                "SELECT fetched_at FROM sp500_constituents ORDER BY fetched_at DESC LIMIT 1"
            ).fetchone()
            if latest_row and latest_row["fetched_at"] and not force:
                try:
                    fetched_ts = datetime.fromisoformat(
                        latest_row["fetched_at"].replace("Z", "+00:00")
                    ).timestamp()
                except ValueError:
                    fetched_ts = 0.0
                if fetched_ts and now - fetched_ts < _SP500_SYNC_MAX_AGE_SECONDS:
                    return latest_row["fetched_at"]

            payload = _fetch_sp500_constituents()
            fetched_at = datetime.utcnow().isoformat() + "Z"
            rows = []
            for item in payload:
                raw_symbol = str(item.get("symbol", "")).strip()
                if not raw_symbol:
                    continue
                ticker = get_canonical_ticker(raw_symbol) or normalize_ticker(raw_symbol)
                rows.append((
                    ticker,
                    item.get("name"),
                    item.get("sector"),
                    item.get("subSector"),
                    item.get("headQuarter"),
                    item.get("dateFirstAdded"),
                    item.get("cik"),
                    item.get("founded"),
                    fetched_at,
                ))

            if not rows:
                raise ValueError("Empty S&P 500 constituents payload")

            conn.execute("BEGIN")
            conn.execute("DELETE FROM sp500_constituents")
            conn.executemany(
                """
                INSERT INTO sp500_constituents (
                    ticker, company_name, sector, sub_sector,
                    headquarters, date_first_added, cik, founded, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
            return fetched_at
        finally:
            conn.close()


def _query_sp500_heatmap() -> dict:
    """Build S&P 500 heatmap payload from the local database."""
    conn = get_db()
    try:
        latest_sync = conn.execute(
            "SELECT fetched_at FROM sp500_constituents ORDER BY fetched_at DESC LIMIT 1"
        ).fetchone()
        count_row = conn.execute(
            "SELECT COUNT(*) AS count FROM sp500_constituents"
        ).fetchone()

        rows = conn.execute(
            """
            WITH latest_dates AS (
                SELECT
                    s.ticker,
                    (
                        SELECT o1.date
                        FROM ohlc_daily o1
                        WHERE o1.ticker = s.ticker
                        ORDER BY o1.date DESC
                        LIMIT 1
                    ) AS latest_date,
                    (
                        SELECT o2.date
                        FROM ohlc_daily o2
                        WHERE o2.ticker = s.ticker
                        ORDER BY o2.date DESC
                        LIMIT 1 OFFSET 1
                    ) AS previous_date
                FROM sp500_constituents s
            ),
            latest_prices AS (
                SELECT
                    d.ticker,
                    d.latest_date,
                    d.previous_date,
                    latest.close AS latest_close,
                    previous.close AS previous_close
                FROM latest_dates d
                LEFT JOIN ohlc_daily latest
                    ON latest.ticker = d.ticker
                   AND latest.date = d.latest_date
                LEFT JOIN ohlc_daily previous
                    ON previous.ticker = d.ticker
                   AND previous.date = d.previous_date
            )
            SELECT
                s.ticker,
                COALESCE(sm.name, s.company_name, s.ticker) AS name,
                COALESCE(sm.sector, s.sector, 'Unknown') AS sector,
                COALESCE(sm.industry, s.sub_sector, 'Unknown') AS industry,
                sm.market_cap AS market_cap,
                lp.latest_date,
                lp.previous_date,
                lp.latest_close,
                lp.previous_close,
                CASE
                    WHEN lp.previous_close IS NOT NULL AND lp.previous_close != 0
                    THEN ROUND(((lp.latest_close - lp.previous_close) / lp.previous_close) * 100.0, 4)
                    ELSE NULL
                END AS change_pct,
                CASE
                    WHEN lp.previous_close IS NOT NULL
                    THEN ROUND(lp.latest_close - lp.previous_close, 4)
                    ELSE NULL
                END AS change_value
            FROM sp500_constituents s
            LEFT JOIN stocks_master sm
                ON sm.ticker = s.ticker
            LEFT JOIN latest_prices lp
                ON lp.ticker = s.ticker
            ORDER BY COALESCE(sm.market_cap, 0) DESC, s.ticker ASC
            """
        ).fetchall()
    finally:
        conn.close()

    stocks = []
    latest_dates = set()
    for row in rows:
        market_cap = row["market_cap"]
        if market_cap is None or market_cap <= 0:
            continue
        if row["latest_date"]:
            latest_dates.add(row["latest_date"])
        stocks.append({
            "ticker": row["ticker"],
            "name": row["name"] or row["ticker"],
            "sector": row["sector"] or "Unknown",
            "industry": row["industry"] or "Unknown",
            "market_cap": market_cap,
            "price": row["latest_close"],
            "change_pct": row["change_pct"],
            "change_value": row["change_value"],
            "latest_date": row["latest_date"],
            "previous_date": row["previous_date"],
        })

    return {
        "stocks": stocks,
        "rendered_count": len(stocks),
        "constituent_count": count_row["count"] if count_row else 0,
        "synced_at": latest_sync["fetched_at"] if latest_sync else None,
        "latest_ohlc_date": max(latest_dates) if latest_dates else None,
    }


def _get_cached_sp500_heatmap() -> tuple[dict, str]:
    """Return shared cached S&P 500 heatmap data."""
    now = time.time()
    with _sp500_heatmap_cache_lock:
        if _sp500_heatmap_cache["data"] and now < _sp500_heatmap_cache["expires_at"]:
            return _sp500_heatmap_cache["data"], _sp500_heatmap_cache["updated_at"]

    _sync_sp500_constituents(force=False)
    data = _query_sp500_heatmap()
    updated_at = datetime.utcnow().isoformat() + "Z"

    with _sp500_heatmap_cache_lock:
        _sp500_heatmap_cache["data"] = data
        _sp500_heatmap_cache["updated_at"] = updated_at
        _sp500_heatmap_cache["expires_at"] = now + _SP500_HEATMAP_CACHE_TTL
    return data, updated_at


@stock_bp.route('/')
def home():
    return redirect(f'/{Config.DEFAULT_TICKER}')


@stock_bp.route('/markets')
def markets():
    """美股指數頁面"""
    lang = get_current_lang()
    t = get_translations(lang)
    return render_template('markets/index.html', lang=lang, t=t, date=get_today())


@stock_bp.route('/<ticker_raw>')
def index(ticker_raw: str):
    """股票分析主頁"""

    # ── 第一層：靜態資源黑名單 ────────────────────────────────
    if ticker_raw.lower() in _STATIC_BLACKLIST:
        abort(404)

    # ── 第二層：格式白名單 ────────────────────────────────────
    if not is_valid_ticker(ticker_raw):
        abort(404)

    # ── 第三層：解析成官方代碼 ────────────────────────────────
    ticker = resolve_ticker(ticker_raw)

    # ── 第四層：301 跳轉到標準 URL ────────────────────────────
    if ticker != ticker_raw.upper():
        return redirect(f'/{ticker}', code=301)

    # ── 讀取快取 ─────────────────────────────────────────────
    db_info = get_stock_info(ticker)

    if db_info is None:
        lang = get_current_lang()
        t    = get_translations(lang)
        return render_template('stock/error.html', ticker=ticker_raw, date=get_today(), lang=lang, t=t), 404

    # 確保 file cache 存在（供其他功能使用）
    if not get_stock(ticker):
        save_stock(ticker=ticker, stock_name=db_info["name"] or ticker,
                   chinese_name=db_info["name_zh_hk"] or db_info["name"] or ticker,
                   exchange=db_info["exchange"] or "")
        _log.info("儲存新股票基本資料 %s -> cache/", ticker)

    lang = get_current_lang()
    t    = get_translations(lang)

    # 根據用戶語言偏好選擇顯示名稱
    # 優先用中文名（zh_hk → name_zh_hk，zh_cn → name_zh_cn），fallback 到英文名
    stock_name = db_info["name"] or ticker
    if lang == "zh_hk":
        display_name = db_info["name_zh_hk"] or stock_name
    elif lang == "zh_cn":
        display_name = db_info["name_zh_cn"] or stock_name
    else:
        # 若 lang 非預期值，預設用 zh_hk
        display_name = db_info["name_zh_hk"] or stock_name
    chinese_name = display_name

    # Sector / Industry i18n
    sector_eng   = db_info.get("sector") or ""
    industry_eng = db_info.get("industry") or ""
    sector_i18n, industry_i18n = _get_sector_industry_i18n(sector_eng, industry_eng, lang)

    # ── Markdown 格式 ────────────────────────────────────────
    if request.args.get('md'):
        md_sections = {}
        for section in VALID_SECTIONS:
            md = get_section_md(ticker, section, lang)
            if md:
                md_sections[section] = md

        if not md_sections:
            return jsonify({"error": f"找不到 {ticker} 的 Markdown 快取"}), 404

        if request.args.get('download'):
            combined_md = f"# {stock_name} ({ticker}) — 完整分析報告\n\n"
            combined_md += f"**生成日期**: {get_today()}\n"
            combined_md += f"**語言**: {lang}\n\n---\n\n"

            for section, md_content in md_sections.items():
                section_name = prompt_manager.get_section_names().get(section, section)
                combined_md += f"\n## {section_name}\n\n{md_content}\n\n---\n\n"

            response = make_response(combined_md)
            response.headers['Content-Type'] = 'text/markdown; charset=utf-8'
            response.headers['Content-Disposition'] = f'attachment; filename="{ticker}_analysis.md"'
            return response
        else:
            combined_html = f"<h1>{stock_name} ({ticker}) — 完整分析報告</h1>\n"
            combined_html += f"<p><strong>生成日期</strong>: {get_today()} | <strong>語言</strong>: {lang}</p>\n"
            combined_html += '<hr>\n'

            for section, md_content in md_sections.items():
                section_name = prompt_manager.get_section_names().get(section, section)
                section_html = markdown.markdown(
                    md_content,
                    extensions=['tables', 'fenced_code', 'nl2br']
                )
                combined_html += f"\n<h2>{section_name}</h2>\n{section_html}\n<hr>\n"

            return render_template(
                'stock/markdown_viewer.html',
                ticker=ticker,
                stock_name=stock_name,
                content=combined_html,
                download_url=f'/{ticker}?md&download=true&lang={lang}',
                date=get_today(),
                lang=lang,
                t=t,
            )

    return render_template(
        'stock/index.html',
        ticker=ticker,
        stock_name=stock_name,
        chinese_name=chinese_name,
        sector=sector_i18n,
        industry=industry_i18n,
        lang=lang,
        t=t,
    )


@stock_bp.route('/api/stock_display')
def api_stock_display():
    """回傳股票的多語顯示資訊（公司名稱 + sector + industry）"""
    ticker = request.args.get('ticker', '').strip()
    lang   = request.args.get('lang', 'zh_hk').strip()
    if not ticker:
        return jsonify({}), 400

    db_info = get_stock_info(ticker)
    if db_info is None:
        return jsonify({}), 404

    if lang == "zh_hk":
        display_name = db_info["name_zh_hk"] or db_info["name"] or ticker
    elif lang == "zh_cn":
        display_name = db_info["name_zh_cn"] or db_info["name"] or ticker
    else:
        display_name = db_info["name"] or ticker

    sector_eng   = db_info.get("sector") or ""
    industry_eng = db_info.get("industry") or ""
    sector, industry = _get_sector_industry_i18n(sector_eng, industry_eng, lang)

    return jsonify({
        "display_name": display_name,
        "sector": sector,
        "industry": industry,
    })



@stock_bp.route('/api/etf-holders/<ticker>')
def api_etf_holders(ticker: str):
    """回傳持有該股票的 ETF 清單（來自 etf_holdings 表），Top 15 by weight_pct"""
    ticker = ticker.upper().strip()
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT h.etf_symbol, e.name, h.weight_pct, h.market_value, e.aum, e.avg_volume
            FROM etf_holdings h
            LEFT JOIN etf_list e ON e.symbol = h.etf_symbol
            WHERE h.asset = ?
            ORDER BY h.weight_pct DESC
            LIMIT 15
        """, (ticker,)).fetchall()
    finally:
        conn.close()

    if not rows:
        return jsonify({"etfs": []})

    etfs = []
    for r in rows:
        etfs.append({
            "symbol":      r["etf_symbol"],
            "name":        r["name"] or r["etf_symbol"],
            "weight_pct":  round(r["weight_pct"], 4) if r["weight_pct"] else None,
            "market_value": r["market_value"],
            "aum":         r["aum"],
            "avg_volume":  r["avg_volume"],
        })
    return jsonify({"etfs": etfs})


@stock_bp.route('/api/etf-detail/<etf_symbol>')
def api_etf_detail(etf_symbol: str):
    """回傳某 ETF 的持倉清單（Top 20 by weight_pct）+ ETF 基本資料"""
    etf_symbol = etf_symbol.upper().strip()
    conn = get_db()
    try:
        info = conn.execute(
            "SELECT symbol, name, aum, avg_volume, expense_ratio, asset_class FROM etf_list WHERE symbol = ?",
            (etf_symbol,)
        ).fetchone()

        rows = conn.execute("""
            SELECT asset, name, weight_pct, market_value, shares
            FROM etf_holdings
            WHERE etf_symbol = ?
            ORDER BY weight_pct DESC
            LIMIT 20
        """, (etf_symbol,)).fetchall()

        total_rows = conn.execute(
            "SELECT COUNT(*) FROM etf_holdings WHERE etf_symbol = ?",
            (etf_symbol,)
        ).fetchone()[0]
    finally:
        conn.close()

    holdings = []
    weight_shown = 0.0
    for r in rows:
        w = r["weight_pct"] or 0
        weight_shown += w
        holdings.append({
            "asset":        r["asset"],
            "name":         r["name"] or r["asset"],
            "weight_pct":   round(w, 4),
            "market_value": r["market_value"],
            "shares":       r["shares"],
        })

    # Others = remaining weight
    others_pct = max(0, round(100 - weight_shown, 2))

    etf_info = {}
    if info:
        etf_info = {
            "symbol":       info["symbol"],
            "name":         info["name"] or etf_symbol,
            "aum":          info["aum"],
            "avg_volume":   info["avg_volume"],
            "expense_ratio":info["expense_ratio"],
            "asset_class":  info["asset_class"],
        }

    return jsonify({
        "etf":        etf_info,
        "holdings":   holdings,
        "others_pct": others_pct,
        "total_count": total_rows,
    })

@stock_bp.route('/api/search_stock')
def search_stock():
    """股票代碼自動完成 API"""
    query = request.args.get('q', '').strip()
    if not query or len(query) < 1:
        return jsonify([])
    lang = get_current_lang()
    results = search_stocks(query, limit=8)
    for r in results:
        # 根據用戶語言偏好顯示對應中文名稱（支持 zh_hk, zh_cn）
        if lang == "zh_hk":
            r["display_name"] = r["name_zh_hk"] or r["name"]
        elif lang == "zh_cn":
            r["display_name"] = r["name_zh_cn"] or r["name"]
    return jsonify(results)


@stock_bp.route('/api/markdown/<ticker_raw>/<section>')
def api_markdown_section(ticker_raw: str, section: str):
    """REST API — 取得單一 section 的 Markdown"""
    if not is_valid_ticker(ticker_raw):
        abort(404)
    if section not in VALID_SECTIONS:
        abort(404)

    ticker = resolve_ticker(ticker_raw)
    lang = request.args.get('lang', DEFAULT_LANG).strip()
    if lang not in SUPPORTED_LANGS:
        lang = DEFAULT_LANG

    md_content = get_section_md(ticker, section, lang)
    if not md_content:
        abort(404)

    response = make_response(md_content)
    response.headers['Content-Type'] = 'text/markdown; charset=utf-8'
    response.headers['Content-Disposition'] = f'attachment; filename="{ticker}_{section}_{lang}.md"'
    return response


@stock_bp.route('/api/markdown/<ticker_raw>')
def api_markdown_combined(ticker_raw: str):
    """REST API — 取得多個 sections 合併的 Markdown"""
    if not is_valid_ticker(ticker_raw):
        abort(404)

    ticker = resolve_ticker(ticker_raw)
    lang = request.args.get('lang', DEFAULT_LANG).strip()
    if lang not in SUPPORTED_LANGS:
        lang = DEFAULT_LANG

    sections_param = request.args.get('sections', '').strip()
    if sections_param:
        requested_sections = [s.strip() for s in sections_param.split(',') if s.strip()]
        requested_sections = [s for s in requested_sections if s in VALID_SECTIONS]
    else:
        requested_sections = list(VALID_SECTIONS)

    if not requested_sections:
        abort(404)

    md_sections = {}
    for section in requested_sections:
        md = get_section_md(ticker, section, lang)
        if md:
            md_sections[section] = md

    if not md_sections:
        abort(404)

    stock_info = get_stock(ticker)
    stock_name = stock_info['stock_name'] if stock_info else ticker

    combined_md = f"# {stock_name} ({ticker}) — 完整分析報告\n\n"
    combined_md += f"**生成日期**: {get_today()}\n"
    combined_md += f"**語言**: {lang}\n\n---\n\n"

    for section in requested_sections:
        if section in md_sections:
            section_name = prompt_manager.get_section_names().get(section, section)
            combined_md += f"\n## {section_name}\n\n{md_sections[section]}\n\n---\n\n"

    response = make_response(combined_md)
    response.headers['Content-Type'] = 'text/markdown; charset=utf-8'
    response.headers['Content-Disposition'] = f'attachment; filename="{ticker}_analysis_{lang}.md"'
    return response


@stock_bp.route('/analyze/<section>', methods=['POST'])
def analyze_section(section: str):
    """AI 分析 API"""
    if section not in VALID_SECTIONS:
        return jsonify({"success": False, "error": "非法的分析類別"}), 400

    if not request.json:
        return jsonify({"success": False, "error": "請求格式錯誤，需要 JSON"}), 400

    raw_ticker   = request.json.get('ticker', '')
    force_update = request.json.get('force_update', False)
    lang         = request.json.get('lang', DEFAULT_LANG)
    if lang not in SUPPORTED_LANGS:
        lang = DEFAULT_LANG

    if not is_valid_ticker(raw_ticker):
        return jsonify({"success": False, "error": "無效的股票代碼格式"}), 400

    ticker = resolve_ticker(raw_ticker)

    # ── 1. 檢查目標語言快取 ───────────────────────────────────
    if not force_update:
        cached_html = get_section_html(ticker, section, lang)
        if cached_html and is_translation_stale(ticker, section, lang):
            _log.info("%s - %s (%s) 翻譯快取已過期，將重新翻譯", ticker, section, lang)
            cached_html = None

        if cached_html:
            _log.info("從快取讀取 %s - %s (%s)", ticker, section, lang)
            cached_md = get_section_md(ticker, section, lang)
            cached_summary = extract_card_summary(cached_md) if cached_md else None
            return jsonify({"success": True, "report": cached_html, "from_cache": True,
                            "summary": cached_summary,
                            "cache_date": get_section_date(ticker, section, lang)})

    db_info = get_stock_info(ticker)
    if db_info is None:
        return jsonify({"success": False, "error": f"找不到 {ticker} 的資料"}), 404

    stock_name   = db_info["name"] or ticker
    chinese_name = db_info["name_zh_hk"] or stock_name
    exchange     = db_info["exchange"] or ""
    if not get_stock(ticker):
        save_stock(ticker, stock_name, chinese_name, exchange)

    lock = _get_analysis_lock(f"{ticker}:{section}")
    if not lock.acquire(timeout=0.1):
        return jsonify({"success": False, "error": "該分析正在進行中，請稍候"}), 409

    try:
        # 取得鎖後再檢查一次快取（可能在等待期間已有結果）
        if not force_update:
            cached_html = get_section_html(ticker, section, lang)
            if cached_html and not is_translation_stale(ticker, section, lang):
                _log.info("鎖內快取命中 %s - %s (%s)", ticker, section, lang)
                cached_md = get_section_md(ticker, section, lang)
                cached_summary = extract_card_summary(cached_md) if cached_md else None
                return jsonify({"success": True, "report": cached_html, "from_cache": True,
                                "summary": cached_summary,
                                "cache_date": get_section_date(ticker, section, lang)})

        # ── 2. 確保有繁中版本 ────────────────────────────────────
        zh_tw_html = get_section_html(ticker, section, "zh_hk")

        if not zh_tw_html or force_update:
            prompt = prompt_manager.build(
                section=section,
                ticker=ticker,
                stock_name=stock_name,
                exchange=exchange,
                today=get_today(),
                chinese_name=chinese_name,
            )
            _log.info("呼叫 Gemini AI 分析 %s - %s (zh-TW)", ticker, section)
            response_text = call_gemini_api(prompt, use_search=True)

            zh_summary = extract_card_summary(response_text)

            save_section_md(ticker, section, response_text, lang="zh_hk")
            _log.info("已儲存 Markdown %s - %s → zh-TW", ticker, section)

            zh_tw_html = markdown.markdown(
                response_text,
                extensions=['tables', 'fenced_code', 'nl2br']
            )
            zh_tw_html = strip_card_summary(zh_tw_html)
            save_section_html(ticker, section, zh_tw_html, lang="zh_hk")
            clear_verdict(ticker)  # section 更新後清除 verdict 快取
            _log.info("已儲存 HTML %s - %s → zh-TW", ticker, section)

        # ── 3. 繁中直接回傳 ──────────────────────────────────────
        if lang == "zh_hk":
            return jsonify({"success": True, "report": zh_tw_html, "from_cache": False,
                            "summary": locals().get('zh_summary'),
                            "cache_date": get_section_date(ticker, section, lang)})

        # ── 4. 其他語言：翻譯 ────────────────────────────────────
        _log.info("翻譯 %s - %s → %s", ticker, section, lang)
        # 取得繁中摘要，加回 HTML 頂部讓翻譯 prompt 一起翻譯
        zh_summary_text = locals().get('zh_summary') or ''
        if not zh_summary_text:
            zh_md = get_section_md(ticker, section, "zh_hk")
            zh_summary_text = extract_card_summary(zh_md) if zh_md else ''
        source_for_translation = zh_tw_html
        if zh_summary_text:
            source_for_translation = f'<card-summary>{zh_summary_text}</card-summary>\n{zh_tw_html}'
        translation_prompt = prompt_manager.build_translation_prompt(source_for_translation, lang)
        translated_text    = call_gemini_api(translation_prompt, use_search=False)

        if translated_text.strip().startswith('<'):
            translated_html = translated_text
        else:
            translated_html = markdown.markdown(
                translated_text,
                extensions=['tables', 'fenced_code', 'nl2br']
            )

        translated_summary = extract_card_summary(translated_html)
        translated_html = strip_card_summary(translated_html)
        save_section_html(ticker, section, translated_html, lang=lang)
        save_section_md(ticker, section, translated_text, lang=lang)
        _log.info("已儲存 %s - %s → %s", ticker, section, lang)

        return jsonify({"success": True, "report": translated_html, "from_cache": False,
                        "summary": translated_summary,
                        "cache_date": get_section_date(ticker, section, lang)})

    except Exception as e:
        _log.error("analyze_section %s/%s: %s", ticker, section, e)
        return jsonify({"success": False, "error": "伺服器內部錯誤，請稍後重試"}), 500
    finally:
        lock.release()


@stock_bp.route('/api/translations')
def api_translations():
    """回傳指定語言的翻譯字典 JSON（供前端語言切換用）"""
    lang = request.args.get('lang', DEFAULT_LANG).strip()
    if lang not in SUPPORTED_LANGS:
        lang = DEFAULT_LANG
    t = get_translations(lang)
    return jsonify(t)


@stock_bp.route('/api/key-metrics')
def api_key_metrics():
    """關鍵指標 API — 從 ratios_ttm + financial_statements 讀取"""
    ticker = request.args.get('symbol', '').strip().upper()
    if not ticker or not is_valid_ticker(ticker):
        return jsonify({"error": "Invalid ticker"}), 400
    ticker = resolve_ticker(ticker)

    conn = get_db()
    try:
        # stocks_master: market_cap, currency
        stock = conn.execute(
            "SELECT market_cap, currency FROM stocks_master WHERE ticker = ?", (ticker,)
        ).fetchone()

        # ratios_ttm: PE, PEG, margins, ROE, D/E, dividend yield, EPS
        ratios = conn.execute(
            "SELECT pe, peg, eps, gross_margin, net_margin, "
            "       debt_to_equity, dividend_yield, dividend_per_share "
            "FROM ratios_ttm WHERE ticker = ?", (ticker,)
        ).fetchone()

        # financial_statements: 最新 income 的 Revenue + YoY
        income = conn.execute(
            "SELECT revenue, period, fiscal_year, fiscal_quarter "
            "FROM financial_statements WHERE ticker = ? AND statement_type = 'income' "
            "ORDER BY period DESC LIMIT 1",
            (ticker,)
        ).fetchone()

        # 營收 YoY: 最近兩個同季度比較
        revenue_yoy = None
        if income and income['fiscal_quarter']:
            prev = conn.execute(
                "SELECT revenue FROM financial_statements "
                "WHERE ticker = ? AND statement_type = 'income' "
                "AND fiscal_quarter = ? AND fiscal_year = ? LIMIT 1",
                (ticker, income['fiscal_quarter'],
                 (income['fiscal_year'] or 0) - 1)
            ).fetchone()
            if prev and prev['revenue'] and income['revenue'] and prev['revenue'] != 0:
                revenue_yoy = round(
                    (income['revenue'] - prev['revenue']) / abs(prev['revenue']) * 100, 1)

        # ohlc_daily: 最新收盤價
        price_rows = conn.execute(
            "SELECT close, date FROM ohlc_daily WHERE ticker = ? ORDER BY date DESC LIMIT 2",
            (ticker,)
        ).fetchall()
    finally:
        conn.close()

    data = {}

    if price_rows:
        latest_price = price_rows[0]
        data['price'] = latest_price['close']
        data['price_date'] = latest_price['date']
        if len(price_rows) > 1 and latest_price['close'] is not None:
            previous_close = price_rows[1]['close']
            if previous_close not in (None, 0):
                change = latest_price['close'] - previous_close
                data['change'] = change
                data['change_pct'] = change / abs(previous_close) * 100

    if stock:
        if stock['market_cap']:
            data['market_cap'] = stock['market_cap']
        if stock['currency']:
            data['currency'] = stock['currency']

    if ratios:
        for key in ('pe', 'peg', 'eps', 'gross_margin', 'net_margin',
                     'debt_to_equity', 'dividend_yield', 'dividend_per_share'):
            v = ratios[key]
            if v is not None:
                data[key] = v

    if income:
        if income['revenue'] is not None:
            data['revenue'] = income['revenue']
        data['period'] = income['period']
        fy = income['fiscal_year'] or ''
        fq = f"Q{income['fiscal_quarter']}" if income['fiscal_quarter'] else ''
        data['fiscal'] = f"FY{fy} {fq}".strip()

    if revenue_yoy is not None:
        data['revenue_yoy'] = revenue_yoy

    return jsonify(data)


@stock_bp.route('/api/market-indices')
def api_market_indices():
    """Dow / Nasdaq 即時資料 API"""
    try:
        data, updated_at = _get_cached_market_indices()
    except RuntimeError as e:
        _log.error("market_indices config error: %s", e)
        return jsonify({"error": "FMP API key not configured"}), 500
    except requests.RequestException as e:
        _log.warning("market_indices request failed: %s", e)
        return jsonify({"error": "Failed to fetch market indices"}), 502
    except ValueError as e:
        _log.warning("market_indices payload invalid: %s", e)
        return jsonify({"error": "Invalid market indices payload"}), 502

    return jsonify({
        "indices": data,
        "updated_at": updated_at,
        "cache_ttl": _MARKET_INDEX_CACHE_TTL,
    })


@stock_bp.route('/api/sector-performance')
def api_sector_performance():
    """Sector ETF performance API."""
    try:
        data, updated_at = _get_cached_sector_performance()
    except RuntimeError as e:
        _log.error("sector_performance config error: %s", e)
        return jsonify({"error": "FMP API key not configured"}), 500
    except requests.RequestException as e:
        _log.warning("sector_performance request failed: %s", e)
        return jsonify({"error": "Failed to fetch sector performance"}), 502
    except ValueError as e:
        _log.warning("sector_performance payload invalid: %s", e)
        return jsonify({"error": "Invalid sector performance payload"}), 502

    return jsonify({
        **data,
        "updated_at": updated_at,
        "cache_ttl": _SECTOR_PERFORMANCE_CACHE_TTL,
    })


@stock_bp.route('/api/sp500-heatmap')
def api_sp500_heatmap():
    """S&P 500 heatmap data API."""
    try:
        data, updated_at = _get_cached_sp500_heatmap()
    except RuntimeError as e:
        _log.error("sp500_heatmap config error: %s", e)
        return jsonify({"error": "FMP API key not configured"}), 500
    except requests.RequestException as e:
        _log.warning("sp500_heatmap sync failed: %s", e)
        return jsonify({"error": "Failed to sync S&P 500 constituents"}), 502
    except sqlite3.Error as e:
        _log.error("sp500_heatmap database error: %s", e)
        return jsonify({"error": "Database error"}), 500
    except ValueError as e:
        _log.warning("sp500_heatmap payload invalid: %s", e)
        return jsonify({"error": "Invalid S&P 500 payload"}), 502

    return jsonify({
        **data,
        "updated_at": updated_at,
        "cache_ttl": _SP500_HEATMAP_CACHE_TTL,
    })


@stock_bp.route('/api/ohlc')
def api_ohlc():
    """K 線圖 OHLC 數據 API"""
    ticker = request.args.get('symbol', '').strip().upper()
    days = request.args.get('days', '180', type=str)

    if not ticker or not is_valid_ticker(ticker):
        return jsonify({"error": "Invalid ticker"}), 400

    ticker = resolve_ticker(ticker)

    try:
        days_int = max(1, min(730, int(days)))
    except ValueError:
        days_int = 180

    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT date, open, high, low, close, volume "
            "FROM ohlc_daily WHERE ticker = ? "
            "ORDER BY date DESC LIMIT ?",
            (ticker, days_int)
        ).fetchall()
    finally:
        conn.close()

    # 轉成 ASC 順序
    data = [
        {
            "time": r["date"],
            "open": r["open"],
            "high": r["high"],
            "low": r["low"],
            "close": r["close"],
            "volume": r["volume"],
        }
        for r in reversed(rows)
    ]

    return jsonify(data)


@stock_bp.route('/api/price-analysis', methods=['POST'])
def api_price_analysis():
    """時段走勢分析：將指定日期範圍的 OHLC 數據送給 AI 分析"""
    body = request.json or {}
    symbol = body.get('symbol', '').strip().upper()
    start_date = body.get('start_date', '').strip()
    lang = body.get('lang', DEFAULT_LANG)
    if lang not in SUPPORTED_LANGS:
        lang = DEFAULT_LANG

    if not symbol or not is_valid_ticker(symbol):
        return jsonify({"success": False, "error": "無效的股票代碼"}), 400
    if not start_date:
        return jsonify({"success": False, "error": "請提供開始日期"}), 400
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError:
        return jsonify({"success": False, "error": "日期格式錯誤，請使用 YYYY-MM-DD"}), 400

    symbol = resolve_ticker(symbol)

    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT date, open, high, low, close, volume "
            "FROM ohlc_daily WHERE ticker = ? AND date >= ? ORDER BY date ASC",
            (symbol, start_date)
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return jsonify({"success": False, "error": "該時段無價格數據"}), 404

    end_date = rows[-1]['date']
    db_info = get_stock_info(symbol)
    stock_name = db_info["name"] if db_info else symbol
    exchange   = db_info["exchange"] if db_info else ""

    # 組成 Markdown 表格
    price_data = "| 日期 | 開盤 | 最高 | 最低 | 收盤 | 成交量 |\n"
    price_data += "|------|------|------|------|------|--------|\n"
    for r in rows:
        vol = r['volume'] if r['volume'] else 'N/A'
        price_data += f"| {r['date']} | {r['open']} | {r['high']} | {r['low']} | {r['close']} | {vol} |\n"

    try:
        prompt = prompt_manager.build(
            section='price_analysis',
            ticker=symbol,
            stock_name=stock_name or symbol,
            exchange=exchange or 'US',
            today=get_today(),
            chinese_name=stock_name or symbol,
        )
        prompt = prompt.replace('{start_date}', start_date)
        prompt = prompt.replace('{end_date}', end_date)
        prompt = prompt.replace('{price_data}', price_data)

        response_text = call_gemini_api(prompt, use_search=True)

        # 解析事件 JSON
        events = []
        events_match = re.search(
            r'<!--\s*EVENTS_JSON\s*\n?\s*(\[[\s\S]*?\])\s*\n?\s*-->',
            response_text
        )
        if events_match:
            try:
                events = json.loads(events_match.group(1))
            except (json.JSONDecodeError, ValueError):
                events = []
            # 從報告文字中移除 events 區塊
            response_text = response_text[:events_match.start()] + response_text[events_match.end():]

        html = markdown.markdown(response_text.strip(), extensions=['tables', 'fenced_code', 'nl2br'])

        if lang != 'zh_hk':
            translation_prompt = prompt_manager.build_translation_prompt(html, lang)
            translated = call_gemini_api(translation_prompt, use_search=False)
            html = translated if translated.strip().startswith('<') else markdown.markdown(
                translated, extensions=['tables', 'fenced_code', 'nl2br'])

        return jsonify({"success": True, "report": html, "events": events,
                        "start_date": start_date, "end_date": end_date})

    except Exception as e:
        _log.error("price_analysis %s: %s", symbol, e)
        return jsonify({"success": False, "error": "分析失敗，請稍後重試"}), 500


def _to_float(value):
    """Best-effort float parsing for API payload values."""
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _round_to_half(value: float) -> float:
    return round(value * 2) / 2


def _quality_score(scores: dict) -> float:
    values = [_to_float(v) for v in scores.values()]
    values = [v for v in values if v is not None]
    if not values:
        return 0.0
    return round(sum(values) / len(values), 1)


def _quality_cap(quality: float) -> float:
    """Cap valuation stars by business quality to reduce value-trap false positives."""
    if quality >= 8:
        return 5.0
    if quality >= 6:
        return 4.5
    if quality >= 5:
        return 3.5
    return 3.0


def _stars_from_valuation(current_price, fair_value, quality: float) -> tuple[float, float | None, str]:
    """
    Convert current price versus fair value into a deterministic 0.5-5.0 star rating.

    discount_pct is positive when current price is below fair value, negative when above.
    """
    current = _to_float(current_price)
    fair = _to_float(fair_value)
    quality_fallback = max(0.5, min(5.0, _round_to_half((quality / 10) * 5)))

    if not current or not fair or current <= 0 or fair <= 0:
        return quality_fallback, None, "quality_only"

    discount_pct = ((fair - current) / fair) * 100
    if discount_pct >= 20:
        valuation_stars = 5.0
    elif discount_pct >= 10:
        valuation_stars = 4.0
    elif discount_pct > -10:
        valuation_stars = 3.0
    elif discount_pct > -30:
        valuation_stars = 2.0
    else:
        valuation_stars = 1.0

    stars = min(valuation_stars, _quality_cap(quality))
    return _round_to_half(max(0.5, min(5.0, stars))), round(discount_pct, 1), "price_vs_fair_value"


@stock_bp.route('/api/rating_verdict', methods=['POST'])
def rating_verdict():
    """
    根據 7 個 section 評分，用 AI 生成一句話投資判定。
    輕量級請求：僅發送分數，不重新分析內容。
    """
    data = request.json or {}
    ticker = data.get('ticker', '').strip().upper()
    scores = data.get('scores', {})
    summaries = data.get('summaries', {})
    lang = data.get('lang', DEFAULT_LANG)
    current_price = _to_float(data.get('current_price'))

    if not ticker or not scores:
        return jsonify({"success": False, "error": "Missing data"}), 400

    if not is_valid_ticker(ticker):
        return jsonify({"success": False, "error": "Invalid ticker format"}), 400

    # 組裝 section 名稱 → 分數 + 摘要
    t = get_translations(lang)
    section_labels = {
        'biz': t.get('card_biz', 'Business'),
        'finance': t.get('card_finance', 'Finance'),
        'exec': t.get('card_exec', 'Governance'),
        'call': t.get('card_call', 'Outlook'),
        'ta_price': t.get('card_ta_price', 'Price Action'),
        'ta_analyst': t.get('card_ta_analyst', 'Analyst'),
        'ta_social': t.get('card_ta_social', 'Sentiment'),
    }

    detail_lines = []
    for key, label in section_labels.items():
        s = scores.get(key)
        if s is not None:
            section_info = summaries.get(key, {})
            summary_text = section_info.get('summary', '') if isinstance(section_info, dict) else ''
            if summary_text:
                detail_lines.append(f"【{label}】{s}/10 — {summary_text}")
            else:
                detail_lines.append(f"【{label}】{s}/10")

    if not detail_lines:
        return jsonify({"success": False, "error": "No scores"}), 400

    detail_text = '\n'.join(detail_lines)
    quality = _quality_score(scores)

    lang_instruction = {
        'zh_hk': '請用繁體中文回答',
        'zh_cn': '请用简体中文回答',
        'en': 'Please answer in English',
    }.get(lang, '請用繁體中文回答')

    price_section = ""
    if current_price:
        price_section = f"\n現時股價：{current_price}\n"

    prompt = (
        f"你是一位專業投資分析師。以下是股票 {ticker} 的 AI 分析結果：\n\n"
        f"{detail_text}\n"
        f"{price_section}\n"
        f"{lang_instruction}。請根據以上分析，回傳以下 JSON 格式（只回傳 JSON，不要加任何其他文字）：\n\n"
        f"{{\n"
        f'  "verdict": "2-3句話總結投資評價（50-80字），具體指出關鍵原因，不要只說強弱，要解釋為什麼",\n'
        f'  "fair_value": 數字（根據基本面分析估算公允價值，單位與現價相同，整數或一位小數）,\n'
        f'  "fair_value_basis": "一句話說明估值依據，例如：基於 DCF 與同業 P/E 中位數"\n'
        f"}}\n\n"
        f"verdict 請聚焦公司質素、估值依據與主要風險，不要在 verdict 內提及星級、現價折讓或溢價百分比。"
        f"不要回傳 stars；系統會用固定公式根據現價、公允價值與基本面平均分 {quality:.1f}/10 計算星級。"
    )

    # 先查快取
    cached = get_verdict(ticker, lang)
    if cached:
        # 舊快取為純文字，直接回傳 verdict 相容舊格式
        try:
            cached_payload = json.loads(cached)
            fair_value = cached_payload.get("fair_value")
            stars, discount_pct, star_source = _stars_from_valuation(current_price, fair_value, quality)
            return jsonify({
                "success": True,
                "verdict": cached_payload.get("verdict", ""),
                "stars": stars,
                "fair_value": fair_value,
                "fair_value_basis": cached_payload.get("fair_value_basis", ""),
                "discount_pct": discount_pct,
                "star_source": star_source,
                "quality_score": quality,
            })
        except (json.JSONDecodeError, TypeError, ValueError):
            stars, discount_pct, star_source = _stars_from_valuation(current_price, None, quality)
            return jsonify({
                "success": True,
                "verdict": cached,
                "stars": stars,
                "fair_value": None,
                "fair_value_basis": None,
                "discount_pct": discount_pct,
                "star_source": star_source,
                "quality_score": quality,
            })

    try:
        raw = call_gemini_api(prompt, use_search=False).strip()
        # 嘗試解析 JSON
        import json as _json
        import re as _re
        json_match = _re.search(r'\{[\s\S]*\}', raw)
        if json_match:
            parsed = _json.loads(json_match.group())
            verdict  = str(parsed.get('verdict', '')).strip('"\'「」『』')
            fair_value       = parsed.get('fair_value')
            fair_value_basis = parsed.get('fair_value_basis', '')
        else:
            # fallback：舊格式純文字
            verdict  = raw.strip('"\'「」『』')
            fair_value       = None
            fair_value_basis = None

        stars, discount_pct, star_source = _stars_from_valuation(current_price, fair_value, quality)
        save_verdict(ticker, json.dumps({
            "verdict": verdict,
            "fair_value": fair_value,
            "fair_value_basis": fair_value_basis,
        }, ensure_ascii=False), lang)
        return jsonify({
            "success": True,
            "verdict": verdict,
            "stars": stars,
            "fair_value": fair_value,
            "fair_value_basis": fair_value_basis,
            "discount_pct": discount_pct,
            "star_source": star_source,
            "quality_score": quality,
        })
    except Exception as e:
        _log.error("rating_verdict %s: %s", ticker, e)
        return jsonify({"success": False, "error": "AI verdict failed"}), 500
