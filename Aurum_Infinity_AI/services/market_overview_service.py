"""
market_overview_service.py — 市場看板資料服務
=================================================
Zone 1: 市場脈搏橫條（大盤指數 + 宏觀指標）
Zone 3: Gainers / Losers / Most Active
"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests

from config import Config
from database import get_db
from logger import get_logger

_log = get_logger(__name__)

# ── Cache TTL ──────────────────────────────────────────────────
_PULSE_TTL      = 12           # 市場脈搏：12 秒（FMP 15 秒更新，前端 15 秒輪詢）
_MOVERS_TTL     = 10 * 60      # Gainers/Losers：10 分鐘
_ACTIVE_TTL     = 10 * 60      # Most Active（DB）：10 分鐘
_SPARKLINE_TTL  = 6 * 60 * 60  # Sparkline 歷史：6 小時（日線不需要頻繁更新）

_pulse_lock      = threading.Lock()
_movers_lock     = threading.Lock()
_active_lock     = threading.Lock()
_sparkline_lock  = threading.Lock()

_pulse_cache     = {"data": None, "expires_at": 0.0}
_movers_cache    = {"data": None, "expires_at": 0.0}
_active_cache    = {"data": None, "expires_at": 0.0}
_sparkline_cache = {"data": None, "expires_at": 0.0}  # symbol -> [close, ...]

# ── FMP 宏觀指標符號 ────────────────────────────────────────────
_MACRO_SYMBOLS = [
    {"symbol": "^GSPC",    "label": "S&P 500",  "group": "index"},
    {"symbol": "^IXIC",    "label": "NASDAQ",   "group": "index"},
    {"symbol": "^DJI",     "label": "DOW",      "group": "index"},
    {"symbol": "^VIX",     "label": "VIX",      "group": "fear"},
    {"symbol": "DX-Y.NYB", "label": "DXY",      "group": "macro"},
    {"symbol": "^TNX",     "label": "US 10Y",   "group": "macro"},
    {"symbol": "GCUSD",    "label": "Gold",      "group": "commodity"},
    {"symbol": "CLUSD",    "label": "Oil WTI",   "group": "commodity"},
    {"symbol": "BTCUSD",   "label": "Bitcoin",   "group": "crypto"},
    {"symbol": "ETHUSD",   "label": "Ethereum",  "group": "crypto"},
    {"symbol": "EURUSD",   "label": "EUR/USD",   "group": "forex"},
    {"symbol": "USDJPY",   "label": "USD/JPY",   "group": "forex"},
    {"symbol": "AAPL",     "label": "AAPL",      "group": "stock"},
    {"symbol": "NVDA",     "label": "NVDA",      "group": "stock"},
    {"symbol": "GOOGL",    "label": "GOOGL",     "group": "stock"},
    {"symbol": "TSLA",     "label": "TSLA",      "group": "stock"},
    {"symbol": "AMZN",     "label": "AMZN",      "group": "stock"},
    {"symbol": "NFLX",     "label": "NFLX",      "group": "stock"},
    {"symbol": "META",     "label": "META",      "group": "stock"},
]

_FMP_BASE = "https://financialmodelingprep.com/stable"


def _fmp_get(endpoint: str, params: dict | None = None, timeout: int = 8):
    url = f"{_FMP_BASE}/{endpoint}"
    p = {"apikey": Config.FMP_API_KEY, **(params or {})}
    return requests.get(url, params=p, timeout=timeout)


def _fmt_price(val) -> str | None:
    if val is None:
        return None
    try:
        f = float(val)
        if f >= 10_000:
            return f"{f:,.0f}"
        if f >= 100:
            return f"{f:,.2f}"
        return f"{f:.4g}"
    except (TypeError, ValueError):
        return str(val)


def _fmt_change(val) -> str | None:
    if val is None:
        return None
    try:
        f = float(val)
        sign = "+" if f >= 0 else ""
        return f"{sign}{f:.2f}%"
    except (TypeError, ValueError):
        return str(val)


def _change_dir(val) -> str:
    try:
        f = float(val or 0)
        if f > 0:
            return "up"
        if f < 0:
            return "down"
    except (TypeError, ValueError):
        pass
    return "flat"


# ── Sparkline 歷史收盤價（20 日，每日快取）─────────────────────

def _fetch_one_sparkline(sym: str, from_date: str, to_date: str) -> tuple[str, list[float]]:
    try:
        r = _fmp_get(
            "historical-price-eod/full",
            {"symbol": sym, "from": from_date, "to": to_date},
            timeout=12,
        )
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list) or not data:
            return sym, []
        closes = [float(row["close"]) for row in reversed(data) if row.get("close") is not None]
        return sym, closes[-20:]
    except Exception as e:
        _log.warning(f"sparkline fetch failed for {sym}: {e}")
        return sym, []


def _fetch_sparklines() -> dict[str, list[float]]:
    """並行拿每個 pulse symbol 最近 20 日收盤價"""
    from datetime import date, timedelta
    to_date   = date.today().isoformat()
    from_date = (date.today() - timedelta(days=35)).isoformat()
    result: dict[str, list[float]] = {}

    with ThreadPoolExecutor(max_workers=9) as pool:
        futures = {
            pool.submit(_fetch_one_sparkline, item["symbol"], from_date, to_date): item["symbol"]
            for item in _MACRO_SYMBOLS
        }
        for future in as_completed(futures):
            sym, closes = future.result()
            result[sym] = closes

    return result


def get_sparklines() -> dict[str, list[float]]:
    """阻塞版：等待完整 sparkline 資料（用於 API endpoint）"""
    now = time.time()
    with _sparkline_lock:
        if _sparkline_cache["data"] and now < _sparkline_cache["expires_at"]:
            return _sparkline_cache["data"]
        data = _fetch_sparklines()
        _sparkline_cache["data"] = data
        _sparkline_cache["expires_at"] = now + _SPARKLINE_TTL
        return data


def _sparkline_cache_safe() -> dict[str, list[float]]:
    """非阻塞版：有 cache 就回傳，沒有就在背景預熱並回傳空 dict"""
    now = time.time()
    with _sparkline_lock:
        if _sparkline_cache["data"] and now < _sparkline_cache["expires_at"]:
            return _sparkline_cache["data"]

    # cache 不存在或過期 → 背景預熱，不阻塞請求
    def _warm():
        data = _fetch_sparklines()
        with _sparkline_lock:
            _sparkline_cache["data"] = data
            _sparkline_cache["expires_at"] = time.time() + _SPARKLINE_TTL

    threading.Thread(target=_warm, daemon=True).start()
    return {}  # 前端 JS 會在 /api/markets/pulse 拿到 sparkline


# ── Zone 1：市場脈搏 ────────────────────────────────────────────

def _fetch_pulse() -> list[dict]:
    """1 次 batch-quote 拿全部 9 個 pulse symbol"""
    symbols_str = ",".join(item["symbol"] for item in _MACRO_SYMBOLS)
    label_map = {item["symbol"]: item for item in _MACRO_SYMBOLS}

    try:
        r = _fmp_get("batch-quote", {"symbols": symbols_str})
        r.raise_for_status()
        payload = r.json()
        if not isinstance(payload, list):
            payload = []
    except Exception as e:
        _log.warning(f"batch-quote fetch failed: {e}")
        payload = []

    # index by symbol for O(1) lookup
    quote_map: dict[str, dict] = {}
    for row in payload:
        sym = row.get("symbol", "")
        if sym:
            quote_map[sym] = row

    result = []
    for item in _MACRO_SYMBOLS:
        sym = item["symbol"]
        row = quote_map.get(sym, {})
        chg_pct = row.get("changesPercentage") or row.get("changePercentage")
        result.append({
            "symbol":  sym,
            "label":   item["label"],
            "group":   item["group"],
            "price":   _fmt_price(row.get("price")),
            "change":  _fmt_change(chg_pct),
            "dir":     _change_dir(chg_pct),
            "raw_chg": float(chg_pct) if chg_pct is not None else None,
        })
    return result


def get_pulse() -> tuple[list[dict], str]:
    now = time.time()
    with _pulse_lock:
        if _pulse_cache["data"] and now < _pulse_cache["expires_at"]:
            return _pulse_cache["data"], _pulse_cache.get("updated_at", "")
        data = _fetch_pulse()
        updated_at = datetime.utcnow().strftime("%H:%M UTC")
        _pulse_cache["data"] = data
        _pulse_cache["updated_at"] = updated_at
        _pulse_cache["expires_at"] = now + _PULSE_TTL
        return data, updated_at


# ── Zone 3：Gainers / Losers ────────────────────────────────────

def _parse_mover(row: dict) -> dict:
    chg_pct = row.get("changesPercentage") or row.get("changePercentage")
    return {
        "symbol":  row.get("symbol", ""),
        "name":    row.get("name", ""),
        "price":   _fmt_price(row.get("price")),
        "change":  _fmt_change(chg_pct),
        "dir":     _change_dir(chg_pct),
        "raw_chg": float(chg_pct) if chg_pct is not None else None,
        "volume":  row.get("volume"),
    }


def _fetch_movers() -> dict:
    gainers, losers = [], []
    try:
        r = _fmp_get("biggest-gainers")
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            gainers = [_parse_mover(row) for row in data[:8]]
    except Exception as e:
        _log.warning(f"biggest-gainers fetch failed: {e}")

    try:
        r = _fmp_get("biggest-losers")
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            losers = [_parse_mover(row) for row in data[:8]]
    except Exception as e:
        _log.warning(f"biggest-losers fetch failed: {e}")

    return {"gainers": gainers, "losers": losers}


def get_movers() -> tuple[dict, str]:
    now = time.time()
    with _movers_lock:
        if _movers_cache["data"] and now < _movers_cache["expires_at"]:
            return _movers_cache["data"], _movers_cache.get("updated_at", "")
        data = _fetch_movers()
        updated_at = datetime.utcnow().strftime("%H:%M UTC")
        _movers_cache["data"] = data
        _movers_cache["updated_at"] = updated_at
        _movers_cache["expires_at"] = now + _MOVERS_TTL
        return data, updated_at


# ── Zone 3：Most Active（本機 DB） ──────────────────────────────

def _fetch_most_active(limit: int = 8) -> list[dict]:
    try:
        conn = get_db()
        latest = conn.execute(
            "SELECT MAX(date) FROM ohlc_daily"
        ).fetchone()[0]
        if not latest:
            return []

        rows = conn.execute(
            """
            SELECT o.ticker, o.volume, o.close,
                   o.close - o.open AS chg_abs,
                   CASE WHEN o.open > 0
                        THEN ROUND((o.close - o.open) / o.open * 100.0, 2)
                        ELSE NULL END AS chg_pct,
                   s.name AS company_name
            FROM ohlc_daily o
            LEFT JOIN stocks_master s ON o.ticker = s.ticker
            WHERE o.date = ?
              AND o.volume IS NOT NULL
            ORDER BY o.volume DESC
            LIMIT ?
            """,
            (latest, limit),
        ).fetchall()
        conn.close()

        result = []
        for row in rows:
            chg_pct = row["chg_pct"]
            result.append({
                "symbol":  row["ticker"],
                "name":    row["company_name"] or row["ticker"],
                "price":   _fmt_price(row["close"]),
                "change":  _fmt_change(chg_pct),
                "dir":     _change_dir(chg_pct),
                "raw_chg": float(chg_pct) if chg_pct is not None else None,
                "volume":  row["volume"],
            })
        return result
    except Exception as e:
        _log.warning(f"most_active DB query failed: {e}")
        return []


def get_most_active() -> tuple[list[dict], str]:
    now = time.time()
    with _active_lock:
        if _active_cache["data"] and now < _active_cache["expires_at"]:
            return _active_cache["data"], _active_cache.get("updated_at", "")
        data = _fetch_most_active()
        updated_at = datetime.utcnow().strftime("%H:%M UTC")
        _active_cache["data"] = data
        _active_cache["updated_at"] = updated_at
        _active_cache["expires_at"] = now + _ACTIVE_TTL
        return data, updated_at
