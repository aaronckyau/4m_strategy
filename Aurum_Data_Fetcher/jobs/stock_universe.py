"""
jobs/stock_universe.py — 股票名單維護
============================================================================
從 FMP company-screener 拉取 CN / HK / US 股票清單，
填入三語名稱（EN / 繁體 / 簡體），寫入 aurum.db 並備份 JSON/CSV。

用法（直接執行）:
  python -m jobs.stock_universe --full-rebuild
  python -m jobs.stock_universe --weekly-refresh

由 updater.py 呼叫:
  from jobs.stock_universe import full_rebuild, weekly_refresh
============================================================================
"""
import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from time import sleep

import requests
from opencc import OpenCC

# 確保能 import 同層的 config / db / logger
_here = Path(__file__).resolve().parent.parent
if str(_here) not in sys.path:
    sys.path.insert(0, str(_here))

from config import Config
from db import get_db, upsert_stock, upsert_sector_i18n, remove_delisted
from logger import log

# ============================================================================
# Config
# ============================================================================
FMP_BASE = "https://financialmodelingprep.com/stable"

_DATA_DIR = Path(Config.STOCK_LIST_PATH).resolve().parent
_DATA_DIR.mkdir(parents=True, exist_ok=True)

UNIVERSE_FILE = _DATA_DIR / "stock_code.json"
UNIVERSE_CSV  = _DATA_DIR / "stock_code.csv"
I18N_FILE     = _DATA_DIR / "sector_industry_i18n.json"
CN_NAME_FILE  = _DATA_DIR / "cn_name_map.json"

OBSIDIAN_LOG  = Path(Config.STOCK_LOG_PATH) if hasattr(Config, "STOCK_LOG_PATH") else \
                _DATA_DIR.parent / "logs" / "stock_code.md"
OBSIDIAN_LOG.parent.mkdir(parents=True, exist_ok=True)

TODAY_STR = datetime.now().strftime("%Y-%m-%d")

FILTER = {
    "CN_StockConnect": 5_000_000_000,
    "HK":              2_000_000_000,
    "US":              500_000_000,
}
SPAC_KW = ["acquisition corp", "blank check", "spac ", "merger corp"]
PRESERVED_TICKERS = {item["symbol"] for item in Config.SECTOR_ETFS}

S2T = OpenCC("s2t")
T2S = OpenCC("t2s")

# ============================================================================
# FMP request
# ============================================================================
def _get(path: str, params: dict | None = None) -> list:
    p = dict(params or {})
    p["apikey"] = Config.FMP_API_KEY
    url = f"{FMP_BASE}/{path}"
    for attempt in range(2):
        try:
            r = requests.get(url, params=p, timeout=120)
            if r.status_code in (401, 403, 404):
                return []
            if r.status_code == 429:
                sleep(30)
                continue
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "Error Message" in data:
                return []
            return data if isinstance(data, list) else ([data] if data else [])
        except Exception:
            if attempt == 0:
                sleep(5)
    return []

# ============================================================================
# Helpers
# ============================================================================
def _is_spac(n: str) -> bool:
    return any(k in (n or "").lower() for k in SPAC_KW)

def _is_gem(s: str) -> bool:
    c = s.replace(".HK", "")
    return c.startswith("8") and len(c) == 4

def _dedup(stocks: list[dict]) -> list[dict]:
    seen: set[str] = set()
    return [s for s in stocks if s["symbol"] not in seen and not seen.add(s["symbol"])]

def _count(stocks: list[dict], market: str | None = None) -> int:
    if market:
        return sum(1 for s in stocks if s["market"] == market)
    return len(stocks)

# ============================================================================
# Fetchers
# ============================================================================
def _fetch_cn() -> tuple[list[dict], list[str]]:
    stocks, errors = [], []
    for exch in ("SHH", "SHZ"):
        rows = _get("company-screener", {
            "exchange": exch, "isActivelyTrading": True,
            "isEtf": False, "isFund": False, "limit": 5000,
        })
        if not rows:
            errors.append(f"CN/{exch}")
            continue
        for s in rows:
            sym = s.get("symbol", "")
            code = sym.replace(".SS", "").replace(".SZ", "")
            if exch == "SHH" and code.startswith("9"):
                continue
            if exch == "SHZ" and code.startswith("200"):
                continue
            stocks.append({
                "symbol": sym, "market": "CN_StockConnect", "exchange": exch,
                "sector": s.get("sector", ""), "industry": s.get("industry", ""),
                "marketCap": s.get("marketCap", 0), "currency": "CNY",
                "name_eng": s.get("companyName") or s.get("name", ""),
            })
    return stocks, errors


def _fetch_hk() -> tuple[list[dict], list[str]]:
    rows = _get("company-screener", {
        "exchange": "HKSE", "marketCapMoreThan": FILTER["HK"],
        "isActivelyTrading": True, "isEtf": False, "isFund": False, "limit": 5000,
    })
    if not rows:
        return [], ["HK"]
    stocks = []
    for s in rows:
        sym = s.get("symbol", "")
        name = s.get("companyName") or s.get("name", "")
        if _is_gem(sym) or _is_spac(name):
            continue
        stocks.append({
            "symbol": sym, "market": "HK", "exchange": "HKSE",
            "sector": s.get("sector", ""), "industry": s.get("industry", ""),
            "marketCap": s.get("marketCap", 0), "currency": "HKD",
            "name_eng": name,
        })
    return stocks, []


def _fetch_us() -> tuple[list[dict], list[str]]:
    stocks: list[dict] = []
    seen: set[str] = set()
    errors: list[str] = []
    for exch in ("NYSE", "NASDAQ", "AMEX"):
        rows = _get("company-screener", {
            "exchange": exch, "marketCapMoreThan": FILTER["US"],
            "volumeMoreThan": 100000, "isActivelyTrading": True,
            "isEtf": False, "isFund": False, "limit": 5000,
        })
        if not rows:
            errors.append(f"US/{exch}")
            continue
        for s in rows:
            sym = s.get("symbol", "")
            name = s.get("companyName") or s.get("name", "")
            if sym in seen or _is_spac(name):
                continue
            seen.add(sym)
            stocks.append({
                "symbol": sym, "market": "US", "exchange": exch,
                "sector": s.get("sector", ""), "industry": s.get("industry", ""),
                "marketCap": s.get("marketCap", 0), "currency": "USD",
                "name_eng": name,
            })
    return stocks, errors

# ============================================================================
# Filter
# ============================================================================
def _apply_filter(raw: list[dict]) -> list[dict]:
    return [s for s in raw if s.get("marketCap", 0) >= FILTER.get(s["market"], 0)]

# ============================================================================
# 中文名稱
# ============================================================================
def _load_cn_name_map() -> dict:
    if not CN_NAME_FILE.exists():
        log.warning(f"[StockUniverse] 找不到中文名映射 {CN_NAME_FILE}，將只有英文名")
        return {}
    with open(CN_NAME_FILE, encoding="utf-8") as f:
        return json.load(f)


def _apply_names(stocks: list[dict], cn_map: dict) -> list[dict]:
    for s in stocks:
        market = s["market"]
        eng = s.get("name_eng", "")
        sym = s["symbol"]
        if market == "US":
            s["name_zh_hk"] = eng
            s["name_zh_cn"] = eng
        elif market == "HK":
            cn = cn_map.get(sym, {}).get("name", "")
            s["name_zh_hk"] = cn or eng
            s["name_zh_cn"] = T2S.convert(cn) if cn else eng
        elif market == "CN_StockConnect":
            cn = cn_map.get(sym, {}).get("name", "")
            s["name_zh_cn"] = cn or eng
            s["name_zh_hk"] = S2T.convert(cn) if cn else eng
    return stocks

# ============================================================================
# Merge / IO
# ============================================================================
def _build_record(s: dict, added_date: str | None = None) -> dict:
    return {
        "symbol":     s["symbol"],
        "name_eng":   s.get("name_eng", ""),
        "name_zh_hk": s.get("name_zh_hk", ""),
        "name_zh_cn": s.get("name_zh_cn", ""),
        "market":     s["market"],
        "exchange":   s["exchange"],
        "sector":     s.get("sector", ""),
        "industry":   s.get("industry", ""),
        "marketCap":  s.get("marketCap", 0),
        "currency":   s["currency"],
        "addedDate":  added_date or TODAY_STR,
    }


def _load_universe() -> list[dict]:
    if UNIVERSE_FILE.exists():
        with open(UNIVERSE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return []


def _merge_with_existing(new_stocks: list[dict], existing: list[dict]) -> list[dict]:
    old_map = {s["symbol"]: s for s in existing}
    return [
        _build_record(s, old_map[s["symbol"]]["addedDate"] if s["symbol"] in old_map else TODAY_STR)
        for s in new_stocks
    ]


def _save_universe(universe: list[dict]):
    with open(UNIVERSE_FILE, "w", encoding="utf-8") as f:
        json.dump(universe, f, ensure_ascii=False, indent=2)
    if universe:
        with open(UNIVERSE_CSV, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(universe[0].keys()))
            w.writeheader()
            w.writerows(universe)

# ============================================================================
# DB writes
# ============================================================================
def _write_to_db(universe: list[dict]):
    conn = get_db()
    try:
        for s in universe:
            upsert_stock(conn, {
                "ticker":     s["symbol"],
                "name":       s["name_eng"],
                "name_zh_hk": s.get("name_zh_hk"),
                "name_zh_cn": s.get("name_zh_cn"),
                "market":     s["market"],
                "exchange":   s["exchange"],
                "sector":     s.get("sector"),
                "industry":   s.get("industry"),
                "market_cap": s.get("marketCap"),
                "currency":   s["currency"],
            })
        log.info(f"[StockUniverse] stocks_master: {len(universe)} 筆 upserted")

        if I18N_FILE.exists():
            with open(I18N_FILE, encoding="utf-8") as f:
                i18n_data = json.load(f)
            count = upsert_sector_i18n(conn, i18n_data)
            log.info(f"[StockUniverse] sector_industry_i18n: {count} 筆 upserted")

        active = {s["symbol"] for s in universe} | PRESERVED_TICKERS
        removed = remove_delisted(conn, active)
        if removed:
            log.info(f"[StockUniverse] 下市移除: {removed} 筆")
    finally:
        conn.close()

# ============================================================================
# Obsidian log
# ============================================================================
def _write_obsidian_log(mode: str, raw_cn: int, raw_hk: int, raw_us: int,
                        filt_cn: int, filt_hk: int, filt_us: int, errors: list[str]):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    raw_total = raw_cn + raw_hk + raw_us
    filt_total = filt_cn + filt_hk + filt_us
    status = f"FAIL {', '.join(errors)}" if errors else "OK"
    line = (f"| {now} | {mode} | {raw_total} | CN {raw_cn} / HK {raw_hk} / US {raw_us} "
            f"| {filt_total} | CN {filt_cn} / HK {filt_hk} / US {filt_us} | {status} |")
    header = (
        "# Stock Universe Update Log\n\n"
        "| Time | Mode | Raw | Raw Detail | Filtered | Filtered Detail | Status |\n"
        "|------|------|-----|------------|----------|-----------------|--------|"
    )
    if not OBSIDIAN_LOG.exists() or OBSIDIAN_LOG.stat().st_size == 0:
        with open(OBSIDIAN_LOG, "w", encoding="utf-8") as f:
            f.write(header + "\n")
    with open(OBSIDIAN_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ============================================================================
# Core workflow (shared by full_rebuild and weekly_refresh)
# ============================================================================
def _run(mode: str) -> dict:
    cn, e1 = _fetch_cn()
    hk, e2 = _fetch_hk()
    us, e3 = _fetch_us()
    errors = e1 + e2 + e3

    raw = _dedup(cn + hk + us)
    filtered = _apply_filter(raw)
    cn_map = _load_cn_name_map()
    filtered = _apply_names(filtered, cn_map)

    existing = _load_universe()
    universe = _merge_with_existing(filtered, existing)

    rc = _count(raw, "CN_StockConnect"); rh = _count(raw, "HK"); ru = _count(raw, "US")
    fc = _count(universe, "CN_StockConnect"); fh = _count(universe, "HK"); fu = _count(universe, "US")

    result = (f"[{datetime.now().strftime('%H:%M')}] {mode} | "
              f"raw {len(raw)} (CN {rc} HK {rh} US {ru}) -> "
              f"filtered {len(universe)} (CN {fc} HK {fh} US {fu})")
    result += f" | FAIL {','.join(errors)}" if errors else " | OK"

    if errors:
        log.error("[StockUniverse] partial fetch failure detected; skip saving universe and DB sync to avoid accidental deletions")
        log.error(f"[StockUniverse] {result}")
        print(result, flush=True)
        _write_obsidian_log(mode, rc, rh, ru, fc, fh, fu, errors)
        raise RuntimeError(
            "stock universe fetch incomplete; skipped save/remove_delisted to protect existing data"
        )

    _save_universe(universe)
    _write_to_db(universe)

    log.info(f"[StockUniverse] {result}")
    print(result, flush=True)
    _write_obsidian_log(mode, rc, rh, ru, fc, fh, fu, errors)
    return {
        "records_written": len(universe),
        "total_items": len(universe),
        "success_items": len(universe),
        "failed_items": 0,
        "skipped_items": 0,
        "items": [
            {"item_key": "CN/SHH", "item_type": "exchange", "status": "success"},
            {"item_key": "CN/SHZ", "item_type": "exchange", "status": "success"},
            {"item_key": "HK/HKSE", "item_type": "exchange", "status": "success"},
            {"item_key": "US/NYSE", "item_type": "exchange", "status": "success"},
            {"item_key": "US/NASDAQ", "item_type": "exchange", "status": "success"},
            {"item_key": "US/AMEX", "item_type": "exchange", "status": "success"},
        ],
        "summary_text": result,
    }

# ============================================================================
# Public entry points (called by updater.py)
# ============================================================================
def full_rebuild():
    return _run("full-rebuild")

def weekly_refresh():
    return _run("weekly-refresh")

# ============================================================================
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="stock_universe — 股票清單 + 三語名稱")
    p.add_argument("--full-rebuild", action="store_true")
    p.add_argument("--weekly-refresh", action="store_true")
    args = p.parse_args()
    if not Config.FMP_API_KEY:
        print("FAIL: FMP_API_KEY not set")
        sys.exit(1)
    if args.full_rebuild:
        full_rebuild()
    elif args.weekly_refresh:
        weekly_refresh()
    else:
        p.print_help()
