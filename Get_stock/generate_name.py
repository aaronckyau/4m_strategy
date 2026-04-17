"""
Aurum Infinity AI — generate_name.py
=====================================
合併 get_stock_code + add_cn_name 的功能，產出包含三語名稱的股票清單。
寫入 aurum.db (stocks_master + sector_industry_i18n) 並備份 JSON/CSV。

用法:
  python generate_name.py --full-rebuild
  python generate_name.py --weekly-refresh
"""

import requests, json, csv, os, sys, argparse, sqlite3
from datetime import datetime
from pathlib import Path
from time import sleep
from opencc import OpenCC
from dotenv import load_dotenv

# 載入上層 Aurum_Infinity_AI/.env
load_dotenv(Path(__file__).resolve().parent / ".." / "Aurum_Infinity_AI" / ".env")

# Import shared db helpers from Aurum_Data_Fetcher
sys.path.insert(0, str(Path(__file__).resolve().parent / ".." / "Aurum_Data_Fetcher"))
from db import get_db, upsert_stock, remove_delisted

# ============================================================
# CONFIG
# ============================================================
FMP_API_KEY = os.environ.get("FMP_API_KEY", "")

FMP_BASE = "https://financialmodelingprep.com/stable"

DATA_DIR = Path("./data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

TODAY_STR = datetime.now().strftime("%Y-%m-%d")
UNIVERSE_FILE = DATA_DIR / "stock_code.json"
UNIVERSE_CSV  = DATA_DIR / "stock_code.csv"
I18N_FILE     = DATA_DIR / "sector_industry_i18n.json"

# 中文名映射來源
CN_NAME_SOURCE = Path(os.environ.get("CN_NAME_MAP_PATH", "./data/cn_name_map.json"))

# 資料庫路徑
DB_PATH = os.environ.get("DATABASE_URL") or Path("../Aurum_Infinity_AI/aurum.db")

# Log 輸出
OBSIDIAN_LOG = Path(os.environ.get("STOCK_LOG_PATH", "./logs/stock_code.md"))
OBSIDIAN_LOG.parent.mkdir(parents=True, exist_ok=True)

FILTER = {
    "CN_StockConnect": 5_000_000_000,   # CNY 50億
    "HK":              2_000_000_000,   # HKD 20億
    "US":              500_000_000,     # USD 5億
}

SPAC_KW = ["acquisition corp", "blank check", "spac ", "merger corp"]

# OpenCC 轉換器
S2T = OpenCC("s2t")   # 簡體 → 繁體
T2S = OpenCC("t2s")   # 繁體 → 簡體

# ============================================================
# FMP API
# ============================================================
def _get(path, params=None):
    if params is None: params = {}
    params["apikey"] = FMP_API_KEY
    url = f"{FMP_BASE}/{path}"
    for attempt in range(2):
        try:
            r = requests.get(url, params=params, timeout=120)
            if r.status_code in (401, 403, 404): return []
            if r.status_code == 429: sleep(30); continue
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "Error Message" in data: return []
            return data if isinstance(data, list) else [data] if data else []
        except:
            if attempt == 0: sleep(5)
    return []

# ============================================================
# HELPERS
# ============================================================
def _is_spac(n): return any(k in (n or "").lower() for k in SPAC_KW)
def _is_gem(s): c = s.replace(".HK",""); return c.startswith("8") and len(c)==4

def _dedup(stocks):
    seen = set()
    return [s for s in stocks if s["symbol"] not in seen and not seen.add(s["symbol"])]

def _count(stocks, market=None):
    if market: return sum(1 for s in stocks if s["market"] == market)
    return len(stocks)

# ============================================================
# FETCHERS
# ============================================================
def fetch_cn():
    stocks, errors = [], []
    for exch in ("SHH", "SHZ"):
        r = _get("company-screener", {"exchange": exch, "isActivelyTrading": True, "isEtf": False, "isFund": False, "limit": 5000})
        if not r:
            errors.append(f"CN/{exch}")
            continue
        for s in r:
            sym = s.get("symbol", "")
            code = sym.replace(".SS","").replace(".SZ","")
            if exch == "SHH" and code.startswith("9"): continue
            if exch == "SHZ" and code.startswith("200"): continue
            stocks.append({
                "symbol": sym, "market": "CN_StockConnect", "exchange": exch,
                "sector": s.get("sector",""), "industry": s.get("industry",""),
                "marketCap": s.get("marketCap",0), "currency": "CNY",
                "name_eng": s.get("companyName", s.get("name","")),
            })
    return stocks, errors

def fetch_hk():
    r = _get("company-screener", {"exchange": "HKSE", "marketCapMoreThan": FILTER["HK"],
        "isActivelyTrading": True, "isEtf": False, "isFund": False, "limit": 5000})
    if not r: return [], ["HK"]
    stocks = []
    for s in r:
        sym = s.get("symbol","")
        name = s.get("companyName", s.get("name",""))
        if _is_gem(sym) or _is_spac(name): continue
        stocks.append({
            "symbol": sym, "market": "HK", "exchange": "HKSE",
            "sector": s.get("sector",""), "industry": s.get("industry",""),
            "marketCap": s.get("marketCap",0), "currency": "HKD",
            "name_eng": name,
        })
    return stocks, []

def fetch_us():
    stocks, seen, errors = [], set(), []
    for exch in ("NYSE", "NASDAQ", "AMEX"):
        r = _get("company-screener", {"exchange": exch, "marketCapMoreThan": FILTER["US"],
            "volumeMoreThan": 100000, "isActivelyTrading": True,
            "isEtf": False, "isFund": False, "limit": 5000})
        if not r:
            errors.append(f"US/{exch}")
            continue
        for s in r:
            sym = s.get("symbol","")
            name = s.get("companyName", s.get("name",""))
            if sym in seen or _is_spac(name): continue
            seen.add(sym)
            stocks.append({
                "symbol": sym, "market": "US", "exchange": exch,
                "sector": s.get("sector",""), "industry": s.get("industry",""),
                "marketCap": s.get("marketCap",0), "currency": "USD",
                "name_eng": name,
            })
    return stocks, errors

# ============================================================
# FILTER
# ============================================================
def apply_filter(raw):
    return [s for s in raw if s.get("marketCap", 0) >= FILTER.get(s["market"], 0)]

# ============================================================
# 中文名稱處理
# ============================================================
def load_cn_name_map():
    if not CN_NAME_SOURCE.exists():
        print(f"WARN 找不到中文名映射 {CN_NAME_SOURCE}，將只有英文名")
        return {}
    return json.load(open(CN_NAME_SOURCE, encoding="utf-8"))

def apply_names(stocks, cn_map):
    """為每筆股票填入 name_eng / name_zh_hk / name_zh_cn"""
    for s in stocks:
        market = s["market"]
        sym = s["symbol"]
        eng = s.get("name_eng", "")

        if market == "US":
            s["name_zh_hk"] = eng
            s["name_zh_cn"] = eng

        elif market == "HK":
            cn_name_hk = cn_map.get(sym, {}).get("name", "")
            if cn_name_hk:
                s["name_zh_hk"] = cn_name_hk
                s["name_zh_cn"] = T2S.convert(cn_name_hk)
            else:
                s["name_zh_hk"] = eng
                s["name_zh_cn"] = eng

        elif market == "CN_StockConnect":
            cn_name_cn = cn_map.get(sym, {}).get("name", "")
            if cn_name_cn:
                s["name_zh_cn"] = cn_name_cn
                s["name_zh_hk"] = S2T.convert(cn_name_cn)
            else:
                s["name_zh_hk"] = eng
                s["name_zh_cn"] = eng

    return stocks

def _build_record(s, added_date=None):
    """組裝最終輸出欄位順序"""
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

# ============================================================
# MERGE — 與既有名單合併
# ============================================================
def merge_with_existing(new_stocks, existing):
    old_map = {s["symbol"]: s for s in existing}
    merged = []
    for s in new_stocks:
        sym = s["symbol"]
        added = old_map[sym]["addedDate"] if sym in old_map else TODAY_STR
        merged.append(_build_record(s, added))
    return merged

# ============================================================
# I/O — JSON / CSV 備份
# ============================================================
def save_universe(u):
    with open(UNIVERSE_FILE, "w", encoding="utf-8") as f:
        json.dump(u, f, ensure_ascii=False, indent=2)
    if u:
        with open(UNIVERSE_CSV, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(u[0].keys()))
            w.writeheader()
            w.writerows(u)

def load_universe():
    if UNIVERSE_FILE.exists():
        with open(UNIVERSE_FILE, "r", encoding="utf-8") as f: return json.load(f)
    return []

# ============================================================
# DB — 寫入 aurum.db
# ============================================================
def write_stocks_to_db(universe):
    """UPSERT stocks_master（透過共用 db.upsert_stock，coalesce 策略）"""
    conn = get_db()
    try:
        for s in universe:
            upsert_stock(conn, {
                'ticker':     s['symbol'],
                'name':       s['name_eng'],
                'name_zh_hk': s.get('name_zh_hk'),
                'name_zh_cn': s.get('name_zh_cn'),
                'market':     s['market'],
                'exchange':   s['exchange'],
                'sector':     s.get('sector'),
                'industry':   s.get('industry'),
                'market_cap': s.get('marketCap'),
                'currency':   s['currency'],
            })
    finally:
        conn.close()
    print(f"  -> DB stocks_master: {len(universe)} rows upserted")

def write_i18n_to_db():
    """將 sector_industry_i18n.json 寫入 DB"""
    if not I18N_FILE.exists():
        print(f"WARN 找不到 {I18N_FILE}，跳過 i18n 寫入")
        return
    data = json.load(open(I18N_FILE, encoding="utf-8"))
    conn = get_db()
    c = conn.cursor()
    count = 0
    for category in ("sectors", "industries"):
        type_val = "sector" if category == "sectors" else "industry"
        for key, val in data.get(category, {}).items():
            c.execute("""
                INSERT INTO sector_industry_i18n (key, type, zh_hk, zh_cn)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    type  = excluded.type,
                    zh_hk = excluded.zh_hk,
                    zh_cn = excluded.zh_cn
            """, (key, type_val, val.get("zh_hk", ""), val.get("zh_cn", "")))
            count += 1
    conn.commit()
    conn.close()
    print(f"  -> DB sector_industry_i18n: {count} rows upserted")

def remove_delisted_from_db(universe):
    """移除 DB 中不再在名單內的股票"""
    if not universe:
        return
    conn = get_db()
    try:
        current_symbols = {s["symbol"] for s in universe}
        removed = remove_delisted(conn, current_symbols)
        if removed:
            print(f"  -> DB removed {removed} delisted stocks")
    finally:
        conn.close()

# ============================================================
# OBSIDIAN LOG
# ============================================================
def write_obsidian_log(mode, raw_cn, raw_hk, raw_us, filt_cn, filt_hk, filt_us, errors):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    raw_total = raw_cn + raw_hk + raw_us
    filt_total = filt_cn + filt_hk + filt_us

    line = f"| {now} | {mode} | {raw_total} | CN {raw_cn} / HK {raw_hk} / US {raw_us} | {filt_total} | CN {filt_cn} / HK {filt_hk} / US {filt_us} |"
    line += f" FAIL {', '.join(errors)} |" if errors else " OK |"

    header = """# Stock Universe Update Log

| Time | Mode | Raw | Raw Detail | Filtered | Filtered Detail | Status |
|------|------|-----|------------|----------|-----------------|--------|"""

    if not OBSIDIAN_LOG.exists() or OBSIDIAN_LOG.stat().st_size == 0:
        with open(OBSIDIAN_LOG, "w", encoding="utf-8") as f:
            f.write(header + "\n")

    with open(OBSIDIAN_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ============================================================
# WORKFLOWS
# ============================================================
def full_rebuild():
    cn, e1 = fetch_cn()
    hk, e2 = fetch_hk()
    us, e3 = fetch_us()
    errors = e1 + e2 + e3

    raw = _dedup(cn + hk + us)
    filtered = apply_filter(raw)

    cn_map = load_cn_name_map()
    filtered = apply_names(filtered, cn_map)

    existing = load_universe()
    universe = merge_with_existing(filtered, existing)

    # 備份 JSON / CSV
    save_universe(universe)

    # 寫入 DB
    write_stocks_to_db(universe)
    write_i18n_to_db()
    remove_delisted_from_db(universe)

    rc, rh, ru = _count(raw,"CN_StockConnect"), _count(raw,"HK"), _count(raw,"US")
    fc, fh, fu = _count(universe,"CN_StockConnect"), _count(universe,"HK"), _count(universe,"US")

    print(f"[{datetime.now().strftime('%H:%M')}] full-rebuild | raw {len(raw)} (CN {rc} HK {rh} US {ru}) -> filtered {len(universe)} (CN {fc} HK {fh} US {fu})" + (f" | FAIL {','.join(errors)}" if errors else " | OK"))
    write_obsidian_log("full-rebuild", rc, rh, ru, fc, fh, fu, errors)

def weekly_refresh():
    cn, e1 = fetch_cn()
    hk, e2 = fetch_hk()
    us, e3 = fetch_us()
    errors = e1 + e2 + e3

    raw = _dedup(cn + hk + us)
    filtered = apply_filter(raw)

    cn_map = load_cn_name_map()
    filtered = apply_names(filtered, cn_map)

    existing = load_universe()
    universe = merge_with_existing(filtered, existing)

    # 備份 JSON / CSV
    save_universe(universe)

    # 寫入 DB
    write_stocks_to_db(universe)
    write_i18n_to_db()
    remove_delisted_from_db(universe)

    rc, rh, ru = _count(raw,"CN_StockConnect"), _count(raw,"HK"), _count(raw,"US")
    fc, fh, fu = _count(universe,"CN_StockConnect"), _count(universe,"HK"), _count(universe,"US")

    print(f"[{datetime.now().strftime('%H:%M')}] weekly-refresh | raw {len(raw)} (CN {rc} HK {rh} US {ru}) -> filtered {len(universe)} (CN {fc} HK {fh} US {fu})" + (f" | FAIL {','.join(errors)}" if errors else " | OK"))
    write_obsidian_log("weekly", rc, rh, ru, fc, fh, fu, errors)

# ============================================================
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="generate_name — 股票清單 + 三語名稱")
    p.add_argument("--full-rebuild", action="store_true", help="全量重建")
    p.add_argument("--weekly-refresh", action="store_true", help="週更新")
    args = p.parse_args()

    if not FMP_API_KEY or "YOUR" in FMP_API_KEY:
        print("FAIL No API Key"); sys.exit(1)

    if args.full_rebuild: full_rebuild()
    elif args.weekly_refresh: weekly_refresh()
    else: p.print_help()
