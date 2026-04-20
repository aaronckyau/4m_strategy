"""
fetch_etf_master.py - 只更新 11 檔 sector ETF 的 master 資訊
============================================================================
資料來源：
  GET /stable/etf/info?symbol=XLF

範圍：
  - 只更新 Config.SECTOR_ETFS
  - 不更新 ETF holdings
  - 不更新 OHLC（仍由 fetch_ohlc.py 負責）
============================================================================
"""
import json
import sys
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

from config import Config
from db import get_db, upsert_etf
from utils import log

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

load_dotenv()

FMP_API_KEY = Config.FMP_API_KEY
if not FMP_API_KEY:
    print("ERROR: FMP_API_KEY not set in .env")
    sys.exit(1)

BASE_URL = "https://financialmodelingprep.com/stable"
SECTOR_ETFS = list(Config.SECTOR_ETFS)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_one(session: requests.Session, symbol: str) -> tuple[bool, dict | None, str]:
    url = f"{BASE_URL}/etf/info"
    try:
        resp = session.get(
            url,
            params={"symbol": symbol, "apikey": FMP_API_KEY},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data:
            return True, data[0], ""
        if isinstance(data, dict) and data.get("symbol"):
            return True, data, ""
        return False, None, "empty response"
    except Exception as exc:
        return False, None, str(exc)[:200]


def main() -> int:
    conn = get_db()
    session = requests.Session()
    success = 0
    failed = 0
    items: list[dict] = []

    try:
        log("=== ETF Master Fetcher ===")
        log(f"Targets: {len(SECTOR_ETFS)} sector ETFs")

        for idx, etf_meta in enumerate(SECTOR_ETFS, 1):
            symbol = etf_meta["symbol"]
            started_at = _now_iso()
            t0 = time.time()
            ok, payload, err = fetch_one(session, symbol)
            elapsed = time.time() - t0
            finished_at = _now_iso()

            if ok and payload:
                merged = {
                    "symbol": symbol,
                    "name": payload.get("name") or etf_meta.get("name"),
                    "exchange": payload.get("exchange"),
                    "assetClass": payload.get("assetClass"),
                    "assetsUnderManagement": payload.get("assetsUnderManagement"),
                    "avgVolume": payload.get("avgVolume"),
                    "expenseRatio": payload.get("expenseRatio"),
                    "holdingsCount": payload.get("holdingsCount"),
                    "etfCompany": payload.get("etfCompany"),
                    "inceptionDate": payload.get("inceptionDate"),
                    "website": payload.get("website"),
                }
                upsert_etf(conn, merged)
                success += 1
                items.append({
                    "item_key": symbol,
                    "item_type": "etf",
                    "status": "success",
                    "attempts": 1,
                    "records_written": 1,
                    "started_at": started_at,
                    "finished_at": finished_at,
                })
                log(
                    f"  [{idx:>2}/{len(SECTOR_ETFS)}] OK {symbol:<5} "
                    f"{(merged.get('name') or '')[:48]:<48} {elapsed:.1f}s"
                )
            else:
                failed += 1
                items.append({
                    "item_key": symbol,
                    "item_type": "etf",
                    "status": "failed",
                    "attempts": 1,
                    "records_written": 0,
                    "error_message": err,
                    "started_at": started_at,
                    "finished_at": finished_at,
                })
                log(f"  [{idx:>2}/{len(SECTOR_ETFS)}] FAIL {symbol:<5} {err} {elapsed:.1f}s")

            time.sleep(0.2)
    finally:
        session.close()
        conn.close()

    summary = {
        "total_items": len(SECTOR_ETFS),
        "success_items": success,
        "failed_items": failed,
        "skipped_items": 0,
        "records_written": success,
        "summary_text": f"{success} ok / {failed} fail",
        "items": items,
    }
    log(f"=== DONE: {summary['summary_text']} ===")
    print(f"RUN_SUMMARY_JSON:{json.dumps(summary, ensure_ascii=False)}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
