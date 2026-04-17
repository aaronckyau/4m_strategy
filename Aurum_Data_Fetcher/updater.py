"""
updater.py — 集中式更新排程器
============================================================================
用法（SSH / crontab）:

  # 每日任務（OHLC + ratios）
  python updater.py --daily

  # 每週任務（股票名單 + 財報 + ETF + 13F）
  python updater.py --weekly

  # 執行單一 job
  python updater.py --job stock_universe
  python updater.py --job ohlc
  python updater.py --job financials
  python updater.py --job ratios
  python updater.py --job etf
  python updater.py --job 13f

每個 job 開始/結束都會寫入 update_log 表，可透過管理後台查看。
============================================================================
"""
import argparse
import subprocess
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

# 確保能 import 同目錄的 db.py
sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import get_db, start_update_log, finish_update_log, init_tables
from jobs.stock_universe import full_rebuild as _su_full_rebuild
from jobs.stock_universe import weekly_refresh as _su_weekly_refresh

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _run_script(script: str, extra_args: list[str] | None = None) -> tuple[int, str, int]:
    """在子程序執行同目錄的 Python 腳本，即時輸出 stdout，回傳 (returncode, stderr, records)"""
    script_dir = str(Path(__file__).parent)
    cmd = [sys.executable, str(Path(__file__).parent / script)] + (extra_args or [])
    # 用 Popen 即時輸出 stdout，同時捕獲 stderr
    import io
    proc = subprocess.Popen(
        cmd, text=True, encoding="utf-8", errors="replace",
        cwd=script_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # 即時讀取 stdout 並印出
    for line in iter(proc.stdout.readline, ''):
        print(line, end='', flush=True)
    proc.stdout.close()
    stderr_out = proc.stderr.read()
    proc.stderr.close()
    proc.wait()
    if proc.returncode != 0:
        err = (stderr_out or "unknown error")[:2000]
        return proc.returncode, err, 0
    return 0, "", 0


# ------------------------------------------------------------------------------
# Job runners
# ------------------------------------------------------------------------------

def run_job(job_name: str, mode: str | None = None) -> bool:
    """執行一個 job，寫入 update_log，回傳成功與否"""
    conn = get_db()
    log_id = start_update_log(conn, job_name, mode)
    conn.close()

    print(f"[{_now()}] ▶ {job_name} 開始", flush=True)
    try:
        rc, err, records = _dispatch(job_name, mode)
    except Exception as exc:
        rc, err, records = 1, str(exc), 0

    conn = get_db()
    if rc == 0:
        finish_update_log(conn, log_id, "done", records=records)
        print(f"[{_now()}] ✓ {job_name} 完成", flush=True)
        conn.close()
        return True
    else:
        finish_update_log(conn, log_id, "failed", error=err)
        print(f"[{_now()}] ✗ {job_name} 失敗: {err[:200]}", flush=True)
        conn.close()
        return False


def _dispatch(job_name: str, mode: str | None) -> tuple[int, str, int]:
    if job_name == "stock_universe":
        try:
            if mode == "full-rebuild":
                records = _su_full_rebuild()
            else:
                records = _su_weekly_refresh()
            return 0, "", records or 0
        except Exception as exc:
            return 1, str(exc), 0

    SCRIPTS = {
        "ohlc":       ("fetch_ohlc.py",           ["--incremental"]),
        "financials": ("fetch_all_financials.py",  []),
        "ratios":     ("fetch_ratios_ttm.py",      []),
        "etf":        ("fetch_etf.py",             []),
        "13f":        ("fetch_13f.py",             []),
    }
    if job_name not in SCRIPTS:
        return 1, f"Unknown job: {job_name}", 0

    script, args = SCRIPTS[job_name]
    return _run_script(script, args)


# ------------------------------------------------------------------------------
# Preset groups
# ------------------------------------------------------------------------------

DAILY_JOBS  = ["ohlc", "ratios"]
WEEKLY_JOBS = ["stock_universe", "financials", "etf", "13f"]


def run_group(group: str, jobs: list[str]) -> None:
    print(f"\n{'='*60}", flush=True)
    print(f"[{_now()}] === {group.upper()} UPDATE START ===", flush=True)
    print(f"{'='*60}\n", flush=True)

    results = {}
    for job in jobs:
        results[job] = run_job(job, mode=group)

    # Summary
    print(f"\n{'='*60}", flush=True)
    print(f"[{_now()}] === {group.upper()} UPDATE DONE ===", flush=True)
    for job, ok in results.items():
        status = "✓ OK" if ok else "✗ FAIL"
        print(f"  {status}  {job}", flush=True)
    print(f"{'='*60}\n", flush=True)


# ------------------------------------------------------------------------------
# Entry
# ------------------------------------------------------------------------------

def main():
    # 確保 DB 表已建立
    init_tables()

    parser = argparse.ArgumentParser(description="Aurum 集中式更新排程器")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--daily",  action="store_true", help=f"每日任務: {DAILY_JOBS}")
    group.add_argument("--weekly", action="store_true", help=f"每週任務: {WEEKLY_JOBS}")
    group.add_argument("--job",    metavar="JOB",
                       help="執行單一 job: stock_universe | ohlc | financials | ratios | etf | 13f")
    args = parser.parse_args()

    if args.daily:
        run_group("daily", DAILY_JOBS)
    elif args.weekly:
        run_group("weekly", WEEKLY_JOBS)
    elif args.job:
        ok = run_job(args.job)
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
