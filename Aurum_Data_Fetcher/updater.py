"""
updater.py — 集中式更新排程器
============================================================================
用法（SSH / crontab）:

  # 每日任務（OHLC + ratios）
  python updater.py --daily

  # 每週任務（股票名單 + 財報 + 13F）
  python updater.py --weekly

  # 執行單一 job
  python updater.py --job stock_universe
  python updater.py --job ohlc
  python updater.py --job financials
  python updater.py --job ratios
  python updater.py --job etf   # 只更新 11 檔 sector ETF master
  python updater.py --job 13f
  python updater.py --job insider_sec

每個 job 開始/結束都會寫入 update_log 表，可透過管理後台查看。
============================================================================
"""
import argparse
import json
import os
import subprocess
import sys
import socket
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

# 確保能 import 同目錄的 db.py
sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import (
    get_db,
    start_update_log,
    finish_update_log,
    init_tables,
    start_update_run,
    finish_update_run,
    replace_update_run_items,
)
from jobs.stock_universe import full_rebuild as _su_full_rebuild
from jobs.stock_universe import weekly_refresh as _su_weekly_refresh

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _console_line(text: str) -> str:
    try:
        text.encode(sys.stdout.encoding or "utf-8")
        return text
    except Exception:
        return text.encode("ascii", "replace").decode("ascii")


def _run_script(script: str, extra_args: list[str] | None = None) -> tuple[int, str, dict]:
    """在子程序執行同目錄的 Python 腳本，即時輸出 stdout+stderr。"""
    script_dir = str(Path(__file__).parent)
    cmd = [sys.executable, str(Path(__file__).parent / script)] + (extra_args or [])
    last_lines: list[str] = []
    summary: dict = {}
    proc = subprocess.Popen(
        cmd, text=True, encoding="utf-8", errors="replace",
        cwd=script_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # 合併 stderr → stdout，避免 buffer deadlock
    )
    for line in iter(proc.stdout.readline, ''):
        print(line, end='', flush=True)
        if line.startswith("RUN_SUMMARY_JSON:"):
            try:
                summary = json.loads(line.split(":", 1)[1].strip())
            except json.JSONDecodeError:
                pass
        last_lines.append(line)
        if len(last_lines) > 50:
            last_lines.pop(0)
    proc.stdout.close()
    proc.wait()
    if proc.returncode != 0:
        err = ''.join(last_lines)[-2000:]
        return proc.returncode, err, summary
    return 0, "", summary


def _count_updated_records(job_name: str, started_at: str) -> int:
    """依 job 類型計算本次更新數量。"""
    conn = get_db()
    try:
        if job_name == "ohlc":
            row = conn.execute(
                "SELECT COUNT(*) FROM stocks_master WHERE ohlc_updated_at >= ?",
                (started_at,),
            ).fetchone()
        elif job_name == "financials":
            row = conn.execute(
                "SELECT COUNT(*) FROM stocks_master WHERE financials_updated_at >= ?",
                (started_at,),
            ).fetchone()
        elif job_name == "ratios":
            row = conn.execute(
                "SELECT COUNT(*) FROM ratios_ttm WHERE fetched_at >= ?",
                (started_at,),
            ).fetchone()
        elif job_name == "etf":
            row = conn.execute(
                "SELECT COUNT(*) FROM etf_list WHERE fetched_at >= ?",
                (started_at,),
            ).fetchone()
        elif job_name == "13f":
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT ticker)
                FROM institutional_holdings
                WHERE fetched_at >= ?
                """,
                (started_at,),
            ).fetchone()
        elif job_name == "analyst_forecast":
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT ticker) FROM (
                    SELECT ticker FROM analyst_price_targets WHERE fetched_at >= ?
                    UNION
                    SELECT ticker FROM analyst_grades_consensus WHERE fetched_at >= ?
                    UNION
                    SELECT ticker FROM analyst_grades_historical WHERE fetched_at >= ?
                    UNION
                    SELECT ticker FROM analyst_grade_events WHERE fetched_at >= ?
                )
                """,
                (started_at, started_at, started_at, started_at),
            ).fetchone()
        else:
            return 0
        return int(row[0] or 0)
    finally:
        conn.close()


def _normalize_summary(job_name: str, started_at: str, raw_summary: dict | None) -> dict:
    summary = dict(raw_summary or {})
    records_written = int(summary.get("records_written") or 0)
    total_items = int(summary.get("total_items") or 0)
    success_items = int(summary.get("success_items") or 0)
    failed_items = int(summary.get("failed_items") or 0)
    skipped_items = int(summary.get("skipped_items") or 0)

    if records_written <= 0:
        records_written = _count_updated_records(job_name, started_at)
    if total_items <= 0:
        total_items = records_written
    if success_items <= 0 and failed_items == 0:
        success_items = total_items

    summary["records_written"] = records_written
    summary["total_items"] = total_items
    summary["success_items"] = success_items
    summary["failed_items"] = failed_items
    summary["skipped_items"] = skipped_items
    summary["items"] = summary.get("items") or []
    return summary


# ------------------------------------------------------------------------------
# Job runners
# ------------------------------------------------------------------------------

def run_job(job_name: str, mode: str | None = None, triggered_by: str = "scheduler",
            run_group_id: str | None = None, log_path: str | None = None,
            extra_args: list[str] | None = None) -> bool:
    """執行一個 job，寫入 update_log，回傳成功與否"""
    conn = get_db()
    started_at = _now_iso()
    log_id = start_update_log(
        conn, job_name, mode, triggered_by=triggered_by,
        run_group_id=run_group_id, started_at=started_at
    )
    run_id = start_update_run(
        conn, job_name, mode=mode, trigger_source=triggered_by,
        run_group_id=run_group_id, started_at=started_at,
        host=socket.gethostname(), log_path=log_path,
        pid=os.getpid(),
    )
    conn.close()

    print(_console_line(f"[{_now()}] [START] {job_name}"), flush=True)
    try:
        rc, err, summary = _dispatch(job_name, mode, extra_args=extra_args)
    except Exception as exc:
        rc, err, summary = 1, str(exc), {}

    conn = get_db()
    if rc == 0:
        normalized = _normalize_summary(job_name, started_at, summary)
        finish_update_log(conn, log_id, "done", records=normalized["records_written"])
        finish_update_run(
            conn, run_id, "done",
            total_items=normalized["total_items"],
            success_items=normalized["success_items"],
            failed_items=normalized["failed_items"],
            skipped_items=normalized["skipped_items"],
            records_written=normalized["records_written"],
            error_summary=normalized.get("summary_text"),
        )
        replace_update_run_items(conn, run_id, normalized["items"])
        print(_console_line(f"[{_now()}] [DONE] {job_name}"), flush=True)
        conn.close()
        return True
    else:
        finish_update_log(conn, log_id, "failed", error=err)
        finish_update_run(conn, run_id, "failed", error_summary=err[:1000])
        # 即使 fetcher 整體失敗，也保留它已回報的失敗清單
        partial_items = (summary or {}).get("items") or []
        if partial_items:
            try:
                replace_update_run_items(conn, run_id, partial_items)
            except Exception:
                pass
        print(_console_line(f"[{_now()}] [FAIL] {job_name}: {err[:200]}"), flush=True)
        conn.close()
        return False


def _dispatch(job_name: str, mode: str | None, extra_args: list[str] | None = None) -> tuple[int, str, dict]:
    if job_name == "stock_universe":
        try:
            if mode == "full-rebuild":
                summary = _su_full_rebuild()
            else:
                summary = _su_weekly_refresh()
            return 0, "", summary or {}
        except Exception as exc:
            return 1, str(exc), {}

    SCRIPTS = {
        "ohlc":       ("fetch_ohlc.py",           []),
        "financials": ("fetch_all_financials.py",  []),
        "ratios":     ("fetch_ratios_ttm.py",      []),
        "etf":        ("fetch_etf_master.py",      []),
        "13f":        ("fetch_13f.py",             []),
        "insider_sec": ("fetch_sec_insider.py",     []),
        "analyst_forecast": ("fetch_analyst_forecast.py", []),
    }
    if job_name not in SCRIPTS:
        return 1, f"Unknown job: {job_name}", {}

    script, args = SCRIPTS[job_name]
    if job_name == "ohlc" and not extra_args:
        args = ["--incremental"]
    return _run_script(script, args + (extra_args or []))


# ------------------------------------------------------------------------------
# Preset groups
# ------------------------------------------------------------------------------

DAILY_JOBS  = ["ohlc", "ratios", "insider_sec"]
WEEKLY_JOBS = ["stock_universe", "financials", "13f", "analyst_forecast"]


def run_group(group: str, jobs: list[str], triggered_by: str = "scheduler",
              run_group_id: str | None = None, log_path: str | None = None,
              extra_args_by_job: dict[str, list[str]] | None = None) -> None:
    run_group_id = run_group_id or uuid4().hex
    print(_console_line(f"\n{'='*60}"), flush=True)
    print(_console_line(f"[{_now()}] === {group.upper()} UPDATE START ==="), flush=True)
    print(_console_line(f"{'='*60}\n"), flush=True)

    results = {}
    for job in jobs:
        results[job] = run_job(
            job, mode=group, triggered_by=triggered_by,
            run_group_id=run_group_id, log_path=log_path,
            extra_args=(extra_args_by_job or {}).get(job)
        )

    # Summary
    print(_console_line(f"\n{'='*60}"), flush=True)
    print(_console_line(f"[{_now()}] === {group.upper()} UPDATE DONE ==="), flush=True)
    for job, ok in results.items():
        status = "OK" if ok else "FAIL"
        print(_console_line(f"  {status}  {job}"), flush=True)
    print(_console_line(f"{'='*60}\n"), flush=True)


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
    group.add_argument("--all",    action="store_true", help="執行完整更新流程")
    group.add_argument("--job",    metavar="JOB",
                       help="執行單一 job: stock_universe | ohlc | financials | ratios | etf | 13f | insider_sec | analyst_forecast")
    parser.add_argument(
        "--triggered-by",
        default="scheduler",
        choices=["scheduler", "manual", "admin"],
        help="標記這次更新由誰觸發",
    )
    parser.add_argument(
        "--run-group-id",
        default=None,
        help="同一批更新的識別碼，供 admin 端追蹤一鍵更新全部",
    )
    parser.add_argument(
        "--log-path",
        default=None,
        help="將這次更新執行紀錄綁定到指定 log 檔路徑",
    )
    parser.add_argument(
        "--backfill-from",
        default=None,
        help="只在 --job ohlc 時可用，指定日線回補起始日 YYYY-MM-DD",
    )

    args = parser.parse_args()

    if args.daily:
        run_group(
            "daily", DAILY_JOBS, triggered_by=args.triggered_by,
            run_group_id=args.run_group_id, log_path=args.log_path
        )
    elif args.weekly:
        run_group(
            "weekly", WEEKLY_JOBS, triggered_by=args.triggered_by,
            run_group_id=args.run_group_id, log_path=args.log_path
        )
    elif args.all:
        run_group(
            "manual-all",
            ["stock_universe", "ohlc", "financials", "ratios", "13f", "insider_sec", "analyst_forecast"],
            triggered_by=args.triggered_by,
            run_group_id=args.run_group_id,
            log_path=args.log_path,
        )
    elif args.job:
        extra_args = None
        if args.job == "ohlc" and args.backfill_from:
            extra_args = ["--backfill-from", args.backfill_from]
        ok = run_job(
            args.job, triggered_by=args.triggered_by,
            run_group_id=args.run_group_id, log_path=args.log_path,
            extra_args=extra_args
        )
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
