"""
blueprints/admin/routes.py - 管理後台路由
============================================================================
從 app.py 搬移的所有 Admin 相關路由 + IPO 管理。
url_prefix='/admin' 已在 __init__.py 設定。
============================================================================
"""
import json
import os
from datetime import datetime
from datetime import timezone
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

import markdown as md_lib
from flask import render_template, request, jsonify, redirect, abort, make_response
from werkzeug.utils import secure_filename

import yaml

from blueprints.admin import admin_bp
from logger import get_logger
from database import get_db

_log = get_logger(__name__)
from extensions import prompt_manager
from services.gemini_service import call_gemini_api, strip_card_summary
from services.feature_article_service import (
    delete_feature_article,
    get_feature_manifest_item,
    load_feature_articles,
    parse_feature_tags,
    save_feature_article,
    slugify_feature,
)
from services.news_service import resolve_news_cache_path

from file_cache import save_section_md, save_section_html
from read_stock_code import get_stock_info
from admin_auth import (
    verify_admin_password, create_admin_session,
    delete_admin_session, admin_required, verify_admin_session,
)

# 借用 stock blueprint 的工具函數
from blueprints.stock.routes import resolve_ticker, get_today

# IPO 檔案儲存

UPDATE_JOB_LABELS = {
    "stock_universe": "股票名單",
    "ohlc": "OHLC 日線",
    "financials": "財報",
    "ratios": "TTM 比率",
    "etf": "ETF",
    "13f": "13F",
    "analyst_forecast": "Analyst Forecast",
}
UPDATE_JOB_ORDER = ["stock_universe", "ohlc", "financials", "ratios", "etf", "13f", "analyst_forecast"]
FETCHER_DIR = (Path(__file__).resolve().parents[2] / ".." / "Aurum_Data_Fetcher").resolve()
UPDATER_PATH = FETCHER_DIR / "updater.py"
UPDATE_JOB_LOG_DIR = (Path(__file__).resolve().parents[2] / "logs" / "update-jobs").resolve()

# 防止 admin 按鈕雙擊觸發兩個子進程：每個 trigger key 記錄最後觸發時間
_TRIGGER_THROTTLE: dict[str, float] = {}
_TRIGGER_THROTTLE_SECONDS = 5.0


def _throttle_check(key: str) -> float | None:
    """若距上次觸發 < _TRIGGER_THROTTLE_SECONDS 秒，回傳剩餘秒數（float）；否則記錄並回 None。"""
    import time
    now = time.monotonic()
    last = _TRIGGER_THROTTLE.get(key)
    if last is not None and (now - last) < _TRIGGER_THROTTLE_SECONDS:
        return _TRIGGER_THROTTLE_SECONDS - (now - last)
    _TRIGGER_THROTTLE[key] = now
    return None


def _parse_iso(ts: str | None):
    if not ts:
        return None
    try:
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None


def _format_duration(started_at: str | None, finished_at: str | None) -> str:
    started = _parse_iso(started_at)
    finished = _parse_iso(finished_at)
    if not started or not finished:
        return "—"
    seconds = max(int((finished - started).total_seconds()), 0)
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {sec}s"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def _is_stale_running(row_dict: dict) -> bool:
    return _is_dataset_stale_running(
        row_dict.get("started_at"), row_dict.get("status"), None
    )


def _job_is_running(conn, job_names: list[str]) -> bool:
    return _dataset_is_running(conn, job_names)


def _safe_log_label(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in value).strip("-_") or "job"


def _tail_log_file(path: Path, lines: int = 160) -> str:
    if not path.exists():
        return ""
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        data = fh.readlines()
    return "".join(data[-lines:])


def _resolve_allowed_log_path(raw_path: str | None) -> Path | None:
    if not raw_path:
        return None
    try:
        path = Path(raw_path).resolve()
    except OSError:
        return None
    if not path.is_relative_to(UPDATE_JOB_LOG_DIR):
        return None
    return path


def _launch_updater(args: list[str]):
    if not UPDATER_PATH.exists():
        raise FileNotFoundError(f"找不到 updater.py：{UPDATER_PATH}")
    UPDATE_JOB_LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    job_key = "batch"
    if "--job" in args:
        idx = args.index("--job")
        if idx + 1 < len(args):
            job_key = _safe_log_label(args[idx + 1])
    elif "--all" in args:
        job_key = "all"
    elif "--daily" in args:
        job_key = "daily"
    elif "--weekly" in args:
        job_key = "weekly"
    log_path = (UPDATE_JOB_LOG_DIR / f"{timestamp}-{job_key}.log").resolve()
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    log_file = open(log_path, "a", encoding="utf-8", errors="replace")
    popen_kwargs = {
        "cwd": str(FETCHER_DIR),
        "stdout": log_file,
        "stderr": subprocess.STDOUT,
        "env": env,
        "close_fds": True,
    }
    if sys.platform == "win32":
        popen_kwargs["creationflags"] = (
            subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
        )
    else:
        popen_kwargs["start_new_session"] = True
    proc = subprocess.Popen(
        [sys.executable, str(UPDATER_PATH), *args, "--log-path", str(log_path)],
        **popen_kwargs,
    )
    log_file.close()
    return log_path, proc.pid


def _format_run_duration(started_at: str | None, finished_at: str | None,
                         duration_seconds: int | None = None) -> str:
    if duration_seconds is not None:
        seconds = max(int(duration_seconds), 0)
    else:
        started = _parse_iso(started_at)
        finished = _parse_iso(finished_at)
        if not started or not finished:
            return "—"
        seconds = max(int((finished - started).total_seconds()), 0)
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {sec}s"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def _is_dataset_stale_running(started_at: str | None, status: str | None,
                              timeout_minutes: int | None) -> bool:
    if status != "running":
        return False
    started = _parse_iso(started_at)
    if not started:
        return False
    elapsed_minutes = (datetime.now(timezone.utc) - started.replace(tzinfo=timezone.utc)).total_seconds() / 60
    return elapsed_minutes >= (timeout_minutes or 60)


def _format_frequency_label(frequency_type: str | None, sla_minutes: int | None) -> str:
    if frequency_type == "manual":
        return "手動"
    if frequency_type == "daily":
        return "每日 / 24h"
    if frequency_type == "weekly":
        return "每週 / 7d"
    if sla_minutes:
        if sla_minutes % (24 * 60) == 0:
            return f"{sla_minutes // (24 * 60)}d"
        if sla_minutes % 60 == 0:
            return f"{sla_minutes // 60}h"
        return f"{sla_minutes}m"
    return "—"


def _compute_dataset_freshness(last_success_at: str | None, sla_minutes: int | None,
                               last_status: str | None) -> tuple[str, str]:
    if not last_success_at:
        return "failed", "沒有成功紀錄"
    if not sla_minutes:
        return "healthy", "無 SLA"
    success_dt = _parse_iso(last_success_at)
    if not success_dt:
        return "warning", "時間格式異常"
    age_minutes = (datetime.now(timezone.utc) - success_dt.replace(tzinfo=timezone.utc)).total_seconds() / 60
    if age_minutes > sla_minutes:
        return "stale", "超過 SLA"
    if last_status == "failed":
        return "warning", "最近一次執行失敗"
    return "healthy", "在 SLA 內"


def _normalize_dataset_run(row):
    item = dict(row)
    item["job_name"] = item["dataset_key"]
    item["job_label"] = item["label"]
    item["duration"] = _format_run_duration(
        item.get("started_at"), item.get("finished_at"), item.get("duration_seconds")
    )
    item["display_status"] = (
        "stale"
        if _is_dataset_stale_running(
            item.get("started_at"), item.get("status"), item.get("running_timeout_minutes")
        )
        else (item.get("status") or "idle")
    )
    item["triggered_by"] = item.get("trigger_source")
    item["error_message"] = item.get("error_summary")
    item["records_updated"] = item.get("records_written", 0)
    item["frequency_label"] = _format_frequency_label(
        item.get("frequency_type"), item.get("freshness_sla_minutes")
    )
    freshness_state, freshness_label = _compute_dataset_freshness(
        item.get("last_success_at"),
        item.get("freshness_sla_minutes"),
        item.get("status"),
    )
    item["freshness_state"] = freshness_state
    item["freshness_label"] = freshness_label
    if item.get("display_status") == "done" and (item.get("failed_items") or 0) > 0:
        item["freshness_state"] = "warning"
        item["freshness_label"] = f"完成但有 {item.get('failed_items')} 個項目失敗"
    return item


def _dataset_status_rows(conn):
    rows = conn.execute("""
        SELECT
            dr.dataset_key,
            dr.label,
            dr.source_key,
            dr.frequency_type,
            dr.freshness_sla_minutes,
            dr.running_timeout_minutes,
            dr.criticality,
            dr.enabled,
            dr.manual_run_allowed,
            dr.sort_order,
            dr.notes,
            ur.id,
            ur.started_at,
            ur.finished_at,
            ur.status,
            ur.duration_seconds,
            ur.total_items,
            ur.success_items,
            ur.failed_items,
            ur.skipped_items,
            ur.records_written,
            ur.error_summary,
            ur.log_path,
            ur.trigger_source,
            ur.run_group_id,
            ur.mode,
            (
                SELECT MAX(finished_at)
                FROM update_runs s
                WHERE s.dataset_key = dr.dataset_key AND s.status = 'done'
            ) AS last_success_at
        FROM dataset_registry dr
        LEFT JOIN update_runs ur
          ON ur.id = (
              SELECT id
              FROM update_runs latest
              WHERE latest.dataset_key = dr.dataset_key
              ORDER BY latest.started_at DESC
              LIMIT 1
          )
        WHERE dr.enabled = 1
        ORDER BY dr.sort_order ASC, dr.dataset_key ASC
    """).fetchall()
    return [_normalize_dataset_run(row) for row in rows]


def _update_run_history(conn, limit: int = 120):
    rows = conn.execute("""
        SELECT
            ur.id,
            ur.dataset_key,
            ur.mode,
            ur.started_at,
            ur.finished_at,
            ur.status,
            ur.duration_seconds,
            ur.total_items,
            ur.success_items,
            ur.failed_items,
            ur.skipped_items,
            ur.records_written,
            ur.error_summary,
            ur.log_path,
            ur.trigger_source,
            ur.run_group_id,
            dr.label,
            dr.running_timeout_minutes,
            dr.frequency_type,
            dr.freshness_sla_minutes,
            dr.source_key,
            dr.criticality,
            (
                SELECT MAX(finished_at)
                FROM update_runs s
                WHERE s.dataset_key = ur.dataset_key AND s.status = 'done'
            ) AS last_success_at
        FROM update_runs ur
        LEFT JOIN dataset_registry dr ON dr.dataset_key = ur.dataset_key
        ORDER BY ur.started_at DESC
        LIMIT ?
    """, (limit,)).fetchall()
    return [_normalize_dataset_run(row) for row in rows]


def _build_console_summary(jobs: list[dict], rows: list[dict]):
    running = sum(1 for job in jobs if job.get("display_status") == "running")
    stale = sum(1 for job in jobs if job.get("freshness_state") == "stale")
    failed = sum(1 for row in rows[:50] if row.get("display_status") == "failed")
    partial_failed = sum(
        1
        for job in jobs
        if job.get("display_status") == "done" and (job.get("failed_items") or 0) > 0
    )
    last_success = next(
        (row.get("finished_at") for row in rows if row.get("display_status") == "done" and row.get("finished_at")),
        None,
    )
    last_full = next(
        (row.get("finished_at") for row in rows if row.get("run_group_id") and row.get("display_status") == "done"),
        None,
    )
    health = "healthy"
    if stale or failed or partial_failed:
        health = "warning"
    if any(job.get("criticality") == "high" and job.get("freshness_state") == "stale" for job in jobs):
        health = "critical"
    return {
        "health": health,
        "running_jobs": running,
        "failed_jobs": failed,
        "partial_failed_jobs": partial_failed,
        "stale_datasets": stale,
        "last_success_at": last_success,
        "last_full_update_at": last_full,
    }


def _collect_news_cache_health() -> dict:
    cache_path = resolve_news_cache_path()
    if cache_path is None:
        return {
            "key": "news_cache",
            "label": "News Cache",
            "value": "missing",
            "hint": "找不到新聞快取檔案",
            "status": "critical",
        }

    try:
        stat = cache_path.stat()
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "key": "news_cache",
            "label": "News Cache",
            "value": "error",
            "hint": f"快取不可讀：{exc}",
            "status": "critical",
        }

    articles = payload.get("articles", [])
    article_count = len(articles) if isinstance(articles, list) else 0
    fetched_at = str(payload.get("fetched_at", "")).strip() or "未知時間"
    mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    age_hours = (datetime.now(timezone.utc) - mtime).total_seconds() / 3600

    if article_count == 0:
        status = "critical"
        hint = f"{fetched_at}，沒有文章"
    elif age_hours > 24:
        status = "critical"
        hint = f"{fetched_at}，檔案已 {age_hours:.1f} 小時未更新"
    elif age_hours > 6:
        status = "warning"
        hint = f"{fetched_at}，檔案已 {age_hours:.1f} 小時未更新"
    else:
        status = "ok"
        hint = f"{fetched_at}，約 {age_hours:.1f} 小時前更新"

    return {
        "key": "news_cache",
        "label": "News Cache",
        "value": f"{article_count:,} 篇",
        "hint": hint,
        "status": status,
    }


def _collect_data_health(conn) -> list[dict]:
    """檢查實際資料層的新鮮度與完整性，補足 update_log 看不到的缺口。

    每個項目 status:
      - ok       : 綠燈，資料正常
      - warning  : 黃燈，偏離正常但仍可用
      - critical : 紅燈，影響前端功能
    """
    def safe(query: str, default=0):
        try:
            row = conn.execute(query).fetchone()
            return row[0] if row and row[0] is not None else default
        except Exception:
            return default

    ohlc_latest = safe("SELECT MAX(date) FROM ohlc_daily", default=None)
    ohlc_ticker_count = safe("SELECT COUNT(DISTINCT ticker) FROM ohlc_daily")
    ohlc_stale = safe(
        "SELECT COUNT(DISTINCT ticker) FROM ohlc_daily "
        "WHERE ticker NOT IN (SELECT ticker FROM ohlc_daily WHERE date > date('now','-5 days'))"
    )
    sp500_count = safe("SELECT COUNT(*) FROM sp500_constituents")
    sm_total = safe("SELECT COUNT(*) FROM stocks_master")
    sm_with_cap = safe("SELECT COUNT(*) FROM stocks_master WHERE market_cap IS NOT NULL AND market_cap > 0")
    etf_count = safe("SELECT COUNT(*) FROM etf_list")
    etf_with_holdings = safe(
        "SELECT COUNT(DISTINCT etf_symbol) FROM etf_holdings"
    )
    fin_latest_q = safe(
        "SELECT MAX(period || '-Q' || fiscal_quarter) FROM financial_statements "
        "WHERE fiscal_quarter IS NOT NULL",
        default=None,
    )
    ratios_count = safe("SELECT COUNT(*) FROM ratios_ttm")
    inst_tickers = safe("SELECT COUNT(DISTINCT ticker) FROM institutional_holdings")

    checks = []

    # OHLC 最新日期（最關鍵）
    if not ohlc_latest:
        checks.append({"key": "ohlc_latest", "label": "OHLC 最新交易日",
                       "value": "—", "hint": "無任何 OHLC 資料", "status": "critical"})
    else:
        try:
            latest_dt = datetime.strptime(ohlc_latest, "%Y-%m-%d")
            age_days = (datetime.now() - latest_dt).days
            if age_days > 5:
                status = "critical"
            elif age_days > 2:
                status = "warning"
            else:
                status = "ok"
            hint = f"距今 {age_days} 天" if age_days else "今日已同步"
        except Exception:
            status = "warning"
            hint = "日期格式異常"
        checks.append({"key": "ohlc_latest", "label": "OHLC 最新交易日",
                       "value": ohlc_latest or "—", "hint": hint, "status": status})

    # OHLC 斷線股票數
    stale_ratio = (ohlc_stale / ohlc_ticker_count * 100) if ohlc_ticker_count else 0
    if stale_ratio > 10:
        status = "critical"
    elif stale_ratio > 3:
        status = "warning"
    else:
        status = "ok"
    checks.append({
        "key": "ohlc_stale",
        "label": "OHLC 斷線 ≥5 天",
        "value": f"{ohlc_stale:,} 檔",
        "hint": f"佔總 {ohlc_ticker_count:,} 檔的 {stale_ratio:.1f}%",
        "status": status,
    })

    # S&P500 成分股（Heatmap 依賴）
    if sp500_count == 0:
        status = "critical"
        hint = "Heatmap 無法渲染"
    elif sp500_count < 450:
        status = "warning"
        hint = "數量偏低，可能同步異常"
    else:
        status = "ok"
        hint = "S&P500 heatmap 資料齊備"
    checks.append({"key": "sp500", "label": "S&P 500 成分股",
                   "value": f"{sp500_count:,} 檔", "hint": hint, "status": status})

    # Market cap 覆蓋率
    cap_ratio = (sm_with_cap / sm_total * 100) if sm_total else 0
    if cap_ratio < 80:
        status = "critical"
    elif cap_ratio < 95:
        status = "warning"
    else:
        status = "ok"
    checks.append({
        "key": "market_cap",
        "label": "Market Cap 覆蓋率",
        "value": f"{cap_ratio:.1f}%",
        "hint": f"{sm_with_cap:,} / {sm_total:,} 檔",
        "status": status,
    })

    # Ratios TTM
    if ratios_count == 0:
        status = "critical"
    elif ratios_count < sm_total * 0.5:
        status = "warning"
    else:
        status = "ok"
    checks.append({
        "key": "ratios",
        "label": "TTM 比率筆數",
        "value": f"{ratios_count:,} 檔",
        "hint": f"主表 {sm_total:,} 檔",
        "status": status,
    })

    # 財報最新季度
    checks.append({
        "key": "financials",
        "label": "最新財報季度",
        "value": fin_latest_q or "—",
        "hint": f"財報來源：FMP",
        "status": "ok" if fin_latest_q else "warning",
    })

    # ETF master + holdings
    if etf_count == 0:
        status = "critical"
        hint = "ETF 清單為空"
    elif etf_with_holdings == 0:
        status = "warning"
        hint = f"{etf_count:,} 個 ETF，但無持倉明細"
    else:
        status = "ok"
        hint = f"{etf_count:,} 個 ETF，{etf_with_holdings:,} 有持倉明細"
    checks.append({"key": "etf", "label": "ETF Master / Holdings",
                   "value": f"{etf_count:,}", "hint": hint, "status": status})

    # 13F
    if inst_tickers == 0:
        status = "warning"
        hint = "無 13F 資料"
    else:
        status = "ok"
        hint = f"覆蓋 {inst_tickers:,} 檔股票"
    checks.append({"key": "insider_13f", "label": "13F 機構持倉",
                   "value": f"{inst_tickers:,} 檔", "hint": hint, "status": status})

    checks.append(_collect_news_cache_health())

    return checks


def _today_pulse(jobs: list[dict]) -> dict:
    """一眼看懂今天的更新脈搏。"""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    pulse_items = []
    for job in jobs:
        started = job.get("started_at") or ""
        last_success = job.get("last_success_at") or ""
        is_today = started.startswith(today)

        if job.get("display_status") == "running":
            tone = "running"
            label = f"⟳ {started[11:16] if is_today else '—'} running"
        elif is_today and job.get("display_status") == "done":
            tone = "ok"
            records = job.get("records_updated") or 0
            label = f"✓ {started[11:16]} ({records:,})"
        elif is_today and job.get("display_status") == "failed":
            tone = "failed"
            label = f"✗ {started[11:16]} failed"
        elif job.get("freshness_state") == "healthy":
            tone = "ok"
            label = f"✓ {last_success[:10] if last_success else '—'}"
        elif job.get("freshness_state") == "stale":
            tone = "stale"
            label = f"⏱ stale (SLA 超時)"
        else:
            tone = "idle"
            label = "— 未執行"

        pulse_items.append({
            "job_name": job.get("job_name") or job.get("dataset_key"),
            "job_label": job.get("job_label") or job.get("label"),
            "tone": tone,
            "label": label,
            "criticality": job.get("criticality") or "medium",
        })

    counts = {"ok": 0, "failed": 0, "running": 0, "stale": 0, "idle": 0}
    for item in pulse_items:
        counts[item["tone"]] = counts.get(item["tone"], 0) + 1

    if counts["failed"] > 0 or counts["stale"] > 0:
        overall = "warning"
    elif counts["running"] > 0:
        overall = "running"
    elif counts["ok"] >= len(pulse_items) - counts["idle"] and counts["ok"] > 0:
        overall = "healthy"
    else:
        overall = "idle"

    return {
        "today": today,
        "items": pulse_items,
        "counts": counts,
        "overall": overall,
    }


def _consecutive_failures(rows: list[dict]) -> dict:
    """計算每個 dataset 最近連續失敗次數（從最新開始往回數）。"""
    streaks: dict[str, dict] = {}
    # rows 已按 started_at DESC 排好序
    for row in rows:
        key = row.get("dataset_key") or row.get("job_name")
        if not key:
            continue
        status = row.get("status") or row.get("display_status")
        if key not in streaks:
            streaks[key] = {
                "count": 0,
                "terminated": False,
                "latest_error": None,
            }
        bucket = streaks[key]
        if bucket["terminated"]:
            continue
        if status == "failed":
            bucket["count"] += 1
            if not bucket["latest_error"]:
                bucket["latest_error"] = row.get("error_summary") or row.get("error_message")
        elif status == "done":
            bucket["terminated"] = True
        # running / idle / stale 不終止也不累加
    return {k: v for k, v in streaks.items() if v["count"] >= 2}


def _log_mtime(log_path: str | None) -> datetime | None:
    """取得 log 檔最後修改時間（UTC），找不到回 None。"""
    if not log_path:
        return None
    try:
        path = Path(log_path)
        if not path.exists():
            return None
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return None


def _pid_is_alive(pid: int | None) -> bool:
    """檢查 pid 是否還在執行。無 psutil 時保守回 True 避免誤殺。"""
    if not pid:
        return False
    try:
        import psutil
        return psutil.pid_exists(int(pid))
    except Exception:
        return True


# dataset → (table, timestamp_column)
# liveness 就看這些欄位的 MAX() 有沒有在動。DB 在寫 = fetcher 活著，log 是否靜默不影響。
_DATASET_LIVENESS_TABLES: dict[str, tuple[str, str]] = {
    "financials": ("stocks_master", "financials_updated_at"),
    "ohlc":       ("stocks_master", "ohlc_updated_at"),
    "ratios":     ("ratios_ttm", "fetched_at"),
    "etf":        ("etf_list", "fetched_at"),
    "13f":        ("institutional_holdings", "fetched_at"),
    "analyst_forecast": ("analyst_price_targets", "fetched_at"),
    # stock_universe 沒單一時間欄，跳過 DB liveness（它本來就跑很快）
}


def _db_last_write_age_seconds(conn, dataset_key: str) -> int | None:
    """查該 dataset 對應表最後寫入到現在的秒數。無對應表 / 查詢失敗回 None。"""
    mapping = _DATASET_LIVENESS_TABLES.get(dataset_key)
    if not mapping:
        return None
    table, col = mapping
    try:
        row = conn.execute(f"SELECT MAX({col}) FROM {table}").fetchone()
    except Exception:
        return None
    if not row or not row[0]:
        return None
    latest = _parse_iso(str(row[0]))
    if not latest:
        return None
    return int((datetime.now(timezone.utc) - latest.replace(tzinfo=timezone.utc)).total_seconds())


def _reconcile_stale_runs(conn) -> int:
    """掃瞄 status='running' 的 run，只判定「真死掉」的進程為 failed。

    判定原則：**DB 在寫 = fetcher 活著**。log 靜默 / pid 消失都不足以判死。
    只有同時滿足「DB 沒動 + pid 消失」或「超過 timeout 2 倍」才判死。

    避免重演「fetcher 高速寫 DB 但 log buffer 沒 flush，被誤標 failed」的歷史。
    """
    rows = conn.execute(
        """
        SELECT ur.id, ur.dataset_key, ur.started_at, ur.pid, ur.log_path,
               COALESCE(dr.running_timeout_minutes, 120) AS timeout_min
        FROM update_runs ur
        LEFT JOIN dataset_registry dr ON dr.dataset_key = ur.dataset_key
        WHERE ur.status = 'running'
        """
    ).fetchall()

    if not rows:
        return 0

    now_utc = datetime.now(timezone.utc)
    reconciled = 0
    for row in rows:
        started = _parse_iso(row["started_at"])
        if not started:
            continue
        elapsed_min = (now_utc - started.replace(tzinfo=timezone.utc)).total_seconds() / 60
        timeout_min = float(row["timeout_min"] or 120)

        reason = None
        db_age = _db_last_write_age_seconds(conn, row["dataset_key"])

        # 【第一關】DB 在動就直接判活，不管 log / pid
        # db_age < 120 秒 = 最近兩分鐘還有寫入 = fetcher 絕對活著
        if db_age is not None and db_age < 120:
            continue

        # 【第二關】DB 沒動 + pid 消失 = 才判死
        # 剛起跑的前 60 秒跳過（pid 可能還在啟動）
        if elapsed_min > 1 and row["pid"] and not _pid_is_alive(row["pid"]):
            reason = f"pid {row['pid']} 已結束且 DB 無新寫入"

        # 【第三關】DB 沒動 + 無 pid + log 超久沒輸出（保守 20 分鐘）
        if not reason and not row["pid"] and elapsed_min > 5:
            mtime = _log_mtime(row["log_path"])
            if mtime:
                idle_sec = (now_utc - mtime).total_seconds()
                if idle_sec > 1200:  # 20 分鐘
                    reason = f"log 停止 {int(idle_sec)} 秒、DB 無新寫入、無 pid 可驗證"

        # 【最後防線】跑超過 timeout 2 倍強制判死
        if not reason and elapsed_min > timeout_min * 2:
            reason = f"執行超過 {int(elapsed_min)} 分鐘（2× 上限 {int(timeout_min)} 分鐘）"

        if not reason:
            continue

        finished_iso = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        duration = int((now_utc - started.replace(tzinfo=timezone.utc)).total_seconds())
        conn.execute(
            """
            UPDATE update_runs
            SET status = 'failed',
                finished_at = ?,
                duration_seconds = ?,
                error_summary = ?
            WHERE id = ? AND status = 'running'
            """,
            (finished_iso, duration, ("進程已中斷：" + reason)[:1000], row["id"]),
        )
        reconciled += 1

    if reconciled:
        conn.commit()
        _log.warning("reconciled %d stale update_runs", reconciled)
    return reconciled


def _dataset_is_running(conn, job_names: list[str]) -> bool:
    placeholders = ",".join("?" for _ in job_names)
    rows = conn.execute(
        f"""
        SELECT ur.dataset_key, ur.started_at, ur.status, dr.running_timeout_minutes
        FROM update_runs ur
        JOIN dataset_registry dr ON dr.dataset_key = ur.dataset_key
        WHERE ur.status = 'running' AND ur.dataset_key IN ({placeholders})
        """,
        job_names,
    ).fetchall()
    return any(
        not _is_dataset_stale_running(row["started_at"], row["status"], row["running_timeout_minutes"])
        for row in rows
    )



# ============================================================================
# 路由
# ============================================================================

@admin_bp.route('/')
def admin_root():
    token = request.cookies.get('admin_token')
    if verify_admin_session(token):
        return redirect('/admin/dashboard')
    return redirect('/admin/login')


@admin_bp.route('/login', methods=['GET', 'POST'])
def admin_login():
    error = None
    if request.method == 'POST':
        password = request.form.get('password', '')
        if verify_admin_password(password):
            token    = create_admin_session()
            response = make_response(redirect('/admin/dashboard'))
            response.set_cookie(
                'admin_token', token,
                httponly=True, samesite='Lax', max_age=86400
            )
            return response
        error = '密碼錯誤，請重試。'
    return render_template('admin/login.html', error=error)


@admin_bp.route('/logout')
def admin_logout():
    token = request.cookies.get('admin_token')
    if token:
        delete_admin_session(token)
    response = make_response(redirect('/admin/login'))
    response.delete_cookie('admin_token')
    return response


@admin_bp.route('/dashboard')
@admin_required
def admin_dashboard():
    sections    = prompt_manager.get_section_names()
    cache_count = 0
    feature_count = len(load_feature_articles())
    cache_dir   = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'cache')
    if os.path.exists(cache_dir):
        cache_count = sum(
            1 for d in os.listdir(cache_dir)
            if os.path.isdir(os.path.join(cache_dir, d))
        )
    return render_template('admin/dashboard.html',
                           sections=sections,
                           cache_count=cache_count,
                           feature_count=feature_count)


def _feature_form_payload(form=None, feature=None):
    form = form or {}
    feature = feature or {}
    tags = form.get("tags") if hasattr(form, "get") else None
    if tags is None:
        tags = ", ".join(feature.get("tags", []))
    return {
        "slug": form.get("slug", feature.get("slug", "")),
        "title": form.get("title", feature.get("title", "")),
        "summary": form.get("summary", feature.get("summary", "")),
        "date": form.get("date", feature.get("date", "")),
        "tags": tags,
        "source": form.get("source", feature.get("source", "4M 專題")),
        "html_file": feature.get("html_file", ""),
        "image_file": feature.get("image_file", ""),
    }


@admin_bp.route('/features')
@admin_required
def admin_features():
    features = load_feature_articles()
    return render_template('admin/features_list.html', features=features)


@admin_bp.route('/features/new', methods=['GET', 'POST'])
@admin_required
def admin_feature_add():
    error = None
    if request.args.get('error') == 'file_too_large':
        error = '上傳檔案過大，請將 HTML 與圖片總大小控制在 8 MB 內。'
    form_data = _feature_form_payload()

    if request.method == 'POST':
        form_data = _feature_form_payload(request.form)
        upload = request.files.get('html_file')
        image_upload = request.files.get('image_file')
        html_bytes = None
        image_bytes = None
        image_filename = None
        if upload and upload.filename:
            filename = secure_filename(upload.filename)
            if not filename.lower().endswith('.html'):
                error = '只接受 .html 檔案'
            else:
                html_bytes = upload.read()
                if not html_bytes:
                    error = '上傳的 HTML 檔案是空的'
        if error is None and image_upload and image_upload.filename:
            image_filename = secure_filename(image_upload.filename)
            image_bytes = image_upload.read()
            if not image_bytes:
                error = 'ä¸Šå‚³çš„åœ–ç‰‡æª”æ¡ˆæ˜¯ç©ºçš„'
        if error is None:
            try:
                feature = save_feature_article(
                    slug=form_data["slug"],
                    title=form_data["title"],
                    summary=form_data["summary"],
                    date=form_data["date"],
                    tags=parse_feature_tags(form_data["tags"]),
                    source=form_data["source"],
                    html_bytes=html_bytes,
                    image_bytes=image_bytes,
                    image_filename=image_filename,
                )
                return redirect(f"/admin/features/{feature['slug']}/edit?saved=1")
            except Exception as exc:
                error = str(exc)

    return render_template(
        'admin/feature_form.html',
        mode='add',
        feature=form_data,
        error=error,
    )


@admin_bp.route('/features/<slug>/edit', methods=['GET', 'POST'])
@admin_required
def admin_feature_edit(slug: str):
    feature_item = get_feature_manifest_item(slug)
    if not feature_item:
        abort(404)

    error = None
    if request.args.get('error') == 'file_too_large':
        error = '上傳檔案過大，請將 HTML 與圖片總大小控制在 8 MB 內。'
    form_data = _feature_form_payload(feature=feature_item)
    saved = request.args.get("saved") == "1"

    if request.method == 'POST':
        form_data = _feature_form_payload(request.form, feature=feature_item)
        upload = request.files.get('html_file')
        image_upload = request.files.get('image_file')
        html_bytes = None
        image_bytes = None
        image_filename = None
        if upload and upload.filename:
            filename = secure_filename(upload.filename)
            if not filename.lower().endswith('.html'):
                error = '只接受 .html 檔案'
            else:
                html_bytes = upload.read()
                if not html_bytes:
                    error = '上傳的 HTML 檔案是空的'
        if error is None and image_upload and image_upload.filename:
            image_filename = secure_filename(image_upload.filename)
            image_bytes = image_upload.read()
            if not image_bytes:
                error = 'ä¸Šå‚³çš„åœ–ç‰‡æª”æ¡ˆæ˜¯ç©ºçš„'
        if error is None:
            try:
                feature = save_feature_article(
                    slug=form_data["slug"],
                    title=form_data["title"],
                    summary=form_data["summary"],
                    date=form_data["date"],
                    tags=parse_feature_tags(form_data["tags"]),
                    source=form_data["source"],
                    html_bytes=html_bytes,
                    image_bytes=image_bytes,
                    image_filename=image_filename,
                    original_slug=slug,
                )
                return redirect(f"/admin/features/{feature['slug']}/edit?saved=1")
            except Exception as exc:
                error = str(exc)
                saved = False

    return render_template(
        'admin/feature_form.html',
        mode='edit',
        feature=form_data,
        error=error,
        saved=saved,
        preview_url=f"/news/features/{slugify_feature(form_data['slug'] or slug)}",
    )


@admin_bp.route('/features/<slug>/delete', methods=['POST'])
@admin_required
def admin_feature_delete(slug: str):
    deleted = delete_feature_article(slug)
    if not deleted:
        abort(404)
    return redirect('/admin/features')


@admin_bp.route('/prompts/<section_key>', methods=['GET', 'POST'])
@admin_required
def admin_prompt_editor(section_key: str):
    sections = prompt_manager.get_section_names()
    if section_key not in sections:
        abort(404)

    msg = None
    if request.method == 'POST':
        new_prompt = request.form.get('prompt_content', '')
        prompt_manager.update_section_prompt(section_key, new_prompt)
        msg = '已儲存'

    current_prompt = prompt_manager.get_section_prompt(section_key)
    return render_template('admin/prompt_editor.html',
                           section_key=section_key,
                           section_name=sections[section_key],
                           prompt_content=current_prompt,
                           sections=sections,
                           msg=msg)


@admin_bp.route('/prompts/<section_key>/save', methods=['POST'])
@admin_required
def admin_save_prompt(section_key: str):
    """儲存編輯後的 prompt 內容到 prompts.yaml"""
    sections = prompt_manager.get_section_names()
    if section_key not in sections:
        return jsonify({"success": False, "error": "找不到此 section"}), 404

    if not request.json:
        return jsonify({"success": False, "error": "請求格式錯誤，需要 JSON"}), 400
    new_content = request.json.get('content', '')
    if not new_content.strip():
        return jsonify({"success": False, "error": "Prompt 內容不可為空"}), 400

    try:
        prompt_manager.update_section_prompt(section_key, new_content)
        return jsonify({"success": True})
    except Exception as e:
        _log.error("save_prompt %s: %s", section_key, e)
        return jsonify({"success": False, "error": "儲存失敗，請稍後重試"}), 500


def _build_prompt_variables(ticker, stock_name, exchange, db_info=None):
    """組裝 prompt 模板變數"""
    ctx = prompt_manager._get_exchange_context(exchange)
    from services.data_vars import resolve_data_vars
    data_vars = resolve_data_vars(ticker)
    chinese_name = (db_info or {}).get("name_zh_hk") or stock_name
    currency = (db_info or {}).get("currency") or ctx.get("currency", "")
    market = (db_info or {}).get("market", "")
    return {
        "ticker":         ticker,
        "stock_name":     stock_name,
        "chinese_name":   chinese_name,
        "exchange":       exchange,
        "market":         market,
        "today":          get_today(),
        "currency":       currency,
        "data_source":    ctx.get("data_source", ""),
        "legal_focus":    ctx.get("legal_focus", ""),
        "extra_analysis": ctx.get("extra_analysis", ""),
        **data_vars,
    }


@admin_bp.route('/resolve_vars', methods=['POST'])
@admin_required
def admin_resolve_vars():
    """查詢某個股票代碼對應的所有變數實際值"""
    if not request.json:
        return jsonify({"success": False, "error": "請求格式錯誤，需要 JSON"}), 400
    raw_ticker = request.json.get('ticker', '').strip()
    if not raw_ticker:
        return jsonify({"success": False, "error": "請輸入股票代碼"}), 400

    ticker  = resolve_ticker(raw_ticker)
    db_info = get_stock_info(ticker)

    if db_info is None:
        return jsonify({"success": False, "error": f"找不到 {raw_ticker} 的資料"}), 404

    stock_name = db_info["name"] or ticker
    exchange   = db_info["exchange"] or ""
    variables  = _build_prompt_variables(ticker, stock_name, exchange, db_info=db_info)

    return jsonify({
        "success":   True,
        "ticker":    ticker,
        "stock_name": stock_name,
        "exchange":  exchange,
        "variables": variables,
    })


@admin_bp.route('/prompts/<section_key>/preview', methods=['POST'])
@admin_required
def admin_preview_prompt(section_key: str):
    """用當前編輯中的 prompt 內容對指定股票執行 AI 預覽"""
    data       = request.json
    raw_ticker = data.get('ticker', '').strip()
    content    = data.get('content', '').strip()

    if not raw_ticker:
        return jsonify({"success": False, "error": "請輸入股票代碼"}), 400
    if not content:
        return jsonify({"success": False, "error": "Prompt 內容不可為空"}), 400

    ticker  = resolve_ticker(raw_ticker)
    db_info = get_stock_info(ticker)

    if db_info is None:
        return jsonify({"success": False, "error": f"找不到 {raw_ticker} 的資料"}), 404

    stock_name = db_info["name"] or ticker
    exchange   = db_info["exchange"] or ""
    variables  = _build_prompt_variables(ticker, stock_name, exchange, db_info=db_info)

    global_cfg  = prompt_manager._config.get('global', {})
    system_role = global_cfg.get('system_role', '')
    format_rules = global_cfg.get('format_rules', '')
    full_prompt = f"{system_role}\n\n{content}\n\n{format_rules}"
    for key, val in variables.items():
        full_prompt = full_prompt.replace(f'{{{key}}}', str(val))

    try:
        response_text = call_gemini_api(full_prompt, use_search=True)

        save_section_md(ticker, section_key, response_text, lang="zh_hk")
        _log.info("已儲存預覽 Markdown %s - %s → zh-TW", ticker, section_key)

        html_content = md_lib.markdown(
            response_text,
            extensions=['tables', 'fenced_code', 'nl2br']
        )
        html_content = strip_card_summary(html_content)
        return jsonify({
            "success":    True,
            "html":       html_content,
            "ticker":     ticker,
            "stock_name": stock_name,
            "exchange":   exchange,
        })
    except Exception as e:
        _log.error("preview_prompt %s/%s: %s", ticker, section_key, e)
        return jsonify({"success": False, "error": "預覽執行失敗，請稍後重試"}), 500


# ============================================================================
# 更新日誌
# ============================================================================

@admin_bp.route('/update-log')
@admin_required
def update_log():
    """資料更新中心：摘要、狀態與歷史紀錄 + Data Health + Today's Pulse。"""
    conn = get_db()
    data_health: list[dict] = []
    try:
        try:
            _reconcile_stale_runs(conn)
        except Exception as exc:
            _log.warning("reconcile stale runs failed: %s", exc)
        jobs = _dataset_status_rows(conn)
        rows = _update_run_history(conn, limit=200)
        summary = _build_console_summary(jobs, rows)
        pulse = _today_pulse(jobs)
        failure_streaks = _consecutive_failures(rows)
        try:
            data_health = _collect_data_health(conn)
        except Exception as exc:
            _log.warning("data_health check failed: %s", exc)
            data_health = []
    except Exception:
        summary = {
            "health": "warning",
            "running_jobs": 0,
            "failed_jobs": 0,
            "stale_datasets": 0,
            "last_success_at": None,
            "last_full_update_at": None,
        }
        jobs = [
            {
                "job_name": job,
                "job_label": job,
                "dataset_key": job,
                "source_key": "—",
                "frequency_label": "—",
                "freshness_state": "idle",
                "freshness_label": "—",
                "status": "idle",
                "display_status": "idle",
                "total_items": 0,
                "success_items": 0,
                "failed_items": 0,
                "records_updated": 0,
                "started_at": None,
                "finished_at": None,
                "duration": "—",
                "mode": None,
                "error_message": None,
                "triggered_by": None,
                "run_group_id": None,
            }
            for job in UPDATE_JOB_ORDER
        ]
        rows = []
        pulse = {"today": "", "items": [], "counts": {}, "overall": "idle"}
        failure_streaks = {}
    finally:
        conn.close()
    return render_template(
        'admin/update_log.html',
        summary=summary,
        jobs=jobs,
        rows=rows,
        job_order=UPDATE_JOB_ORDER,
        pulse=pulse,
        failure_streaks=failure_streaks,
        data_health=data_health,
    )


@admin_bp.route('/update-log/run/<job_name>', methods=['POST'])
@admin_required
def run_update_job(job_name: str):
    if job_name not in UPDATE_JOB_ORDER:
        return jsonify({"success": False, "error": "找不到指定資料集"}), 404

    remaining = _throttle_check(f"job:{job_name}")
    if remaining is not None:
        return jsonify({
            "success": False,
            "error": f"請等待 {remaining:.1f} 秒再觸發（防止重複點擊）",
        }), 429

    conn = get_db()
    try:
        meta = conn.execute(
            "SELECT label, manual_run_allowed FROM dataset_registry WHERE dataset_key = ?",
            (job_name,),
        ).fetchone()
        label = meta["label"] if meta else job_name
        if meta and not meta["manual_run_allowed"]:
            return jsonify({"success": False, "error": f"{label} 已停用手動更新"}), 409
        if _dataset_is_running(conn, [job_name]):
            return jsonify({"success": False, "error": f"{label} 正在執行中"}), 409
        # 任何資料集都不允許在 run-all 期間單獨觸發
        if _job_is_running(conn, UPDATE_JOB_ORDER):
            return jsonify({"success": False, "error": "目前已有其他更新工作執行中"}), 409
    finally:
        conn.close()

    try:
        _, pid = _launch_updater(["--job", job_name, "--triggered-by", "admin"])
    except Exception as exc:
        _log.error("launch update job failed: %s", exc)
        return jsonify({"success": False, "error": "無法啟動背景更新工作"}), 500

    return jsonify({
        "success": True,
        "message": f"{label} 已送出更新工作",
        "job_name": job_name,
        "pid": pid,
    })


@admin_bp.route('/update-log/run-all', methods=['POST'])
@admin_required
def run_update_all():
    remaining = _throttle_check("all")
    if remaining is not None:
        return jsonify({
            "success": False,
            "error": f"請等待 {remaining:.1f} 秒再觸發（防止重複點擊）",
        }), 429

    conn = get_db()
    try:
        if _job_is_running(conn, UPDATE_JOB_ORDER):
            return jsonify({"success": False, "error": "目前已有更新工作執行中"}), 409
    finally:
        conn.close()

    run_group_id = uuid4().hex
    try:
        _, pid = _launch_updater([
            "--all",
            "--triggered-by", "admin",
            "--run-group-id", run_group_id,
        ])
    except Exception as exc:
        _log.error("launch update all failed: %s", exc)
        return jsonify({"success": False, "error": "無法啟動一鍵更新全部"}), 500

    return jsonify({
        "success": True,
        "message": "已送出一鍵更新全部工作",
        "run_group_id": run_group_id,
        "pid": pid,
    })


@admin_bp.route('/update-log/log/<job_name>')
@admin_required
def update_log_stream(job_name: str):
    if job_name not in UPDATE_JOB_ORDER:
        return jsonify({"success": False, "error": "找不到指定資料集"}), 404

    lines = request.args.get("lines", default=160, type=int) or 160
    lines = max(20, min(lines, 600))

    conn = get_db()
    try:
        try:
            _reconcile_stale_runs(conn)
        except Exception as exc:
            _log.warning("reconcile stale runs (log stream) failed: %s", exc)
        run = conn.execute(
            """
            SELECT id, dataset_key, status, started_at, finished_at, log_path, error_summary
            FROM update_runs
            WHERE dataset_key = ?
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (job_name,),
        ).fetchone()
        db_age = _db_last_write_age_seconds(conn, job_name)
    finally:
        conn.close()

    if not run:
        return jsonify({
            "success": True,
            "job_name": job_name,
            "status": "idle",
            "started_at": None,
            "finished_at": None,
            "log_path": None,
            "content": "",
            "log_mtime_iso": None,
            "log_age_seconds": None,
            "db_age_seconds": db_age,
            "error_summary": None,
        })

    log_path = _resolve_allowed_log_path(run["log_path"])
    safe_log_path = str(log_path) if log_path else None
    content = _tail_log_file(log_path, lines=lines) if log_path else ""
    mtime = _log_mtime(safe_log_path)
    log_age = None
    mtime_iso = None
    if mtime:
        mtime_iso = mtime.strftime("%Y-%m-%dT%H:%M:%SZ")
        log_age = int((datetime.now(timezone.utc) - mtime).total_seconds())
    return jsonify({
        "success": True,
        "job_name": job_name,
        "status": run["status"],
        "started_at": run["started_at"],
        "finished_at": run["finished_at"],
        "log_path": safe_log_path,
        "content": content,
        "log_mtime_iso": mtime_iso,
        "log_age_seconds": log_age,
        "db_age_seconds": db_age,
        "error_summary": run["error_summary"],
    })


@admin_bp.route('/update-log/run-status')
@admin_required
def update_run_status():
    """全局輪詢 API：目前有哪個資料集在跑，進度如何。前端 banner / 按鈕 disable 用。"""
    conn = get_db()
    try:
        try:
            _reconcile_stale_runs(conn)
        except Exception as exc:
            _log.warning("reconcile in run-status failed: %s", exc)
        rows = conn.execute(
            """
            SELECT ur.id, ur.dataset_key, ur.status, ur.started_at, ur.finished_at,
                   ur.total_items, ur.success_items, ur.failed_items, ur.skipped_items,
                   ur.records_written, ur.error_summary, ur.log_path, ur.trigger_source,
                   dr.label
            FROM update_runs ur
            LEFT JOIN dataset_registry dr ON dr.dataset_key = ur.dataset_key
            WHERE ur.status = 'running'
            ORDER BY ur.started_at ASC
            """
        ).fetchall()

        now_utc = datetime.now(timezone.utc)
        active = []
        for row in rows:
            started = _parse_iso(row["started_at"])
            elapsed_sec = int((now_utc - started.replace(tzinfo=timezone.utc)).total_seconds()) if started else 0

            # 從 log 抓最後一個進度行作為「現在在做什麼」的提示
            progress_hint = None
            safe_path = _resolve_allowed_log_path(row["log_path"]) if row["log_path"] else None
            mtime = _log_mtime(str(safe_path)) if safe_path else None
            log_age = int((now_utc - mtime).total_seconds()) if mtime else None
            if safe_path and safe_path.exists():
                path = safe_path
                if path.exists():
                    try:
                        with path.open("r", encoding="utf-8", errors="replace") as fh:
                            tail = fh.readlines()[-5:]
                        for line in reversed(tail):
                            stripped = line.strip()
                            if not stripped:
                                continue
                            progress_hint = stripped[:200]
                            import re
                            m = re.search(r"\[\s*(\d+)\s*/\s*(\d+)\s*\]", stripped)
                            if m:
                                done, total = int(m.group(1)), int(m.group(2))
                                progress_hint = f"{done}/{total}（{done/total*100:.1f}%）"
                            break
                    except OSError:
                        pass

            # DB 寫入年齡 — 這才是真正的 liveness 指標
            db_age = _db_last_write_age_seconds(conn, row["dataset_key"])

            active.append({
                "id": row["id"],
                "dataset_key": row["dataset_key"],
                "label": row["label"] or row["dataset_key"],
                "trigger_source": row["trigger_source"],
                "started_at": row["started_at"],
                "elapsed_seconds": elapsed_sec,
                "progress_hint": progress_hint,
                "log_age_seconds": log_age,
                "db_age_seconds": db_age,
            })
    finally:
        conn.close()

    return jsonify({
        "success": True,
        "is_any_running": bool(active),
        "active": active,
    })


@admin_bp.route('/update-log/run/<int:run_id>/failures')
@admin_required
def update_run_failures(run_id: int):
    """查看特定 run 的失敗 / 空資料明細（供前端 modal 顯示）。"""
    conn = get_db()
    try:
        run = conn.execute(
            """
            SELECT ur.id, ur.dataset_key, ur.status, ur.started_at, ur.finished_at,
                   ur.total_items, ur.success_items, ur.failed_items, ur.skipped_items,
                   ur.records_written, ur.error_summary,
                   dr.label
            FROM update_runs ur
            LEFT JOIN dataset_registry dr ON dr.dataset_key = ur.dataset_key
            WHERE ur.id = ?
            """,
            (run_id,),
        ).fetchone()
        if not run:
            return jsonify({"success": False, "error": "找不到這筆更新紀錄"}), 404

        items = conn.execute(
            """
            SELECT item_key, item_type, status, error_message, records_written,
                   started_at, finished_at
            FROM update_run_items
            WHERE run_id = ? AND status IN ('failed', 'skipped')
            ORDER BY CASE status WHEN 'failed' THEN 0 ELSE 1 END, item_key
            LIMIT 1000
            """,
            (run_id,),
        ).fetchall()
    finally:
        conn.close()

    return jsonify({
        "success": True,
        "run": {
            "id": run["id"],
            "dataset_key": run["dataset_key"],
            "label": run["label"] or run["dataset_key"],
            "status": run["status"],
            "started_at": run["started_at"],
            "finished_at": run["finished_at"],
            "total_items": run["total_items"],
            "success_items": run["success_items"],
            "failed_items": run["failed_items"],
            "skipped_items": run["skipped_items"],
            "records_written": run["records_written"],
            "error_summary": run["error_summary"],
        },
        "items": [
            {
                "ticker": r["item_key"],
                "item_type": r["item_type"],
                "status": r["status"],
                "reason": r["error_message"],
                "records": r["records_written"],
            }
            for r in items
        ],
    })

