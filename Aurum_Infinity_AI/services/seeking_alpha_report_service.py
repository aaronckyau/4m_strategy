from __future__ import annotations

import json
from pathlib import Path
from typing import Any


_APP_DIR = Path(__file__).resolve().parents[1]
_WORKSPACE_DIR = _APP_DIR.parent
_REPORT_PATHS = [
    _WORKSPACE_DIR / "News_fetcher" / "data" / "seeking_alpha_report.json",
    _WORKSPACE_DIR / "data" / "seeking_alpha_report.json",
]


def resolve_seeking_alpha_report_path() -> Path | None:
    for path in _REPORT_PATHS:
        if path.exists():
            return path
    return None


def load_seeking_alpha_report() -> dict[str, Any]:
    report_path = resolve_seeking_alpha_report_path()
    if report_path is None:
        return {
            "available": False,
            "message": "找不到市場熱話報告。請先執行 News_fetcher/fetch_seeking_alpha_report.py。",
        }

    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "available": False,
            "message": f"市場熱話報告讀取失敗：{exc}",
            "path": str(report_path),
        }

    return {
        "available": bool(payload.get("report")),
        "message": "" if payload.get("report") else "市場熱話報告尚未包含 Gemini 分析結果。",
        "path": str(report_path),
        "generated_at": payload.get("generated_at"),
        "source": payload.get("source") or {},
        "meta": payload.get("meta") or {},
        "articles": payload.get("articles") or [],
        "report": payload.get("report") or {},
    }
