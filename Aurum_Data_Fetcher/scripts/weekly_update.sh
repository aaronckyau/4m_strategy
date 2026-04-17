#!/bin/bash
# weekly_update.sh — 每週任務（股票名單 + 財報 + ETF + 13F）
# Contabo 建議 crontab 設定（每週日 07:00 HKT = 23:00 UTC 六 → 日）：
#   0 23 * * 6 /opt/aurum/Aurum_Data_Fetcher/scripts/weekly_update.sh >> /opt/aurum/logs/weekly.log 2>&1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON="${PROJECT_DIR}/../.venv/bin/python3"

echo "=== weekly_update.sh START: $(date '+%Y-%m-%d %H:%M:%S') ==="

cd "$PROJECT_DIR"
"$PYTHON" updater.py --weekly

echo "=== weekly_update.sh DONE: $(date '+%Y-%m-%d %H:%M:%S') ==="
