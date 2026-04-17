#!/bin/bash
# daily_update.sh — 每日任務（OHLC + Ratios TTM）
# Contabo 建議 crontab 設定（每日 06:00 HKT = 22:00 UTC）：
#   0 22 * * * /opt/aurum/Aurum_Data_Fetcher/scripts/daily_update.sh >> /opt/aurum/logs/daily.log 2>&1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON="${PROJECT_DIR}/../.venv/bin/python3"

echo "=== daily_update.sh START: $(date '+%Y-%m-%d %H:%M:%S') ==="

cd "$PROJECT_DIR"
"$PYTHON" updater.py --daily

echo "=== daily_update.sh DONE: $(date '+%Y-%m-%d %H:%M:%S') ==="
