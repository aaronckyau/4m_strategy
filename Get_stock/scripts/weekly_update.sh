#!/bin/bash
# weekly_update.sh — 每週更新：日更新 + 財報（12季）
# Cron: 0 19 * * 5 (UTC 週五 19:00 = HKT 週六 03:00)
set -euo pipefail

BASE="/home/ec2-user/prod"
LOG="$BASE/logs/weekly_$(date +%Y%m%d).log"
source "$BASE/shared.env"

echo "=== Weekly Update $(date) ===" >> "$LOG"

# 先執行日更新
"$BASE/scripts/daily_update.sh"

cd "$BASE/Aurum_Data_Fetcher"
python3.11 fetch_all_financials.py --concurrency 30 >> "$LOG" 2>&1

# Form 13F 機構持倉（僅美股）
python3.11 fetch_13f.py --market US --concurrency 30 >> "$LOG" 2>&1

echo "=== Done $(date) ===" >> "$LOG"
