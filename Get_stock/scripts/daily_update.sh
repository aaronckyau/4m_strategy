#!/bin/bash
# daily_update.sh — 每日更新：股票清單 + OHLC 增量 + Ratios TTM
# Cron: 0 9 * * 1-5 (UTC 09:00 = HKT 17:00)
set -euo pipefail

BASE="/home/ec2-user/prod"
LOG="$BASE/logs/daily_$(date +%Y%m%d).log"
source "$BASE/shared.env"

echo "=== Daily Update $(date) ===" >> "$LOG"

cd "$BASE/Get_stock"
python3.11 generate_name.py --weekly-refresh >> "$LOG" 2>&1

cd "$BASE/Aurum_Data_Fetcher"
python3.11 fetch_ohlc.py --incremental --concurrency 30 >> "$LOG" 2>&1
python3.11 fetch_ratios_ttm.py --concurrency 30 >> "$LOG" 2>&1

echo "=== Done $(date) ===" >> "$LOG"
