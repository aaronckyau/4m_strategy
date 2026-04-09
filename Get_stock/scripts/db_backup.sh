#!/bin/bash
# db_backup.sh — 每日 DB 備份，保留 7 天
# Cron: 0 18 * * * (UTC 18:00 = HKT 02:00)

DB="/home/ec2-user/Aurum-Infinity-AI-new/aurum.db"
BACKUP_DIR="/home/ec2-user/backups"
FILENAME="aurum_$(date +%Y%m%d_%H%M).db"

mkdir -p "$BACKUP_DIR"
sqlite3 "$DB" ".backup '$BACKUP_DIR/$FILENAME'"

# 保留最近 7 天備份
find "$BACKUP_DIR" -name "aurum_*.db" -mtime +7 -delete

echo "[$(date)] Backup done: $FILENAME"
