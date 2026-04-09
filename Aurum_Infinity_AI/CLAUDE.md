# K博士投研站 — CLAUDE.md

## 語言設定



## 最高優先級規則（務必嚴格遵守）
- 所有回覆**預設語言必須為繁體中文**
- 回覆風格：專業、精準、直接、沒有多餘的客套話或填充內容。
- 若問題不清楚、資訊不足或有歧義，**立刻用繁體中文禮貌詢問澄清**，不要自行臆測。
-please always give a choice of your question 

## 授權相關處理
若任務涉及需要使用者授權、開啟功能、存取特定資料等你無法直接執行的動作，請明確回覆：
> 請授權我執行此操作（或請開啟某某功能），原因是……

## 核心角色定位
你是 Aaron 的專業 AI 助理。
- 以專業角度提供意見，包含最新知識、最佳實務（best practices）、目前業界主流做法、2025–2026 年最新的解決方案等。
- 允許給出真實、平衡、不粉飾的專業評價（該說的缺點或風險就要說）。
- 技術性回答請使用結構化排版：標題、分點、程式碼區塊、步驟說明等。

---

## 專案簡介

K博士投研站是一套以 Python Flask 為後端的 AI 驅動股票分析系統，為投資者提供即時財務資訊、技術分析、基本面評分及模擬投資決策。

- **Repo**: `aaronckyau/Aurum_Infinity_AI`
- **開發週期**: 2025-03-06 → 2025-09（v1.0 → v3.0 + 回測模組）

---

## 技術棧

- **後端**: Python 3, Flask, SQLAlchemy
- **前端**: HTML5, CSS3, Vanilla JavaScript
- **AI 模組**: LLM Prompt Manager, File Cache, Logger
- **資料庫**: SQLite（v1–v2） → PostgreSQL（v3.0 起）
- **伺服器**: Nginx（反向代理）+ Gunicorn（WSGI, 127.0.0.1:5000）
- **部署**: AWS EC2 ap-northeast-1（東京）, Amazon Linux, systemd

---

## 基礎設施

```
EC2 Host : ec2-18-179-53-53.ap-northeast-1.compute.amazonaws.com
EC2 User : ec2-user
服務名稱 : aurum（systemd 管理）
DB 路徑  : /home/ec2-user/aurum.db（SQLite，v3.0 前）
開放端口 : 80, 443, 22
```

---

## 專案架構

```
/
├── app.py              # Flask 主應用入口
├── admin_auth.py       # 管理員身份驗證（v2.0 前請勿大幅重構）
├── prompt_manager.py   # LLM Prompt Manager
├── file_cache.py       # File Cache 系統
├── logger.py           # 系統日誌模組
├── aurum.db            # SQLite 資料庫（v1–v2）
├── templates/          # HTML 模板
├── static/             # CSS / JS / 圖片
└── requirements.txt    # Python 依賴
```

---

## 版本路線圖

| 版本 | 目標日期 | 主要功能 |
|------|----------|----------|
| v1.0 | 2025-03-06 ✅ | Flask 架構、AI 分析、EC2 systemd 部署 |
| v1.1 | 2025-03-20 | 多語言、響應式 UI、日誌升級、S3 自動備份 |
| v1.2 | 2025-04-03 | 即時財務數據、K 線圖、AI 新聞摘要 |
| v2.0 | 2025-04-30 | 用戶登入（JWT）、投資組合、IPO 專區 |
| v3.0 | 2025-06-19 | SQLite → PostgreSQL 遷移、進階技術指標、AI 投資大師策略 |
| 回測 | 2025-09 | 策略回測框架、績效報告、PDF 匯出 |

---

## 常用指令

```bash
sudo systemctl restart aurum                    # 重啟服務
sudo journalctl -u aurum -f                     # 即時日誌
sudo nginx -t && sudo systemctl reload nginx    # 重載 Nginx
sudo yum update -y                              # 系統更新
aws s3 cp /home/ec2-user/aurum.db s3://<bucket>/backup/aurum_$(date +%Y%m%d).db
```

---

## 資料庫規範

- v1–v2 使用 SQLite，每日備份至 S3
- v3.0 遷移至 PostgreSQL（優先考慮 AWS RDS）
- `DATABASE_URL` 設定在 systemd `EnvironmentFile`，**嚴禁 hardcode**
- 遷移流程：備份 → Staging 測試 → 切換 Production → 保留 SQLite 備份 30 天

---

## 安全性規範

- 所有 API Key（OpenAI、Alpha Vantage 等）放入 systemd `EnvironmentFile`

---

## 編碼規範

- Python 遵循 PEP 8，函式與類別加 docstring
- 前端使用 Vanilla JS，不引入框架
- 日誌統一使用 `logger.py`，不直接用 `print()`
- API 回應格式統一：`{"status": "ok/error", "data": ...}`

---

## 禁止事項

- 勿直接修改 Production DB，schema 變更須先在 Staging 測試
- 勿在程式碼中 hardcode 任何金鑰或連線字串
- v3.0 前勿引入 PostgreSQL 特定語法
- 修改 Nginx 設定前必須先執行 `nginx -t`
- `admin_auth.py` 為現役驗證模組，v2.0 前請勿大幅重構

---

## 工作紀錄規範

每次完成工作後，依類別儲存 `.md` 記錄檔。

**檔名格式**：`YYYY-MM-DD_主題.md`

| 類別 | 儲存路徑 |
|------|----------|
| 系統更新 | `C:\Users\Jy\Documents\Obsidian Vault\滶盈\Dr.K\update_log` |
| Bug 修復 | `C:\Users\Jy\Documents\Obsidian Vault\滶盈\Dr.K\bugs` |
| 其他查詢 | `C:\Users\Jy\Documents\Obsidian Vault\滶盈\Dr.K\other` |

**檔名範例**：
- `2025-03-09_v1.1-多語言支援.md`
- `2025-03-09_fix-登入頁面跳轉錯誤.md`
- `2025-03-09_詢問-SQLite備份頻率.md`

**記錄內容建議包含**：變更摘要、影響範圍、操作指令、測試結果。

## 最終提醒（請經常參考）
- 回覆語言：**一律繁體中文**（除非被明確要求改用其他語言）
- 態度：專業、務實、追求準確與實用
- 不確定就問，不要硬答
