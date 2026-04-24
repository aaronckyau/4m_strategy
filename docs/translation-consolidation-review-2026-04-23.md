# 翻譯架構整併 — 獨立驗證與專業判斷

日期：2026-04-23
審閱：Claude（對照 Codex 之 checklist 獨立覆核）
來源：`C:\Users\aaron\Documents\Obsidian Vault\contabo\4m_system\translation-architecture-consolidation-checklist.md`

---

## 一、方法

逐一覆核 Codex 在 checklist 中指出的檔案與行號，實際讀原始碼比對。以下判斷皆附 `file_path:line_number` 作為依據。

---

## 二、四層翻譯模型（同意 Codex）

Codex 將翻譯相關程式歸納為四層，這個分類是正確的：

| 層級 | 權威位置 | 說明 |
|------|----------|------|
| L1 UI 字串 | [translations.py](Aurum_Infinity_AI/translations.py) | 中央 dict，`SUPPORTED_LANGS = ["zh_hk", "zh_cn"]` |
| L2 語言偵測 | [blueprints/stock/routes.py:232](Aurum_Infinity_AI/blueprints/stock/routes.py#L232) `get_current_lang()` | URL → Cookie → Accept-Language → default |
| L3 內容翻譯（AI） | [prompt_manager.py:203](Aurum_Infinity_AI/prompt_manager.py#L203) `build_translation_prompt()` | Gemini 動態翻譯 + `file_cache` 快取 |
| L4 名稱字典（DB） | `stocks.name_zh_hk/name_zh_cn` + `sector_industry_i18n` | 建 index 時由 OpenCC 寫入 |

這個四層模型要當作整併的**錨點**，不能混用。

---

## 三、逐點覆核 Codex 的判斷

### ✅ 同意 #1：`get_current_lang()` 重複定義

Codex 說有兩份。實際驗證：

- [blueprints/stock/routes.py:232-249](Aurum_Infinity_AI/blueprints/stock/routes.py#L232-L249) — 原始版本（帶 docstring）
- [blueprints/news_radar/routes.py:39-53](Aurum_Infinity_AI/blueprints/news_radar/routes.py#L39-L53) — **逐字抄寫**，只是省略 docstring

**使用端點**（共 7 處 import + 12 處呼叫）：
- `trending/routes.py:7`, `news/routes.py:6`, `insider/routes.py:4`, `ipo/routes.py:15` — 都從 `stock.routes` import
- `news_radar` 自己定義 — 唯一特例

**優先級：P0**（風險低、收益高，消除最明顯的重複）

---

### ✅ 同意 #2：`display_name` 選擇邏輯重複

Codex 指出 stock/routes.py 有多處 `lang → name_zh_hk/name_zh_cn/name` 三分支重複。驗證結果：

| 位置 | 用途 | 是否帶 lang |
|------|------|-------------|
| [stock/routes.py:689-697](Aurum_Infinity_AI/blueprints/stock/routes.py#L689-L697) | 頁面 render | ✅ |
| [stock/routes.py:781-785](Aurum_Infinity_AI/blueprints/stock/routes.py#L781-L785) | `/api/stock_display` | ✅ |
| [stock/routes.py:905-908](Aurum_Infinity_AI/blueprints/stock/routes.py#L905-L908) | 搜尋結果 | ✅ |
| [stock/routes.py:682](Aurum_Infinity_AI/blueprints/stock/routes.py#L682) | render kwarg（固定用繁） | ❌ |
| [stock/routes.py:1029](Aurum_Infinity_AI/blueprints/stock/routes.py#L1029) | AI prompt 輸入（固定用繁） | ❌ |
| [admin/routes.py:1070](Aurum_Infinity_AI/blueprints/admin/routes.py#L1070) | 管理頁 | ❌ |

**修正我先前的誤判**：1029 不是 bug。它是「AI 報告 input 永遠用繁中當 source of truth」，後面第 1053 行才按 lang 翻譯。這是刻意設計。

**優先級：P0**（3 處帶 lang 的選擇器可合併成 `get_display_name(db_info, lang)` helper）

---

### ⚠️ 部分同意 #3：`app.py` 的 `inject_html_lang`

Codex 把它當成「第三份語言偵測」。驗證：

[app.py:68-72](Aurum_Infinity_AI/app.py#L68-L72) 確實是獨立的偵測路徑（只讀 URL + Cookie，**缺 Accept-Language**）。

**但**這裡不能直接呼叫 `get_current_lang()` — 因為 context_processor 可能在路由函式之前執行，不能依賴 `flask.g` 已填好。整併方式要用：

```python
# utils/request_helpers.py
def detect_lang_from_request() -> str:
    # 純函式，讀 request，不依賴 g
    ...
```

然後 `app.py` 與各 blueprint 的 `get_current_lang()` 都呼叫這個。

**優先級：P1**（改善面小，但消除「三份偵測」的認知負擔）

---

### ✅ 同意 #4：`build_translation_prompt` 是內容翻譯的唯一入口

驗證：只在 [stock/routes.py:1102](Aurum_Infinity_AI/blueprints/stock/routes.py#L1102) 與 [stock/routes.py:1663](Aurum_Infinity_AI/blueprints/stock/routes.py#L1663) 被呼叫。

Codex 建議包一個 `TranslationService`。我的判斷：**現階段不必**。只有 2 個呼叫點，抽服務層反而增加間接層。等到第三個場景需要 AI 翻譯時再抽。

**優先級：P3（延後）**

---

### ⚠️ 調整 #5：OpenCC 的定位

Codex 說「OpenCC 規則應移到 translation 資產」。驗證：

OpenCC 只在 [Get_stock/generate_name.py](Get_stock/generate_name.py) 使用，而 `Get_stock/` 已在 [docs/cleanup-2026-04-22.md](docs/cleanup-2026-04-22.md) 規劃刪除。活躍版本在 `Aurum_Data_Fetcher/jobs/stock_universe.py`，並非運行時翻譯。

**結論**：OpenCC 屬 L4 的離線 ETL，**不該**併入翻譯服務。Codex 的分類正確，但動作錯誤 — 這塊不需要整併，只需等 `Get_stock/` 刪除後，讓 `stock_universe.py` 成為唯一的 name dictionary 產生器。

**優先級：跳過**（由 cleanup 任務自然消解）

---

## 四、Codex 遺漏的項目

### 🔴 遺漏 #1：殘留的 `lang == 'en'` 死碼

[templates/ipo/index.html:71](Aurum_Infinity_AI/templates/ipo/index.html#L71)：
```jinja
{% if lang == 'en' %}{{ month_names_en[cal.month] }}
```

但 `SUPPORTED_LANGS = ["zh_hk", "zh_cn"]`，`'en'` 永遠不會命中 → **死分支**。

[templates/admin/ipo_form.html:376](Aurum_Infinity_AI/templates/admin/ipo_form.html#L376) 提示文字：
```
將重新翻譯所有內容為簡體中文和英文
```

誤導使用者以為有英文支援。

**優先級：P1**（屬翻譯層的資訊潔淨，不處理會埋雷）

---

### 🟡 遺漏 #2：`translations.py` 裡的 `get_translations()` 可配合語言偵測

現在各 blueprint 是：
```python
lang = get_current_lang()
t = get_translations(lang)
```

Codex 沒提到這個 pair 其實可以合併成 `get_translations_for_request()` 一次到位，減少每個 route 兩行樣板。

**優先級：P2**

---

### 🟡 遺漏 #3：`is_translation_stale` 快取失效判斷

[stock/routes.py:1045](Aurum_Infinity_AI/blueprints/stock/routes.py#L1045) 有 `is_translation_stale(ticker, section, lang)`。這是 L3 的附屬邏輯，Codex 的服務層 proposal 應把這個一併納入才完整。

**優先級：P3**（跟 #4 綁）

---

## 五、最終優先排序（我的建議）

| # | 動作 | 優先級 | 預估改動 | 風險 |
|---|------|--------|----------|------|
| 1 | 建 `utils/request_helpers.py`，移入 `detect_lang_from_request()` 與 `get_current_lang()` 作 thin wrapper；刪 `news_radar` 的重複版本 | **P0** | 3 檔 | 低 |
| 2 | 建 `get_display_name(db_info, lang)` helper（放 `utils/` 或 `services/stock_display.py`），替換 3 處帶 lang 的選擇器 | **P0** | 3 檔 | 低 |
| 3 | 清理 `lang == 'en'` 死分支（IPO 模板 + admin 表單文案） | **P1** | 2 檔 | 極低 |
| 4 | `app.py:inject_html_lang` 改呼 `detect_lang_from_request()` | **P1** | 1 檔 | 低（要測 context_processor 執行時機） |
| 5 | 合併 `get_translations_for_request()` 樣板 | **P2** | 所有 blueprint | 中（改動點多） |
| 6 | 抽 `TranslationService` | **P3** | 3 檔 | 中（目前價值不高） |

---

## 六、與 Codex 主要差異

| 議題 | Codex 立場 | 我的立場 | 差異原因 |
|------|-----------|---------|----------|
| `stock/routes.py:1029` 無 lang check | 未提及 | **非 bug，是設計** | AI input 固定用繁中作 source |
| OpenCC 移入翻譯資產 | 建議移動 | **跳過** | `Get_stock/` 將刪除 |
| `TranslationService` | 建議立刻抽 | **延後** | 只 2 個呼叫點，過早抽象 |
| 死碼 `lang == 'en'` | 未提及 | **P1 必清** | 屬翻譯潔淨，不清會誤導 |
| `app.py:inject_html_lang` | 當重複 | **半獨立**（context_processor 時機） | 不能無腦合併 |

---

## 七、執行建議

**分 PR**：
- PR-1：`utils/request_helpers.py` + 刪 `news_radar` 重複（P0 #1）
- PR-2：`get_display_name` helper（P0 #2）
- PR-3：死碼清理（P1 #3）— 可跟 cleanup 任務合併
- PR-4 以後：視需要再做

**先做 PR-1 + PR-2** 就能回收 90% 收益。其餘等功能穩定再動。
