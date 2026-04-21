from __future__ import annotations

import os
import sqlite3
from logger import get_logger

_log = get_logger(__name__)

# ============================================================
# DB 連線
# ============================================================
_DB_PATH = os.environ.get("DATABASE_URL") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "aurum.db"
)

def _get_db():
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _row_to_dict(row) -> dict | None:
    if row is None:
        return None
    return dict(row)


# ============================================================
# Ticker 標準化（純邏輯，不查 DB）
# ============================================================
def _cn_exchange_suffix(code: str) -> str:
    if code[0] == '6':
        return '.SS'
    if code[0] in ('0', '2', '3'):
        return '.SZ'
    return '.SS'


def normalize_ticker(ticker: str) -> str:
    """
    標準化 ticker 至 FMP 格式（= DB PK）：
      - 英文代碼：AAPL -> AAPL
      - 帶後綴：0700.HK -> 0700.HK
      - 純數字 <=4 位：700 -> 0700.HK（港股）
      - 純數字 5 位：00700 -> 0700.HK（港股）
      - 純數字 6 位：600519 -> 600519.SS（A 股）
    """
    raw = ticker.upper().strip()
    if '.' in raw or not raw.isdigit():
        return raw
    if len(raw) <= 4:
        return raw.zfill(4) + '.HK'
    if len(raw) == 5:
        return raw.lstrip('0').zfill(4) + '.HK'
    return raw + _cn_exchange_suffix(raw)


# ============================================================
# DB 查詢
# ============================================================
def _find_in_db(ticker: str) -> dict | None:
    """
    嘗試多種格式在 DB 中查找股票。
    回傳 dict 或 None。
    """
    code = normalize_ticker(ticker)

    conn = _get_db()
    try:
        c = conn.cursor()

        # 1. 直接命中
        c.execute("SELECT * FROM stocks_master WHERE ticker = ?", (code,))
        row = _row_to_dict(c.fetchone())
        if row:
            return row

        base = code.split('.')[0]
        suffix = code[len(base):]

        candidates = []

        # 2. 港股：嘗試不同補位
        if suffix == '.HK' and base.isdigit():
            for n in (4, 5):
                candidates.append(base.lstrip('0').zfill(n) + '.HK')

        # 3. 純 base 嘗試 + 各種後綴
        for b in [base] + [base.zfill(n) for n in (4, 5, 6)]:
            candidates.append(b)
            for sfx in ('.HK', '.SS', '.SZ'):
                candidates.append(b + sfx)

        if candidates:
            placeholders = ",".join("?" * len(candidates))
            c.execute(f"SELECT * FROM stocks_master WHERE ticker IN ({placeholders})", candidates)
            rows = c.fetchall()
            if rows:
                best = sorted(rows, key=lambda r: _exchange_priority(r["exchange"] or ""))
                return _row_to_dict(best[0])

        return None
    finally:
        conn.close()


# ============================================================
# 公開 API
# ============================================================
def get_canonical_ticker(ticker: str) -> str | None:
    """
    返回 DB 中的官方股票代碼。
    例如：輸入 '388' 返回 '0388.HK'
    """
    info = _find_in_db(ticker)
    return info["ticker"] if info else None


def get_stock_info(ticker: str) -> dict | None:
    """
    回傳股票完整資訊 dict，找不到回傳 None。

    回傳欄位：
        ticker, name, name_zh_hk, name_zh_cn, market, exchange,
        sector, industry, market_cap, currency, description, ...
    """
    return _find_in_db(ticker)


def _is_chinese(text: str) -> bool:
    return any('\u4e00' <= ch <= '\u9fff' for ch in text)


def _market_priority_numeric(market: str) -> int:
    """數字輸入：HK > CN > US"""
    m = (market or "").upper()
    if m == "HK": return 0
    if m == "CN_STOCKCONNECT": return 1
    return 2


def _market_priority_alpha(market: str) -> int:
    """英文輸入：US > HK > CN"""
    m = (market or "").upper()
    if m == "US": return 0
    if m == "HK": return 1
    return 2


def _market_priority_chinese(market: str) -> int:
    """中文輸入：HK > CN（不會有 US）"""
    m = (market or "").upper()
    if m == "HK": return 0
    if m == "CN_STOCKCONNECT": return 1
    return 2


def _exchange_priority(exchange: str) -> int:
    ex = (exchange or "").upper()
    if ex in ('HK', 'HKEX', 'HKSE'):
        return 0
    if ex in ('NYSE', 'NASDAQ', 'AMEX', 'US'):
        return 1
    return 2


def search_stocks(query: str, limit: int = 8) -> list[dict]:
    """
    搜尋股票代碼或名稱（支援三語），回傳最多 limit 筆結果。

    排序邏輯：
      - 數字輸入：HK > CN > US
      - 英文輸入：ticker 前綴匹配優先，US > HK > CN
      - 中文輸入：HK > CN（美股不會有中文名）
    """
    q = query.strip()
    if not q:
        return []

    q_upper = q.upper()
    is_numeric = q_upper.isdigit()
    is_chinese = _is_chinese(q)

    conn = _get_db()
    c = conn.cursor()

    if is_numeric:
        # 補零至 4/5 位以匹配港股和 A 股格式
        padded4 = q_upper.zfill(4)   # 港股：8 → 0008
        padded5 = q_upper.zfill(5)   # 部分港股：8 → 00008
        padded6 = q_upper.zfill(6)   # A 股：8 → 000008

        # ── 第一段：exact match（HK padded + A股 padded，不受 LIMIT 限制）──
        exact_rows = []
        exact_tickers = set()
        for candidate in (
            f"{padded4}.HK",
            f"{padded5}.HK",
            q_upper,                  # 純數字無後綴
            f"{padded6}.SS",
            f"{padded6}.SZ",
        ):
            c.execute(
                "SELECT ticker, name, name_zh_hk, name_zh_cn, exchange, market "
                "FROM stocks_master WHERE ticker = ?",
                (candidate,)
            )
            row = c.fetchone()
            if row and row["ticker"] not in exact_tickers:
                exact_tickers.add(row["ticker"])
                exact_rows.append(dict(row))

        # ── 第二段：模糊查詢補足剩餘名額 ──
        c.execute("""
            SELECT ticker, name, name_zh_hk, name_zh_cn, exchange, market
            FROM stocks_master
            WHERE ticker LIKE ?
            LIMIT ?
        """, (f"%{q_upper}%", limit * 8))
        fuzzy_rows = [dict(r) for r in c.fetchall()]

        conn.close()

        # 合并去重（exact 排前面，fuzzy 補足）
        seen = set(exact_tickers)
        rows = list(exact_rows)
        for r in fuzzy_rows:
            if r["ticker"] not in seen:
                seen.add(r["ticker"])
                rows.append(r)
        # 注意：跳過後面的 rows = [dict(...)] 統一處理，直接進排序

    elif is_chinese:
        pattern = f"%{q}%"
        c.execute("""
            SELECT ticker, name, name_zh_hk, name_zh_cn, exchange, market
            FROM stocks_master
            WHERE name_zh_hk LIKE ?
               OR name_zh_cn LIKE ?
            LIMIT ?
        """, (pattern, pattern, limit * 3))
    else:
        if len(q) < 3:
            # 短輸入：只匹配 ticker 前綴
            c.execute("""
                SELECT ticker, name, name_zh_hk, name_zh_cn, exchange, market
                FROM stocks_master
                WHERE ticker LIKE ?
                LIMIT ?
            """, (f"{q_upper}%", limit * 10))
        else:
            # 3 字以上：ticker 前綴 + 公司名包含，分市場各取一批確保排序正確
            pattern = f"%{q}%"
            c.execute("""
                SELECT ticker, name, name_zh_hk, name_zh_cn, exchange, market FROM (
                    SELECT *, 0 AS sort_group FROM stocks_master
                    WHERE (ticker LIKE ? OR name LIKE ?) AND market = 'US'
                    LIMIT ?
                ) UNION ALL SELECT ticker, name, name_zh_hk, name_zh_cn, exchange, market FROM (
                    SELECT *, 1 AS sort_group FROM stocks_master
                    WHERE (ticker LIKE ? OR name LIKE ?) AND market = 'HK'
                    LIMIT ?
                ) UNION ALL SELECT ticker, name, name_zh_hk, name_zh_cn, exchange, market FROM (
                    SELECT *, 2 AS sort_group FROM stocks_master
                    WHERE (ticker LIKE ? OR name LIKE ?) AND market = 'CN_StockConnect'
                    LIMIT ?
                )
            """, (f"{q_upper}%", pattern, limit,
                  f"{q_upper}%", pattern, limit,
                  f"{q_upper}%", pattern, limit))

    if not is_numeric:
        rows = [dict(r) for r in c.fetchall()]
        conn.close()

    # 排序
    if is_numeric:
        q_stripped = q_upper.lstrip('0') or '0'

        def _numeric_sort_key(row):
            ticker = row["ticker"]
            num_part = ticker.split('.')[0].lstrip('0') or '0'

            # 0. 精確 exact match（最高優先，一定排第一）
            if ticker in exact_tickers:
                exact_rank = 0
            else:
                exact_rank = 1

            # 1. 匹配等級
            if num_part == q_stripped:
                rank = 0      # 完全匹配數字部分（如 8 → 0008.HK）
            elif num_part.startswith(q_stripped):
                rank = 1      # 前綴匹配（如 8 → 800.HK）
            else:
                rank = 2      # 包含匹配（如 8 → 1008.HK）

            # 2. 市場優先級 (HK > CN > US)
            market_rank = _market_priority_numeric(row["market"] or "")

            return (exact_rank, rank, market_rank, ticker)

        rows.sort(key=_numeric_sort_key)
    elif is_chinese:
        rows.sort(key=lambda x: (
            _market_priority_chinese(x["market"] or ""),
            x["ticker"],
        ))
    else:
        rows.sort(key=lambda x: (
            0 if x["ticker"].startswith(q_upper) else 1,
            _market_priority_alpha(x["market"] or ""),
            x["ticker"],
        ))

    results = []
    for r in rows[:limit]:
        results.append({
            "code":       r["ticker"],
            "name":       r["name"] or "",
            "name_zh_hk": r["name_zh_hk"] or "",
            "name_zh_cn": r["name_zh_cn"] or "",
            "exchange":   r["exchange"] or "",
            "market":     r["market"] or "",
        })
    return results


# ============================================================
if __name__ == "__main__":
    conn = _get_db()
    count = conn.execute("SELECT COUNT(*) FROM stocks_master").fetchone()[0]
    conn.close()
    print(f"DB: {count:,} entries from {_DB_PATH}")
    print("Type 'q' to quit.\n")
    while True:
        code = input("Stock code: ").strip()
        if code.lower() in ("q", "quit", "exit"):
            break
        if code:
            canonical = get_canonical_ticker(code)
            print(f"  canonical -> {canonical}")
            info = get_stock_info(code)
            if info:
                print(f"  name:    {info['name']}")
                print(f"  zh_hk:   {info['name_zh_hk']}")
                print(f"  zh_cn:   {info['name_zh_cn']}")
                print(f"  market:  {info['market']}")
                print(f"  exchange:{info['exchange']}\n")
            else:
                print(f"  Not found\n")
