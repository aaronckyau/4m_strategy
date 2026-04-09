"""
ticker.py - 股票代碼標準化與轉換
============================================================================
PK 格式 = FMP 格式，統一為：
  AAPL          美股
  0388.HK       港股（4 位數 + .HK）
  600519.SS     A 股上海
  002594.SZ     A 股深圳

resolve_ticker() 查主站 stock_code.json 取得 canonical key（= DB PK = FMP 格式）。
to_fmp_ticker() / from_fmp_ticker() 現為直通，保留供呼叫端語意清晰。
============================================================================
"""
import json
import os

from config import Config
from logger import log


# ── 載入主站 stock_code.json 作為 canonical 來源 ──────────────
def _load_stock_lookup() -> dict:
    path = Config.STOCK_LIST_PATH
    if not os.path.exists(path):
        log.warning("stock_code.json 不存在: %s", path)
        return {}
    with open(path, encoding='utf-8') as f:
        return json.load(f)

_lookup: dict = _load_stock_lookup()


def _cn_exchange_suffix(code: str) -> str:
    """根據 6 位 A 股代碼判斷交易所後綴：6→.SS，0/2/3→.SZ"""
    if code[0] == '6':
        return '.SS'
    if code[0] in ('0', '2', '3'):
        return '.SZ'
    return '.SS'


def normalize_ticker(ticker: str) -> str:
    """
    純格式轉換（不查資料庫），輸出 FMP 格式：
      700    → 0700.HK
      00700  → 0700.HK
      600519 → 600519.SS
      AAPL   → AAPL
    """
    raw = ticker.upper().strip()
    if '.' in raw or not raw.isdigit():
        return raw
    if len(raw) <= 5:
        return raw.lstrip('0').zfill(4) + '.HK'
    # 6 位 → A 股
    return raw + _cn_exchange_suffix(raw)


def resolve_ticker(ticker: str) -> str:
    """
    解析成 stock_code.json 的 canonical key（= DB PK = FMP 格式）。
    找不到時 fallback 到 normalize_ticker 結果。

    例：388 → 0388.HK, 00700 → 0700.HK, 600519 → 600519.SS
    """
    normalized = normalize_ticker(ticker)

    # 直接命中
    if normalized in _lookup:
        return normalized

    base = normalized.split('.')[0]
    suffix = normalized[len(base):]

    # 港股：嘗試不同補位
    if suffix == '.HK' and base.isdigit():
        for n in (4, 5):
            candidate = base.lstrip('0').zfill(n) + '.HK'
            if candidate in _lookup:
                return candidate

    # 嘗試 base + 各種後綴
    for candidate in [base] + [base.zfill(n) for n in (4, 5, 6)]:
        if candidate in _lookup:
            return candidate
        for sfx in ('.HK', '.SS', '.SZ'):
            key = candidate + sfx
            if key in _lookup:
                return key

    return normalized


def to_fmp_ticker(ticker: str) -> str:
    """PK → FMP 格式（現在是同一格式，直通）"""
    return ticker


def from_fmp_ticker(fmp_ticker: str) -> str:
    """FMP → PK 格式（現在是同一格式，直通）"""
    return fmp_ticker.upper().strip()
