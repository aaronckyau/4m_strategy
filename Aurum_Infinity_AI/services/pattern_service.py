"""
services/pattern_service.py — K 線形態偵測服務
Layer 1 核心 9 個形態，純 pandas 計算，無 TA-Lib C dependency
每個形態均需同時滿足「價格結構」+ 「成交量確認」才會輸出
"""
from __future__ import annotations

import pandas as pd

PATTERN_META: dict[str, dict] = {
    "doji":              {"name_zh": "十字星",   "name_en": "Doji",             "direction": "neutral"},
    "hammer":            {"name_zh": "錘子線",   "name_en": "Hammer",           "direction": "bullish"},
    "inverted_hammer":   {"name_zh": "倒錘子",   "name_en": "Inverted Hammer",  "direction": "bullish"},
    "hanging_man":       {"name_zh": "上吊線",   "name_en": "Hanging Man",      "direction": "bearish"},
    "shooting_star":     {"name_zh": "流星線",   "name_en": "Shooting Star",    "direction": "bearish"},
    "bullish_engulfing": {"name_zh": "看多吞噬", "name_en": "Bullish Engulfing","direction": "bullish"},
    "bearish_engulfing": {"name_zh": "看空吞噬", "name_en": "Bearish Engulfing","direction": "bearish"},
    "morning_star":      {"name_zh": "晨星",     "name_en": "Morning Star",     "direction": "bullish"},
    "evening_star":      {"name_zh": "暮星",     "name_en": "Evening Star",     "direction": "bearish"},
}

# 各形態的成交量確認規則（倍數 vs N日均量）
# vol_mult: 當日量需 >= vol_mult × vol_ma_n 日均量
# vol_ma_n: 均量計算天數
PATTERN_VOL_RULE: dict[str, dict] = {
    "doji":              {"vol_mult": 1.0, "vol_ma_n": 10},  # 任何量均接受（量越大越重要，但不強制）
    "hammer":            {"vol_mult": 1.2, "vol_ma_n": 10},  # 量需高於10日均量20%
    "inverted_hammer":   {"vol_mult": 1.2, "vol_ma_n": 10},
    "hanging_man":       {"vol_mult": 1.2, "vol_ma_n": 10},
    "shooting_star":     {"vol_mult": 1.2, "vol_ma_n": 10},
    "bullish_engulfing": {"vol_mult": 1.5, "vol_ma_n": 10},  # 吞噬要求量明顯放大
    "bearish_engulfing": {"vol_mult": 1.5, "vol_ma_n": 10},
    "morning_star":      {"vol_mult": 1.3, "vol_ma_n": 10},  # 第三根確認K放量
    "evening_star":      {"vol_mult": 1.3, "vol_ma_n": 10},
}


def detect_patterns(
    ohlc_rows: list[dict],
    doji_scalar: float = 0.1,
) -> dict[str, list[str]]:
    """
    對 OHLC+Volume 陣列偵測 K 線形態（價格結構 + 成交量雙重確認）。

    Parameters
    ----------
    ohlc_rows : list[dict]
        每筆含 {time, open, high, low, close, volume}，time 為 "YYYY-MM-DD"
        volume 欄位若缺失或為 0，該形態不輸出（嚴格模式）
    doji_scalar : float
        Doji body 佔全幅比例閾值（0.05~0.25），預設 0.1

    Returns
    -------
    dict[str, list[str]]
        key = "YYYY-MM-DD"，value = list of pattern codes
    """
    if not ohlc_rows or len(ohlc_rows) < 3:
        return {}

    df = pd.DataFrame(ohlc_rows)
    df["time"] = pd.to_datetime(df["time"])
    df = df.sort_values("time").reset_index(drop=True)

    for col in ("open", "high", "low", "close"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # volume 欄位處理：缺失則填 0
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
    else:
        df["volume"] = 0.0

    df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
    if len(df) < 3:
        return {}

    result: dict[str, list[str]] = {}

    body         = (df["close"] - df["open"]).abs()
    total_range  = df["high"] - df["low"]
    lower_shadow = df[["open", "close"]].min(axis=1) - df["low"]
    upper_shadow = df["high"] - df[["open", "close"]].max(axis=1)

    # 預計算各形態對應的成交量均值（10日移動平均，用 shift(1) 避免當日自身）
    vol_ma10 = df["volume"].shift(1).rolling(10, min_periods=5).mean()

    def _vol_ok(idx: int, pattern_code: str) -> bool:
        """檢查 idx 那根 K 的成交量是否達到確認門檻"""
        rule = PATTERN_VOL_RULE.get(pattern_code, {"vol_mult": 1.0, "vol_ma_n": 10})
        v = df.loc[idx, "volume"]
        ma = vol_ma10.iloc[idx]
        if v <= 0 or pd.isna(ma) or ma <= 0:
            return False
        return v >= rule["vol_mult"] * ma

    # ── Doji ────────────────────────────────────────────────────
    doji_mask = (total_range > 0) & (body / total_range <= doji_scalar)
    _add_mask_with_vol(df, doji_mask, "doji", result, _vol_ok)

    # ── 趨勢判斷（前 5 根收盤均值 vs 前 1 根收盤）────────────────
    prev5_avg  = df["close"].shift(1).rolling(5, min_periods=3).mean()
    prev1      = df["close"].shift(1)
    downtrend  = prev5_avg > prev1
    uptrend    = prev5_avg < prev1

    # ── Hammer ──────────────────────────────────────────────────
    hammer_mask = (
        downtrend
        & (body > 0)
        & (lower_shadow >= 2 * body)
        & (upper_shadow <= 0.1 * total_range.clip(lower=1e-9))
    )
    _add_mask_with_vol(df, hammer_mask, "hammer", result, _vol_ok)

    # ── Inverted Hammer ──────────────────────────────────────────
    inv_hammer_mask = (
        downtrend
        & (body > 0)
        & (upper_shadow >= 2 * body)
        & (lower_shadow <= 0.1 * total_range.clip(lower=1e-9))
    )
    _add_mask_with_vol(df, inv_hammer_mask, "inverted_hammer", result, _vol_ok)

    # ── Hanging Man ──────────────────────────────────────────────
    hanging_mask = (
        uptrend
        & (body > 0)
        & (lower_shadow >= 2 * body)
        & (upper_shadow <= 0.1 * total_range.clip(lower=1e-9))
    )
    _add_mask_with_vol(df, hanging_mask, "hanging_man", result, _vol_ok)

    # ── Shooting Star ────────────────────────────────────────────
    shooting_mask = (
        uptrend
        & (body > 0)
        & (upper_shadow >= 2 * body)
        & (lower_shadow <= 0.1 * total_range.clip(lower=1e-9))
    )
    _add_mask_with_vol(df, shooting_mask, "shooting_star", result, _vol_ok)

    # ── Bullish Engulfing ────────────────────────────────────────
    prev_bearish = df["close"].shift(1) < df["open"].shift(1)
    curr_bullish = df["close"] > df["open"]
    bull_eng_mask = (
        prev_bearish
        & curr_bullish
        & (df["open"]  <= df["close"].shift(1))
        & (df["close"] >= df["open"].shift(1))
    )
    _add_mask_with_vol(df, bull_eng_mask, "bullish_engulfing", result, _vol_ok)

    # ── Bearish Engulfing ────────────────────────────────────────
    prev_bullish = df["close"].shift(1) > df["open"].shift(1)
    curr_bearish = df["close"] < df["open"]
    bear_eng_mask = (
        prev_bullish
        & curr_bearish
        & (df["open"]  >= df["close"].shift(1))
        & (df["close"] <= df["open"].shift(1))
    )
    _add_mask_with_vol(df, bear_eng_mask, "bearish_engulfing", result, _vol_ok)

    # ── Morning Star & Evening Star（3 根逐行掃描）───────────────
    for i in range(2, len(df)):
        d1 = df.iloc[i - 2]
        d2 = df.iloc[i - 1]
        d3 = df.iloc[i]

        d1_range = d1["high"] - d1["low"]
        d3_range = d3["high"] - d3["low"]
        d2_body  = abs(d2["close"] - d2["open"])
        d2_range = d2["high"] - d2["low"]

        if d1_range <= 0 or d3_range <= 0:
            continue

        d2_body_ratio = (d2_body / d2_range) if d2_range > 0 else 1.0
        d3_date = d3["time"].strftime("%Y-%m-%d")

        # Morning Star — 以第三根（確認K）做量確認
        if (
            d1["close"] < d1["open"]
            and (d1["open"] - d1["close"]) > 0.3 * d1_range
            and d2_body_ratio < 0.35
            and d3["close"] > d3["open"]
            and (d3["close"] - d3["open"]) > 0.3 * d3_range
            and d3["close"] > (d1["open"] + d1["close"]) / 2
            and _vol_ok(i, "morning_star")
        ):
            result.setdefault(d3_date, []).append("morning_star")

        # Evening Star — 以第三根（確認K）做量確認
        if (
            d1["close"] > d1["open"]
            and (d1["close"] - d1["open"]) > 0.3 * d1_range
            and d2_body_ratio < 0.35
            and d3["close"] < d3["open"]
            and (d3["open"] - d3["close"]) > 0.3 * d3_range
            and d3["close"] < (d1["open"] + d1["close"]) / 2
            and _vol_ok(i, "evening_star")
        ):
            result.setdefault(d3_date, []).append("evening_star")

    return result


def _add_mask_with_vol(
    df: pd.DataFrame,
    mask: pd.Series,
    pattern_code: str,
    result: dict[str, list[str]],
    vol_ok_fn,
) -> None:
    for idx in df.index[mask]:
        if not vol_ok_fn(idx, pattern_code):
            continue
        date_key = df.loc[idx, "time"].strftime("%Y-%m-%d")
        result.setdefault(date_key, []).append(pattern_code)
