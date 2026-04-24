"""
services/pattern_service.py — K 線形態偵測服務
Layer 1 核心 9 個形態，純 pandas 計算，無 TA-Lib C dependency
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


def detect_patterns(
    ohlc_rows: list[dict],
    doji_scalar: float = 0.1,
) -> dict[str, list[str]]:
    """
    對 OHLC 陣列偵測 K 線形態。

    Parameters
    ----------
    ohlc_rows : list[dict]
        每筆含 {time, open, high, low, close}，time 為 "YYYY-MM-DD"
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

    df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
    if len(df) < 3:
        return {}

    result: dict[str, list[str]] = {}

    body         = (df["close"] - df["open"]).abs()
    total_range  = df["high"] - df["low"]
    lower_shadow = df[["open", "close"]].min(axis=1) - df["low"]
    upper_shadow = df["high"] - df[["open", "close"]].max(axis=1)

    # ── Doji ────────────────────────────────────────────────────
    doji_mask = (total_range > 0) & (body / total_range <= doji_scalar)
    _add_mask(df, doji_mask, "doji", result)

    # ── 趨勢判斷（前 5 根收盤均值 vs 前 1 根收盤）────────────────
    prev5_avg  = df["close"].shift(1).rolling(5, min_periods=3).mean()
    prev1      = df["close"].shift(1)
    downtrend  = prev5_avg > prev1   # 均值高於前一日 → 近期下跌
    uptrend    = prev5_avg < prev1   # 均值低於前一日 → 近期上漲

    # ── Hammer ──────────────────────────────────────────────────
    hammer_mask = (
        downtrend
        & (body > 0)
        & (lower_shadow >= 2 * body)
        & (upper_shadow <= 0.1 * total_range.clip(lower=1e-9))
    )
    _add_mask(df, hammer_mask, "hammer", result)

    # ── Inverted Hammer ──────────────────────────────────────────
    inv_hammer_mask = (
        downtrend
        & (body > 0)
        & (upper_shadow >= 2 * body)
        & (lower_shadow <= 0.1 * total_range.clip(lower=1e-9))
    )
    _add_mask(df, inv_hammer_mask, "inverted_hammer", result)

    # ── Hanging Man ──────────────────────────────────────────────
    hanging_mask = (
        uptrend
        & (body > 0)
        & (lower_shadow >= 2 * body)
        & (upper_shadow <= 0.1 * total_range.clip(lower=1e-9))
    )
    _add_mask(df, hanging_mask, "hanging_man", result)

    # ── Shooting Star ────────────────────────────────────────────
    shooting_mask = (
        uptrend
        & (body > 0)
        & (upper_shadow >= 2 * body)
        & (lower_shadow <= 0.1 * total_range.clip(lower=1e-9))
    )
    _add_mask(df, shooting_mask, "shooting_star", result)

    # ── Bullish Engulfing ────────────────────────────────────────
    prev_bearish = df["close"].shift(1) < df["open"].shift(1)
    curr_bullish = df["close"] > df["open"]
    bull_eng_mask = (
        prev_bearish
        & curr_bullish
        & (df["open"]  <= df["close"].shift(1))
        & (df["close"] >= df["open"].shift(1))
    )
    _add_mask(df, bull_eng_mask, "bullish_engulfing", result)

    # ── Bearish Engulfing ────────────────────────────────────────
    prev_bullish = df["close"].shift(1) > df["open"].shift(1)
    curr_bearish = df["close"] < df["open"]
    bear_eng_mask = (
        prev_bullish
        & curr_bearish
        & (df["open"]  >= df["close"].shift(1))
        & (df["close"] <= df["open"].shift(1))
    )
    _add_mask(df, bear_eng_mask, "bearish_engulfing", result)

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

        # Morning Star
        if (
            d1["close"] < d1["open"]
            and (d1["open"] - d1["close"]) > 0.3 * d1_range
            and d2_body_ratio < 0.35
            and d3["close"] > d3["open"]
            and (d3["close"] - d3["open"]) > 0.3 * d3_range
            and d3["close"] > (d1["open"] + d1["close"]) / 2
        ):
            result.setdefault(d3_date, []).append("morning_star")

        # Evening Star
        if (
            d1["close"] > d1["open"]
            and (d1["close"] - d1["open"]) > 0.3 * d1_range
            and d2_body_ratio < 0.35
            and d3["close"] < d3["open"]
            and (d3["open"] - d3["close"]) > 0.3 * d3_range
            and d3["close"] < (d1["open"] + d1["close"]) / 2
        ):
            result.setdefault(d3_date, []).append("evening_star")

    return result


def _add_mask(
    df: pd.DataFrame,
    mask: pd.Series,
    pattern_code: str,
    result: dict[str, list[str]],
) -> None:
    for idx in df.index[mask]:
        date_key = df.loc[idx, "time"].strftime("%Y-%m-%d")
        result.setdefault(date_key, []).append(pattern_code)
