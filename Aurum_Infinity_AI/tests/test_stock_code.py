"""
針對 read_stock_code.py 的基本測試
——測試最核心、最不可能壞的功能
"""
import sqlite3

import pytest

import read_stock_code
from read_stock_code import get_canonical_ticker, normalize_ticker, search_stocks


class TestNormalizeTicker:
    """測試股票代碼標準化"""

    def test_hk_short_number(self):
        """純數字短碼 → 補零加 .HK"""
        assert normalize_ticker("700") == "0700.HK"

    def test_hk_four_digit(self):
        """四位數字 → 加 .HK"""
        assert normalize_ticker("0700") == "0700.HK"

    def test_us_ticker(self):
        """英文代碼維持不變（大寫）"""
        assert normalize_ticker("nvda") == "NVDA"
        assert normalize_ticker("AAPL") == "AAPL"

    def test_already_has_suffix(self):
        """已有 .HK 後綴不重複加"""
        assert normalize_ticker("0700.HK") == "0700.HK"

    def test_whitespace_stripped(self):
        """前後空白要清除"""
        assert normalize_ticker("  700  ") == "0700.HK"


@pytest.fixture
def stock_db(monkeypatch, tmp_path):
    db_path = tmp_path / "stocks.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE stocks_master (
            ticker TEXT,
            name TEXT,
            name_zh_hk TEXT,
            name_zh_cn TEXT,
            market TEXT,
            exchange TEXT
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO stocks_master (ticker, name, name_zh_hk, name_zh_cn, market, exchange)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            ("0700.HK", "Tencent", "騰訊控股", "腾讯控股", "HK", "HKEX"),
            ("700.HK", "Tencent Legacy", "騰訊舊版", "腾讯旧版", "HK", "HKEX"),
            ("AAPL", "Apple Inc.", "蘋果", "苹果", "US", "NASDAQ"),
            ("AMD", "Advanced Micro Devices", "超微", "超微", "US", "NASDAQ"),
            ("600519.SS", "Kweichow Moutai", "貴州茅台", "贵州茅台", "CN_StockConnect", "SHH"),
        ],
    )
    conn.commit()
    conn.close()

    def _get_db():
        db_conn = sqlite3.connect(db_path)
        db_conn.row_factory = sqlite3.Row
        return db_conn

    monkeypatch.setattr(read_stock_code, "_get_db", _get_db)
    return db_path


class TestDbBackedTickerResolution:
    def test_get_canonical_ticker_prefers_db_match(self, stock_db):
        assert stock_db.exists()
        assert get_canonical_ticker("700") == "0700.HK"

    def test_search_stocks_numeric_query_prefers_exact_hk_match(self, stock_db):
        results = search_stocks("700", limit=3)

        assert results[0]["code"] == "0700.HK"

    def test_search_stocks_english_query_prefers_us_prefix_match(self, stock_db):
        results = search_stocks("aa", limit=3)

        assert results[0]["code"] == "AAPL"

    def test_search_stocks_supports_chinese_query(self, stock_db):
        results = search_stocks("騰訊", limit=3)

        assert results[0]["code"] == "0700.HK"
