"""
針對 read_stock_code.py 的基本測試
——測試最核心、最不可能壞的功能
"""
from read_stock_code import normalize_ticker


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