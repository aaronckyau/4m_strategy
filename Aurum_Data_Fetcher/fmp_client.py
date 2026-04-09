"""
fmp_client.py - Financial Modeling Prep API 封裝
============================================================================
使用 FMP stable 端點（免費方案支援）。
內建 retry、rate limit、錯誤處理。
============================================================================
"""
import time
import requests

from config import Config
from logger import log


class FMPClient:
    """FMP API 客戶端（stable 端點）"""

    BASE_URL = "https://financialmodelingprep.com/stable"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or Config.FMP_API_KEY
        if not self.api_key:
            raise ValueError("FMP_API_KEY 未設定，請在 .env 中填入")
        self._last_request_time = 0.0

    def _rate_limit(self):
        elapsed = time.time() - self._last_request_time
        if elapsed < Config.FMP_REQUEST_INTERVAL:
            time.sleep(Config.FMP_REQUEST_INTERVAL - elapsed)

    def _get(self, endpoint: str, params: dict | None = None) -> dict | list | None:
        """發送 GET 請求，內建 retry"""
        url = f"{self.BASE_URL}/{endpoint}"
        if params is None:
            params = {}
        params['apikey'] = self.api_key

        for attempt in range(Config.FMP_MAX_RETRIES + 1):
            self._rate_limit()
            self._last_request_time = time.time()

            try:
                resp = requests.get(url, params=params, timeout=30)

                if resp.status_code == 429:
                    wait = Config.FMP_RETRY_DELAY * (2 ** attempt)
                    log.warning(f"Rate limited (429), wait {wait:.0f}s ...")
                    time.sleep(wait)
                    continue

                if resp.status_code >= 500:
                    if attempt < Config.FMP_MAX_RETRIES:
                        log.warning(f"Server error {resp.status_code}, retry {attempt + 1}...")
                        time.sleep(Config.FMP_RETRY_DELAY)
                        continue
                    log.error(f"Server error {resp.status_code} failed: {url}")
                    return None

                resp.raise_for_status()
                data = resp.json()

                if isinstance(data, dict) and 'Error Message' in data:
                    log.error(f"FMP API Error: {data['Error Message']}")
                    return None

                return data

            except requests.exceptions.Timeout:
                if attempt < Config.FMP_MAX_RETRIES:
                    log.warning(f"Timeout, retry {attempt + 1}...")
                    continue
                log.error(f"Timeout failed: {url}")
                return None
            except requests.exceptions.RequestException as e:
                log.error(f"Request failed: {e}")
                return None

        return None

    # ====================================================================
    # API 端點（stable 格式）
    # ====================================================================

    def get_profile(self, symbol: str) -> dict | None:
        """公司基本資料 — /stable/profile?symbol=AAPL"""
        data = self._get("profile", {'symbol': symbol})
        if isinstance(data, list) and data:
            return data[0]
        return None

    def get_historical_prices(self, symbol: str,
                               from_date: str, to_date: str) -> list[dict]:
        """歷史日線 OHLC — /stable/historical-price-eod/full?symbol=AAPL&from=...&to=..."""
        data = self._get("historical-price-eod/full", {
            'symbol': symbol,
            'from': from_date,
            'to': to_date,
        })
        if isinstance(data, list):
            return data
        return []

    def get_income_statement(self, symbol: str,
                              period: str = 'quarter', limit: int = 12) -> list[dict]:
        """損益表 — /stable/income-statement?symbol=AAPL&period=quarter"""
        data = self._get("income-statement", {
            'symbol': symbol, 'period': period, 'limit': limit,
        })
        return data if isinstance(data, list) else []

    def get_balance_sheet(self, symbol: str,
                           period: str = 'quarter', limit: int = 12) -> list[dict]:
        """資產負債表 — /stable/balance-sheet-statement?symbol=AAPL&period=quarter"""
        data = self._get("balance-sheet-statement", {
            'symbol': symbol, 'period': period, 'limit': limit,
        })
        return data if isinstance(data, list) else []

    def get_cash_flow(self, symbol: str,
                       period: str = 'quarter', limit: int = 12) -> list[dict]:
        """現金流量表 — /stable/cash-flow-statement?symbol=AAPL&period=quarter"""
        data = self._get("cash-flow-statement", {
            'symbol': symbol, 'period': period, 'limit': limit,
        })
        return data if isinstance(data, list) else []
