"""
config.py - 應用程式設定
============================================================================
從 .env 讀取 FMP API Key 及 DB 路徑。
============================================================================
"""
import os
from dotenv import load_dotenv

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_BASE_DIR, ".env"), override=False)
load_dotenv(os.path.normpath(os.path.join(_BASE_DIR, "..", "Aurum_Infinity_AI", ".env")), override=False)


class Config:
    FMP_API_KEY = os.getenv("FMP_API_KEY")
    DB_PATH = (
        os.getenv("DATABASE_PATH")
        or os.getenv("DATABASE_URL")
        or os.getenv("DB_PATH")
        or os.path.normpath(os.path.join(
            _BASE_DIR,
            '..', 'Aurum_Infinity_AI', 'aurum.db',
        ))
    )

    # FMP API 設定
    FMP_BASE_URL = "https://financialmodelingprep.com/api/v3"
    FMP_MAX_RETRIES = 2
    FMP_RETRY_DELAY = 3.0
    FMP_REQUEST_INTERVAL = 0.25  # 秒，避免超出 rate limit

    # 資料範圍
    OHLC_YEARS = 2                  # 拉取幾年日線
    FINANCIAL_QUARTERS = 12         # 拉取幾季財報（12季 = 3年）
    INCREMENTAL_REFRESH_DAYS = 30   # 財報增量更新間隔（天）

    # 股票清單路徑（Aurum_Data_Fetcher/data/stock_code.json 為統一來源）
    STOCK_LIST_PATH = os.getenv("STOCK_LIST_PATH",
                                 os.path.join(_BASE_DIR,
                                              'data', 'stock_code.json'))

    # 股票更新日誌（Obsidian）
    STOCK_LOG_PATH = os.getenv("STOCK_LOG_PATH",
                                os.path.join(_BASE_DIR,
                                             '..', 'Get_stock', 'logs', 'stock_code.md'))

    # 美股 11 大 sector ETF，固定納入市場板塊表現與 OHLC 更新
    SECTOR_ETFS = (
        {"symbol": "XLC", "name": "Communication Services Select Sector SPDR Fund", "sector": "Communication Services"},
        {"symbol": "XLY", "name": "Consumer Discretionary Select Sector SPDR Fund", "sector": "Consumer Discretionary"},
        {"symbol": "XLP", "name": "Consumer Staples Select Sector SPDR Fund", "sector": "Consumer Staples"},
        {"symbol": "XLE", "name": "Energy Select Sector SPDR Fund", "sector": "Energy"},
        {"symbol": "XLF", "name": "Financial Select Sector SPDR Fund", "sector": "Financials"},
        {"symbol": "XLV", "name": "Health Care Select Sector SPDR Fund", "sector": "Health Care"},
        {"symbol": "XLI", "name": "Industrial Select Sector SPDR Fund", "sector": "Industrials"},
        {"symbol": "XLB", "name": "Materials Select Sector SPDR Fund", "sector": "Materials"},
        {"symbol": "XLRE", "name": "Real Estate Select Sector SPDR Fund", "sector": "Real Estate"},
        {"symbol": "XLK", "name": "Technology Select Sector SPDR Fund", "sector": "Technology"},
        {"symbol": "XLU", "name": "Utilities Select Sector SPDR Fund", "sector": "Utilities"},
    )
