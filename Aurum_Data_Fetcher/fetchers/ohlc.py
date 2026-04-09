"""
fetchers/ohlc.py - 日線 OHLC 數據
"""
from datetime import datetime, timedelta

from fmp_client import FMPClient
from ticker import resolve_ticker, to_fmp_ticker
from config import Config
from db import get_db, get_last_ohlc_date, upsert_ohlc_batch
from logger import log


def fetch_ohlc(ticker: str, incremental: bool = False,
               client: FMPClient | None = None):
    """
    拉取日線 OHLC 並寫入 ohlc_daily。
    incremental=True 時只拉最後日期之後的資料。
    """
    ticker = resolve_ticker(ticker)
    fmp_sym = to_fmp_ticker(ticker)
    client = client or FMPClient()

    conn = get_db()
    try:
        to_date = datetime.now().strftime('%Y-%m-%d')

        if incremental:
            last_date = get_last_ohlc_date(conn, ticker)
            if last_date:
                # 從最後日期的下一天開始
                from_dt = datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)
                from_date = from_dt.strftime('%Y-%m-%d')
                if from_date > to_date:
                    log.info(f"[OHLC] {ticker} — 已是最新")
                    return True
                log.info(f"[OHLC] {ticker} 增量 {from_date} → {to_date}")
            else:
                # 無資料，fallback 到全量
                from_date = (datetime.now() - timedelta(days=365 * Config.OHLC_YEARS)).strftime('%Y-%m-%d')
                log.info(f"[OHLC] {ticker} 無歷史資料，全量拉取 {from_date} → {to_date}")
        else:
            from_date = (datetime.now() - timedelta(days=365 * Config.OHLC_YEARS)).strftime('%Y-%m-%d')
            log.info(f"[OHLC] {ticker} 全量 {from_date} → {to_date}")

        rows = client.get_historical_prices(fmp_sym, from_date, to_date)
        if not rows:
            log.warning(f"[OHLC] {ticker} — 無資料")
            return False

        upsert_ohlc_batch(conn, ticker, rows)
        log.info(f"[OHLC] {ticker} — 已寫入 {len(rows)} 筆")
        return True
    finally:
        conn.close()
