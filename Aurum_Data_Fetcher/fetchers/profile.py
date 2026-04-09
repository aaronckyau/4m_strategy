"""
fetchers/profile.py - 公司基本資料
"""
from fmp_client import FMPClient
from ticker import resolve_ticker, to_fmp_ticker
from db import get_db, upsert_stock
from logger import log


def fetch_profile(ticker: str, client: FMPClient | None = None):
    """拉取公司 profile 並寫入 stocks_master"""
    ticker = resolve_ticker(ticker)
    fmp_sym = to_fmp_ticker(ticker)
    client = client or FMPClient()

    log.info(f"[Profile] {ticker} ...")
    data = client.get_profile(fmp_sym)
    if not data:
        log.warning(f"[Profile] {ticker} — 無資料")
        return False

    conn = get_db()
    try:
        price = data.get('price')
        mkt_cap = data.get('marketCap') or data.get('mktCap')
        shares = None
        if price and mkt_cap and price > 0:
            shares = mkt_cap / price

        upsert_stock(conn, {
            'ticker': ticker,
            'name': data.get('companyName'),
            'exchange': data.get('exchange') or data.get('exchangeShortName'),
            'sector': data.get('sector'),
            'industry': data.get('industry'),
            'market_cap': mkt_cap,
            'currency': data.get('currency'),
            'description': data.get('description'),
            'shares_outstanding': shares,
        })
        log.info(f"[Profile] {ticker} — {data.get('companyName')} 已儲存")
        return True
    finally:
        conn.close()
