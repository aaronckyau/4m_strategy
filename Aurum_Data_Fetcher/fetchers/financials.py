"""
fetchers/financials.py - 三大財務報表
"""
from datetime import datetime, timedelta, timezone

from fmp_client import FMPClient
from ticker import resolve_ticker, to_fmp_ticker
from config import Config
from db import get_db, get_stock_timestamp, upsert_financial_statement
from logger import log


def fetch_financials(ticker: str, incremental: bool = False,
                     client: FMPClient | None = None):
    """
    拉取損益表、資產負債表、現金流量表並寫入 DB。
    incremental=True 時，若 financials_updated_at < 30 天則跳過。
    """
    ticker = resolve_ticker(ticker)
    fmp_sym = to_fmp_ticker(ticker)
    client = client or FMPClient()
    limit = Config.FINANCIAL_QUARTERS

    conn = get_db()
    try:
        if incremental:
            updated_at = get_stock_timestamp(conn, ticker, 'financials_updated_at')
            if updated_at:
                try:
                    last_update = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                    age_days = (datetime.now(timezone.utc) - last_update).days
                    if age_days < Config.INCREMENTAL_REFRESH_DAYS:
                        log.info(f"[Financials] {ticker} — 上次更新 {age_days} 天前，跳過")
                        return True
                except (ValueError, TypeError):
                    pass

        # Income Statement
        log.info(f"[Financials] {ticker} — 損益表 ...")
        income = client.get_income_statement(fmp_sym, period='quarter', limit=limit)
        if income:
            upsert_financial_statement(conn, ticker, 'income', income)
            log.info(f"[Financials] {ticker} — 損益表 {len(income)} 筆")

        # Balance Sheet
        log.info(f"[Financials] {ticker} — 資產負債表 ...")
        balance = client.get_balance_sheet(fmp_sym, period='quarter', limit=limit)
        if balance:
            upsert_financial_statement(conn, ticker, 'balance', balance)
            log.info(f"[Financials] {ticker} — 資產負債表 {len(balance)} 筆")

        # Cash Flow
        log.info(f"[Financials] {ticker} — 現金流量表 ...")
        cashflow = client.get_cash_flow(fmp_sym, period='quarter', limit=limit)
        if cashflow:
            upsert_financial_statement(conn, ticker, 'cashflow', cashflow)
            log.info(f"[Financials] {ticker} — 現金流量表 {len(cashflow)} 筆")

        total = len(income) + len(balance) + len(cashflow)
        if total == 0:
            log.warning(f"[Financials] {ticker} — 三表皆無資料")
            return False

        return True
    finally:
        conn.close()
