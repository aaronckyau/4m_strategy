"""
main.py - Aurum Data Fetcher CLI 入口
============================================================================
用法：
  python main.py init                              # 建表
  python main.py fetch --ticker AAPL               # 單股全量
  python main.py fetch --ticker AAPL --incremental # 增量
  python main.py fetch --all                       # 全部股票
  python main.py fetch --index sp500               # S&P 500 全量
  python main.py fetch --index sp500 --incremental # S&P 500 增量
  python main.py compute --ticker AAPL             # 計算指標
  python main.py run --ticker AAPL                 # fetch + compute
  python main.py run --index sp500 --incremental   # S&P 500 一條龍
  python main.py run --all --incremental           # 生產用

  # ── Bulk 財報（一次拉全市場，再篩入 DB）──
  python main.py bulk-financials --year 2025       # 全量：拉指定年度全部季報
  python main.py bulk-financials                   # 更新：自動拉最近 2 年
============================================================================
"""
import argparse
import csv
import io
import json
import sys
import time
from datetime import datetime

from config import Config
from db import init_tables, get_db, upsert_financial_statement
from fmp_client import FMPClient
from fetchers.profile import fetch_profile
from fetchers.ohlc import fetch_ohlc
from fetchers.financials import fetch_financials
from compute import compute_metrics
from ticker import resolve_ticker
from logger import log


def _load_stock_list() -> list[str]:
    """從主站的 stock_code.json 載入所有 ticker"""
    try:
        with open(Config.STOCK_LIST_PATH, encoding='utf-8') as f:
            data = json.load(f)
        # 支援 list[{symbol:...}] 或 dict{ticker:...} 兩種格式
        if isinstance(data, list):
            return [d['symbol'] for d in data if 'symbol' in d]
        return list(data.keys())
    except FileNotFoundError:
        log.error(f"股票清單不存在: {Config.STOCK_LIST_PATH}")
        sys.exit(1)


def cmd_init(_args):
    """建立資料庫表"""
    init_tables()


def cmd_fetch(args):
    """拉取數據"""
    client = FMPClient()
    tickers = _resolve_tickers(args, client)
    incremental = args.incremental

    for i, ticker in enumerate(tickers):
        log.info(f"===== [{i+1}/{len(tickers)}] {ticker} =====")
        try:
            fetch_profile(ticker, client)
            fetch_ohlc(ticker, incremental=incremental, client=client)
            fetch_financials(ticker, incremental=incremental, client=client)
        except Exception as e:
            log.error(f"{ticker} 失敗: {e}")
            continue

    log.info(f"Fetch 完成：{len(tickers)} 支股票")


def cmd_compute(args):
    """計算衍生指標"""
    tickers = _resolve_tickers(args, FMPClient() if getattr(args, 'index', None) else None)
    for i, ticker in enumerate(tickers):
        log.info(f"===== [Compute {i+1}/{len(tickers)}] {ticker} =====")
        try:
            compute_metrics(ticker)
        except Exception as e:
            log.error(f"[Compute] {ticker} 失敗: {e}")


def cmd_run(args):
    """fetch + compute"""
    client = FMPClient()
    tickers = _resolve_tickers(args, client)
    incremental = args.incremental

    for i, ticker in enumerate(tickers):
        log.info(f"===== [{i+1}/{len(tickers)}] {ticker} =====")
        try:
            fetch_profile(ticker, client)
            fetch_ohlc(ticker, incremental=incremental, client=client)
            fetch_financials(ticker, incremental=incremental, client=client)
            compute_metrics(ticker)
        except Exception as e:
            log.error(f"{ticker} 失敗: {e}")
            continue

    log.info(f"Run 完成：{len(tickers)} 支股票")


def _load_index(index_name: str, client: FMPClient) -> list[str]:
    """從 FMP 取得指數成分股列表"""
    endpoints = {
        'sp500': 'sp500-constituent',
        'nasdaq': 'nasdaq-constituent',
        'dowjones': 'dowjones-constituent',
    }
    endpoint = endpoints.get(index_name.lower())
    if not endpoint:
        log.error(f"不支援的指數: {index_name}（可用: {', '.join(endpoints.keys())}）")
        sys.exit(1)

    data = client._get(endpoint)
    if not data:
        log.error(f"無法取得 {index_name} 成分股列表")
        sys.exit(1)

    tickers = [d['symbol'] for d in data if d.get('symbol')]
    log.info(f"{index_name.upper()} 成分股：{len(tickers)} 支")
    return sorted(tickers)


def _resolve_tickers(args, client: FMPClient | None = None) -> list[str]:
    """從 CLI 參數解析 ticker 列表"""
    if getattr(args, 'index', None):
        return _load_index(args.index, client or FMPClient())
    if getattr(args, 'all', False):
        return _load_stock_list()
    if args.ticker:
        return [resolve_ticker(args.ticker)]
    log.error("請指定 --ticker、--index 或 --all")
    sys.exit(1)


def cmd_bulk_financials(args):
    """Bulk 下載財報（一次 API call 拉全市場 CSV，篩出目標股票寫入 DB）"""
    client = FMPClient()

    # 決定要拉哪些年份
    if args.year:
        years = [int(args.year)]
    else:
        # 預設拉最近 2 年
        current_year = datetime.now().year
        years = [current_year, current_year - 1]

    # 決定要篩哪些 ticker
    if args.index:
        sp_data = client._get(f'{args.index.lower()}-constituent')
        if not sp_data:
            log.error(f"無法取得 {args.index} 成分股")
            sys.exit(1)
        target_symbols = {d['symbol'] for d in sp_data}
        log.info(f"目標：{args.index.upper()} {len(target_symbols)} 支")
    elif args.all:
        # 篩選 stock_code.json 裡的所有股票
        target_symbols = set(_load_stock_list())
        log.info(f"目標：stock_code.json ({len(target_symbols)} 支)")
    else:
        # 預設 SP500
        sp_data = client._get('sp500-constituent')
        if not sp_data:
            log.error("無法取得 S&P 500 成分股")
            sys.exit(1)
        target_symbols = {d['symbol'] for d in sp_data}
        log.info(f"目標：S&P 500 ({len(target_symbols)} 支)")

    # 三表 endpoint 對照
    statements = [
        ('income-statement-bulk', 'income'),
        ('balance-sheet-statement-bulk', 'balance'),
        ('cash-flow-statement-bulk', 'cashflow'),
    ]

    conn = get_db()
    total_written = 0

    try:
        for year in years:
            for endpoint, stmt_type in statements:
                log.info(f"[Bulk] {stmt_type} {year} — 下載中 ...")
                import requests
                resp = requests.get(
                    f"{client.BASE_URL}/{endpoint}",
                    params={'year': str(year), 'period': 'quarter', 'apikey': client.api_key},
                    timeout=60,
                )
                if resp.status_code != 200:
                    log.error(f"[Bulk] {endpoint} {year} 失敗: HTTP {resp.status_code}")
                    continue

                reader = csv.DictReader(io.StringIO(resp.text))
                rows_by_ticker = {}
                for row in reader:
                    sym = row.get('symbol', '')
                    if target_symbols is not None and sym not in target_symbols:
                        continue
                    if sym not in rows_by_ticker:
                        rows_by_ticker[sym] = []
                    rows_by_ticker[sym].append(row)

                count = sum(len(v) for v in rows_by_ticker.values())
                log.info(f"[Bulk] {stmt_type} {year} — {count} 筆 / {len(rows_by_ticker)} 支股票")

                for ticker, rows in rows_by_ticker.items():
                    # 確保 stocks_master 有這支
                    existing = conn.execute(
                        "SELECT ticker FROM stocks_master WHERE ticker = ?", (ticker,)
                    ).fetchone()
                    if not existing:
                        conn.execute(
                            "INSERT OR IGNORE INTO stocks_master (ticker) VALUES (?)",
                            (ticker,)
                        )
                        conn.commit()
                    upsert_financial_statement(conn, ticker, stmt_type, rows)

                total_written += count

        log.info(f"[Bulk] 完成！共寫入 {total_written} 筆財報資料")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='Aurum Data Fetcher — 金融數據拉取工具'
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    # init
    sub_init = subparsers.add_parser('init', help='初始化資料庫表')
    sub_init.set_defaults(func=cmd_init)

    # fetch
    sub_fetch = subparsers.add_parser('fetch', help='拉取金融數據')
    sub_fetch.add_argument('--ticker', type=str, help='股票代碼')
    sub_fetch.add_argument('--index', type=str, help='指數成分股 (sp500, nasdaq, dowjones)')
    sub_fetch.add_argument('--all', action='store_true', help='拉取所有股票')
    sub_fetch.add_argument('--incremental', action='store_true', help='增量模式')
    sub_fetch.set_defaults(func=cmd_fetch)

    # compute
    sub_compute = subparsers.add_parser('compute', help='計算衍生指標')
    sub_compute.add_argument('--ticker', type=str, help='股票代碼')
    sub_compute.add_argument('--index', type=str, help='指數成分股 (sp500, nasdaq, dowjones)')
    sub_compute.add_argument('--all', action='store_true', help='全部股票')
    sub_compute.set_defaults(func=cmd_compute)

    # run
    sub_run = subparsers.add_parser('run', help='拉取 + 計算（一條龍）')
    sub_run.add_argument('--ticker', type=str, help='股票代碼')
    sub_run.add_argument('--index', type=str, help='指數成分股 (sp500, nasdaq, dowjones)')
    sub_run.add_argument('--all', action='store_true', help='全部股票')
    sub_run.add_argument('--incremental', action='store_true', help='增量模式')
    sub_run.set_defaults(func=cmd_run)

    # bulk-financials
    sub_bulk = subparsers.add_parser('bulk-financials', help='Bulk 下載財報（全市場 CSV）')
    sub_bulk.add_argument('--year', type=str, help='指定年度（如 2025），不指定則拉最近 2 年')
    sub_bulk.add_argument('--index', type=str, help='篩選指數 (sp500, nasdaq, dowjones)，預設 sp500')
    sub_bulk.add_argument('--all', action='store_true', help='全市場，不篩選')
    sub_bulk.set_defaults(func=cmd_bulk_financials)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
