"""
cache_warmer.py - 批量預熱快取系統（獨立運行）
============================================================================
用途：
  自動讀取 stock_list.txt，按批次呼叫 Flask API 預先生成快取
  防止用戶首次訪問時需要等待 AI 分析

批次說明：
  - 1 個股票 = 7 個請求（7 個分析分類）
  - BATCH_SIZE = 5  → 每批 35 個請求
  - BATCH_SIZE = 10 → 每批 70 個請求
  - 批次間隔 60 秒，避免 API 過載

配置：
  - BATCH_SIZE：每批處理 N 個股票（5-10 個）
  - BATCH_DELAY：批次間隔（秒），避免 API 過載
  - STOCK_LIST_FILE：股票列表檔案路徑

運行方式（獨立於 app.py）：
  終端 1：python app.py              ← Flask 應用
  終端 2：python cache_warmer.py     ← 批量快取預熱
============================================================================
"""

import requests
import time
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed


# ■■■■■ 配置區 ■■■■■
FLASK_URL = "http://127.0.0.1:5000"  # Flask 應用位址（本機開發）
STOCK_LIST_FILE = "stock_list.txt"   # 股票列表檔案（與 app.py 同目錄）
BATCH_SIZE = 10                        # 每批處理的股票數量
BATCH_DELAY = 5                      # 批次間隔時間（秒）
SECTIONS = [
    'biz',          # 商業模式
    'exec',         # 管理層
    'finance',      # 財務質量
    'call',         # 會議展望
    'ta_price',     # 價格行為
    'ta_analyst',   # 分析師預測
    'ta_social'     # 社群情緒
]


def read_stock_list(filename: str) -> List[str]:
    """
    讀取股票列表檔案
    
    支持的格式：
      1. 逗號分隔：AAPL,NVDA,GOOGL,V
      2. 換行分隔：
         AAPL
         NVDA
         GOOGL
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read().strip()
    except FileNotFoundError:
        print(f"❌ 找不到檔案: {filename}")
        return []
    
    # 解析股票代碼
    if ',' in content:
        tickers = [t.strip() for t in content.split(',')]
    else:
        tickers = [t.strip() for t in content.split('\n')]
    
    # 去除空字符串
    tickers = [t for t in tickers if t]
    
    return tickers


def trigger_analysis(ticker: str, section: str) -> bool:
    """
    呼叫 Flask API 分析某個股票的某個分類
    
    參數：
      ticker: 股票代碼（如 'AAPL'、'0700.HK'）
      section: 分析分類（biz, exec, finance, call, ta_price, ta_analyst, ta_social）
    
    返回：
      True 成功，False 失敗
    """
    try:
        url = f"{FLASK_URL}/analyze/{section}"
        payload = {
            "ticker": ticker,
            "force_update": False  # 不強制更新（優先使用已有快取，節省 API 配額）
        }
        
        response = requests.post(url, json=payload, timeout=120)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                from_cache = data.get("from_cache", False)
                status = "📦" if from_cache else "✅"
                return (status, True)
            else:
                error_msg = data.get('error', '未知錯誤')
                return ("❌", False)
        else:
            return ("❌", False)
    
    except requests.exceptions.Timeout:
        return ("⏱️", False)
    except requests.exceptions.ConnectionError:
        return ("🔌", False)
    except Exception as e:
        return ("❌", False)


def process_single_analysis(ticker: str, section: str) -> Tuple[str, str, bool]:
    """
    處理單個股票的單個分析（用於並行執行）
    
    返回：
      (ticker, section, success: bool)
    """
    status, success = trigger_analysis(ticker, section)
    return (ticker, section, success, status)


def main():
    """主程序"""
    print("\n" + "=" * 75)
    print("  🚀 批量快取預熱系統 - 開始運行")
    print("=" * 75 + "\n")
    
    # 讀取股票列表
    tickers = read_stock_list(STOCK_LIST_FILE)
    if not tickers:
        print("❌ 沒有找到任何股票代碼\n")
        return
    
    print(f"📋 找到 {len(tickers)} 個股票代碼: {', '.join(tickers)}\n")
    
    # 計算批次和請求數
    total_batches = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE
    total_requests = len(tickers) * len(SECTIONS)
    batch_requests = BATCH_SIZE * len(SECTIONS)
    
    print(f"📊 預計請求數：{total_requests} 個請求（{len(tickers)} 個股票 × {len(SECTIONS)} 個分析）")
    print(f"   每批 {BATCH_SIZE} 個股票 = {batch_requests} 個請求")
    print(f"   共 {total_batches} 批")
    print(f"   ⚡ 並行執行模式（同時發送所有請求）\n")
    
    # 分批處理
    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(tickers))
        batch_tickers = tickers[start_idx:end_idx]
        
        print(f"\n{'─' * 75}")
        print(f"⏱️  第 {batch_num + 1} 批 / 共 {total_batches} 批")
        print(f"    股票區間：第 {start_idx + 1} ~ {end_idx} 個（共 {len(tickers)} 個）")
        print(f"    股票代碼：{', '.join(batch_tickers)}")
        print(f"    本批請求：{len(batch_tickers)} × {len(SECTIONS)} = {len(batch_tickers) * len(SECTIONS)} 個")
        print(f"{'─' * 75}\n")
        
        # 準備所有任務
        tasks = []
        for ticker in batch_tickers:
            for section in SECTIONS:
                tasks.append((ticker, section))
        
        print(f"📤 發送 {len(tasks)} 個請求（同時進行）...\n")
        
        # 並行執行所有請求
        results = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_task = {
                executor.submit(process_single_analysis, ticker, section): (ticker, section)
                for ticker, section in tasks
            }
            
            completed = 0
            for future in as_completed(future_to_task):
                ticker, section, success, status = future.result()
                if (ticker, section) not in results:
                    results[(ticker, section)] = (success, status)
                completed += 1
                print(f"    [{completed:2d}/{len(tasks)}] {ticker:10} {section:15} {status} {'✓' if success else '✗'}")
        
        # 統計結果
        successful_requests = sum(1 for success, _ in results.values() if success)
        successful_stocks = len(set(ticker for ticker, _ in results.keys() if all(
            results.get((ticker, s), (False, ''))[0] for s in SECTIONS
        )))
        
        print(f"\n  ✅ 完成：{successful_stocks}/{len(batch_tickers)} 個股票")
        print(f"  📊 成功率：{successful_requests}/{len(tasks)} 個請求成功\n")
        
        # 批次間延遲（最後一批不需要等待）
        if batch_num < total_batches - 1:
            print(f"⏸️  等待 {BATCH_DELAY} 秒後處理下一批...")
            for i in range(BATCH_DELAY, 0, -10):
                remaining = i if i > 0 else 0
                print(f"    倒數計時：{remaining} 秒", end="\r")
                time.sleep(10 if i > 10 else i)
            print("    " + " " * 20, end="\r")  # 清除倒數計時
    
    print("\n" + "=" * 75)
    print("  ✅ 全部股票快取預熱完畢！")
    print("=" * 75 + "\n")


if __name__ == "__main__":
    main()
