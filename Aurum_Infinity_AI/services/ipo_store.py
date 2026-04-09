"""
services/ipo_store.py - IPO 檔案儲存服務
============================================================
每支 IPO 存為獨立 JSON 檔案：data/ipo/{ticker}.json

檔案結構：
{
    "ticker": "01989.HK",
    "company_name": "...",
    "industry": "...",
    "offer_price": "...",
    "lot_size": 100,
    "entry_fee": "...",
    "close_date": "...",
    "listing_date": "...",
    "sections": {
        "ipo_biz": "...",
        "ipo_finance": "...",
        "ipo_mgmt": "...",
        "ipo_market": "..."
    },
    "scores": {
        "ipo_biz": 7.5,
        "ipo_finance": null,
        ...
    }
}
============================================================
"""
import json
import os
import re

# IPO 資料目錄
IPO_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'ipo')


def _ensure_dir():
    """確保 data/ipo/ 目錄存在"""
    os.makedirs(IPO_DIR, exist_ok=True)


def _ticker_to_filename(ticker: str) -> str:
    """股票代碼 → 檔名（安全化）"""
    safe = re.sub(r'[^\w.\-]', '_', ticker)
    return f"{safe}.json"


def _filepath(ticker: str) -> str:
    """取得 IPO JSON 檔案完整路徑"""
    return os.path.join(IPO_DIR, _ticker_to_filename(ticker))


def list_all() -> list[dict]:
    """列出所有 IPO 資料，按檔案修改時間倒序"""
    _ensure_dir()
    results = []
    for fname in os.listdir(IPO_DIR):
        if not fname.endswith('.json'):
            continue
        fpath = os.path.join(IPO_DIR, fname)
        try:
            with open(fpath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not isinstance(data, dict):
                continue
            data['_mtime'] = os.path.getmtime(fpath)
            results.append(data)
        except (json.JSONDecodeError, IOError):
            continue
    results.sort(key=lambda x: x.get('_mtime', 0), reverse=True)
    for r in results:
        r.pop('_mtime', None)
    return results


def get(ticker: str) -> dict | None:
    """讀取單支 IPO 資料，找不到回傳 None"""
    fpath = _filepath(ticker)
    if not os.path.exists(fpath):
        return None
    try:
        with open(fpath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (json.JSONDecodeError, IOError):
        return None


def exists(ticker: str) -> bool:
    """檢查某支 IPO 是否已存在"""
    return os.path.exists(_filepath(ticker))


def save(data: dict) -> None:
    """儲存 IPO 資料（新增或覆蓋），使用原子寫入避免損毀"""
    import tempfile
    _ensure_dir()
    ticker = data.get('ticker', '')
    if not ticker:
        raise ValueError("ticker is required")
    fpath = _filepath(ticker)
    fd, tmp_path = tempfile.mkstemp(dir=IPO_DIR, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, fpath)
    except BaseException:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def delete(ticker: str) -> bool:
    """刪除 IPO 資料，成功回傳 True"""
    fpath = _filepath(ticker)
    if os.path.exists(fpath):
        os.remove(fpath)
        return True
    return False
