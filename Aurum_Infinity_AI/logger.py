"""
logger.py - 集中日誌系統
============================================================
功能：
  - 同時輸出到 console 和 logs/ 資料夾的檔案
  - 自動每天產生新日誌檔（logs/app_2026-02-28.log）
  - 5 個日誌級別：DEBUG / INFO / WARNING / ERROR / CRITICAL
  - 記錄：時間、級別、模組、訊息、耗時、錯誤堆疊
  - 超過 30 天的舊日誌自動清除

使用方式：
  from logger import get_logger
  log = get_logger(__name__)

  log.info("訊息")
  log.error("錯誤", exc_info=True)   # 自動記錄 stack trace
  log.debug("除錯訊息")
============================================================
"""
import logging
import logging.handlers
import os
import glob
import time
import threading
from datetime import datetime, timedelta
from functools import wraps

# ── 日誌存放資料夾 ──────────────────────────────────────
LOG_DIR       = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
LOG_KEEP_DAYS = 30   # 保留最近 N 天的日誌


def _ensure_log_dir():
    os.makedirs(LOG_DIR, exist_ok=True)


def _clean_old_logs():
    """刪除超過 LOG_KEEP_DAYS 天的舊日誌"""
    cutoff = datetime.now() - timedelta(days=LOG_KEEP_DAYS)
    for path in glob.glob(os.path.join(LOG_DIR, 'app_*.log')):
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            if mtime < cutoff:
                os.remove(path)
        except OSError:
            pass


# ── 自訂格式器（console 帶顏色，檔案不帶顏色）──────────
class _SafeFormatter(logging.Formatter):
    """確保 request_id 欄位存在，即使 filter 未執行"""
    def format(self, record):
        if not hasattr(record, 'request_id'):
            record.request_id = '-'
        return super().format(record)


class ColorFormatter(_SafeFormatter):
    """Console 用：不同級別顯示不同顏色"""
    COLORS = {
        'DEBUG':    '\033[36m',   # 青色
        'INFO':     '\033[32m',   # 綠色
        'WARNING':  '\033[33m',   # 黃色
        'ERROR':    '\033[31m',   # 紅色
        'CRITICAL': '\033[35m',   # 紫色
    }
    RESET = '\033[0m'

    def format(self, record):
        color = self.COLORS.get(record.levelname, '')
        record.levelname = f"{color}{record.levelname:<8}{self.RESET}"
        return super().format(record)


# ── 日誌格式 ────────────────────────────────────────────
FILE_FORMAT    = '[%(asctime)s] %(levelname)-8s [%(name)s] [%(request_id)s] %(message)s'
CONSOLE_FORMAT = '[%(asctime)s] %(levelname)s [%(name)s] [%(request_id)s] %(message)s'
DATE_FORMAT    = '%Y-%m-%d %H:%M:%S'


# ── Request ID Filter ──────────────────────────────────
class RequestIdFilter(logging.Filter):
    """注入 request_id 到每條日誌記錄（Flask 請求外為 '-'）"""
    def filter(self, record):
        try:
            from flask import g
            record.request_id = getattr(g, 'request_id', '-')
        except (RuntimeError, ImportError):
            record.request_id = '-'
        return True


# ── 全域初始化（只執行一次）────────────────────────────
_initialized = False
_init_lock = threading.Lock()

def _init_logging():
    global _initialized
    with _init_lock:
        if _initialized:
            return
        _initialized = True

    _ensure_log_dir()
    _clean_old_logs()

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # 注入 request_id filter
    root.addFilter(RequestIdFilter())

    # 避免重複加 handler（Flask reload 時會重跑）
    if root.handlers:
        return

    # ── 1. Console Handler（INFO 以上）──────────────────
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(ColorFormatter(CONSOLE_FORMAT, datefmt=DATE_FORMAT))
    root.addHandler(console_handler)

    # ── 2. 每日滾動檔案 Handler（DEBUG 以上，完整記錄）──
    today     = datetime.now().strftime('%Y-%m-%d')
    log_path  = os.path.join(LOG_DIR, f'app_{today}.log')
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename    = log_path,
        when        = 'midnight',
        interval    = 1,
        backupCount = LOG_KEEP_DAYS,
        encoding    = 'utf-8',
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(_SafeFormatter(FILE_FORMAT, datefmt=DATE_FORMAT))
    file_handler.suffix = '%Y-%m-%d'
    root.addHandler(file_handler)

    # ── 3. 獨立 Error 檔案（ERROR 以上，方便快速查問題）
    error_path = os.path.join(LOG_DIR, 'error.log')
    error_handler = logging.handlers.RotatingFileHandler(
        filename    = error_path,
        maxBytes    = 5 * 1024 * 1024,   # 5MB
        backupCount = 3,
        encoding    = 'utf-8',
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(_SafeFormatter(FILE_FORMAT, datefmt=DATE_FORMAT))
    root.addHandler(error_handler)

    # 抑制 werkzeug 的重複訊息（Flask 內建伺服器）
    logging.getLogger('werkzeug').setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """取得 logger，name 通常傳入 __name__"""
    _init_logging()
    return logging.getLogger(name)


# ── 裝飾器：自動記錄 Flask route 的耗時與錯誤 ──────────
def log_route(logger: logging.Logger = None):
    """
    Flask route 裝飾器，自動記錄：
      - 請求進入（IP、method、路徑）
      - 回應狀態碼和耗時
      - 任何未捕捉的 Exception（含 stack trace）

    用法：
        @app.route('/AAPL')
        @log_route(log)
        def index(ticker_raw):
            ...
    """
    def decorator(func):
        _log = logger or get_logger(func.__module__)

        @wraps(func)
        def wrapper(*args, **kwargs):
            from flask import request as req
            start   = time.perf_counter()
            ip      = req.remote_addr
            method  = req.method
            path    = req.full_path.rstrip('?')

            _log.info(f"→ {method} {path}  [from {ip}]")
            try:
                result  = func(*args, **kwargs)
                elapsed = (time.perf_counter() - start) * 1000

                # 取得 status code
                status = getattr(result, 'status_code', None)
                if status is None and isinstance(result, tuple):
                    status = result[1] if len(result) > 1 else 200
                status = status or 200

                level = logging.WARNING if status >= 400 else logging.INFO
                _log.log(level, f"← {method} {path}  {status}  {elapsed:.1f}ms")
                return result

            except Exception as e:
                elapsed = (time.perf_counter() - start) * 1000
                _log.error(
                    f"✗ {method} {path}  EXCEPTION after {elapsed:.1f}ms — {e}",
                    exc_info=True
                )
                raise

        return wrapper
    return decorator


# ── 計時 context manager ────────────────────────────────
class Timer:
    """
    用法：
        with Timer(log, "呼叫 Gemini API"):
            result = call_gemini_api(prompt)
        # 自動輸出：[呼叫 Gemini API] 完成，耗時 3241ms
    """
    def __init__(self, logger: logging.Logger, label: str):
        self._log   = logger
        self._label = label
        self._start = None

    def __enter__(self):
        self._start = time.perf_counter()
        self._log.debug(f"⏱ [{self._label}] 開始")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = (time.perf_counter() - self._start) * 1000
        if exc_type:
            self._log.error(f"⏱ [{self._label}] 失敗，耗時 {elapsed:.1f}ms — {exc_val}", exc_info=True)
        else:
            self._log.info(f"⏱ [{self._label}] 完成，耗時 {elapsed:.1f}ms")
        return False   # 不吞掉 exception
