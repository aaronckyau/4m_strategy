"""
logger.py - 日誌設定
"""
import io
import logging
import sys

def setup_logger(name: str = "data_fetcher", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(level)
    # Windows: 強制 UTF-8 輸出避免編碼錯誤
    stream = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter(
        "[%(asctime)s] %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(handler)
    return logger

log = setup_logger()
