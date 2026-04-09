"""
blueprints.stock - 股票分析模組
============================================================================
路由：
  /              → 跳轉到預設標的
  /<ticker>      → 股票分析主頁
  /analyze/<sec> → AI 分析 API
  /api/search_stock     → 自動完成
  /api/markdown/*       → Markdown 匯出
============================================================================
"""
from flask import Blueprint

stock_bp = Blueprint('stock', __name__)

from blueprints.stock import routes  # noqa: E402, F401
