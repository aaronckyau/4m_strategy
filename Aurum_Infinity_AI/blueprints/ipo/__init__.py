"""
blueprints.ipo - IPO 追蹤模組
============================================================================
路由（url_prefix='/ipo'）：
  /              → IPO 追蹤主頁（正在招股列表）
============================================================================
"""
from flask import Blueprint

ipo_bp = Blueprint('ipo', __name__, url_prefix='/ipo')

from blueprints.ipo import routes  # noqa: E402, F401
