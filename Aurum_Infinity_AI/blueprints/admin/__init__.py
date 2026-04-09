"""
blueprints.admin - 管理後台模組
============================================================================
路由（url_prefix='/admin'）：
  /              → 入口跳轉
  /login         → 登入
  /logout        → 登出
  /dashboard     → 後台首頁
  /prompts/*     → Prompt 編輯
  /resolve_vars  → 變數查詢
============================================================================
"""
from flask import Blueprint

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')

from blueprints.admin import routes  # noqa: E402, F401
