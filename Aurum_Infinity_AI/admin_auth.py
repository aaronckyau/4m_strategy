"""
admin_auth.py - Admin 登入驗證邏輯
============================================================
功能：
  - 驗證 admin 密碼（對比 .env 的 ADMIN_PASSWORD）
  - 產生 / 驗證 / 刪除 session token
  - admin_required 裝飾器（保護 /admin 路由）

Session 機制比喻：
  登入成功 → 產生一張「通行證」（token）存到 Cookie
  每次訪問 /admin → 檢查 Cookie 裡的通行證是否有效
  登出 → 把通行證作廢
============================================================
"""
import os
import secrets
import hmac
from datetime import datetime, timedelta, timezone
from functools import wraps

from dotenv import load_dotenv
from flask import request, redirect, session
from database import get_db

# 確保 .env 在這個模組被 import 時也能讀到
load_dotenv()

# Admin session 有效期：24 小時
ADMIN_SESSION_HOURS = 24


def _get_admin_password() -> str:
    """從 .env 讀取 ADMIN_PASSWORD"""
    pwd = os.getenv('ADMIN_PASSWORD', '')
    if not pwd:
        raise RuntimeError("❌ .env 檔案裡沒有設定 ADMIN_PASSWORD！")
    return pwd


def verify_admin_password(password: str) -> bool:
    """驗證輸入的密碼是否正確（使用 constant-time 比較防止時序攻擊）"""
    return hmac.compare_digest(password, _get_admin_password())


def create_admin_session() -> str:
    """
    建立新的 admin session token
    token = 64 個隨機字元（極難猜到）
    """
    token      = secrets.token_hex(32)   # 64 字元隨機字串
    now        = datetime.now(timezone.utc)
    expires_at = now + timedelta(hours=ADMIN_SESSION_HOURS)

    conn = get_db()
    try:
        # 清除過期的舊 session
        conn.execute(
            "DELETE FROM admin_sessions WHERE expires_at < ?",
            (now.isoformat(),)
        )
        # 寫入新 session
        conn.execute(
            "INSERT INTO admin_sessions (token, expires_at, created_at) VALUES (?, ?, ?)",
            (token, expires_at.isoformat(), now.isoformat())
        )
        conn.commit()
    finally:
        conn.close()

    return token


def verify_admin_session(token: str) -> bool:
    """驗證 session token 是否有效且未過期"""
    if not token:
        return False

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT expires_at FROM admin_sessions WHERE token = ?",
            (token,)
        ).fetchone()

        if not row:
            return False

        # 檢查是否過期（若 DB 中無時區資訊則視為 UTC）
        expires_at = datetime.fromisoformat(row['expires_at'])
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) < expires_at
    finally:
        conn.close()


def delete_admin_session(token: str):
    """登出：刪除 session token"""
    conn = get_db()
    try:
        conn.execute("DELETE FROM admin_sessions WHERE token = ?", (token,))
        conn.commit()
    finally:
        conn.close()


def admin_required(f):
    """
    裝飾器：保護 admin 路由
    未登入 → 自動跳轉到 /admin/login

    用法：
        @app.route('/admin/prompts')
        @admin_required
        def admin_prompts():
            ...
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        token = request.cookies.get('admin_token')
        if not verify_admin_session(token):
            return redirect('/admin/login')
        return f(*args, **kwargs)
    return wrapper
