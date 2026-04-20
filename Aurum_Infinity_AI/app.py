"""
app.py - Flask 應用入口（Blueprint 架構）
============================================================================
職責：
  1. 建立 Flask app
  2. 註冊 Blueprint（stock, admin, news_radar）
  3. 全域中間件（語言 Cookie）
  4. 初始化資料庫
  5. APScheduler — 每日 06:00 HKT 更新熱門話題

所有業務邏輯已搬入各 Blueprint，此檔案保持精簡。
============================================================================
"""
import uuid

from flask import Flask, request, jsonify, g
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

from config import Config, get_config
from translations import SUPPORTED_LANGS
from database import init_db


# ============================================================================
# 建立 Flask App
# ============================================================================

AppConfig = get_config()

app = Flask(__name__)
app.config['SECRET_KEY'] = __import__('os').getenv('FLASK_SECRET_KEY') or (
    'dev-insecure-key' if AppConfig.DEBUG else None
)
app.config['MAX_CONTENT_LENGTH'] = 1 * 1024 * 1024  # 1 MB

# 速率限制（AI 分析端點另有更嚴格的限制，見 stock blueprint）
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["60 per minute"],
    storage_uri="memory://",
)
# 匯出供 Blueprint 使用
app.limiter = limiter

# 啟動驗證
AppConfig.validate()

# 初始化資料庫（建立 admin_sessions 表等）
init_db()


# ============================================================================
# 全域中間件
# ============================================================================

@app.before_request
def _inject_request_id():
    """為每個請求生成唯一 ID，供日誌追蹤"""
    g.request_id = uuid.uuid4().hex[:8]

_LANG_TO_HTML = {'zh_hk': 'zh-TW', 'zh_cn': 'zh-CN', 'en': 'en'}

@app.context_processor
def inject_html_lang():
    """讓所有模板都能使用 html_lang 變數"""
    lang = request.args.get('lang', '') or request.cookies.get('lang', 'zh_hk')
    return {'html_lang': _LANG_TO_HTML.get(lang, 'zh-TW')}

@app.after_request
def set_lang_cookie(response):
    """若 URL 帶了 ?lang=，將語言存入 Cookie"""
    param_lang = request.args.get('lang', '').strip()
    if param_lang in SUPPORTED_LANGS:
        response.set_cookie(
            'lang', param_lang,
            max_age=365 * 24 * 3600,
            samesite='Lax'
        )
    return response


# ============================================================================
# Health Check
# ============================================================================

@app.route('/health')
@limiter.exempt
def health_check():
    """健康檢查端點，供部署監控與 load balancer 使用"""
    status = {"status": "ok"}
    try:
        from database import get_db
        conn = get_db()
        conn.execute("SELECT 1")
        conn.close()
        status["db"] = "ok"
    except Exception:
        status["db"] = "error"
        status["status"] = "degraded"

    try:
        from extensions import gemini_client
        status["gemini"] = "ok" if gemini_client else "not_configured"
    except Exception:
        status["gemini"] = "error"
        status["status"] = "degraded"

    code = 200 if status["status"] == "ok" else 503
    return jsonify(status), code


# ============================================================================
# 註冊 Blueprint
# ============================================================================

# Admin（url_prefix='/admin'，必須在 stock 之前註冊）
from blueprints.admin import admin_bp
app.register_blueprint(admin_bp)

# News Radar（必須在 stock 之前註冊，避免 /<ticker> 萬用路由攔截 /news-radar）
from blueprints.news_radar import news_radar_bp
app.register_blueprint(news_radar_bp)
limiter.limit("3 per minute")(app.view_functions['news_radar.analyze'])

# Futunn（必須在 stock 之前註冊，避免 /<ticker> 萬用路由攔截 /futunn）
from blueprints.futunn import futunn_bp
app.register_blueprint(futunn_bp)

# Trending（必須在 stock 之前註冊，避免 /<ticker> 萬用路由攔截 /trending）
from blueprints.trending import trending_bp
app.register_blueprint(trending_bp)

# Stock（無 prefix，含萬用路由 /<ticker>，必須最後註冊）
from blueprints.stock import stock_bp
app.register_blueprint(stock_bp)

# AI 分析端點加嚴格速率限制（10 次/分鐘，防止濫用 Gemini 配額）
limiter.limit("10 per minute")(app.view_functions['stock.analyze_section'])
limiter.limit("10 per minute")(app.view_functions['stock.rating_verdict'])
limiter.limit("10 per minute")(app.view_functions['stock.api_price_analysis'])


# ============================================================================
# APScheduler — 每日 06:00 HKT 更新熱門話題
# ============================================================================

def _schedule_radar_topics():
    """啟動 APScheduler，只在非 debug reloader 子進程中執行"""
    import os
    # Werkzeug debug mode 會啟動兩個進程，只讓主進程跑 scheduler
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        return

    from services.radar_topics_service import fetch_and_cache_topics
    from logger import get_logger
    _sched_log = get_logger('scheduler')

    scheduler = BackgroundScheduler(timezone=pytz.utc)
    # 每日 06:00 HKT = 每日 22:00 UTC（前一天）
    scheduler.add_job(
        func=fetch_and_cache_topics,
        trigger=CronTrigger(hour=22, minute=0, timezone=pytz.utc),
        id='radar_topics_daily',
        name='每日熱門話題更新',
        replace_existing=True,
        misfire_grace_time=300,   # 允許最多 5 分鐘的延遲觸發
    )
    scheduler.start()
    _sched_log.info("APScheduler 已啟動，每日 06:00 HKT 更新熱門話題")

    # 啟動時若今日快取不存在，立即抓一次
    from services.radar_topics_service import _today_hkt, _load_cache
    if _load_cache(_today_hkt()) is None:
        _sched_log.info("今日熱門話題快取不存在，立即抓取一次")
        try:
            fetch_and_cache_topics()
        except Exception as e:
            _sched_log.warning("啟動時抓取熱門話題失敗: %s", e)

    return scheduler


_scheduler = _schedule_radar_topics()


# ============================================================================
# 啟動
# ============================================================================

if __name__ == '__main__':
    app.run(debug=True, port=5000)
