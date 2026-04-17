"""
blueprints/admin/routes.py - 管理後台路由
============================================================================
從 app.py 搬移的所有 Admin 相關路由 + IPO 管理。
url_prefix='/admin' 已在 __init__.py 設定。
============================================================================
"""
import json
import os
from datetime import datetime

import markdown as md_lib
from flask import render_template, request, jsonify, redirect, abort, make_response

import yaml

from blueprints.admin import admin_bp
from logger import get_logger
from database import get_db

_log = get_logger(__name__)
from extensions import prompt_manager
from services.gemini_service import call_gemini_api, strip_card_summary

from file_cache import save_section_md, save_section_html
from read_stock_code import get_stock_info
from admin_auth import (
    verify_admin_password, create_admin_session,
    delete_admin_session, admin_required, verify_admin_session,
)

# 借用 stock blueprint 的工具函數
from blueprints.stock.routes import resolve_ticker, get_today

# IPO 檔案儲存



def _parse_score(raw: str):
    """將表單的評分字串轉為 float 或 None"""
    if not raw:
        return None
    try:
        val = round(float(raw), 1)
        return max(0.0, min(10.0, val))
    except (ValueError, TypeError):
        return None


def _parse_comma_list(raw: str) -> list:
    """逗號分隔字串 → list of strings"""
    return [s.strip() for s in raw.split(',') if s.strip()]


def _parse_cornerstone_investors(raw: str) -> list:
    """每行 'Name: xx%' → list of {name, pct}"""
    result = []
    for line in raw.strip().splitlines():
        if ':' in line:
            parts = line.rsplit(':', 1)
            result.append({'name': parts[0].strip(), 'pct': parts[1].strip()})
    return result


    _log.info("IPO 翻譯：%s", list(payload.keys()))
    combined = json.dumps(payload, ensure_ascii=False)

    # 翻譯為簡體中文
    try:
        prompt_cn = (
            "請將以下 JSON 中所有繁體中文內容翻譯成簡體中文，保持原有格式和段落結構。\n"
            "JSON 結構不變，只替換 value 為簡體中文。\n"
            "只回傳純 JSON，不要加任何其他文字或 markdown 格式。\n\n"
            f"{combined}"
        )
        resp_cn = call_gemini_api(prompt_cn, use_search=False)
        data_cn = json.loads(_strip_code_block(resp_cn))
        if 'company_name' in data_cn:
            result['company_name_zh_cn'] = data_cn['company_name']
        if 'industry' in data_cn:
            result['industry_zh_cn'] = data_cn['industry']
        if 'sections' in data_cn:
            if 'sections_zh_cn' not in result:
                result['sections_zh_cn'] = {}
            result['sections_zh_cn'].update(data_cn['sections'])
        _log.info("IPO 簡體中文翻譯完成")
    except Exception as e:
        _log.error("IPO 簡體中文翻譯失敗: %s", e)

    # 翻譯為英文
    try:
        prompt_en = (
            "Please translate ALL values in the following JSON from Traditional Chinese to English. "
            "Keep the exact same JSON structure, only replace values with English.\n"
            "Return pure JSON only, no extra text or markdown formatting.\n\n"
            f"{combined}"
        )
        resp_en = call_gemini_api(prompt_en, use_search=False)
        data_en = json.loads(_strip_code_block(resp_en))
        if 'company_name' in data_en:
            result['company_name_en'] = data_en['company_name']
        if 'industry' in data_en:
            result['industry_en'] = data_en['industry']
        if 'sections' in data_en:
            if 'sections_en' not in result:
                result['sections_en'] = {}
            result['sections_en'].update(data_en['sections'])
        _log.info("IPO 英文翻譯完成")
    except Exception as e:
        _log.error("IPO 英文翻譯失敗: %s", e)

    return result


# ============================================================================
# 路由
# ============================================================================

@admin_bp.route('/')
def admin_root():
    token = request.cookies.get('admin_token')
    if verify_admin_session(token):
        return redirect('/admin/dashboard')
    return redirect('/admin/login')


@admin_bp.route('/login', methods=['GET', 'POST'])
def admin_login():
    error = None
    if request.method == 'POST':
        password = request.form.get('password', '')
        if verify_admin_password(password):
            token    = create_admin_session()
            response = make_response(redirect('/admin/dashboard'))
            response.set_cookie(
                'admin_token', token,
                httponly=True, samesite='Lax', max_age=86400
            )
            return response
        error = '密碼錯誤，請重試。'
    return render_template('admin/login.html', error=error)


@admin_bp.route('/logout')
def admin_logout():
    token = request.cookies.get('admin_token')
    if token:
        delete_admin_session(token)
    response = make_response(redirect('/admin/login'))
    response.delete_cookie('admin_token')
    return response


@admin_bp.route('/dashboard')
@admin_required
def admin_dashboard():
    sections    = prompt_manager.get_section_names()
    cache_count = 0
    cache_dir   = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'cache')
    if os.path.exists(cache_dir):
        cache_count = sum(
            1 for d in os.listdir(cache_dir)
            if os.path.isdir(os.path.join(cache_dir, d))
        )
    return render_template('admin/dashboard.html',
                           sections=sections,
                           cache_count=cache_count)


@admin_bp.route('/prompts/<section_key>', methods=['GET', 'POST'])
@admin_required
def admin_prompt_editor(section_key: str):
    sections = prompt_manager.get_section_names()
    if section_key not in sections:
        abort(404)

    msg = None
    if request.method == 'POST':
        new_prompt = request.form.get('prompt_content', '')
        prompt_manager.update_section_prompt(section_key, new_prompt)
        msg = '已儲存'

    current_prompt = prompt_manager.get_section_prompt(section_key)
    return render_template('admin/prompt_editor.html',
                           section_key=section_key,
                           section_name=sections[section_key],
                           prompt_content=current_prompt,
                           sections=sections,
                           msg=msg)


@admin_bp.route('/prompts/<section_key>/save', methods=['POST'])
@admin_required
def admin_save_prompt(section_key: str):
    """儲存編輯後的 prompt 內容到 prompts.yaml"""
    sections = prompt_manager.get_section_names()
    if section_key not in sections:
        return jsonify({"success": False, "error": "找不到此 section"}), 404

    if not request.json:
        return jsonify({"success": False, "error": "請求格式錯誤，需要 JSON"}), 400
    new_content = request.json.get('content', '')
    if not new_content.strip():
        return jsonify({"success": False, "error": "Prompt 內容不可為空"}), 400

    try:
        prompt_manager.update_section_prompt(section_key, new_content)
        return jsonify({"success": True})
    except Exception as e:
        _log.error("save_prompt %s: %s", section_key, e)
        return jsonify({"success": False, "error": "儲存失敗，請稍後重試"}), 500


def _build_prompt_variables(ticker, stock_name, exchange, db_info=None):
    """組裝 prompt 模板變數"""
    ctx = prompt_manager._get_exchange_context(exchange)
    from services.data_vars import resolve_data_vars
    data_vars = resolve_data_vars(ticker)
    chinese_name = (db_info or {}).get("name_zh_hk") or stock_name
    currency = (db_info or {}).get("currency") or ctx.get("currency", "")
    market = (db_info or {}).get("market", "")
    return {
        "ticker":         ticker,
        "stock_name":     stock_name,
        "chinese_name":   chinese_name,
        "exchange":       exchange,
        "market":         market,
        "today":          get_today(),
        "currency":       currency,
        "data_source":    ctx.get("data_source", ""),
        "legal_focus":    ctx.get("legal_focus", ""),
        "extra_analysis": ctx.get("extra_analysis", ""),
        **data_vars,
    }


@admin_bp.route('/resolve_vars', methods=['POST'])
@admin_required
def admin_resolve_vars():
    """查詢某個股票代碼對應的所有變數實際值"""
    if not request.json:
        return jsonify({"success": False, "error": "請求格式錯誤，需要 JSON"}), 400
    raw_ticker = request.json.get('ticker', '').strip()
    if not raw_ticker:
        return jsonify({"success": False, "error": "請輸入股票代碼"}), 400

    ticker  = resolve_ticker(raw_ticker)
    db_info = get_stock_info(ticker)

    if db_info is None:
        return jsonify({"success": False, "error": f"找不到 {raw_ticker} 的資料"}), 404

    stock_name = db_info["name"] or ticker
    exchange   = db_info["exchange"] or ""
    variables  = _build_prompt_variables(ticker, stock_name, exchange, db_info=db_info)

    return jsonify({
        "success":   True,
        "ticker":    ticker,
        "stock_name": stock_name,
        "exchange":  exchange,
        "variables": variables,
    })


@admin_bp.route('/prompts/<section_key>/preview', methods=['POST'])
@admin_required
def admin_preview_prompt(section_key: str):
    """用當前編輯中的 prompt 內容對指定股票執行 AI 預覽"""
    data       = request.json
    raw_ticker = data.get('ticker', '').strip()
    content    = data.get('content', '').strip()

    if not raw_ticker:
        return jsonify({"success": False, "error": "請輸入股票代碼"}), 400
    if not content:
        return jsonify({"success": False, "error": "Prompt 內容不可為空"}), 400

    ticker  = resolve_ticker(raw_ticker)
    db_info = get_stock_info(ticker)

    if db_info is None:
        return jsonify({"success": False, "error": f"找不到 {raw_ticker} 的資料"}), 404

    stock_name = db_info["name"] or ticker
    exchange   = db_info["exchange"] or ""
    variables  = _build_prompt_variables(ticker, stock_name, exchange, db_info=db_info)

    global_cfg  = prompt_manager._config.get('global', {})
    system_role = global_cfg.get('system_role', '')
    format_rules = global_cfg.get('format_rules', '')
    full_prompt = f"{system_role}\n\n{content}\n\n{format_rules}"
    for key, val in variables.items():
        full_prompt = full_prompt.replace(f'{{{key}}}', str(val))

    try:
        response_text = call_gemini_api(full_prompt, use_search=True)

        save_section_md(ticker, section_key, response_text, lang="zh_hk")
        _log.info("已儲存預覽 Markdown %s - %s → zh-TW", ticker, section_key)

        html_content = md_lib.markdown(
            response_text,
            extensions=['tables', 'fenced_code', 'nl2br']
        )
        html_content = strip_card_summary(html_content)
        return jsonify({
            "success":    True,
            "html":       html_content,
            "ticker":     ticker,
            "stock_name": stock_name,
            "exchange":   exchange,
        })
    except Exception as e:
        _log.error("preview_prompt %s/%s: %s", ticker, section_key, e)
        return jsonify({"success": False, "error": "預覽執行失敗，請稍後重試"}), 500


# ============================================================================
# 更新日誌
# ============================================================================

@admin_bp.route('/update-log')
@admin_required
def update_log():
    """顯示最近 200 筆 update_log 記錄"""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT id, job_name, mode, started_at, finished_at,
                   status, records_updated, error_message
            FROM update_log
            ORDER BY started_at DESC
            LIMIT 200
        """).fetchall()
    except Exception:
        rows = []
    finally:
        conn.close()
    return render_template('admin/update_log.html', rows=rows)

