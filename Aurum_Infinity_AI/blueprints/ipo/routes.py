"""
blueprints/ipo/routes.py - IPO 追蹤公開頁面
============================================================================
從 data/ipo/*.json 讀取 IPO 資料，顯示在前台。
自動根據 listing_date 分類：正在招股 / 半新股（上市 30 天內）。
============================================================================
"""
import calendar as _cal
from datetime import datetime, timedelta, timezone

import markdown as md_lib
from flask import render_template, request

from blueprints.ipo import ipo_bp
from blueprints.stock.routes import get_current_lang
from services.ipo_store import list_all as ipo_list_all
from translations import get_translations

# IPO section key 列表
IPO_SECTION_KEYS = ['ipo_biz', 'ipo_finance', 'ipo_mgmt', 'ipo_market']

# section key → 翻譯 key 對應
IPO_SECTION_T_KEYS = {
    'ipo_biz':     'ipo_section_biz',
    'ipo_finance': 'ipo_section_finance',
    'ipo_mgmt':    'ipo_section_mgmt',
    'ipo_market':  'ipo_section_market',
}

# 香港時區 UTC+8
HKT = timezone(timedelta(hours=8))


def _parse_date(date_str):
    """解析日期字串（支援 YYYY/MM/DD 和 YYYY-MM-DD）"""
    if not date_str:
        return None
    for fmt in ('%Y/%m/%d', '%Y-%m-%d'):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _classify_ipos(all_ipos):
    """
    將 IPO 分為 subscribing（正在招股）和 listed（半新股）兩組。
    規則：
      - listing_date 為空或 >= 今天 → 正在招股
      - listing_date < 今天 且 <= 30 天前 → 半新股
      - listing_date < 今天 - 30 天 → 不顯示
    """
    today = datetime.now(HKT).date()
    cutoff = today - timedelta(days=30)

    subscribing = []
    listed = []

    for ipo in all_ipos:
        ld = _parse_date(ipo.get('listing_date', ''))
        if ld is None or ld >= today:
            subscribing.append(ipo)
        elif ld >= cutoff:
            listed.append(ipo)
        # else: older than 30 days, skip

    return subscribing, listed


def _build_calendar(all_ipos, year, month):
    """
    產生月曆資料結構。
    回傳 dict:
      weeks    - 6×7 二維陣列，每格 = { day, events[] }
      year     - 年
      month    - 月
      prev     - (year, month) 上個月
      next     - (year, month) 下個月
    events 類型: 'close'（截止日）, 'listing'（上市日）
    """
    first_weekday, days_in_month = _cal.monthrange(year, month)
    # 週一 = 0
    grid = []
    day_counter = 1
    # 建立事件對照表 {date_str: [{type, ticker, name}]}
    events_map = {}
    for ipo in all_ipos:
        name = ipo.get('company_name', '')
        ticker = ipo.get('ticker', '')
        cd = _parse_date(ipo.get('close_date', ''))
        ld = _parse_date(ipo.get('listing_date', ''))
        if cd:
            key = cd.isoformat()
            events_map.setdefault(key, []).append({
                'type': 'close', 'ticker': ticker, 'name': name
            })
        if ld:
            key = ld.isoformat()
            events_map.setdefault(key, []).append({
                'type': 'listing', 'ticker': ticker, 'name': name
            })

    from datetime import date
    for week_idx in range(6):
        week = []
        max_day_in_week = 0
        for dow in range(7):
            cell_idx = week_idx * 7 + dow
            day_num = cell_idx - first_weekday + 1
            if day_num < 1 or day_num > days_in_month:
                week.append({'day': 0, 'events': []})
            else:
                d = date(year, month, day_num)
                evts = events_map.get(d.isoformat(), [])
                week.append({'day': day_num, 'events': evts})
                max_day_in_week = day_num
        grid.append(week)
        # 如果本週已包含月末最後一天，不需要再產生下一行
        if max_day_in_week >= days_in_month:
            break

    # 上/下月
    if month == 1:
        prev_ym = (year - 1, 12)
    else:
        prev_ym = (year, month - 1)
    if month == 12:
        next_ym = (year + 1, 1)
    else:
        next_ym = (year, month + 1)

    return {
        'weeks': grid,
        'year': year,
        'month': month,
        'prev': prev_ym,
        'next': next_ym,
    }


@ipo_bp.route('/')
def index():
    """IPO 追蹤主頁 — 顯示正在招股 + 半新股 + 日曆"""
    lang = get_current_lang()
    t    = get_translations(lang)

    all_ipos = ipo_list_all()
    subscribing, listed = _classify_ipos(all_ipos)

    # 根據語言選擇 company_name、industry、section 內容
    lang_suffix = '_' + lang if lang != 'zh_hk' else ''
    def _ensure_html(ipo_item):
        """若 IPO 沒有 sections_html，從 Markdown 即時生成"""
        for suffix in ('', '_zh_cn', '_en'):
            html_key = 'sections_html' + suffix
            md_key = 'sections' + suffix
            if not ipo_item.get(html_key) and ipo_item.get(md_key):
                ipo_item[html_key] = {
                    k: md_lib.markdown(v, extensions=['tables', 'fenced_code', 'nl2br'])
                    for k, v in ipo_item[md_key].items() if v and v.strip()
                }

    for ipo in subscribing + listed:
        _ensure_html(ipo)
        if lang_suffix:
            ipo['_display_name'] = ipo.get('company_name' + lang_suffix) or ipo.get('company_name', '')
            ipo['_display_industry'] = ipo.get('industry' + lang_suffix) or ipo.get('industry', '')
            translated = ipo.get('sections' + lang_suffix, {})
            if translated:
                ipo['_display_sections'] = translated
            else:
                ipo['_display_sections'] = ipo.get('sections', {})
            # HTML 版本（Markdown → HTML 預渲染）
            html_key = 'sections_html' + lang_suffix
            translated_html = ipo.get(html_key, {})
            if translated_html:
                ipo['_display_sections_html'] = translated_html
            else:
                ipo['_display_sections_html'] = ipo.get('sections_html', {})
        else:
            ipo['_display_name'] = ipo.get('company_name', '')
            ipo['_display_industry'] = ipo.get('industry', '')
            ipo['_display_sections'] = ipo.get('sections', {})
            ipo['_display_sections_html'] = ipo.get('sections_html', {})

    # 組裝翻譯後的 section 名稱字典
    ipo_sections = {}
    for key in IPO_SECTION_KEYS:
        t_key = IPO_SECTION_T_KEYS[key]
        ipo_sections[key] = t.get(t_key, key)

    # 日曆資料
    today = datetime.now(HKT).date()
    cal_year = request.args.get('cal_y', today.year, type=int)
    cal_month = request.args.get('cal_m', today.month, type=int)
    cal_data = _build_calendar(all_ipos, cal_year, cal_month)

    return render_template(
        'ipo/index.html',
        subscribing_list=subscribing,
        listed_list=listed,
        ipo_sections=ipo_sections,
        lang=lang,
        t=t,
        cal=cal_data,
        today_str=today.isoformat(),
    )
