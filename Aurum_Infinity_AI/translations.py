"""
Translations - 多語言 UI 字串字典
============================================================================
支援語言：
  zh_hk  繁體中文（預設）
  zh_cn  簡體中文
  en     English
============================================================================
"""

TRANSLATIONS: dict = {
    "zh_hk": {
        # 導航列
        "nav_stock": "股票分析",
        "nav_trending": "熱度觀測",
        "nav_futunn": "富途新聞流",
        "nav_ipo": "IPO追蹤",
        "nav_collapse": "摺疊側欄",
        "nav_coming_soon": "即將推出",
        "nav_more": "更多",
        "nav_lang_setting": "語言設定",
        "nav_theme_dark": "深色模式",
        "nav_theme_light": "淺色模式",
        "nav_theme_setting": "外觀設定",
        "nav_about": "關於 DataLab",
        "nav_terms": "使用條款",
        "nav_feedback": "意見回饋",
        "nav_version": "4M DataLab v1.0",

        # Header
        "search_placeholder": "股票代碼或名稱...",
        "search_btn": "查詢",
        "lang_label": "語言",

        # 頁面標題
        "terminal_title": "投資決策終端",

        # 基本面分析區
        "report_date_prefix": "更新：",
        "fundamental_title": "價值透析",
        "fundamental_label": "基本面分析",
        "fundamental_tooltip": "從四個核心維度透析企業內在價值：商業模式的競爭壁壘與護城河、財務報表的盈利質量與現金流健康度、管理層的治理能力與股東回報紀錄、以及業績電話會議透露的未來增長展望與風險訊號。",
        "card_biz":    "商業模式",  "icon_biz":    "策略",
        "card_exec":   "治理效能",  "icon_exec":   "管理層",
        "card_finance":"財務質量",  "icon_finance":"財務",
        "card_call":   "會議展望",  "icon_call":   "展望",

        # 技術面分析區
        "technical_title": "動能透析",
        "technical_label": "市場動態",
        "technical_tooltip": "從三個市場維度透析短期動能方向：K線型態與均線趨勢的技術面訊號、華爾街分析師的目標價共識與評級變化、以及散戶情緒指標與社群媒體輿論風向。",
        "card_ta_price":   "價格行為",  "icon_ta_price":   "技術面",
        "card_ta_analyst": "機構持倉",  "icon_ta_analyst": "13F",
        "card_ta_social":  "社群情緒",  "icon_ta_social":  "輿情",

        # JS 動態訊息
        "loading_msgs": ["正在收集資料...", "正在分析中...", "快完成了！", "結果馬上就出來啦～"],
        "confirm_reanalyze": "確定要重新分析此區塊嗎？\n\n⚠️ 這將重新生成最新分析。",
        "updating": "更新中...",
        "updated":  "✅ 已更新",
        "no_data":  "暫無數據",
        "copied":       "已複製到剪貼板",
        "copy_manual":  "請手動複製：",
        "share_text":   "查看 {ticker} 的 {title} 分析",
        "cache_label":  "非即時數據",
        "fresh_label":  "即時分析",
        "init_msg":     "系統初始化中...",
        "standby_msg":  "待命狀態...",
        "smart_terminal": "智能終端",

        # 彈出視窗按鈕 title
        "btn_update":   "更新報告",
        "etf_holders_title": "持倉 ETF",
        "etf_holders_empty": "無 ETF 持倉數據",
        "etf_col_symbol": "ETF",
        "etf_col_weight": "持倉比重",
        "etf_col_aum": "AUM",
        "btn_minimize": "最小化",
        "btn_maximize": "最大化",
        "btn_close":    "關閉",
        "btn_scroll_top": "回到頂部",
        "btn_share":    "分享",

        # 綜合評級面板
        "rating_title": "綜合評級",
        "rating_score": "綜合評分",
        "rating_analyzing": "分析進行中",
        "progress_label": "分析進度",
        "rating_verdicts": {
            "A+": "各維度均表現卓越，極具投資潛力",
            "A":  "整體表現出色，具備明確投資價值",
            "A-": "多數指標良好，值得深入關注",
            "B+": "表現穩健，部分領域仍有提升空間",
            "B":  "中規中矩，需留意潛在風險",
            "B-": "表現一般，建議謹慎評估",
            "C+": "部分指標欠佳，風險偏高",
            "C":  "多項指標偏弱，不建議貿然投入",
            "D":  "整體評價較差，風險顯著"
        },
        "rating_best": "最強項",
        "rating_worst": "需關注",

        # 關鍵指標
        "metric_price": "股價",
        "metric_mcap":  "市值",
        "metric_rev":   "營收",
        "metric_rev_yoy": "營收 YoY",
        "metric_gm":    "毛利率",
        "metric_nm":    "淨利率",
        "metric_de":    "負債/權益",
        "metric_dy":    "股息率",
        "metric_dps":   "每股股息",
        "tip_price":    "最近一個交易日的收盤價。股價反映市場對公司當前價值的共識。",
        "tip_mcap":     "市值 = 股價 × 總股數。代表買下整間公司需要多少錢。大型股通常 > 100 億美元，穩定性較高。",
        "tip_pe":       "股價除以每股盈餘（EPS）。代表投資者願意為每 $1 盈利付出多少錢。同行業比較更有意義，一般 15-25 為合理區間。",
        "tip_peg":      "本益比除以盈餘成長率。修正了 P/E 忽略成長性的缺點。< 1 可能被低估，1-2 為合理，> 2 可能偏貴。",
        "tip_eps":      "淨利潤除以流通股數。代表每持有一股可分配到多少盈利。持續成長的 EPS 是股價上漲的核心驅動力。",
        "tip_rev":      "公司最近一季的總銷售收入。營收是企業成長最直接的指標，需留意是否能持續增長。",
        "tip_rev_yoy":  "與去年同季相比，營收增長或衰退的幅度。正值代表業務在擴張，負值代表可能面臨逆風。",
        "tip_gm":       "（營收 − 銷貨成本）÷ 營收。衡量核心業務的獲利能力。毛利率越高，代表產品定價能力越強或成本控制越好。軟體業通常 > 70%，零售業約 20-30%。",
        "tip_nm":       "淨利潤 ÷ 營收。扣除所有成本、稅金後，每賺 $1 營收實際留下多少利潤。持續提升的淨利率代表經營效率在改善。",
        "tip_de":       "總負債 ÷ 股東權益。衡量公司用多少借貸來經營。< 1 代表主要靠自有資金，> 2 代表較依賴舉債，需注意利息負擔。不同行業差異大，金融業通常較高。",
        "tip_dy":       "年度股息 ÷ 股價。代表每投入 $1 可獲得多少現金回報。穩定派息的公司通常財務較穩健，但過高的殖利率可能暗示股價已大幅下跌。",
        "tip_dps":      "過去 12 個月每股派發的股息總額。穩定或逐年增長的 DPS 是公司財務健康的良好訊號。",
        "metrics_more": "更多指標",

        # 時段走勢分析
        "pa_title":       "時段走勢分析",
        "pa_tooltip":     "選取任意時段，系統將深度解讀該區間的價格走勢型態、成交量異動、關鍵支撐與阻力位突破，並交叉比對期間實際發生的財報發佈、機構評級調整、行業政策變動等重大事件，還原股價變動的真實原因。",
        "pa_hint":        "點擊 K 線圖上任意一根蠟燭，選擇開始日期。系統將分析從該日到今日的走勢，並搜尋期間實際發生的新聞與事件。",
        "pa_start":       "開始",
        "pa_end":         "結束",
        "pa_today":       "今日",
        "pa_analyze_btn": "分析此時段",
        "pa_view_btn":    "查看報告",
        "pa_analyzing":   "分析中…",
        "pa_events_title":"關鍵事件",

        # K 線圖
        "chart_empty":        "尚無價格數據",
        "chart_empty_sub":    "請先用 Data Fetcher 同步此股票的 OHLC 資料",
        "chart_hide_events":  "隱藏事件標記",
        "chart_show_events":  "顯示事件標記",

        # 卡片 CTA
        "card_cta":       "點擊查看完整報告",
        "card_cta_short": "查看報告 →",

        # 錯誤頁面
        "error_title":       "無效的股票代碼",
        "error_unrecognized": "系統無法識別股票代碼",
        "error_hint":        "請確認代碼是否正確，例如：AAPL、TSLA、NVDA、0700.HK",
        "error_search_placeholder": "重新輸入股票代碼...",
        "error_back_default": "← 返回預設標的",

        # IPO 追蹤
        "ipo_page_title": "IPO 追蹤",
        "ipo_subscribing": "正在招股",
        "ipo_listed": "半新股",
        "ipo_empty": "目前沒有正在招股的 IPO",
        "ipo_offer_price": "招股價",
        "ipo_lot_size": "每手股數",
        "ipo_entry_fee": "入場費",
        "ipo_close_date": "截止日",
        "ipo_listing_date": "上市日",
        "ipo_listing_price": "上市價",
        "ipo_oversubscription": "超額倍數",
        "ipo_one_lot": "穩中一手",
        "ipo_ballot_rate": "中籤率",
        "ipo_first_day": "首日表現",
        "ipo_sponsors": "保薦人",
        "ipo_underwriters": "承銷商",
        "ipo_cornerstone_pct": "基石投資者占比",
        "ipo_cornerstone_investors": "基石投資者",
        "ipo_lockup_date": "基石投資禁售截止日",
        "ipo_detail_toggle": "招股詳情",
        "ipo_is_ah_stock": "A+H 股",
        "ipo_margin_ratio": "認購倍數",
        "ipo_one_hand_win_rate": "1手中籤率",
        "ipo_score": "評分",
        "ipo_report": "報告",
        "ipo_name_code": "名稱 / 代號",
        "ipo_view": "查看",
        "ipo_section_biz": "商業模式分析",
        "ipo_section_finance": "財務分析",
        "ipo_section_mgmt": "管理層評估",
        "ipo_section_market": "市場分析",
        "ipo_calendar": "IPO 日曆",
        "ipo_cal_close": "截止",
        "ipo_cal_listing": "上市",
        "ipo_cal_mon": "一",
        "ipo_cal_tue": "二",
        "ipo_cal_wed": "三",
        "ipo_cal_thu": "四",
        "ipo_cal_fri": "五",
        "ipo_cal_sat": "六",
        "ipo_cal_sun": "日",
        "ipo_countdown_days": "天",
        "ipo_countdown_hours": "小時",
        "ipo_countdown_mins": "分鐘",
        "ipo_countdown_ended": "已截止",

        # 免責聲明
        "disclaimer_title": "免責聲明與風險披露",
        "disclaimer_body": (
            "本資訊僅為一般通訊，僅供提供資訊及參考之用。其性質屬教育性質，並非旨在作為對任何特定投資產品、"
            "策略、計劃特點或其他目的的意見、建議或推薦，亦不構成 Aureum Infinity Capital Limited（滶盈資本有限公司）"
            "參與本文所述任何交易的承諾。本資訊中所使用的任何例子均屬泛化、假設性及僅供說明用途。本材料並未包含足夠資訊"
            "以支持任何投資決定，閣下不應依賴本資訊來評估投資任何證券或產品的優劣。\n"
            "此外，閣下應自行獨立評估任何投資在法律、監管、稅務、信貸及會計方面的影響，並與閣下自身的專業顧問共同決定，"
            "本資訊所述任何投資是否適合閣下的個人目標。投資者應確保在作出任何投資前，取得所有可取得的相關資訊。\n\n"
            "本資訊所載的任何預測、數字、意見、投資技巧及策略僅供資訊用途，基於若干假設及當前市場狀況，並可於無事先通知下變更。"
            "本資訊所呈現的所有內容，本公司已盡力確保於製作時準確，但並無就其準確性、完整性或及時性作出任何保證，"
            "亦不會就任何錯誤、遺漏或因依賴本資訊而產生的任何損失承擔責任。\n\n"
            "必須注意，投資涉及風險，投資價值及來自投資的收入可能會因市場狀況及稅務協議而波動，"
            "投資者可能無法取回全部投資本金。過往表現及收益率並非當前及未來結果的可靠指標。\n\n"
            "本內容並非針對任何特定司法管轄區的投資者而提供，不同司法管轄區的投資者應自行確保使用本內容符合當地法例及規定。"
            "本公司保留隨時修改、更新或撤回本內容的權利，而毋須事先通知。"
        ),
        "copyright": "© 4M Strategies Limited  // 版權所有",

        # 新聞投資雷達
        "nav_radar":            "新聞雷達",
        "futunn_title":         "富途新聞流",
        "futunn_subtitle":      "只讀 Futu 快取 JSON，把焦點放在富途來源的新聞標題、摘要與相關股票。",
        "futunn_live_count":    "目前文章",
        "futunn_updated":       "快取更新",
        "futunn_cache_label":   "資料來源",
        "futunn_categories":    "分類",
        "futunn_all":           "全部",
        "futunn_cache_info":    "Futunn Cache",
        "futunn_cache_desc":    "此頁只讀本地 JSON，不在開頁時即時抓取網站。",
        "futunn_missing":       "目前沒有可顯示的 Futu 快取內容。",
        "futunn_empty":         "請先準備 futunn_cache.json 後再重新整理此頁。",
        "futunn_open_source":   "查看原文",
        "futunn_origin_site":   "來源網站",
        "futunn_related":       "更多同類新聞",
        "futunn_related_stocks":"可能受影響的股票",
        "radar_title":          "新聞投資雷達",
        "radar_subtitle":       "輸入宏觀事件，AI 分析多情境投資機會",
        "radar_placeholder":    "輸入新聞事件、地緣政治、宏觀話題…",
        "radar_btn":            "分析",
        "radar_trending":       "熱門話題",
        "radar_scenario_a":     "情況好轉",
        "radar_scenario_b":     "情況惡化",
        "radar_sectors":        "受益板塊",
        "radar_picks":          "推薦持股",
        "radar_report":         "完整分析報告",
        "radar_report_hide":    "收起報告",
        "radar_reanalyze":      "重新分析",
        "radar_analyzing":      "正在分析…",
        "radar_loading_1":      "正在搜尋最新新聞…",
        "radar_loading_2":      "分析投資邏輯…",
        "radar_loading_3":      "整理投資建議…",
        "radar_error":          "分析失敗，請稍後再試",
        "radar_empty":          "請輸入一個宏觀事件或話題開始分析",
        "radar_impact_score":   "事件影響力",
        "radar_timeline":       "影響時間軸",
        "radar_risk":           "主要風險",
        "radar_click_stock":    "點擊代碼查看股票分析",
        "radar_topics": [
            "伊朗美國衝突", "AI晶片禁令", "美國關稅戰", "能源危機",
            "聯儲局政策", "供應鏈重組", "中美科技戰", "美元走強",
        ],
    },

    "zh_cn": {
        # 导航栏
        "nav_stock": "股票分析",
        "nav_trending": "热度观测",
        "nav_futunn": "富途新闻流",
        "nav_ipo": "IPO追踪",
        "nav_collapse": "折叠侧栏",
        "nav_coming_soon": "即将推出",
        "nav_more": "更多",
        "nav_lang_setting": "语言设置",
        "nav_theme_dark": "深色模式",
        "nav_theme_light": "浅色模式",
        "nav_theme_setting": "外观设定",
        "nav_about": "关于 DataLab",
        "nav_terms": "使用条款",
        "nav_feedback": "意见反馈",
        "nav_version": "4M DataLab v1.0",

        # Header
        "search_placeholder": "股票代码或名称...",
        "search_btn": "查询",
        "lang_label": "语言",

        # 页面标题
        "terminal_title": "投资决策终端",

        # 基本面分析区
        "report_date_prefix": "更新：",
        "fundamental_title": "价值透析",
        "fundamental_label": "基本面分析",
        "fundamental_tooltip": "从四个核心维度透析企业内在价值：商业模式的竞争壁垒与护城河、财务报表的盈利质量与现金流健康度、管理层的治理能力与股东回报纪录、以及业绩电话会议透露的未来增长展望与风险信号。",
        "card_biz":    "商业模式",  "icon_biz":    "策略",
        "card_exec":   "治理效能",  "icon_exec":   "管理层",
        "card_finance":"财务质量",  "icon_finance":"财务",
        "card_call":   "会议展望",  "icon_call":   "展望",

        # 技术面分析区
        "technical_title": "动能透析",
        "technical_label": "市场动态",
        "technical_tooltip": "从三个市场维度透析短期动能方向：K线形态与均线趋势的技术面信号、华尔街分析师的目标价共识与评级变化、以及散户情绪指标与社交媒体舆论风向。",
        "card_ta_price":   "价格行为",  "icon_ta_price":   "技术面",
        "card_ta_analyst": "机构持仓",  "icon_ta_analyst": "13F",
        "card_ta_social":  "社群情绪",  "icon_ta_social":  "舆情",

        # JS 动态消息
        "loading_msgs": ["正在收集数据...", "正在分析中...", "快完成了！", "结果马上就出来啦～"],
        "confirm_reanalyze": "确定要重新分析此区块吗？\n\n⚠️ 这将重新生成最新分析。",
        "updating": "更新中...",
        "updated":  "✅ 已更新",
        "no_data":  "暂无数据",
        "copied":       "已复制到剪贴板",
        "copy_manual":  "请手动复制：",
        "share_text":   "查看 {ticker} 的 {title} 分析",
        "cache_label":  "非实时数据",
        "fresh_label":  "实时分析",
        "init_msg":     "系统初始化中...",
        "standby_msg":  "待命状态...",
        "smart_terminal": "智能终端",

        # 弹出窗口按钮 title
        "btn_update":   "更新报告",
        "etf_holders_title": "持仓 ETF",
        "etf_holders_empty": "无 ETF 持仓数据",
        "etf_col_symbol": "ETF",
        "etf_col_weight": "持仓比重",
        "etf_col_aum": "AUM",
        "btn_minimize": "最小化",
        "btn_maximize": "最大化",
        "btn_close":    "关闭",
        "btn_scroll_top": "回到顶部",
        "btn_share":    "分享",

        # 综合评级面板
        "rating_title": "综合评级",
        "rating_score": "综合评分",
        "rating_analyzing": "分析进行中",
        "progress_label": "分析进度",
        "rating_verdicts": {
            "A+": "各维度均表现卓越，极具投资潜力",
            "A":  "整体表现出色，具备明确投资价值",
            "A-": "多数指标良好，值得深入关注",
            "B+": "表现稳健，部分领域仍有提升空间",
            "B":  "中规中矩，需留意潜在风险",
            "B-": "表现一般，建议谨慎评估",
            "C+": "部分指标欠佳，风险偏高",
            "C":  "多项指标偏弱，不建议贸然投入",
            "D":  "整体评价较差，风险显著"
        },
        "rating_best": "最强项",
        "rating_worst": "需关注",

        # 关键指标
        "metric_price": "股价",
        "metric_mcap":  "市值",
        "metric_rev":   "营收",
        "metric_rev_yoy": "营收 YoY",
        "metric_gm":    "毛利率",
        "metric_nm":    "净利率",
        "metric_de":    "负债/权益",
        "metric_dy":    "股息率",
        "metric_dps":   "每股股息",
        "tip_price":    "最近一个交易日的收盘价。股价反映市场对公司当前价值的共识。",
        "tip_mcap":     "市值 = 股价 × 总股数。代表买下整间公司需要多少钱。大型股通常 > 100 亿美元，稳定性较高。",
        "tip_pe":       "股价除以每股盈余（EPS）。代表投资者愿意为每 $1 盈利付出多少钱。同行业比较更有意义，一般 15-25 为合理区间。",
        "tip_peg":      "市盈率除以盈余增长率。修正了 P/E 忽略成长性的缺点。< 1 可能被低估，1-2 为合理，> 2 可能偏贵。",
        "tip_eps":      "净利润除以流通股数。代表每持有一股可分配到多少盈利。持续增长的 EPS 是股价上涨的核心驱动力。",
        "tip_rev":      "公司最近一季的总销售收入。营收是企业成长最直接的指标，需留意是否能持续增长。",
        "tip_rev_yoy":  "与去年同季相比，营收增长或衰退的幅度。正值代表业务在扩张，负值代表可能面临逆风。",
        "tip_gm":       "（营收 − 销货成本）÷ 营收。衡量核心业务的获利能力。毛利率越高，代表产品定价能力越强或成本控制越好。软件业通常 > 70%，零售业约 20-30%。",
        "tip_nm":       "净利润 ÷ 营收。扣除所有成本、税金后，每赚 $1 营收实际留下多少利润。持续提升的净利率代表经营效率在改善。",
        "tip_de":       "总负债 ÷ 股东权益。衡量公司用多少借贷来经营。< 1 代表主要靠自有资金，> 2 代表较依赖举债，需注意利息负担。不同行业差异大，金融业通常较高。",
        "tip_dy":       "年度股息 ÷ 股价。代表每投入 $1 可获得多少现金回报。稳定派息的公司通常财务较稳健，但过高的股息率可能暗示股价已大幅下跌。",
        "tip_dps":      "过去 12 个月每股派发的股息总额。稳定或逐年增长的 DPS 是公司财务健康的良好信号。",
        "metrics_more": "更多指标",

        # 时段走势分析
        "pa_title":       "时段走势分析",
        "pa_tooltip":     "选取任意时段，系统将深度解读该区间的价格走势形态、成交量异动、关键支撑与阻力位突破，并交叉比对期间实际发生的财报发布、机构评级调整、行业政策变动等重大事件，还原股价变动的真实原因。",
        "pa_hint":        "点击 K 线图上任意一根蜡烛，选择开始日期。系统将分析从该日到今日的走势，并搜寻期间实际发生的新闻与事件。",
        "pa_start":       "开始",
        "pa_end":         "结束",
        "pa_today":       "今日",
        "pa_analyze_btn": "分析此时段",
        "pa_view_btn":    "查看报告",
        "pa_analyzing":   "分析中…",
        "pa_events_title":"关键事件",

        # K 线图
        "chart_empty":        "尚无价格数据",
        "chart_empty_sub":    "请先用 Data Fetcher 同步此股票的 OHLC 数据",
        "chart_hide_events":  "隐藏事件标记",
        "chart_show_events":  "显示事件标记",

        # 卡片 CTA
        "card_cta":       "点击查看完整报告",
        "card_cta_short": "查看报告 →",

        # 错误页面
        "error_title":        "无效的股票代码",
        "error_unrecognized": "系统无法识别股票代码",
        "error_hint":         "请确认代码是否正确，例如：AAPL、TSLA、NVDA、0700.HK",
        "error_search_placeholder": "重新输入股票代码...",
        "error_back_default": "← 返回默认标的",

        # IPO 追踪
        "ipo_page_title": "IPO 追踪",
        "ipo_subscribing": "正在招股",
        "ipo_listed": "半新股",
        "ipo_empty": "目前没有正在招股的 IPO",
        "ipo_offer_price": "招股价",
        "ipo_lot_size": "每手股数",
        "ipo_entry_fee": "入场费",
        "ipo_close_date": "截止日",
        "ipo_listing_date": "上市日",
        "ipo_listing_price": "上市价",
        "ipo_oversubscription": "超额倍数",
        "ipo_one_lot": "稳中一手",
        "ipo_ballot_rate": "中签率",
        "ipo_first_day": "首日表现",
        "ipo_sponsors": "保荐人",
        "ipo_underwriters": "承销商",
        "ipo_cornerstone_pct": "基石投资者占比",
        "ipo_cornerstone_investors": "基石投资者",
        "ipo_lockup_date": "基石投资禁售截止日",
        "ipo_detail_toggle": "招股详情",
        "ipo_is_ah_stock": "A+H 股",
        "ipo_margin_ratio": "认购倍数",
        "ipo_one_hand_win_rate": "1手中签率",
        "ipo_score": "评分",
        "ipo_report": "报告",
        "ipo_name_code": "名称 / 代号",
        "ipo_view": "查看",
        "ipo_section_biz": "商业模式分析",
        "ipo_section_finance": "财务分析",
        "ipo_section_mgmt": "管理层评估",
        "ipo_section_market": "市场分析",
        "ipo_calendar": "IPO 日历",
        "ipo_cal_close": "截止",
        "ipo_cal_listing": "上市",
        "ipo_cal_mon": "一",
        "ipo_cal_tue": "二",
        "ipo_cal_wed": "三",
        "ipo_cal_thu": "四",
        "ipo_cal_fri": "五",
        "ipo_cal_sat": "六",
        "ipo_cal_sun": "日",
        "ipo_countdown_days": "天",
        "ipo_countdown_hours": "小时",
        "ipo_countdown_mins": "分钟",
        "ipo_countdown_ended": "已截止",

        # 免责声明
        "disclaimer_title": "免责声明与风险披露",
        "disclaimer_body": (
            "本资讯仅为一般通讯，仅供提供资讯及参考之用。其性质属教育性质，并非旨在作为对任何特定投资产品、"
            "策略、计划特点或其他目的的意见、建议或推荐，亦不构成 Aureum Infinity Capital Limited（滶盈资本有限公司）"
            "参与本文所述任何交易的承诺。本资讯中所使用的任何例子均属泛化、假设性及仅供说明用途。本材料并未包含足够资讯"
            "以支持任何投资决定，阁下不应依赖本资讯来评估投资任何证券或产品的优劣。\n"
            "此外，阁下应自行独立评估任何投资在法律、监管、税务、信贷及会计方面的影响，并与阁下自身的专业顾问共同决定，"
            "本资讯所述任何投资是否适合阁下的个人目标。投资者应确保在作出任何投资前，取得所有可取得的相关资讯。\n\n"
            "本资讯所载的任何预测、数字、意见、投资技巧及策略仅供资讯用途，基于若干假设及当前市场状况，并可于无事先通知下变更。"
            "本资讯所呈现的所有内容，本公司已尽力确保于制作时准确，但并无就其准确性、完整性或及时性作出任何保证，"
            "亦不会就任何错误、遗漏或因依赖本资讯而产生的任何损失承担责任。\n\n"
            "必须注意，投资涉及风险，投资价值及来自投资的收入可能会因市场状况及税务协议而波动，"
            "投资者可能无法取回全部投资本金。过往表现及收益率并非当前及未来结果的可靠指标。\n\n"
            "本内容并非针对任何特定司法管辖区的投资者而提供，不同司法管辖区的投资者应自行确保使用本内容符合当地法例及规定。"
            "本公司保留随时修改、更新或撤回本内容的权利，而毋须事先通知。"
        ),
        "copyright": "© 4M Strategies Limited  // 版权所有",

        # 新闻投资雷达
        "nav_radar":            "新闻雷达",
        "futunn_title":         "富途新闻流",
        "futunn_subtitle":      "只读 Futu 缓存 JSON，把焦点放在富途来源的新闻标题、摘要与相关股票。",
        "futunn_live_count":    "当前文章",
        "futunn_updated":       "缓存更新",
        "futunn_cache_label":   "数据来源",
        "futunn_categories":    "分类",
        "futunn_all":           "全部",
        "futunn_cache_info":    "Futunn Cache",
        "futunn_cache_desc":    "此页只读本地 JSON，不会在开页时即时抓取网站。",
        "futunn_missing":       "目前没有可显示的 Futu 缓存内容。",
        "futunn_empty":         "请先准备 futunn_cache.json 后再重新整理此页。",
        "futunn_open_source":   "查看原文",
        "futunn_origin_site":   "来源网站",
        "futunn_related":       "更多同类新闻",
        "futunn_related_stocks":"可能受影响的股票",
        "radar_title":          "新闻投资雷达",
        "radar_subtitle":       "输入宏观事件，AI 分析多情境投资机会",
        "radar_placeholder":    "输入新闻事件、地缘政治、宏观话题…",
        "radar_btn":            "分析",
        "radar_trending":       "热门话题",
        "radar_scenario_a":     "情况好转",
        "radar_scenario_b":     "情况恶化",
        "radar_sectors":        "受益板块",
        "radar_picks":          "推荐持股",
        "radar_report":         "完整分析报告",
        "radar_report_hide":    "收起报告",
        "radar_reanalyze":      "重新分析",
        "radar_analyzing":      "正在分析…",
        "radar_loading_1":      "正在搜索最新新闻…",
        "radar_loading_2":      "分析投资逻辑…",
        "radar_loading_3":      "整理投资建议…",
        "radar_error":          "分析失败，请稍后再试",
        "radar_empty":          "请输入一个宏观事件或话题开始分析",
        "radar_impact_score":   "事件影响力",
        "radar_timeline":       "影响时间轴",
        "radar_risk":           "主要风险",
        "radar_click_stock":    "点击代码查看股票分析",
        "radar_topics": [
            "伊朗美国冲突", "AI芯片禁令", "美国关税战", "能源危机",
            "联储局政策", "供应链重组", "中美科技战", "美元走强",
        ],
    },

    "en": {
        # Navigation
        "nav_stock": "Stock Analysis",
        "nav_trending": "Attention Desk",
        "nav_futunn": "Futu Feed",
        "nav_ipo": "IPO Tracker",
        "nav_collapse": "Collapse sidebar",
        "nav_coming_soon": "Coming soon",
        "nav_more": "More",
        "nav_lang_setting": "Language",
        "nav_theme_dark": "Dark Mode",
        "nav_theme_light": "Light Mode",
        "nav_theme_setting": "Appearance",
        "nav_about": "About DataLab",
        "nav_terms": "Terms of Use",
        "nav_feedback": "Feedback",
        "nav_version": "Dr. K Station v1.0",

        # Header
        "search_placeholder": "Ticker or company name...",
        "search_btn": "Search",
        "lang_label": "Language",

        # Page title
        "terminal_title": "Investment Decision Terminal",

        # Fundamental analysis
        "report_date_prefix": "Updated: ",
        "fundamental_title": "Value Diagnostics",
        "fundamental_label": "Fundamentals",
        "fundamental_tooltip": "Dissects intrinsic value across four core dimensions: competitive moats and business model durability, earnings quality and cash flow health from financial statements, management governance track record and shareholder returns, and forward-looking growth signals and risk factors from earnings calls.",
        "card_biz":    "Business Model",  "icon_biz":    "Strategy",
        "card_exec":   "Governance",      "icon_exec":   "Management",
        "card_finance":"Financial Quality","icon_finance":"Financials",
        "card_call":   "Earnings Outlook","icon_call":   "Outlook",

        # Technical analysis
        "technical_title": "Momentum Diagnostics",
        "technical_label": "Market Dynamics",
        "technical_tooltip": "Dissects short-term momentum across three market dimensions: candlestick patterns and moving average trend signals, Wall Street analyst price target consensus and rating changes, and retail sentiment indicators and social media narrative direction.",
        "card_ta_price":   "Price Action",     "icon_ta_price":   "Technical",
        "card_ta_analyst": "Institutional Holdings",  "icon_ta_analyst": "13F",
        "card_ta_social":  "Social Sentiment", "icon_ta_social":  "Sentiment",

        # JS dynamic messages
        "loading_msgs": ["Collecting data...", "Analyzing...", "Almost done!", "Results coming right up!"],
        "confirm_reanalyze": "Re-analyze this section?\n\n⚠️ This will generate a fresh analysis.",
        "updating": "Updating...",
        "updated":  "✅ Updated",
        "no_data":  "No data available",
        "copied":       "Copied to clipboard",
        "copy_manual":  "Please copy manually: ",
        "share_text":   "View {ticker} {title} analysis",
        "cache_label":  "Cached Data",
        "fresh_label":  "Live Analysis",
        "init_msg":     "Initializing...",
        "standby_msg":  "Standby...",
        "smart_terminal": "Smart Terminal",

        # Popup window button titles
        "btn_update":   "Update Report",
        "etf_holders_title": "ETF Holders",
        "etf_holders_empty": "No ETF holding data",
        "etf_col_symbol": "ETF",
        "etf_col_weight": "Weight",
        "etf_col_aum": "AUM",
        "btn_minimize": "Minimize",
        "btn_maximize": "Maximize",
        "btn_close":    "Close",
        "btn_scroll_top": "Back to top",
        "btn_share":    "Share",

        # Rating panel
        "rating_title": "Overall Rating",
        "rating_score": "Composite Score",
        "rating_analyzing": "Analyzing",
        "progress_label": "Progress",
        "rating_verdicts": {
            "A+": "Exceptional across all dimensions, highly promising",
            "A":  "Outstanding overall, clear investment merit",
            "A-": "Strong in most areas, worth close attention",
            "B+": "Solid performance, room for improvement in some areas",
            "B":  "Average, potential risks to monitor",
            "B-": "Below average, careful evaluation advised",
            "C+": "Weak in several areas, elevated risk",
            "C":  "Multiple weaknesses, caution recommended",
            "D":  "Poor overall assessment, significant risks"
        },
        "rating_best": "Strongest",
        "rating_worst": "Watch",

        # Key Metrics
        "metric_price": "Price",
        "metric_mcap":  "Mkt Cap",
        "metric_rev":   "Revenue",
        "metric_rev_yoy": "Rev YoY",
        "metric_gm":    "Gross Margin",
        "metric_nm":    "Net Margin",
        "metric_de":    "D/E",
        "metric_dy":    "Div Yield",
        "metric_dps":   "DPS",
        "tip_price":    "The most recent closing price. Reflects the market's consensus on what the company is worth right now.",
        "tip_mcap":     "Share price × total shares outstanding. Represents the cost to buy the entire company. Large-cap stocks (> $10B) tend to be more stable.",
        "tip_pe":       "Price ÷ Earnings Per Share. Shows how much investors pay for each $1 of profit. Best compared within the same industry. A typical range is 15–25x.",
        "tip_peg":      "P/E ratio ÷ earnings growth rate. Adjusts valuation for growth. Below 1 may signal undervaluation, 1–2 is fair, above 2 may be expensive.",
        "tip_eps":      "Net income ÷ shares outstanding. How much profit each share earns. Consistently growing EPS is a key driver of stock price appreciation.",
        "tip_rev":      "Total sales for the most recent quarter. Revenue is the most direct measure of business growth — watch for consistent trends.",
        "tip_rev_yoy":  "Revenue change vs. the same quarter last year. Positive = business expanding, negative = potential headwinds.",
        "tip_gm":       "(Revenue − Cost of Goods Sold) ÷ Revenue. Measures core profitability. Higher = stronger pricing power or better cost control. Software typically > 70%, retail around 20–30%.",
        "tip_nm":       "Net income ÷ Revenue. The bottom line — how much of each $1 in sales becomes actual profit after all costs and taxes. Improving margins signal better efficiency.",
        "tip_de":       "Total debt ÷ shareholders' equity. Measures reliance on borrowed money. Below 1 = mostly self-funded, above 2 = heavily leveraged. Varies widely by industry — financials tend to be higher.",
        "tip_dy":       "Annual dividends ÷ share price. Your cash return for each $1 invested. Stable dividends suggest financial strength, but unusually high yields may signal a falling stock price.",
        "tip_dps":      "Total dividends paid per share over the past 12 months. Stable or growing DPS is a sign of financial health.",
        "metrics_more": "More Metrics",

        # Period Analysis
        "pa_title":       "Period Analysis",
        "pa_tooltip":     "Select any time period for a deep dive into price action patterns, volume anomalies, key support and resistance breakouts, cross-referenced with actual events including earnings releases, analyst rating changes, and industry policy shifts to uncover the real drivers behind price movement.",
        "pa_hint":        "Click any candle on the chart to select a start date. The system will analyze price movement and search for real news events during the period.",
        "pa_start":       "Start",
        "pa_end":         "End",
        "pa_today":       "Today",
        "pa_analyze_btn": "Analyze Period",
        "pa_view_btn":    "View Report",
        "pa_analyzing":   "Analyzing…",
        "pa_events_title":"Key Events",

        # Chart
        "chart_empty":        "No price data available",
        "chart_empty_sub":    "Please sync OHLC data with Data Fetcher first",
        "chart_hide_events":  "Hide event markers",
        "chart_show_events":  "Show event markers",

        # Card CTA
        "card_cta":       "Click to view full report",
        "card_cta_short": "View report →",

        # Error page
        "error_title":        "Invalid Ticker Symbol",
        "error_unrecognized": "The system could not recognize ticker",
        "error_hint":         "Please verify the ticker is correct, e.g. AAPL, TSLA, NVDA, 0700.HK",
        "error_search_placeholder": "Enter ticker symbol...",
        "error_back_default": "← Back to default",

        # IPO Tracker
        "ipo_page_title": "IPO Tracker",
        "ipo_subscribing": "Subscribing",
        "ipo_listed": "Newly Listed",
        "ipo_empty": "No IPOs currently open for subscription",
        "ipo_offer_price": "Offer Price",
        "ipo_lot_size": "Lot Size",
        "ipo_entry_fee": "Entry Fee",
        "ipo_close_date": "Close Date",
        "ipo_listing_date": "Listing Date",
        "ipo_listing_price": "Listing Price",
        "ipo_oversubscription": "Oversubscription",
        "ipo_one_lot": "1-Lot Chance",
        "ipo_ballot_rate": "Ballot Rate",
        "ipo_first_day": "Day 1 Perf.",
        "ipo_sponsors": "Sponsors",
        "ipo_underwriters": "Underwriters",
        "ipo_cornerstone_pct": "Cornerstone %",
        "ipo_cornerstone_investors": "Cornerstone Investors",
        "ipo_lockup_date": "Cornerstone Lock-up Expiry",
        "ipo_detail_toggle": "IPO Details",
        "ipo_is_ah_stock": "A+H Stock",
        "ipo_margin_ratio": "Subscription Multiple",
        "ipo_one_hand_win_rate": "1-Lot Win Rate",
        "ipo_score": "Score",
        "ipo_report": "Report",
        "ipo_name_code": "Name / Code",
        "ipo_view": "View",
        "ipo_section_biz": "Business Model",
        "ipo_section_finance": "Financial Analysis",
        "ipo_section_mgmt": "Management",
        "ipo_section_market": "Market Analysis",
        "ipo_calendar": "IPO Calendar",
        "ipo_cal_close": "Close",
        "ipo_cal_listing": "List",
        "ipo_cal_mon": "Mon",
        "ipo_cal_tue": "Tue",
        "ipo_cal_wed": "Wed",
        "ipo_cal_thu": "Thu",
        "ipo_cal_fri": "Fri",
        "ipo_cal_sat": "Sat",
        "ipo_cal_sun": "Sun",
        "ipo_countdown_days": "d",
        "ipo_countdown_hours": "h",
        "ipo_countdown_mins": "m",
        "ipo_countdown_ended": "Closed",

        # Disclaimer
        "disclaimer_title": "Disclaimer & Risk Disclosure",
        "disclaimer_body": (
            "This information is for general communication purposes only and is provided for informational and reference purposes. "
            "It is educational in nature and is not intended as advice, recommendation, or solicitation regarding any specific investment "
            "product, strategy, or plan feature, nor does it constitute a commitment by Aureum Infinity Capital Limited to engage in any "
            "transaction described herein. Any examples used are generic, hypothetical, and for illustrative purposes only. "
            "This material does not contain sufficient information to support an investment decision, and you should not rely on it to "
            "evaluate the merits of investing in any securities or products.\n\n"
            "Additionally, you should independently assess the legal, regulatory, tax, credit, and accounting implications of any "
            "investment, and together with your own professional advisors, determine whether any investment described herein is suitable "
            "for your personal objectives. Investors should ensure that they obtain all relevant information available before making "
            "any investment.\n\n"
            "Any forecasts, figures, opinions, investment techniques, and strategies set out in this information are for information "
            "purposes only, based on certain assumptions and current market conditions, and are subject to change without notice. "
            "All information presented has been prepared with care to ensure accuracy at the time of publication, but no warranty is "
            "given as to accuracy, completeness or timeliness, and there should be no reliance on it in connection with any investment "
            "decision.\n\n"
            "It must be noted that investment involves risk. The value of investments and the income from them may fluctuate in "
            "accordance with market conditions and taxation agreements, and investors may not get back the full amount invested. "
            "Past performance and yield are not a reliable indicator of current and future results.\n\n"
            "This content is not directed to investors in any particular jurisdiction. Investors in different jurisdictions should "
            "ensure that their use of this content complies with local laws and regulations. The Company reserves the right to modify, "
            "update or withdraw this content at any time without notice."
        ),
        "copyright": "© 4M Strategies Limited  // All Rights Reserved",

        # News Investment Radar
        "nav_radar":            "News Radar",
        "futunn_title":         "Futu News Feed",
        "futunn_subtitle":      "Reads the cached Futu JSON feed and keeps the page focused on titles, summaries, and related stocks from that source only.",
        "futunn_live_count":    "Articles",
        "futunn_updated":       "Cache Updated",
        "futunn_cache_label":   "Source",
        "futunn_categories":    "Categories",
        "futunn_all":           "All",
        "futunn_cache_info":    "Futunn Cache",
        "futunn_cache_desc":    "This page only reads cached JSON and does not scrape Futu at request time.",
        "futunn_missing":       "No Futu cache content is currently available.",
        "futunn_empty":         "Prepare futunn_cache.json and reload this page.",
        "futunn_open_source":   "Open Original",
        "futunn_origin_site":   "Source Site",
        "futunn_related":       "More In This Topic",
        "futunn_related_stocks":"Potentially Related Stocks",
        "radar_title":          "News Investment Radar",
        "radar_subtitle":       "Enter a macro event, AI analyzes multi-scenario investment opportunities",
        "radar_placeholder":    "Enter a news event, geopolitical topic, or macro theme…",
        "radar_btn":            "Analyze",
        "radar_trending":       "Trending Topics",
        "radar_scenario_a":     "Situation Improves",
        "radar_scenario_b":     "Situation Worsens",
        "radar_sectors":        "Benefiting Sectors",
        "radar_picks":          "Stock Picks",
        "radar_report":         "Full Analysis Report",
        "radar_report_hide":    "Hide Report",
        "radar_reanalyze":      "Re-analyze",
        "radar_analyzing":      "Analyzing…",
        "radar_loading_1":      "Searching latest news…",
        "radar_loading_2":      "Analyzing investment logic…",
        "radar_loading_3":      "Compiling recommendations…",
        "radar_error":          "Analysis failed. Please try again.",
        "radar_empty":          "Enter a macro event or topic to start analysis",
        "radar_impact_score":   "Event Impact Score",
        "radar_timeline":       "Impact Timeline",
        "radar_risk":           "Key Risks",
        "radar_click_stock":    "Click ticker to view stock analysis",
        "radar_topics": [
            "Iran-US Conflict", "AI Chip Ban", "US Tariff War", "Energy Crisis",
            "Fed Policy", "Supply Chain Shift", "US-China Tech War", "USD Strength",
        ],
    },
}

SUPPORTED_LANGS = list(TRANSLATIONS.keys())
DEFAULT_LANG = "zh_hk"


def get_translations(lang: str) -> dict:
    """取得指定語言的翻譯字典，找不到時 fallback 到繁中"""
    return TRANSLATIONS.get(lang, TRANSLATIONS[DEFAULT_LANG])
