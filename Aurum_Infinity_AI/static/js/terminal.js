/* ================================================================
   terminal.js - 分析終端 JavaScript
   ================================================================
   適用於：index.html（主分析頁面）

   依賴：
     - 頁面需有 data-ticker 屬性在 <body> 上
     - DOM 元素 ID 規則：dot-{id}, preview-{id}, score-{id}
     - 手機卡片 ID 規則：m-dot-{id}, m-score-{id}, m-preview-{id}
     - index.html 內需有 id="header-chinese-name", "header-en-name"
     - index.html 內需有 id="popup-container"

   函數索引：
     1. fetchSection()      — 呼叫 API 取得分析報告
     2. openPopUp()         — 開啟彈出報告視窗
     3. toggleMinimize()    — 視窗最小化切換
     4. toggleMaximize()    — 視窗最大化切換
     5. startDrag()         — 視窗拖曳系統
     6. navigateToStock()   — Optimistic UI 切換股票
   ================================================================ */


/* ==========================================================
   全域變數
   ========================================================== */
let analysisCache  = {};   // 儲存各模組的 HTML 報告內容
let _fetchRequestId = 0;   // 競態保護：navigateToStock 時遞增，舊回應自動丟棄

// 從 <body data-ticker="NVDA"> 讀取初始股票代碼
const TICKER = document.body.dataset.ticker || '';

// 從 <body data-lang="zh_hk"> 讀取語言（可變，語言切換時更新）
let LANG = document.body.dataset.lang || 'zh_hk';

// 從 <body data-i18n='...'> 讀取翻譯字典（可變）
let I18N = (function() {
    try {
        return JSON.parse(document.body.dataset.i18n || '{}');
    } catch(e) {
        return {};
    }
})();
window.I18N = I18N;

// 取得當前 ticker（切換後用 _optimisticTicker 覆蓋）
function getCurrentTicker() {
    return window._optimisticTicker || TICKER;
}

/**
 * 限制並發數的批量執行器
 * @param {Array} items - 要處理的項目
 * @param {Function} fn - 處理函數（回傳 Promise）
 * @param {number} concurrency - 最大並發數
 */
async function _runWithConcurrency(items, fn, concurrency) {
    const queue = [...items];
    const workers = Array.from({ length: Math.min(concurrency, queue.length) }, async () => {
        while (queue.length > 0) {
            const item = queue.shift();
            await fn(item);
        }
    });
    await Promise.all(workers);
}

// HTML 跳脫 — 已移至 utils.js（escHtml, escAttr, sanitizeHtml）

// 所有分析模組 ID
const ALL_SECTIONS = ['biz', 'finance', 'exec', 'call', 'ta_price', 'ta_analyst', 'ta_social'];

// 分批並發：基本面 4 個同時 → 技術面 3 個同時
const _SECTION_BATCHES = [
    ['biz', 'finance', 'exec', 'call'],        // 價值透析
    ['ta_price', 'ta_analyst', 'ta_social'],    // 動能透析
];

// 記錄需要重試的 section
let _retrySections = [];

async function _analyzeAllSections(fn) {
    _retrySections = [];
    for (const batch of _SECTION_BATCHES) {
        await Promise.all(batch.map(id => fn(id)));
    }
    // 失敗的 section 延遲 3 秒後自動重試一次
    if (_retrySections.length > 0) {
        const toRetry = [..._retrySections];
        _retrySections = [];
        console.log('[Retry] Retrying failed sections:', toRetry);
        await new Promise(r => setTimeout(r, 3000));
        await Promise.all(toRetry.map(id => fn(id)));
    }
}

// 各 section 的評分 + 摘要（用於綜合評級）
let _sectionScores = {};
let _sectionSummaries = {};
let _sectionDates = {};
let _completedSections = 0;

function _updateBatchDates() {
    var today = new Date().toLocaleDateString('zh-TW', {year:'numeric',month:'2-digit',day:'2-digit'}).replace(/-/g,'/');
    var t = window._currentTranslations || {};
    var datePrefix = t.report_date_prefix || '更新：';
    // 價值透析 — oldest of biz/finance/exec/call
    var fundDates = ['biz','finance','exec','call'].map(function(s){ return _sectionDates[s]; }).filter(Boolean);
    var fundEl = document.getElementById('fundamental-date');
    if (fundEl && fundDates.length > 0) {
        fundDates.sort();
        fundEl.textContent = datePrefix + fundDates[0];
    } else if (fundEl && _completedSections > 0) {
        var hasFund = ['biz','finance','exec','call'].some(function(s){ return analysisCache[s]; });
        if (hasFund) fundEl.textContent = datePrefix + today;
    }
    // 動能透析 — oldest of ta_price/ta_analyst/ta_social
    var techDates = ['ta_price','ta_analyst','ta_social'].map(function(s){ return _sectionDates[s]; }).filter(Boolean);
    var techEl = document.getElementById('technical-date');
    if (techEl && techDates.length > 0) {
        techDates.sort();
        techEl.textContent = datePrefix + techDates[0];
    } else if (techEl && _completedSections > 0) {
        var hasTech = ['ta_price','ta_analyst','ta_social'].some(function(s){ return analysisCache[s]; });
        if (hasTech) techEl.textContent = datePrefix + today;
    }
}

// Section ID → 翻譯 key 對應
const SECTION_NAMES = {
    'biz': 'card_biz', 'finance': 'card_finance', 'exec': 'card_exec', 'call': 'card_call',
    'ta_price': 'card_ta_price', 'ta_analyst': 'card_ta_analyst', 'ta_social': 'card_ta_social'
};


/* ==========================================================
   工具函數：從 HTML 報告中提取綜合評分
   ========================================================== */
function extractCompositeScore(htmlReport) {
    /**
     * 提取評分表的綜合評分（最後一行，通常是 **綜合評分** 或 **加權綜合評分**）
     *
     * 改進版本支援多種格式：
     * - 第一列包含關鍵詞，分數在第 2 或第 3 列
     * - 分數可以在任何單元格中
     */

    try {
        const temp = document.createElement('div');
        temp.innerHTML = sanitizeHtml(htmlReport);

        // 尋找所有表格
        const tables = temp.querySelectorAll('table');

        if (tables.length === 0) {
            console.warn('[Score] No tables found in report');
            return null;
        }

        // 掃描每個表格
        for (let table of tables) {
            const rows = table.querySelectorAll('tr');

            // 逆向掃描，從最後一行開始
            for (let i = rows.length - 1; i >= 0; i--) {
                const row = rows[i];
                const cells = row.querySelectorAll('td, th');

                if (cells.length < 2) continue;

                // 獲取每個單元格的文本
                const cellTexts = Array.from(cells).map(c => c.textContent.trim());
                const firstCell = cellTexts[0];

                // 檢查是否是綜合評分行（支援多種語言與變體）
                const compositeScoreKeywords = [
                    // 繁體中文
                    '綜合評分', '加權', '綜合情緒',
                    // 簡體中文
                    '综合评分', '综合情绪',
                    // 英文
                    'composite', 'overall', 'combined', 'weighted'
                ];

                const isCompositeScoreLine = compositeScoreKeywords.some(keyword =>
                    firstCell.toLowerCase().includes(keyword.toLowerCase())
                );

                if (isCompositeScoreLine) {
                    // 嘗試從所有單元格中提取分數
                    for (let j = 1; j < cellTexts.length; j++) {
                        const cellText = cellTexts[j];
                        const scoreMatch = cellText.match(/\d+(?:\.\d+)?/);

                        if (scoreMatch) {
                            const score = parseFloat(scoreMatch[0]);
                            if (score >= 1 && score <= 10) {
                                // console.log(`[Score] Found: ${score} from cell ${j}`);
                                return Math.round(score * 10) / 10;
                            }
                        }
                    }
                }
            }
        }

        console.warn('[Score] No composite score found in tables');
        return null;

    } catch (error) {
        console.error('[Score] Error extracting score:', error);
        return null;
    }
}


/* ==========================================================
   工具函數：同步手機列表卡片
   接收已提取的摘要和分數，避免重複解析 HTML
   ========================================================== */
function _syncMobileCard(sectionId, summaryText, score) {
    const mScore   = document.getElementById(`m-score-${sectionId}`);
    const mPreview = document.getElementById(`m-preview-${sectionId}`);
    const mDot     = document.getElementById(`m-dot-${sectionId}`);

    if (mPreview && summaryText) {
        mPreview.innerText = summaryText;
        mPreview.style.fontStyle = 'normal';
        mPreview.style.color = '#666';
    }
    if (mScore && score !== null) {
        mScore.textContent = score;
        mScore.classList.remove('no-score');
    }
    if (mDot) {
        mDot.className = 'w-2 h-2 rotate-45 bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.6)]';
    }
}

/* ==========================================================
   工具函數：從 HTML 報告中提取卡片摘要
   ========================================================== */
function extractCardSummary(htmlReport) {
    /**
     * 優先從 <card-summary> 標籤提取 AI 生成摘要，
     * fallback 到純文字截取（最多 80 字）
     */
    try {
        const match = htmlReport.match(/<card-summary>([\s\S]*?)<\/card-summary>/i);
        if (match) {
            const summary = match[1].trim();
            return summary.substring(0, 80);
        }
        // Fallback：從純文字截取
        const temp = document.createElement('div');
        temp.innerHTML = sanitizeHtml(htmlReport);
        const text = temp.innerText.trim();
        if (text && text.length > 20) {
            const truncated = text.substring(0, 80);
            return truncated.length < text.length ? truncated + '…' : truncated;
        }
        return null;
    } catch (error) {
        console.error('[Summary] Error:', error);
        return null;
    }
}


/* ==========================================================
   工具函數：更新綜合評級面板
   ========================================================== */
function _updateRatingPanel() {
    var ratingPanel = document.getElementById('rating-panel');
    var ratingResult = document.getElementById('rating-result');
    if (!ratingPanel) return;

    // 全部完成 → 計算評級
    if (_completedSections >= ALL_SECTIONS.length && ratingResult) {
        var scores = [];
        var scoreMap = {};
        for (var key in _sectionScores) {
            if (_sectionScores[key] !== null) {
                scores.push(_sectionScores[key]);
                scoreMap[key] = _sectionScores[key];
            }
        }

        if (scores.length === 0) {
            ratingPanel.style.display = 'none';
            return;
        }

        var avg = scores.reduce(function(a, b) { return a + b; }, 0) / scores.length;
        avg = Math.round(avg * 10) / 10;

        // 確定等級
        var grade, gradeClass;
        if      (avg >= 8.5) { grade = 'A+'; gradeClass = 'grade-a'; }
        else if (avg >= 7.5) { grade = 'A';  gradeClass = 'grade-a'; }
        else if (avg >= 7.0) { grade = 'A-'; gradeClass = 'grade-a'; }
        else if (avg >= 6.5) { grade = 'B+'; gradeClass = 'grade-b'; }
        else if (avg >= 5.5) { grade = 'B';  gradeClass = 'grade-b'; }
        else if (avg >= 5.0) { grade = 'B-'; gradeClass = 'grade-b'; }
        else if (avg >= 4.0) { grade = 'C+'; gradeClass = 'grade-c'; }
        else if (avg >= 3.0) { grade = 'C';  gradeClass = 'grade-c'; }
        else                 { grade = 'D';  gradeClass = 'grade-d'; }

        // 更新 DOM
        var gradeEl = document.getElementById('rating-grade');
        var scoreVal = document.getElementById('rating-score-value');
        var verdictEl = document.getElementById('rating-verdict');

        if (gradeEl) {
            gradeEl.textContent = grade;
            gradeEl.className = 'rating-grade ' + gradeClass;
        }
        if (scoreVal) scoreVal.textContent = avg.toFixed(1) + ' / 10';

        // 先顯示模板判定語（即時），等 AI 覆蓋
        var verdicts = (I18N.rating_verdicts || {});
        if (verdictEl) verdictEl.textContent = verdicts[grade] || '';

        ratingResult.classList.remove('hidden');

        // 呼叫 AI 生成詳細分析判定語（非阻塞）
        _fetchAiVerdict(scoreMap, verdictEl);
    }
}


/**
 * 呼叫後端 AI 生成詳細判定語，附帶 section 摘要讓 AI 解釋原因
 */
function _fetchAiVerdict(scoreMap, verdictEl) {
    if (!verdictEl) return;
    verdictEl.classList.add('verdict-loading');

    // 競態保護：記錄發起時的 requestId
    var myRequestId = _fetchRequestId;

    // 組裝 section 名稱 + 分數 + 摘要
    var summaries = {};
    for (var k in scoreMap) {
        var name = I18N[SECTION_NAMES[k]] || k;
        summaries[k] = {
            name: name,
            score: scoreMap[k],
            summary: _sectionSummaries[k] || ''
        };
    }

    fetch('/api/rating_verdict', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            ticker: getCurrentTicker(),
            scores: scoreMap,
            summaries: summaries,
            lang: LANG
        })
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (myRequestId !== _fetchRequestId) return; // 已切換股票，丟棄
        verdictEl.classList.remove('verdict-loading');
        if (data.success && data.verdict) {
            verdictEl.textContent = data.verdict;
        }
    })
    .catch(function() {
        if (myRequestId !== _fetchRequestId) return;
        verdictEl.classList.remove('verdict-loading');
    });
}

function _resetRatingPanel() {
    _sectionScores = {};
    _sectionSummaries = {};
    _sectionDates = {};
    _completedSections = 0;
    var fundDateEl = document.getElementById('fundamental-date');
    var techDateEl = document.getElementById('technical-date');
    if (fundDateEl) fundDateEl.textContent = '';
    if (techDateEl) techDateEl.textContent = '';
    var ratingPanel = document.getElementById('rating-panel');
    var ratingResult = document.getElementById('rating-result');
    if (ratingPanel) ratingPanel.style.display = '';
    if (ratingResult) ratingResult.classList.add('hidden');
}


/* ==========================================================
   頁面載入：自動觸發全部分析模組
   ========================================================== */
window.onload = function () {
    _analyzeAllSections(id => fetchSection(id));
    initOhlcChart();
    loadKeyMetrics();
};


/* ==========================================================
   1. fetchSection — 呼叫後端 API 取得分析報告
   ----------------------------------------------------------
   參數：
     sectionId   (str)  : 分析模組 ID，如 'biz', 'finance'
     forceUpdate (bool) : 是否強制重新生成（略過快取）

   流程：
     1. 顯示載入動畫（金色脈衝）
     2. POST /analyze/<sectionId>
     3. 成功 → 預覽文字 + 「開啟報告」+ 「重新分析」
     4. 失敗 → 錯誤訊息 + 紅色指示燈
   ========================================================== */
async function fetchSection(sectionId, forceUpdate = false) {
    const dot     = document.getElementById(`dot-${sectionId}`);
    const preview = document.getElementById(`preview-${sectionId}`);
    const scoreEl = document.getElementById(`score-${sectionId}`);

    // DOM 元素不存在時跳過
    if (!dot || !preview) return;

    // 記錄此次請求的 ID，用於競態保護
    const myRequestId = _fetchRequestId;

    // 進入載入狀態
    dot.className = 'loading-pulse';
    if (scoreEl) scoreEl.classList.add('hidden');

    // 同步手機列表卡片載入狀態
    const mDotL   = document.getElementById(`m-dot-${sectionId}`);
    const mScoreL = document.getElementById(`m-score-${sectionId}`);
    if (mDotL)   mDotL.className = 'loading-pulse';
    if (mScoreL) { mScoreL.textContent = '···'; mScoreL.classList.add('no-score'); }

    // 動態載入訊息（每 3 秒切換）
    const loadingMessages = I18N.loading_msgs || ['Loading...', 'Analyzing...', 'Almost done!', 'Coming right up!'];
    let msgIndex = 0;
    preview.textContent = loadingMessages[0];
    const msgTimer = setInterval(() => {
        if (msgIndex < loadingMessages.length - 1) {
            msgIndex++;
            preview.textContent = loadingMessages[msgIndex];
        }
    }, 3000);

    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 120000);
        const response = await fetch(`/analyze/${sectionId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                ticker: getCurrentTicker(),
                force_update: forceUpdate,
                lang: LANG
            }),
            signal: controller.signal,
        });
        clearTimeout(timeoutId);
        const data = await response.json();
        clearInterval(msgTimer);

        // 競態保護：若期間已切換股票，丟棄此舊回應
        if (myRequestId !== _fetchRequestId) return;

        if (data.success) {
            // ✅ 成功
            analysisCache[sectionId] = data.report;
            if (data.cache_date) _sectionDates[sectionId] = data.cache_date;
            _updateBatchDates();

            // 摘要：優先用後端回傳的 summary，其次從 HTML 提取，最後 fallback 截取
            const cardSummary = data.summary || extractCardSummary(data.report);
            if (cardSummary) {
                preview.innerText = cardSummary;
            } else {
                const temp = document.createElement('div');
                temp.innerHTML = sanitizeHtml(data.report);
                preview.innerText = temp.innerText.substring(0, 120);
            }
            preview.classList.remove('italic', 'text-gray-400');
            preview.classList.add('text-gray-600');

            // 提取評分（只提取一次，桌面 + 手機共用）
            const extractedScore = extractCompositeScore(data.report);
            dot.className = 'w-2 h-2 rotate-45 bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.6)]';

            if (scoreEl) {
                if (extractedScore !== null) {
                    scoreEl.textContent = extractedScore;
                    scoreEl.classList.remove('hidden');
                } else {
                    scoreEl.classList.add('hidden');
                }
            }

            // 同步手機列表卡片（傳入已提取的分數，避免重複解析）
            _syncMobileCard(sectionId, cardSummary, extractedScore);

            // 顯示「查看報告」CTA
            var cta = document.getElementById('cta-' + sectionId);
            if (cta) cta.classList.add('ready');
            var mCta = document.getElementById('m-cta-' + sectionId);
            if (mCta) mCta.classList.add('ready');

            // 更新評級面板進度
            _sectionScores[sectionId] = extractedScore;
            _sectionSummaries[sectionId] = cardSummary || '';
            _completedSections++;
            _updateRatingPanel();

        } else {
            // ❌ API 回傳失敗
            preview.textContent = data.error || 'Analysis failed, please retry';
            dot.className = 'w-2 h-2 rotate-45 bg-red-500';
            _sectionScores[sectionId] = null;
            _completedSections++;
            _updateRatingPanel();
            // 伺服器錯誤（非用戶端問題）→ 標記重試
            if (response.status >= 500) {
                _retrySections.push(sectionId);
            }
        }

    } catch (e) {
        clearInterval(msgTimer);
        if (myRequestId !== _fetchRequestId) return;
        var errMsg = (e.name === 'AbortError')
            ? (I18N.error_timeout || '請求逾時，請稍後重試')
            : 'Connection error, please retry';
        preview.textContent = errMsg;
        dot.className = 'w-2 h-2 rotate-45 bg-red-500';
        _sectionScores[sectionId] = null;
        _completedSections++;
        _updateRatingPanel();
        // 標記為需要重試（network / timeout 錯誤）
        _retrySections.push(sectionId);
    }
}


/* ==========================================================
   2. updateSection — 強制重新分析（透過 openPopUp 內的按鈕呼叫）
   ========================================================== */
// Section group definitions for batch refresh
const SECTION_GROUPS = {
    fundamental: ['biz', 'exec', 'finance', 'call'],
    technical:   ['ta_price', 'ta_analyst', 'ta_social'],
};

async function updateGroup(groupName) {
    const group = SECTION_GROUPS[groupName];
    if (!group) return;
    if (!confirm((I18N.confirm_reanalyze || 'Re-analyze?') + ` (${group.length} sections)\n\n⚠️ This will call the AI API.`)) return;

    // Fetch all sections in group in parallel
    await Promise.all(group.map(id => fetchSection(id, true)));
}


/* ==========================================================
   3. openPopUp — 開啟彈出報告視窗
   ----------------------------------------------------------
   桌面（sm+）：拖曳視窗
   手機（< sm）：DaisyUI modal（底部滑入）
   ========================================================== */
function openPopUp(id, title) {
    const isMobile = window.matchMedia('(max-width: 640px)').matches;
    const content  = analysisCache[id] || `<p style="color:#999;">${I18N.no_data || 'No data available'}</p>`;

    if (isMobile) {
        _openMobileModal(id, title, content);
    } else {
        _openDesktopWindow(id, title, content);
    }
}

/* ── 手機：DaisyUI modal ── */
function _openMobileModal(id, title, content) {
    const modal       = document.getElementById('mobile-modal');
    const modalTitle  = document.getElementById('mobile-modal-title');
    const modalTag    = document.getElementById('mobile-modal-tag');
    const modalBody   = document.getElementById('mobile-modal-content');
    const progressFill = document.getElementById('mobile-progress-fill');
    const floatingPanel = document.getElementById('mobile-floating-panel');
    const scrollTopBtn  = document.getElementById('mobile-scroll-top');
    const shareBtn      = document.getElementById('mobile-share-btn');
    const windowBody    = document.getElementById('mobile-modal-body');

    modalTitle.textContent = title;
    modalTag.textContent   = `// ${getCurrentTicker()} ${I18N.smart_terminal || 'AI Terminal'}`;
    modalBody.innerHTML    = sanitizeHtml(content);

    // 包 table-wrapper + pie chart
    _enhanceTables(modalBody);

    // 重置滾動 + 進度條
    windowBody.scrollTop = 0;
    progressFill.style.width = '0%';
    floatingPanel.classList.remove('visible');

    // 滾動事件
    const onScroll = () => {
        const scrolled  = windowBody.scrollTop;
        const maxScroll = windowBody.scrollHeight - windowBody.clientHeight;
        progressFill.style.width = maxScroll > 0 ? (scrolled / maxScroll * 100) + '%' : '0%';
        floatingPanel.classList.toggle('visible', scrolled > 300);
    };
    windowBody.removeEventListener('scroll', windowBody._scrollHandler);
    windowBody._scrollHandler = onScroll;
    windowBody.addEventListener('scroll', onScroll, { passive: true });

    // 回到頂部
    scrollTopBtn.onclick = () => windowBody.scrollTo({ top: 0, behavior: 'smooth' });

    // 更新報告按鈕已移至首頁 section header，popup 不再顯示
    const updateBtn = document.getElementById('mobile-update-btn');
    if (updateBtn) updateBtn.style.display = 'none';

    // 分享
    shareBtn.onclick = () => _shareReport(title);

    modal.showModal();
    document.body.style.overflow = 'hidden';
    modal.addEventListener('close', function _onClose() {
        document.body.style.overflow = '';
        modal.removeEventListener('close', _onClose);
    });
}

/* ── 桌面：拖曳視窗 ── */
function _openDesktopWindow(id, title, content) {
    const existing = document.getElementById(`win-${id}`);
    if (existing) existing.remove();

    const win = document.createElement('div');
    win.id = `win-${id}`;
    win.className = 'draggable-window';
    win.style.top  = '7.5vh';
    win.style.left = '7.5vw';

    win.innerHTML = `
        <div class="window-header" onmousedown="startDrag(event, 'win-${id}')">
            <div class="window-header-left">
                <div class="window-header-icon"></div>
                <span class="window-header-title">${escHtml(title)}</span>
                <span class="window-header-tag">// ${escHtml(getCurrentTicker())} ${escHtml(I18N.smart_terminal || 'AI Terminal')}</span>
            </div>
            <div class="window-controls">
                <button class="window-ctrl-btn btn-minimize desktop-only" onclick="toggleMinimize('win-${id}')" title="${I18N.btn_minimize || 'Minimize'}">
                    <svg viewBox="0 0 16 16"><line x1="3" y1="8" x2="13" y2="8"/></svg>
                </button>
                <div class="window-ctrl-divider desktop-only"></div>
                <button class="window-ctrl-btn btn-maximize desktop-only" onclick="toggleMaximize('win-${id}')" title="${I18N.btn_maximize || 'Maximize'}">
                    <svg viewBox="0 0 16 16"><rect x="2.5" y="2.5" width="11" height="11" rx="1"/></svg>
                </button>
                <div class="window-ctrl-divider desktop-only"></div>
                <button class="window-ctrl-btn btn-close" onclick="document.getElementById('win-${id}').remove()" title="${I18N.btn_close || 'Close'}">
                    <svg viewBox="0 0 16 16"><line x1="3" y1="3" x2="13" y2="13"/><line x1="13" y1="3" x2="3" y2="13"/></svg>
                </button>
            </div>
        </div>
        <div class="window-body">
            <div class="max-w-4xl mx-auto py-4">${content}</div>
        </div>`;

    document.getElementById('popup-container').appendChild(win);

    // 包 table-wrapper + pie chart
    _enhanceTables(win.querySelector('.window-body'));

    // 進度條
    const windowBody   = win.querySelector('.window-body');
    const windowHeader = win.querySelector('.window-header');
    const progressBar  = document.createElement('div');
    progressBar.className = 'progress-bar-track';
    const progressFill = document.createElement('div');
    progressFill.className = 'progress-bar-fill';
    progressBar.appendChild(progressFill);
    windowHeader.insertAdjacentElement('afterend', progressBar);

    windowBody.addEventListener('scroll', () => {
        const scrolled  = windowBody.scrollTop;
        const maxScroll = windowBody.scrollHeight - windowBody.clientHeight;
        progressFill.style.width = maxScroll > 0 ? (scrolled / maxScroll * 100) + '%' : '0%';
    }, { passive: true });
}

/* ── 表格增強：table-wrapper + 百分比表自動生成 Donut Chart ── */
function _enhanceTables(container) {
    if (!container) return;
    container.querySelectorAll('table').forEach(table => {
        // 已處理過就跳過
        if (table.parentElement.classList.contains('table-wrapper')) return;
        if (table.dataset.chartDone) return;

        // 1) 嘗試多指標折線圖（% 趨勢表，如利潤率）
        const lineData = _extractLineData(table);
        if (lineData) {
            const chart = _buildLineChart(lineData);
            table.parentNode.insertBefore(chart, table);
        }
        // 2) 嘗試多季度分組柱狀圖（持倉 M/B 數值表）
        else {
            const barData = _extractBarData(table);
            if (barData) {
                const chart = _buildGroupedBarChart(barData);
                table.parentNode.insertBefore(chart, table);
            } else {
                // 3) 嘗試百分比 Donut Chart（佔比%表）
                const chartData = _extractPieData(table);
                if (chartData && chartData.length >= 2) {
                    const chart = _buildDonutChart(chartData);
                    table.parentNode.insertBefore(chart, table);
                }
            }
        }

        // 包 table-wrapper
        const wrapper = document.createElement('div');
        wrapper.className = 'table-wrapper';
        table.parentNode.insertBefore(wrapper, table);
        wrapper.appendChild(table);
        table.dataset.chartDone = '1';
    });
}

/**
 * 從 table 提取百分比數據。
 * 偵測含有 % 值的欄位，配對第一欄作為標籤。
 * 回傳 [{label, value}, ...] 或 null
 */
function _extractPieData(table) {
    const rows = table.querySelectorAll('tbody tr');
    if (rows.length < 2 || rows.length > 15) return null;

    const headerCells = table.querySelectorAll('thead th');
    if (headerCells.length < 2) return null;

    // 找出含 % 的欄位 index
    let pctColIdx = -1;
    // 先掃第一行 body 找百分比欄
    const firstRowCells = rows[0].querySelectorAll('td');
    for (let c = 1; c < firstRowCells.length; c++) {
        const text = firstRowCells[c].textContent.trim();
        if (/~?\d+(\.\d+)?\s*%/.test(text)) {
            pctColIdx = c;
            break;
        }
    }
    if (pctColIdx === -1) return null;

    // 驗證大部分行都有百分比
    const data = [];
    let validCount = 0;
    rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length <= pctColIdx) return;
        const label = cells[0].textContent.trim();
        const raw   = cells[pctColIdx].textContent.trim();
        const match = raw.match(/~?(\d+(?:\.\d+)?)\s*%/);
        if (match) {
            data.push({ label, value: parseFloat(match[1]) });
            validCount++;
        }
    });

    // 至少 80% 的行要有有效數據
    if (validCount < rows.length * 0.8) return null;
    // 總和應該在 80-120% 之間（允許估算誤差）
    const total = data.reduce((s, d) => s + d.value, 0);
    if (total < 30 || total > 150) return null;

    return data;
}

/**
 * 生成 SVG Donut Chart + Legend
 */
function _buildDonutChart(data) {
    const COLORS = [
        '#1e3a5f', '#e74c3c', '#2ecc71', '#f39c12', '#9b59b6',
        '#3498db', '#e67e22', '#1abc9c', '#e84393', '#00b894',
        '#6c5ce7', '#fd79a8', '#00cec9', '#fdcb6e', '#636e72',
    ];

    const total = data.reduce((s, d) => s + d.value, 0);
    const cx = 90, cy = 90, r = 70, innerR = 42;
    let currentAngle = -90; // start from top

    // Build SVG paths
    let paths = '';
    let tooltips = '';
    data.forEach((d, i) => {
        const pct = d.value / total;
        const angle = pct * 360;
        const startAngle = currentAngle;
        const endAngle = currentAngle + angle;

        const x1 = cx + r * Math.cos(startAngle * Math.PI / 180);
        const y1 = cy + r * Math.sin(startAngle * Math.PI / 180);
        const x2 = cx + r * Math.cos(endAngle * Math.PI / 180);
        const y2 = cy + r * Math.sin(endAngle * Math.PI / 180);
        const ix1 = cx + innerR * Math.cos(endAngle * Math.PI / 180);
        const iy1 = cy + innerR * Math.sin(endAngle * Math.PI / 180);
        const ix2 = cx + innerR * Math.cos(startAngle * Math.PI / 180);
        const iy2 = cy + innerR * Math.sin(startAngle * Math.PI / 180);

        const largeArc = angle > 180 ? 1 : 0;
        const color = COLORS[i % COLORS.length];

        paths += `<path d="M${x1},${y1} A${r},${r} 0 ${largeArc},1 ${x2},${y2} L${ix1},${iy1} A${innerR},${innerR} 0 ${largeArc},0 ${ix2},${iy2} Z" fill="${color}" class="donut-segment" style="--delay:${i * 0.08}s">
            <title>${escHtml(d.label)}: ${d.value}%</title>
        </path>`;
        currentAngle = endAngle;
    });

    // Center text
    const centerText = `<text x="${cx}" y="${cy - 4}" text-anchor="middle" fill="#1e3a5f" font-size="18" font-weight="700" font-family="'JetBrains Mono',monospace">${total.toFixed(0)}%</text>
        <text x="${cx}" y="${cy + 14}" text-anchor="middle" fill="#999" font-size="9" font-family="'JetBrains Mono',monospace" letter-spacing="0.1em">TOTAL</text>`;

    // Legend items
    let legendHtml = '';
    data.forEach((d, i) => {
        const color = COLORS[i % COLORS.length];
        legendHtml += `<div class="donut-legend-item">
            <span class="donut-legend-dot" style="background:${color}"></span>
            <span class="donut-legend-label">${escHtml(d.label)}</span>
            <span class="donut-legend-value">${d.value}%</span>
        </div>`;
    });

    const wrapper = document.createElement('div');
    wrapper.className = 'donut-chart-container';
    wrapper.innerHTML = `
        <div class="donut-chart-inner">
            <svg viewBox="0 0 180 180" class="donut-svg">${paths}${centerText}</svg>
            <div class="donut-legend">${legendHtml}</div>
        </div>`;

    return wrapper;
}


/**
 * 偵測多季度持倉表（多欄含 M/B 數值）。
 * 回傳 { labels: [...], quarters: [...], series: [[...], ...] } 或 null
 */
function _extractBarData(table) {
    const headers = table.querySelectorAll('thead th');
    if (headers.length < 4) return null;

    // 找含 Q 或季度格式的欄位 (e.g. "2025-Q1", "Q1", "2025-03-31")
    const quarterCols = [];
    headers.forEach((th, i) => {
        if (i === 0) return;
        const t = th.textContent.trim();
        if (/Q\d|20\d{2}/.test(t) && !/QoQ|變動|趨勢|類型/i.test(t)) {
            quarterCols.push({ idx: i, name: t });
        }
    });
    if (quarterCols.length < 2) return null;

    const rows = table.querySelectorAll('tbody tr');
    if (rows.length < 2 || rows.length > 20) return null;

    // 驗證至少 60% 的格子含數值 (M/B/K 或純數字)
    const numPattern = /^[\d,.]+\s*[BMK]?$/i;
    let totalCells = 0, validCells = 0;
    const labels = [];
    const series = []; // series[row][col]

    rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length <= quarterCols[quarterCols.length - 1].idx) return;
        const label = cells[0].textContent.trim();
        if (!label || label === '--') return;

        const rowValues = [];
        let rowValid = false;
        quarterCols.forEach(qc => {
            const raw = cells[qc.idx].textContent.trim();
            totalCells++;
            const val = _parseNumericValue(raw);
            if (val !== null) {
                validCells++;
                rowValid = true;
            }
            rowValues.push(val);
        });

        if (rowValid) {
            labels.push(label.length > 18 ? label.substring(0, 16) + '…' : label);
            series.push(rowValues);
        }
    });

    if (validCells < totalCells * 0.5) return null;
    if (labels.length < 2) return null;

    return {
        labels,
        quarters: quarterCols.map(q => q.name),
        series,
    };
}

/** Parse values like "1.43B", "604.06M", "192.26M", "--" */
function _parseNumericValue(raw) {
    if (!raw || raw === '--' || raw === 'N/A' || raw === 'NEW') return null;
    const match = raw.match(/^([\d,.]+)\s*([BMK])?$/i);
    if (!match) return null;
    let num = parseFloat(match[1].replace(/,/g, ''));
    const suffix = (match[2] || '').toUpperCase();
    if (suffix === 'B') num *= 1000;
    else if (suffix === 'K') num *= 0.001;
    // M stays as M (base unit)
    return num;
}

/**
 * 生成 SVG Grouped Bar Chart
 */
function _buildGroupedBarChart(data) {
    const BAR_COLORS = ['#1e3a5f', '#e74c3c', '#2ecc71', '#f39c12', '#9b59b6', '#3498db'];
    const { labels, quarters, series } = data;

    const numRows = labels.length;
    const numQ = quarters.length;
    const isMobile = window.innerWidth < 640;

    // Chart dimensions — responsive
    const marginLeft = isMobile ? 50 : 130, marginRight = 20;
    const marginTop = 30, marginBottom = isMobile ? 60 : 50;
    const groupGap = isMobile ? 10 : 16;
    const barGap = 2;
    const barWidth = Math.max(isMobile ? 10 : 8, Math.min(18, Math.floor((Math.max(400, 600) - marginLeft - marginRight - groupGap * numRows) / (numRows * numQ))));
    const groupWidth = numQ * barWidth + (numQ - 1) * barGap;
    const chartWidth = marginLeft + numRows * (groupWidth + groupGap) + marginRight;
    const chartHeight = isMobile ? 240 : 280;
    const plotHeight = chartHeight - marginTop - marginBottom;

    // Find max value
    let maxVal = 0;
    series.forEach(row => row.forEach(v => { if (v !== null && v > maxVal) maxVal = v; }));
    if (maxVal === 0) maxVal = 1;
    // Round up for nice axis
    const niceMax = _niceNum(maxVal);

    // Y-axis gridlines (5 lines)
    const gridLines = 5;
    let gridSvg = '';
    let axisSvg = '';
    for (let i = 0; i <= gridLines; i++) {
        const val = (niceMax / gridLines) * i;
        const y = marginTop + plotHeight - (val / niceMax) * plotHeight;
        gridSvg += `<line x1="${marginLeft}" y1="${y}" x2="${chartWidth - marginRight}" y2="${y}" stroke="#e5e7eb" stroke-width="0.5"/>`;
        const label = val >= 1000 ? (val / 1000).toFixed(1) + 'B' : val.toFixed(0) + 'M';
        axisSvg += `<text x="${marginLeft - 8}" y="${y + 4}" text-anchor="end" fill="#94a3b8" font-size="${isMobile ? 11 : 9}" font-family="'JetBrains Mono',monospace">${label}</text>`;
    }

    // Bars
    let barsSvg = '';
    let labelsSvg = '';
    series.forEach((row, ri) => {
        const groupX = marginLeft + ri * (groupWidth + groupGap) + groupGap / 2;

        // X-axis label (institution name)
        const labelX = groupX + groupWidth / 2;
        const labelY = chartHeight - marginBottom + 14;
        labelsSvg += `<text x="${labelX}" y="${labelY}" text-anchor="middle" fill="#64748b" font-size="${isMobile ? 10 : 8}" font-family="'JetBrains Mono',monospace" transform="rotate(-35 ${labelX} ${labelY})">${escHtml(labels[ri])}</text>`;

        row.forEach((val, qi) => {
            if (val === null) return;
            const barH = (val / niceMax) * plotHeight;
            const x = groupX + qi * (barWidth + barGap);
            const y = marginTop + plotHeight - barH;
            const color = BAR_COLORS[qi % BAR_COLORS.length];
            barsSvg += `<rect x="${x}" y="${y}" width="${barWidth}" height="${barH}" fill="${color}" rx="1.5" class="bar-segment" style="--delay:${(ri * numQ + qi) * 0.02}s">
                <title>${escHtml(labels[ri])} ${escHtml(quarters[qi])}: ${val >= 1000 ? (val/1000).toFixed(2)+'B' : val.toFixed(1)+'M'}</title>
            </rect>`;
        });
    });

    // Legend
    let legendHtml = '';
    quarters.forEach((q, i) => {
        const color = BAR_COLORS[i % BAR_COLORS.length];
        legendHtml += `<div class="bar-legend-item">
            <span class="bar-legend-dot" style="background:${color}"></span>
            <span class="bar-legend-label">${escHtml(q)}</span>
        </div>`;
    });

    const wrapper = document.createElement('div');
    wrapper.className = 'bar-chart-container';
    wrapper.innerHTML = `
        <div class="bar-chart-legend">${legendHtml}</div>
        <div class="bar-chart-scroll">
            <svg viewBox="0 0 ${chartWidth} ${chartHeight}" class="bar-chart-svg" preserveAspectRatio="xMinYMid meet">
                ${gridSvg}${axisSvg}
                <line x1="${marginLeft}" y1="${marginTop + plotHeight}" x2="${chartWidth - marginRight}" y2="${marginTop + plotHeight}" stroke="#cbd5e1" stroke-width="1"/>
                ${barsSvg}${labelsSvg}
            </svg>
        </div>`;

    return wrapper;
}

/** Round up to a nice number for chart axis */
function _niceNum(val) {
    const exp = Math.floor(Math.log10(val));
    const base = Math.pow(10, exp);
    const frac = val / base;
    if (frac <= 1.5) return 1.5 * base;
    if (frac <= 2) return 2 * base;
    if (frac <= 3) return 3 * base;
    if (frac <= 5) return 5 * base;
    if (frac <= 7.5) return 7.5 * base;
    return 10 * base;
}


/**
 * 偵測「多指標 % 趨勢表」（如利潤率表：季度 × 毛利率/營業利潤率/淨利率/ROE/ROA）
 * 特徵：第一欄是時間標籤（FY/Q/季度），其餘欄大多含 % 值（可帶正負、箭頭）
 * 回傳 { timeLabels: [...], metrics: [{name, values:[...]}, ...] } 或 null
 */
function _extractLineData(table) {
    const headers = table.querySelectorAll('thead th');
    if (headers.length < 3) return null;

    const rows = table.querySelectorAll('tbody tr');
    if (rows.length < 3 || rows.length > 20) return null;

    // Check: at least 3 data columns (col 1+) should have % values in most rows
    const numCols = headers.length;
    const pctPattern = /[+-]?\d+(\.\d+)?\s*%/;

    // Count % cells per column (skip col 0 = time label)
    const colPctCount = new Array(numCols).fill(0);
    rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        for (let c = 1; c < Math.min(cells.length, numCols); c++) {
            if (pctPattern.test(cells[c].textContent)) colPctCount[c]++;
        }
    });

    // Need at least 2 columns where >= 70% of rows have % values
    const pctCols = [];
    for (let c = 1; c < numCols; c++) {
        if (colPctCount[c] >= rows.length * 0.7) pctCols.push(c);
    }
    if (pctCols.length < 2) return null;

    // Verify col 0 looks like time labels (FY, Q, 季, year, or quarter-like)
    const firstLabel = rows[0]?.querySelectorAll('td')[0]?.textContent.trim() || '';
    if (!/FY|Q\d|20\d{2}|季/i.test(firstLabel)) return null;

    // Extract data
    const timeLabels = [];
    const metricValues = pctCols.map(() => []);

    // Rows are typically newest-first; reverse for chronological chart
    const rowArr = Array.from(rows);
    for (let ri = rowArr.length - 1; ri >= 0; ri--) {
        const cells = rowArr[ri].querySelectorAll('td');
        const label = cells[0]?.textContent.trim() || '';
        timeLabels.push(label.replace(/\s*[↑↓↗↘→]/g, '').trim());

        pctCols.forEach((ci, mi) => {
            const raw = cells[ci]?.textContent.trim() || '';
            const m = raw.match(/([+-]?\d+(?:\.\d+)?)\s*%/);
            metricValues[mi].push(m ? parseFloat(m[1]) : null);
        });
    }

    const metrics = pctCols.map((ci, mi) => ({
        name: headers[ci]?.textContent.trim() || `Metric ${mi + 1}`,
        values: metricValues[mi],
    }));

    return { timeLabels, metrics };
}

/**
 * 生成 SVG Multi-line Chart
 */
function _buildLineChart(data) {
    const LINE_COLORS = [
        '#2ecc71', '#e74c3c', '#3498db', '#f39c12', '#9b59b6',
        '#1abc9c', '#e84393', '#1e3a5f',
    ];
    const { timeLabels, metrics } = data;
    const numPoints = timeLabels.length;
    const isMobile = window.innerWidth < 640;

    // Chart dimensions — responsive
    const marginLeft = isMobile ? 45 : 55, marginRight = 15;
    const marginTop = 20, marginBottom = isMobile ? 55 : 50;
    const ptSpacing = isMobile ? 50 : 75;
    const chartWidth = Math.max(isMobile ? 300 : 400, Math.min(700, numPoints * ptSpacing + marginLeft + marginRight));
    const chartHeight = isMobile ? 220 : 260;
    const plotW = chartWidth - marginLeft - marginRight;
    const plotH = chartHeight - marginTop - marginBottom;

    // Find min/max across all metrics
    let allVals = [];
    metrics.forEach(m => m.values.forEach(v => { if (v !== null) allVals.push(v); }));
    if (allVals.length === 0) return document.createElement('div');
    let minVal = Math.min(...allVals);
    let maxVal = Math.max(...allVals);

    // Add padding
    const range = maxVal - minVal || 1;
    maxVal = maxVal + range * 0.1;
    minVal = minVal - range * 0.1;

    // Nice round bounds
    const step = _niceNum((maxVal - minVal) / 5);
    minVal = Math.floor(minVal / step) * step;
    maxVal = Math.ceil(maxVal / step) * step;
    const yRange = maxVal - minVal || 1;

    function xPos(i) { return marginLeft + (i / (numPoints - 1)) * plotW; }
    function yPos(v) { return marginTop + plotH - ((v - minVal) / yRange) * plotH; }

    // Gridlines + Y-axis
    let gridSvg = '';
    const gridSteps = 5;
    for (let i = 0; i <= gridSteps; i++) {
        const val = minVal + (yRange / gridSteps) * i;
        const y = yPos(val);
        gridSvg += `<line x1="${marginLeft}" y1="${y}" x2="${chartWidth - marginRight}" y2="${y}" stroke="#e5e7eb" stroke-width="0.5"/>`;
        gridSvg += `<text x="${marginLeft - 8}" y="${y + 3}" text-anchor="end" fill="#94a3b8" font-size="${isMobile ? 11 : 9}" font-family="'JetBrains Mono',monospace">${val.toFixed(0)}%</text>`;
    }

    // Zero line if range crosses zero
    if (minVal < 0 && maxVal > 0) {
        const y0 = yPos(0);
        gridSvg += `<line x1="${marginLeft}" y1="${y0}" x2="${chartWidth - marginRight}" y2="${y0}" stroke="#94a3b8" stroke-width="1" stroke-dasharray="4,3"/>`;
    }

    // X-axis labels
    let xLabelsSvg = '';
    timeLabels.forEach((label, i) => {
        const x = xPos(i);
        const y = chartHeight - marginBottom + 16;
        // Shorten label for space
        const short = label.replace(/^FY/, '').trim();
        xLabelsSvg += `<text x="${x}" y="${y}" text-anchor="middle" fill="#64748b" font-size="${isMobile ? 10 : 8}" font-family="'JetBrains Mono',monospace" transform="rotate(-35 ${x} ${y})">${escHtml(short)}</text>`;
    });

    // Lines + dots
    let linesSvg = '';
    metrics.forEach((m, mi) => {
        const color = LINE_COLORS[mi % LINE_COLORS.length];

        // Build polyline points
        let points = [];
        m.values.forEach((v, i) => {
            if (v !== null) points.push({ x: xPos(i), y: yPos(v), val: v, idx: i });
        });

        if (points.length < 2) return;

        // Line path
        const pathD = points.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ');
        linesSvg += `<path d="${pathD}" fill="none" stroke="${color}" stroke-width="${isMobile ? 2.5 : 2}" stroke-linecap="round" stroke-linejoin="round" class="line-path" style="--delay:${mi * 0.15}s; --len:${plotW + plotH}"/>`;

        // Dots
        points.forEach((p, pi) => {
            linesSvg += `<circle cx="${p.x.toFixed(1)}" cy="${p.y.toFixed(1)}" r="${isMobile ? 5 : 3.5}" fill="${color}" stroke="#fff" stroke-width="1.5" class="line-dot" style="--delay:${mi * 0.15 + pi * 0.04}s">
                <title>${escHtml(m.name)} ${escHtml(timeLabels[p.idx])}: ${p.val.toFixed(2)}%</title>
            </circle>`;
        });
    });

    // Legend
    let legendHtml = '';
    metrics.forEach((m, mi) => {
        const color = LINE_COLORS[mi % LINE_COLORS.length];
        legendHtml += `<div class="line-legend-item">
            <span class="line-legend-line" style="background:${color}"></span>
            <span class="line-legend-label">${escHtml(m.name)}</span>
        </div>`;
    });

    const wrapper = document.createElement('div');
    wrapper.className = 'line-chart-container';
    wrapper.innerHTML = `
        <div class="line-chart-legend">${legendHtml}</div>
        <div class="line-chart-scroll">
            <svg viewBox="0 0 ${chartWidth} ${chartHeight}" class="line-chart-svg" preserveAspectRatio="xMinYMid meet">
                ${gridSvg}
                <line x1="${marginLeft}" y1="${marginTop + plotH}" x2="${chartWidth - marginRight}" y2="${marginTop + plotH}" stroke="#cbd5e1" stroke-width="1"/>
                ${linesSvg}${xLabelsSvg}
            </svg>
        </div>`;

    return wrapper;
}


/* ── 分享工具函數 ── */
function _shareReport(title) {
    const shareTextTpl = I18N.share_text || 'View {ticker} {title} analysis';
    const shareText    = shareTextTpl.replace('{ticker}', getCurrentTicker()).replace('{title}', title);
    const shareData    = { title, text: shareText, url: window.location.href };
    const fallbackCopy = () => {
        const copiedMsg = I18N.copied      || 'Copied to clipboard';
        const manualMsg = I18N.copy_manual || 'Please copy manually: ';
        navigator.clipboard.writeText(`${shareData.text}\n${shareData.url}`)
            .then(() => alert(copiedMsg))
            .catch(() => alert(manualMsg + shareData.url));
    };
    if (navigator.share) navigator.share(shareData).catch(fallbackCopy);
    else fallbackCopy();
}


/* ==========================================================
   4. toggleMinimize — 最小化切換
   ========================================================== */
function toggleMinimize(winId) {
    const win = document.getElementById(winId);
    if (!win) return;
    if (win.classList.contains('maximized')) win.classList.remove('maximized');
    win.classList.toggle('minimized');

    const minBtn = win.querySelector('.btn-minimize svg');
    minBtn.innerHTML = win.classList.contains('minimized')
        ? '<polyline points="3 11 8 6 13 11"/>'
        : '<line x1="3" y1="8" x2="13" y2="8"/>';
}


/* ==========================================================
   5. toggleMaximize — 最大化切換
   關鍵：用 inline style 覆蓋 CSS 的 max-width
   ========================================================== */
function toggleMaximize(winId) {
    const win = document.getElementById(winId);
    if (!win) return;

    // 若已最小化，先還原
    if (win.classList.contains('minimized')) {
        win.classList.remove('minimized');
        win.querySelector('.btn-minimize svg').innerHTML = '<line x1="3" y1="8" x2="13" y2="8"/>';
    }

    const isMaximized = win.classList.toggle('maximized');

    if (isMaximized) {
        win.dataset.prevTop  = win.style.top;
        win.dataset.prevLeft = win.style.left;
        win.style.top         = '0px';
        win.style.left        = '0px';
        win.style.width       = '100vw';
        win.style.height      = '100vh';
        win.style.maxWidth    = '100vw';
        win.style.borderRadius = '0';
    } else {
        win.style.top         = win.dataset.prevTop  || '7.5vh';
        win.style.left        = win.dataset.prevLeft || '7.5vw';
        win.style.width       = '';
        win.style.height      = '';
        win.style.maxWidth    = '';
        win.style.borderRadius = '';
    }

    win.querySelector('.btn-maximize svg').innerHTML = isMaximized
        ? '<rect x="4.5" y="1.5" width="10" height="10" rx="1"/><rect x="1.5" y="4.5" width="10" height="10" rx="1"/>'
        : '<rect x="2.5" y="2.5" width="11" height="11" rx="1"/>';
}


/* ==========================================================
   6. 拖曳系統（requestAnimationFrame 高效能版）
   ========================================================== */
let dragObj = null, offX = 0, offY = 0, rafId = null, lastX = 0, lastY = 0;

function startDrag(e, id) {
    if (e.target.closest('.window-controls')) return;
    const win = document.getElementById(id);
    if (win.classList.contains('maximized') || win.classList.contains('minimized')) return;

    dragObj = win;
    document.querySelectorAll('.draggable-window').forEach(w => w.style.zIndex = 1000);
    dragObj.style.zIndex    = 1001;
    dragObj.style.willChange = 'left, top';
    offX = e.clientX - dragObj.offsetLeft;
    offY = e.clientY - dragObj.offsetTop;

    document.onmousemove = (ev) => {
        ev.preventDefault();
        lastX = ev.clientX;
        lastY = ev.clientY;
        if (!rafId) rafId = requestAnimationFrame(updateDrag);
    };
    document.onmouseup = () => {
        if (dragObj) dragObj.style.willChange = 'auto';
        if (rafId) cancelAnimationFrame(rafId);
        dragObj = null;
        rafId   = null;
        document.onmousemove = null;
        document.onmouseup   = null;
    };
}

function updateDrag() {
    if (dragObj) {
        dragObj.style.left = (lastX - offX) + 'px';
        dragObj.style.top  = (lastY - offY) + 'px';
    }
    rafId = null;
}


/* ==========================================================
   7. navigateToStock — Optimistic UI 切換股票
   ----------------------------------------------------------
   流程：
     1. 立即更新 header 名稱（零延遲感）
     2. 所有卡片顯示 skeleton loading
     3. 清空快取，更新 ticker，重新觸發全部分析
   ========================================================== */
async function navigateToStock(code, name) {
    // ── 1. 立即更新 header（先用傳入的 name，再從 API 取精確資料）──
    const elChineseName    = document.getElementById('header-chinese-name');
    const elEnName         = document.getElementById('header-en-name');
    const elSectorIndustry = document.getElementById('header-sector-industry');
    const elSectorDivider  = document.getElementById('header-sector-divider');

    if (elChineseName) {
        elChineseName.style.opacity = '0';
        setTimeout(() => {
            elChineseName.textContent = name || code;
            elChineseName.style.opacity = '1';
        }, 150);
    }
    if (elEnName) {
        elEnName.style.opacity = '0';
        setTimeout(() => {
            elEnName.textContent = code;
            elEnName.style.opacity = '1';
        }, 150);
    }
    // 先清空 sector/industry，等 API 回來再填
    if (elSectorIndustry) elSectorIndustry.textContent = '';
    if (elSectorDivider) elSectorDivider.classList.add('hidden');

    // 從 API 取多語顯示名稱 + sector/industry
    fetch('/api/stock_display?ticker=' + encodeURIComponent(code) + '&lang=' + encodeURIComponent(LANG))
        .then(function(r) { return r.json(); })
        .then(function(data) {
            if (data.display_name && elChineseName) {
                elChineseName.textContent = data.display_name;
            }
            if (elSectorIndustry) {
                var parts = [];
                if (data.sector) parts.push(data.sector);
                if (data.industry) parts.push(data.industry);
                elSectorIndustry.textContent = parts.join(' \u00b7 ');
                if (elSectorDivider) {
                    elSectorDivider.classList.toggle('hidden', parts.length === 0);
                }
            }
            document.title = (data.display_name || name || code) + ' ' +
                             (I18N.terminal_title || 'Investment Decision Terminal') +
                             ' | 4M DataLab';
        })
        .catch(function() {});

    document.title = `${name || code} ${I18N.terminal_title || 'Investment Decision Terminal'} | 4M DataLab`;
    history.pushState({ code, name }, '', `/${code}`);
    loadEtfHolders(code);

    // ── 2. 所有卡片進入 skeleton 狀態 ────────────────────────
    ALL_SECTIONS.forEach(id => {
        const dot     = document.getElementById(`dot-${id}`);
        const preview = document.getElementById(`preview-${id}`);
        const scoreEl = document.getElementById(`score-${id}`);

        if (dot)     dot.className = 'loading-pulse';
        if (scoreEl) scoreEl.classList.add('hidden');
        if (preview) preview.innerHTML = `
            <div class="skeleton-line" style="width:88%"></div>
            <div class="skeleton-line" style="width:65%;margin-top:7px"></div>
            <div class="skeleton-line" style="width:78%;margin-top:7px"></div>`;

        // 重置手機列表卡片
        const mDot = document.getElementById(`m-dot-${id}`);
        const mPre = document.getElementById(`m-preview-${id}`);
        const mSco = document.getElementById(`m-score-${id}`);
        if (mDot) mDot.className = 'loading-pulse';
        if (mPre) { mPre.innerText = ''; mPre.style.color = ''; mPre.style.fontStyle = ''; }
        if (mSco) { mSco.textContent = '···'; mSco.classList.add('no-score'); }
    });

    // 關閉所有彈出視窗
    document.querySelectorAll('.draggable-window').forEach(w => w.remove());

    // ── 3. 更新 ticker，令舊請求失效，重新分析 ───────────────
    analysisCache         = {};
    window._optimisticTicker = code;
    _fetchRequestId++;             // 令所有 in-flight 舊請求失效
    _resetRatingPanel();           // 重置評級面板

    _analyzeAllSections(id => fetchSection(id));
    loadKeyMetrics();

    // 重新載入 K 線圖
    var activeBtn = document.querySelector('.chart-period-btn.active');
    var days = activeBtn ? parseInt(activeBtn.dataset.days) : 180;
    loadOhlcChart(days);

    // 重置時段分析
    _periodStartDate = null;
    _periodEndDate = null;
    _periodReportHtml = null;
    _periodEvents = [];
    var paStart = document.getElementById('pa-start-date');
    var paView = document.getElementById('pa-view-btn');
    var paBtn = document.getElementById('pa-analyze-btn');
    var paHint = document.getElementById('pa-hint');
    if (paStart) paStart.textContent = '—';
    if (paView) paView.classList.add('hidden');
    if (paBtn) paBtn.disabled = true;
    if (paHint) paHint.classList.remove('fade');
    // 清除圖表標記和事件列表
    if (_candleSeries) _candleSeries.setMarkers([]);
    var paEvents = document.getElementById('pa-events');
    if (paEvents) paEvents.classList.add('hidden');
    var clearBtn = document.getElementById('chart-clear-markers');
    var showBtn = document.getElementById('chart-show-markers');
    if (clearBtn) clearBtn.classList.add('hidden');
    if (showBtn) showBtn.classList.add('hidden');

    // 重置 CTA
    ALL_SECTIONS.forEach(function(sid) {
        var c = document.getElementById('cta-' + sid);
        var mc = document.getElementById('m-cta-' + sid);
        if (c) c.classList.remove('ready');
        if (mc) mc.classList.remove('ready');
    });
}

// 瀏覽器上下頁（← →）支援
window.addEventListener('popstate', () => window.location.reload());


/* ==========================================================
   7b. 語言切換（不重載頁面）
   ========================================================== */
async function switchLanguage(newLang) {
    if (newLang === LANG) return;

    // 1. 取得新語言的翻譯字典
    try {
        var res = await fetch('/api/translations?lang=' + encodeURIComponent(newLang));
        var newI18N = await res.json();
    } catch(e) {
        // fallback: 重載頁面
        window.location.href = '?lang=' + newLang;
        return;
    }

    // 2. 更新全域狀態
    LANG = newLang;
    I18N = newI18N;
    window.I18N = newI18N;
    document.body.dataset.lang = newLang;
    document.body.dataset.i18n = JSON.stringify(newI18N);

    // 3. 設定 cookie
    document.cookie = 'lang=' + newLang + ';max-age=31536000;path=/;SameSite=Lax';

    // 4. 更新 URL（不重載）
    var url = new URL(window.location);
    url.searchParams.set('lang', newLang);
    history.replaceState(null, '', url.pathname);

    // 5. 更新語言按鈕高亮
    document.querySelectorAll('.lang-btn').forEach(function(btn) {
        var href = btn.getAttribute('href') || '';
        var match = href.match(/lang=([^&]+)/);
        var btnLang = match ? match[1] : '';
        btn.classList.toggle('lang-active', btnLang === newLang);
    });
    document.querySelectorAll('.more-lang-btn').forEach(function(btn) {
        var href = btn.getAttribute('href') || '';
        var match = href.match(/lang=([^&]+)/);
        var btnLang = match ? match[1] : '';
        btn.classList.toggle('more-lang-active', btnLang === newLang);
    });

    // 6. 更新公司名稱 + sector/industry（從 DB 取多語資料）
    var ticker = getCurrentTicker();
    var cName = document.getElementById('header-chinese-name');
    var sectorIndustry = document.getElementById('header-sector-industry');
    var sectorDivider = document.getElementById('header-sector-divider');

    fetch('/api/stock_display?ticker=' + encodeURIComponent(ticker) + '&lang=' + encodeURIComponent(newLang))
        .then(function(r) { return r.json(); })
        .then(function(data) {
            if (data.display_name && cName) {
                cName.textContent = data.display_name;
            }
            if (sectorIndustry) {
                var parts = [];
                if (data.sector) parts.push(data.sector);
                if (data.industry) parts.push(data.industry);
                sectorIndustry.textContent = parts.join(' \u00b7 ');
                if (sectorDivider) {
                    sectorDivider.classList.toggle('hidden', parts.length === 0);
                }
            }
            // 更新頁面標題
            document.title = (cName ? cName.textContent : ticker) + ' ' +
                             (newI18N.terminal_title || 'Investment Decision Terminal') +
                             ' | 4M DataLab';
        })
        .catch(function() {
            // fallback: 保持原標題
            document.title = (cName ? cName.textContent : ticker) + ' ' +
                             (newI18N.terminal_title || 'Investment Decision Terminal') +
                             ' | 4M DataLab';
        });

    // 7. 更新靜態 UI 文字
    _updateStaticText(newI18N);

    // 8. 重置評級 + 重新分析所有 section（帶新語言）
    analysisCache = {};
    _fetchRequestId++;
    _resetRatingPanel();

    ALL_SECTIONS.forEach(function(id) {
        // skeleton 狀態
        var dot = document.getElementById('dot-' + id);
        var preview = document.getElementById('preview-' + id);
        var scoreEl = document.getElementById('score-' + id);
        if (dot) dot.className = 'loading-pulse';
        if (scoreEl) scoreEl.classList.add('hidden');
        if (preview) preview.innerHTML =
            '<div class="skeleton-line" style="width:88%"></div>' +
            '<div class="skeleton-line" style="width:65%;margin-top:7px"></div>' +
            '<div class="skeleton-line" style="width:78%;margin-top:7px"></div>';

        var mDot = document.getElementById('m-dot-' + id);
        var mSco = document.getElementById('m-score-' + id);
        var mPre = document.getElementById('m-preview-' + id);
        if (mDot) mDot.className = 'loading-pulse';
        if (mSco) { mSco.textContent = '···'; mSco.classList.add('no-score'); }
        if (mPre) mPre.innerText = '';

        // 重置 CTA
        var c = document.getElementById('cta-' + id);
        var mc = document.getElementById('m-cta-' + id);
        if (c) c.classList.remove('ready');
        if (mc) mc.classList.remove('ready');
    });
    _analyzeAllSections(id => fetchSection(id));
}

function _updateStaticText(t) {
    window._currentTranslations = t;
    // 搜尋列
    var searchInput = document.getElementById('stock-search-input');
    if (searchInput) searchInput.placeholder = t.search_placeholder || '';
    var searchBtn = searchInput && searchInput.closest('form');
    if (searchBtn) {
        var btn = searchBtn.querySelector('button[type="submit"]');
        if (btn) btn.textContent = t.search_btn || 'Search';
    }

    // Section 標題
    var secTitles = {
        'fundamental_title': t.fundamental_title,
        'fundamental_label': t.fundamental_label,
        'technical_title': t.technical_title,
        'technical_label': t.technical_label,
    };
    var fundTitle = document.getElementById('fundamental-title');
    var techTitle = document.getElementById('technical-title');
    if (fundTitle && secTitles.fundamental_title) fundTitle.firstChild.textContent = secTitles.fundamental_title;
    if (techTitle && secTitles.technical_title) techTitle.firstChild.textContent = secTitles.technical_title;
    document.querySelectorAll('.section-label').forEach(function(el, i) {
        if (i === 0 && secTitles.fundamental_label) el.textContent = secTitles.fundamental_label;
        if (i === 1 && secTitles.technical_label) el.textContent = secTitles.technical_label;
    });

    // 更新 section tooltips
    if (fundTitle) { var tip = fundTitle.querySelector('.section-tip'); if (tip && t.fundamental_tooltip) tip.dataset.tip = t.fundamental_tooltip; }
    if (techTitle) { var tip = techTitle.querySelector('.section-tip'); if (tip && t.technical_tooltip) tip.dataset.tip = t.technical_tooltip; }
    var paTitle = document.getElementById('pa-title');
    if (paTitle) { var tip = paTitle.querySelector('.section-tip'); if (tip && t.pa_tooltip) tip.dataset.tip = t.pa_tooltip; }

    // 更新報告日期前綴
    _updateBatchDates();

    // 卡片標題（桌面）
    var cardMap = {
        'biz': t.card_biz, 'exec': t.card_exec,
        'finance': t.card_finance, 'call': t.card_call,
        'ta_price': t.card_ta_price, 'ta_analyst': t.card_ta_analyst,
        'ta_social': t.card_ta_social
    };
    var tagMap = {
        'biz': t.icon_biz, 'exec': t.icon_exec,
        'finance': t.icon_finance, 'call': t.icon_call,
        'ta_price': t.icon_ta_price, 'ta_analyst': t.icon_ta_analyst,
        'ta_social': t.icon_ta_social
    };

    ALL_SECTIONS.forEach(function(id) {
        // 桌面卡片
        var card = document.getElementById('cta-' + id);
        if (card) {
            var parent = card.closest('.analysis-card');
            if (parent) {
                var titleEl = parent.querySelector('.card-title-text');
                if (titleEl && cardMap[id]) titleEl.textContent = cardMap[id];
                var tagEl = parent.querySelector('.card-tag');
                if (tagEl && tagMap[id]) tagEl.textContent = tagMap[id];
                var ctaText = parent.querySelector('.card-cta-text');
                if (ctaText) ctaText.textContent = t.card_cta || '點擊查看完整報告';
            }
        }
        // 手機卡片
        var mCard = document.getElementById('m-cta-' + id);
        if (mCard) {
            mCard.textContent = t.card_cta_short || '查看報告 →';
            var mParent = mCard.closest('.mdash-card');
            if (mParent) {
                var mTitle = mParent.querySelector('.mdash-title');
                if (mTitle && cardMap[id]) mTitle.textContent = cardMap[id];
                var mTag = mParent.querySelector('.mdash-tag');
                if (mTag && tagMap[id]) mTag.textContent = tagMap[id];
            }
        }
    });

    // 指標列標籤
    var metricLabels = {
        'metric-price': t.metric_price || '股價',
        'metric-mcap': t.metric_mcap || '市值',
        'metric-rev': t.metric_rev || '營收',
        'metric-rev-yoy': t.metric_rev_yoy || '營收 YoY',
        'metric-gm': t.metric_gm || '毛利率',
        'metric-nm': t.metric_nm || '淨利率',
        'metric-de': t.metric_de || '負債/權益',
        'metric-dy': t.metric_dy || '股息率',
        'metric-dps': t.metric_dps || '每股股息'
    };
    Object.keys(metricLabels).forEach(function(id) {
        var el = document.getElementById(id);
        if (el) {
            var label = el.querySelector('.metric-label');
            if (label) label.childNodes[0].textContent = metricLabels[id];
        }
    });

    // 指標 Tooltip
    var tipMap = {
        'metric-price': t.tip_price, 'metric-mcap': t.tip_mcap,
        'metric-pe': t.tip_pe, 'metric-peg': t.tip_peg,
        'metric-eps': t.tip_eps,
        'metric-rev': t.tip_rev, 'metric-rev-yoy': t.tip_rev_yoy,
        'metric-gm': t.tip_gm, 'metric-nm': t.tip_nm,
        'metric-de': t.tip_de, 'metric-dy': t.tip_dy,
        'metric-dps': t.tip_dps
    };
    Object.keys(tipMap).forEach(function(id) {
        var el = document.getElementById(id);
        if (el && tipMap[id]) el.setAttribute('data-tip', tipMap[id]);
    });

    // 時段走勢分析面板
    var paTitle = document.querySelector('#pa-panel .text-aurum');
    if (paTitle) paTitle.textContent = t.pa_title || '時段走勢分析';
    var paHintText = document.querySelector('.pa-hint-text');
    if (paHintText) paHintText.textContent = t.pa_hint || '';
    var paAnalyzeBtn = document.getElementById('pa-analyze-btn');
    if (paAnalyzeBtn) paAnalyzeBtn.textContent = t.pa_analyze_btn || '分析此時段';
    var paViewBtn = document.getElementById('pa-view-btn');
    if (paViewBtn) paViewBtn.textContent = t.pa_view_btn || '查看報告';
    var paEndDate = document.getElementById('pa-end-date');
    if (paEndDate && paEndDate.textContent.match(/今日|Today/i)) {
        paEndDate.textContent = t.pa_today || '今日';
    }
    var paEventsTitle = document.querySelector('.pa-events-title');
    if (paEventsTitle) paEventsTitle.textContent = t.pa_events_title || '關鍵事件';

    // 免責聲明
    var disclaimerTitle = document.querySelector('footer h5');
    if (disclaimerTitle) disclaimerTitle.textContent = t.disclaimer_title || '';
    var disclaimerBody = document.querySelector('footer p');
    if (disclaimerBody) disclaimerBody.textContent = t.disclaimer_body || '';
    var copyrightEl = document.querySelector('footer .border-t span');
    if (copyrightEl) copyrightEl.textContent = t.copyright || '';

    // 評級面板
    var ratingScoreLabel = document.querySelector('.rating-score-label');
    if (ratingScoreLabel) ratingScoreLabel.textContent = t.rating_score || '綜合評分';

    // 導航列（桌面側欄）
    document.querySelectorAll('.nav-item[data-page="stock"] .nav-label').forEach(function(el) {
        el.textContent = t.nav_stock || '股票分析';
    });
    document.querySelectorAll('.nav-item[data-page="ipo"] .nav-label').forEach(function(el) {
        el.textContent = t.nav_ipo || 'IPO追蹤';
    });

    // 手機底部導航
    var mnStock = document.querySelector('.mn-label-stock');
    if (mnStock) mnStock.textContent = t.nav_stock || '股票分析';
    var mnIpo = document.querySelector('.mn-label-ipo');
    if (mnIpo) mnIpo.textContent = t.nav_ipo || 'IPO追蹤';
    var mnComing = document.querySelector('.mn-label-coming');
    if (mnComing) mnComing.textContent = t.nav_coming_soon || '即將推出';
    var mnMore = document.querySelector('.mn-label-more');
    if (mnMore) mnMore.textContent = t.nav_more || '更多';

    // 手機「更多」選單
    var msLang = document.querySelector('.ms-label-lang');
    if (msLang) msLang.textContent = t.nav_lang_setting || '語言設定';
    var msTheme = document.querySelector('.ms-label-theme');
    if (msTheme) msTheme.textContent = t.nav_theme_setting || '外觀設定';
    var msAbout = document.querySelector('.ms-label-about');
    if (msAbout) msAbout.textContent = t.nav_about || '關於 DataLab';
    var msTerms = document.querySelector('.ms-label-terms');
    if (msTerms) msTerms.textContent = t.nav_terms || '使用條款';
    var msFeedback = document.querySelector('.ms-label-feedback');
    if (msFeedback) msFeedback.textContent = t.nav_feedback || '意見回饋';
    var msVersion = document.querySelector('.ms-label-version');
    if (msVersion) msVersion.textContent = t.nav_version || '4M DataLab v1.0';

    // 深色/淺色模式標籤（同步 nav.js 的 theme label）
    var isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    var themeLabel = isDark ? (t.nav_theme_light || '淺色模式') : (t.nav_theme_dark || '深色模式');
    var tl = document.querySelector('.theme-label');
    if (tl) tl.textContent = themeLabel;
    var ml = document.getElementById('m-theme-label');
    if (ml) ml.textContent = themeLabel;
}

window.switchLanguage = switchLanguage;


/* ==========================================================
   8. 關鍵指標
   ========================================================== */
function loadKeyMetrics() {
    var ticker = getCurrentTicker();
    if (!ticker) return;

    // 先顯示載入狀態
    var _allMetricIds = ['mv-price','mv-mcap','mv-pe','mv-peg','mv-eps','mv-rev',
                         'mv-gm','mv-nm','mv-de','mv-dy','mv-rev-yoy','mv-dps'];
    _allMetricIds.forEach(function(id) {
        var el = document.getElementById(id);
        if (el) { el.textContent = '···'; el.className = 'metric-value'; }
    });

    fetch('/api/key-metrics?symbol=' + encodeURIComponent(ticker))
        .then(function(r) { return r.json(); })
        .then(function(d) {
            var el;
            var csym = {'USD':'$','HKD':'HK$','CNY':'\u00a5','JPY':'\u00a5','GBP':'\u00a3','EUR':'\u20ac'}[d.currency] || '$';

            // 股價
            el = document.getElementById('mv-price');
            if (el) el.textContent = d.price != null ? csym + d.price.toFixed(2) : '—';
            el = document.getElementById('mv-price-date');
            if (el) el.textContent = d.price_date || '';

            // 市值
            el = document.getElementById('mv-mcap');
            if (el) el.textContent = d.market_cap != null ? _fmtMetricMoney(d.market_cap, d.currency) : '—';

            // PE
            el = document.getElementById('mv-pe');
            if (el) el.textContent = d.pe != null ? d.pe.toFixed(1) + 'x' : '—';

            // PEG
            el = document.getElementById('mv-peg');
            if (el) el.textContent = d.peg != null ? d.peg.toFixed(2) + 'x' : '—';

            // EPS
            el = document.getElementById('mv-eps');
            if (el) el.textContent = d.eps != null ? csym + d.eps.toFixed(2) : '—';

            // 營收
            el = document.getElementById('mv-rev');
            if (el) el.textContent = d.revenue != null ? _fmtMetricMoney(d.revenue, d.currency) : '—';
            el = document.getElementById('mv-rev-fiscal');
            if (el) el.textContent = d.fiscal || '';

            // 毛利率
            el = document.getElementById('mv-gm');
            if (el) el.textContent = d.gross_margin != null ? (d.gross_margin * 100).toFixed(1) + '%' : '—';

            // 淨利率
            el = document.getElementById('mv-nm');
            if (el) el.textContent = d.net_margin != null ? (d.net_margin * 100).toFixed(1) + '%' : '—';

            // 負債/權益
            el = document.getElementById('mv-de');
            if (el) el.textContent = d.debt_to_equity != null ? d.debt_to_equity.toFixed(2) : '—';

            // 股息率
            el = document.getElementById('mv-dy');
            if (el) el.textContent = d.dividend_yield != null ? (d.dividend_yield * 100).toFixed(2) + '%' : '—';

            // 營收 YoY
            el = document.getElementById('mv-rev-yoy');
            if (el) {
                if (d.revenue_yoy != null) {
                    var sign = d.revenue_yoy >= 0 ? '+' : '';
                    el.textContent = sign + d.revenue_yoy.toFixed(1) + '%';
                    el.classList.add(d.revenue_yoy >= 0 ? 'up' : 'down');
                } else {
                    el.textContent = '—';
                }
            }

            // 每股股息
            el = document.getElementById('mv-dps');
            if (el) el.textContent = d.dividend_per_share != null ? csym + d.dividend_per_share.toFixed(2) : '—';
        })
        .catch(function() {
            _allMetricIds.forEach(function(id) {
                var el = document.getElementById(id);
                if (el) el.textContent = '—';
            });
        });
}

function _fmtMetricMoney(v, currency) {
    if (v == null) return '—';
    var sym = {'USD':'$','HKD':'HK$','CNY':'\u00a5','JPY':'\u00a5','GBP':'\u00a3','EUR':'\u20ac'}[currency] || (currency ? currency + ' ' : '$');
    var neg = v < 0;
    var a = Math.abs(v);
    var s;
    if (a >= 1e12)      s = (a / 1e12).toFixed(2) + 'T';
    else if (a >= 1e9)  s = (a / 1e9).toFixed(2) + 'B';
    else if (a >= 1e6)  s = (a / 1e6).toFixed(1) + 'M';
    else                s = a.toLocaleString();
    return (neg ? '-' : '') + sym + s;
}

function toggleMetrics() {
    var bar = document.getElementById('metrics-bar');
    var btn = document.getElementById('metrics-toggle');
    if (!bar || !btn) return;
    var expanded = bar.classList.toggle('expanded');
    btn.classList.toggle('expanded', expanded);
}


/* ==========================================================
   9. OHLC K 線圖（TradingView Lightweight Charts v4.2）
   ----------------------------------------------------------
   功能：
     - 蠟燭圖 + 成交量柱狀圖
     - Crosshair 圖例（OHLCV）
     - 天數切換按鈕（30/90/180/365/730）
     - 期間分析（漲跌幅、最高、最低）
   ========================================================== */
let _chart = null;
let _candleSeries = null;
let _volumeSeries = null;
let _chartData = [];
let _periodReportHtml = null;
let _periodStartDate = null;
let _periodEndDate = null;
let _periodEvents = [];

function initOhlcChart() {
    const container = document.getElementById('ohlc-chart');
    if (!container || typeof LightweightCharts === 'undefined') return;

    const _isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    _chart = LightweightCharts.createChart(container, {
        layout: {
            background: { type: 'solid', color: _isDark ? '#242428' : '#ffffff' },
            textColor: _isDark ? '#777' : '#999',
            fontFamily: "'JetBrains Mono', monospace",
            fontSize: 11,
        },
        grid: {
            vertLines: { color: _isDark ? '#333' : '#f0f0f0' },
            horzLines: { color: _isDark ? '#333' : '#f0f0f0' },
        },
        crosshair: {
            mode: LightweightCharts.CrosshairMode.Normal,
        },
        rightPriceScale: {
            borderColor: _isDark ? '#444' : '#e5e5e5',
        },
        timeScale: {
            borderColor: _isDark ? '#444' : '#e5e5e5',
            timeVisible: false,
            rightOffset: 5,
            fixLeftEdge: true,
            fixRightEdge: true,
        },
        handleScroll: { vertTouchDrag: false },
    });
    window._chart = _chart;

    _candleSeries = _chart.addCandlestickSeries({
        upColor: '#26a69a',
        downColor: '#ef5350',
        borderDownColor: '#ef5350',
        borderUpColor: '#26a69a',
        wickDownColor: '#ef5350',
        wickUpColor: '#26a69a',
    });

    _volumeSeries = _chart.addHistogramSeries({
        priceFormat: { type: 'volume' },
        priceScaleId: 'volume',
    });

    _chart.priceScale('volume').applyOptions({
        scaleMargins: { top: 0.8, bottom: 0 },
        drawTicks: false,
    });

    // Crosshair 圖例
    const legendEl = document.getElementById('chart-legend');
    _chart.subscribeCrosshairMove(function(param) {
        if (!legendEl) return;
        if (!param.time || !param.seriesData || param.seriesData.size === 0) {
            legendEl.classList.add('hidden');
            return;
        }
        const candle = param.seriesData.get(_candleSeries);
        const vol = param.seriesData.get(_volumeSeries);
        if (!candle) { legendEl.classList.add('hidden'); return; }

        legendEl.classList.remove('hidden');
        document.getElementById('legend-date').textContent = param.time;
        document.getElementById('legend-open').textContent = candle.open.toFixed(2);
        document.getElementById('legend-high').textContent = candle.high.toFixed(2);
        document.getElementById('legend-low').textContent = candle.low.toFixed(2);
        document.getElementById('legend-close').textContent = candle.close.toFixed(2);
        document.getElementById('legend-vol').textContent = vol ? _fmtVol(vol.value) : '';
    });

    // 天數按鈕
    document.querySelectorAll('.chart-period-btn').forEach(function(btn) {
        btn.addEventListener('click', function() {
            document.querySelectorAll('.chart-period-btn').forEach(function(b) { b.classList.remove('active'); });
            btn.classList.add('active');
            loadOhlcChart(parseInt(btn.dataset.days));
        });
    });

    // 點擊圖表設定開始日期
    _chart.subscribeClick(function(param) {
        if (!param.time) return;
        var dateStr;
        if (typeof param.time === 'number') {
            var d = new Date(param.time * 1000);
            dateStr = d.getUTCFullYear() + '-' + String(d.getUTCMonth()+1).padStart(2,'0') + '-' + String(d.getUTCDate()).padStart(2,'0');
        } else {
            dateStr = param.time;
        }
        _periodStartDate = dateStr;
        var display = document.getElementById('pa-start-date');
        if (display) display.textContent = dateStr;
        // 啟用分析按鈕
        var analyzeBtn = document.getElementById('pa-analyze-btn');
        if (analyzeBtn) analyzeBtn.disabled = false;
        // 淡化提示
        var hint = document.getElementById('pa-hint');
        if (hint) hint.classList.add('fade');
        // 重置舊的分析結果
        var viewBtn = document.getElementById('pa-view-btn');
        if (viewBtn) viewBtn.classList.add('hidden');
        _periodReportHtml = null;
    });

    // Resize
    var resizeTimer;
    window.addEventListener('resize', function() {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(function() {
            if (_chart && container.clientWidth > 0) {
                _chart.applyOptions({ width: container.clientWidth });
            }
        }, 100);
    });

    // 初始載入
    loadOhlcChart(180);
}

function loadOhlcChart(days) {
    var ticker = getCurrentTicker();
    if (!ticker) return;

    fetch('/api/ohlc?symbol=' + encodeURIComponent(ticker) + '&days=' + days)
        .then(function(r) { return r.json(); })
        .then(function(data) {
            var emptyEl = document.getElementById('chart-empty');
            if (!Array.isArray(data) || data.length === 0) {
                _chartData = [];
                if (_candleSeries) _candleSeries.setData([]);
                if (_volumeSeries) _volumeSeries.setData([]);
                _updatePeriodInfo([]);
                if (emptyEl) emptyEl.classList.remove('hidden');
                return;
            }
            if (emptyEl) emptyEl.classList.add('hidden');

            _chartData = data;

            _candleSeries.setData(data.map(function(d) {
                return { time: d.time, open: d.open, high: d.high, low: d.low, close: d.close };
            }));

            _volumeSeries.setData(data.map(function(d) {
                return {
                    time: d.time,
                    value: d.volume || 0,
                    color: d.close >= d.open ? 'rgba(38,166,154,0.3)' : 'rgba(239,83,80,0.3)',
                };
            }));

            _chart.timeScale().fitContent();
            _updatePeriodInfo(data);
        })
        .catch(function(err) {
            console.error('[Chart] Load error:', err);
        });
}

function _updatePeriodInfo(data) {
    var infoEl = document.getElementById('chart-period-info');
    if (!infoEl) return;
    if (!data || data.length < 2) { infoEl.classList.add('hidden'); return; }

    var first = data[0];
    var last = data[data.length - 1];
    var change = ((last.close - first.open) / first.open * 100).toFixed(2);
    var high = Math.max.apply(null, data.map(function(d) { return d.high; }));
    var low = Math.min.apply(null, data.map(function(d) { return d.low; }));

    var changeEl = document.getElementById('period-change');
    var highEl = document.getElementById('period-high');
    var lowEl = document.getElementById('period-low');

    if (changeEl) {
        var sign = change >= 0 ? '+' : '';
        changeEl.textContent = sign + change + '%';
        changeEl.style.color = change >= 0 ? '#26a69a' : '#ef5350';
    }
    if (highEl) highEl.textContent = 'H: ' + high.toFixed(2);
    if (lowEl) lowEl.textContent = 'L: ' + low.toFixed(2);

    infoEl.classList.remove('hidden');
}

function _fmtVol(v) {
    if (v >= 1e9) return (v / 1e9).toFixed(1) + 'B';
    if (v >= 1e6) return (v / 1e6).toFixed(1) + 'M';
    if (v >= 1e3) return (v / 1e3).toFixed(0) + 'K';
    return v.toString();
}


/* ==========================================================
   9. 時段走勢分析
   ========================================================== */
async function runPeriodAnalysis() {
    if (!_periodStartDate) {
        alert(I18N.pa_click_hint || '請先點擊 K 線圖選擇開始日期');
        return;
    }

    var analyzeBtn = document.getElementById('pa-analyze-btn');
    var loadingEl = document.getElementById('pa-loading');
    var viewBtn = document.getElementById('pa-view-btn');

    analyzeBtn.classList.add('hidden');
    loadingEl.classList.remove('hidden');
    viewBtn.classList.add('hidden');
    _periodReportHtml = null;

    try {
        var paController = new AbortController();
        var paTimeoutId = setTimeout(function() { paController.abort(); }, 120000);
        var resp = await fetch('/api/price-analysis', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                symbol: getCurrentTicker(),
                start_date: _periodStartDate,
                lang: LANG,
            }),
            signal: paController.signal,
        });
        clearTimeout(paTimeoutId);
        var json = await resp.json();

        if (!json.success) {
            alert(json.error || 'Analysis failed');
            return;
        }

        _periodReportHtml = json.report;
        _periodStartDate = json.start_date;
        _periodEndDate = json.end_date;
        _periodEvents = json.events || [];

        // 更新日期顯示
        var endEl = document.getElementById('pa-end-date');
        if (endEl) endEl.textContent = _periodEndDate;

        viewBtn.classList.remove('hidden');

        // 渲染事件標記到圖表 + 事件列表
        _renderChartMarkers(_periodEvents);
        _renderEventList(_periodEvents);

    } catch (e) {
        alert(I18N.pa_error || '網路錯誤，請稍後重試');
    } finally {
        loadingEl.classList.add('hidden');
        analyzeBtn.classList.remove('hidden');
    }
}

function openPeriodReport() {
    if (!_periodReportHtml) return;
    var title = getCurrentTicker() + '  ' + _periodStartDate + ' → ' + _periodEndDate;
    var isMobile = window.matchMedia('(max-width: 640px)').matches;
    if (isMobile) {
        _openMobileModal('period_analysis', title, _periodReportHtml);
    } else {
        _openDesktopWindow('period_analysis', title, _periodReportHtml);
    }
}


/* --- 圖表事件標記 --- */
let _markersVisible = true;

function _renderChartMarkers(events) {
    if (!_candleSeries || !events || events.length === 0) return;
    _markersVisible = true;

    // 排序
    var sorted = events.slice().sort(function(a, b) {
        return a.date < b.date ? -1 : a.date > b.date ? 1 : 0;
    });

    var markers = sorted.map(function(evt, idx) {
        var isUp = evt.type === 'up';
        return {
            time: evt.date,
            position: isUp ? 'belowBar' : 'aboveBar',
            color: isUp ? '#26a69a' : '#ef5350',
            shape: isUp ? 'arrowUp' : 'arrowDown',
            text: String(idx + 1),
            size: 2.5,
        };
    });

    _candleSeries.setMarkers(markers);

    // 顯示清除按鈕
    var clearBtn = document.getElementById('chart-clear-markers');
    if (clearBtn) clearBtn.classList.remove('hidden');
}

function clearChartMarkers() {
    if (!_candleSeries) return;
    _candleSeries.setMarkers([]);
    _markersVisible = false;
    var clearBtn = document.getElementById('chart-clear-markers');
    if (clearBtn) clearBtn.classList.add('hidden');
    var showBtn = document.getElementById('chart-show-markers');
    if (showBtn && _periodEvents.length > 0) showBtn.classList.remove('hidden');
}

function showChartMarkers() {
    _renderChartMarkers(_periodEvents);
    _markersVisible = true;
    var showBtn = document.getElementById('chart-show-markers');
    if (showBtn) showBtn.classList.add('hidden');
}


/* --- 事件列表渲染 + 圖表聯動 --- */
function _renderEventList(events) {
    var container = document.getElementById('pa-events');
    var list = document.getElementById('pa-events-list');
    if (!container || !list) return;

    if (!events || events.length === 0) {
        container.classList.add('hidden');
        return;
    }

    // 排序一致
    var sorted = events.slice().sort(function(a, b) {
        return a.date < b.date ? -1 : a.date > b.date ? 1 : 0;
    });

    list.innerHTML = '';
    sorted.forEach(function(evt, idx) {
        var item = document.createElement('div');
        item.className = 'pa-event-item';
        item.dataset.date = evt.date;
        var dotClass = evt.type === 'up' ? 'up' : 'down';
        item.innerHTML =
            '<div class="pa-event-num ' + dotClass + '">' + (idx + 1) + '</div>' +
            '<div class="flex-1 min-w-0">' +
                '<div class="pa-event-date">' + escHtml(evt.date) + '</div>' +
                '<div class="pa-event-title">' + escHtml(evt.title) + '</div>' +
            '</div>';

        // Hover → 圖表跳到該日期
        item.addEventListener('mouseenter', function() {
            list.querySelectorAll('.pa-event-item').forEach(function(el) { el.classList.remove('active'); });
            item.classList.add('active');
            if (_chart) {
                _chart.timeScale().scrollToPosition(-_getBarOffset(evt.date), false);
            }
        });

        // 點擊 → 開啟報告
        item.addEventListener('click', function() {
            openPeriodReport();
        });

        list.appendChild(item);
    });

    container.classList.remove('hidden');
}


/* 計算某日期在圖表中的 bar offset（從右邊算） */
function _getBarOffset(dateStr) {
    if (!_chartData || _chartData.length === 0) return 0;
    for (var i = _chartData.length - 1; i >= 0; i--) {
        if (_chartData[i].time <= dateStr) {
            return _chartData.length - 1 - i - Math.floor(_chartData.length * 0.3);
        }
    }
    return 0;
}

/* ================================================================
   ETF Holders Panel
   ================================================================ */

let _etfPanelOpen = false;

function _fmtAum(val) {
    if (!val) return '—';
    if (val >= 1e12) return (val / 1e12).toFixed(1) + 'T';
    if (val >= 1e9)  return (val / 1e9).toFixed(1) + 'B';
    if (val >= 1e6)  return (val / 1e6).toFixed(1) + 'M';
    return val.toLocaleString();
}

async function loadEtfHolders(ticker) {
    const panel   = document.getElementById('etf-holders-panel');
    const content = document.getElementById('etf-panel-content');
    const badge   = document.getElementById('etf-count-badge');
    if (!panel || !content) return;

    // Reset state
    _etfPanelOpen = false;
    const body    = document.getElementById('etf-panel-body');
    const chevron = document.getElementById('etf-chevron');
    if (body)    body.classList.add('hidden');
    if (chevron) chevron.classList.remove('open');

    content.innerHTML = '<div class="etf-loading"><span class="loading loading-dots loading-xs"></span></div>';

    try {
        const resp = await fetch(`/api/etf-holders/${encodeURIComponent(ticker)}`);
        const data = await resp.json();
        const etfs = data.etfs || [];

        if (etfs.length === 0) {
            panel.classList.add('hidden');
            return;
        }

        panel.classList.remove('hidden');
        if (badge) {
            badge.textContent = etfs.length;
            badge.classList.remove('hidden');
        }

        // Find max weight for bar scaling
        const maxW = Math.max(...etfs.map(e => e.weight_pct || 0));

        const colSymbol = (I18N.etf_col_symbol || 'ETF');
        const colWeight = (I18N.etf_col_weight || 'Weight');
        const colAum    = (I18N.etf_col_aum    || 'AUM');

        let rows = '';
        etfs.forEach(e => {
            const wPct  = e.weight_pct != null ? e.weight_pct.toFixed(2) + '%' : '—';
            const barW  = maxW > 0 && e.weight_pct ? Math.round((e.weight_pct / maxW) * 80) : 0;
            const aum   = _fmtAum(e.aum);
            rows += `<tr class="etf-row-clickable" onclick="openEtfDetail('${escHtml(e.symbol)}')" title="查看 ${escHtml(e.symbol)} 持倉">
                <td>
                    <div class="etf-symbol">${escHtml(e.symbol)}</div>
                    <div class="etf-name">${escHtml(e.name || '')}</div>
                </td>
                <td>
                    <div class="etf-weight-bar-wrap">
                        <div class="etf-weight-bar" style="width:${barW}px"></div>
                        <span class="etf-weight-val">${wPct}</span>
                    </div>
                </td>
                <td class="etf-aum-val">${aum}
                    <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" style="margin-left:4px;opacity:.4"><polyline points="9 18 15 12 9 6"/></svg>
                </td>
            </tr>`;
        });

        content.innerHTML = `<table class="etf-table">
            <thead><tr>
                <th>${escHtml(colSymbol)}</th>
                <th>${escHtml(colWeight)}</th>
                <th style="text-align:right">${escHtml(colAum)}</th>
            </tr></thead>
            <tbody>${rows}</tbody>
        </table>`;

    } catch (e) {
        panel.classList.add('hidden');
    }
}

function toggleEtfPanel() {
    const body    = document.getElementById('etf-panel-body');
    const chevron = document.getElementById('etf-chevron');
    if (!body) return;
    _etfPanelOpen = !_etfPanelOpen;
    body.classList.toggle('hidden', !_etfPanelOpen);
    if (chevron) chevron.classList.toggle('open', _etfPanelOpen);
}

// Auto-load on page init
document.addEventListener('DOMContentLoaded', function() {
    const ticker = document.body.dataset.ticker;
    if (ticker) loadEtfHolders(ticker);
});


/* ================================================================
   ETF Detail Popup
   ================================================================ */
const ETFD_COLORS = [
    '#1e3a5f','#e74c3c','#2ecc71','#f39c12','#9b59b6',
    '#3498db','#e67e22','#1abc9c','#e84393','#00b894',
    '#fd79a8','#6c5ce7','#00cec9','#fdcb6e','#a29bfe',
];

function _etfdFmtNum(val) {
    if (!val) return '\u2014';
    if (val >= 1e12) return (val / 1e12).toFixed(2) + 'T';
    if (val >= 1e9)  return (val / 1e9).toFixed(2) + 'B';
    if (val >= 1e6)  return (val / 1e6).toFixed(2) + 'M';
    if (val >= 1e3)  return (val / 1e3).toFixed(1) + 'K';
    return val.toLocaleString();
}

function _buildEtfdDonut(holdings, othersPct) {
    const items = holdings.map((h, i) => ({
        label: h.name || h.asset, name: h.name || h.asset,
        pct: h.weight_pct || 0, color: ETFD_COLORS[i % ETFD_COLORS.length],
    }));
    if (othersPct > 0.01) items.push({ label: 'Others', name: 'Others', pct: othersPct, color: '#e0e0e0' });
    const total = items.reduce((s, d) => s + d.pct, 0) || 100;
    const cx = 90, cy = 90, R = 72, r = 44;
    let startAngle = -Math.PI / 2, paths = '';
    items.forEach(d => {
        const angle = (d.pct / total) * 2 * Math.PI;
        const endAngle = startAngle + angle;
        const x1 = cx + R * Math.cos(startAngle), y1 = cy + R * Math.sin(startAngle);
        const x2 = cx + R * Math.cos(endAngle),   y2 = cy + R * Math.sin(endAngle);
        const ix1 = cx + r * Math.cos(startAngle), iy1 = cy + r * Math.sin(startAngle);
        const ix2 = cx + r * Math.cos(endAngle),   iy2 = cy + r * Math.sin(endAngle);
        const large = angle > Math.PI ? 1 : 0;
        paths += `<path d="M${x1.toFixed(2)},${y1.toFixed(2)} A${R},${R} 0 ${large},1 ${x2.toFixed(2)},${y2.toFixed(2)} L${ix2.toFixed(2)},${iy2.toFixed(2)} A${r},${r} 0 ${large},0 ${ix1.toFixed(2)},${iy1.toFixed(2)} Z" fill="${d.color}" stroke="#fff" stroke-width="1.5"><title>${d.label}: ${d.pct.toFixed(2)}%</title></path>`;
        startAngle = endAngle;
    });
    const svg = `<svg width="180" height="180" viewBox="0 0 180 180">${paths}<circle cx="${cx}" cy="${cy}" r="${r - 1}" fill="#fff"/><text x="${cx}" y="${cy - 6}" text-anchor="middle" fill="#888" font-size="10" font-family="'JetBrains Mono',monospace">TOP ${holdings.length}</text><text x="${cx}" y="${cy + 9}" text-anchor="middle" fill="#1a1a1a" font-size="13" font-weight="700" font-family="'JetBrains Mono',monospace">Holdings</text></svg>`;
    const legendRows = items.map(d =>
        `<div class="etfd-legend-row"><div class="etfd-legend-dot" style="background:${d.color}"></div><div class="etfd-legend-name">${escHtml(d.label)}</div><div class="etfd-legend-pct">${d.pct.toFixed(2)}%</div></div>`
    ).join('');
    return `<div class="etfd-donut-wrap"><div class="etfd-donut-svg-wrap">${svg}</div><div class="etfd-legend">${legendRows}</div></div>`;
}

function _buildEtfdTable(holdings, othersPct, totalCount) {
    const maxW = Math.max(...holdings.map(h => h.weight_pct || 0), 0.001);
    let rows = holdings.map((h, i) => {
        const w = h.weight_pct || 0, barW = Math.round((w / maxW) * 70);
        const color = ETFD_COLORS[i % ETFD_COLORS.length];
        return `<tr><td><div class="etfd-asset">${escHtml(h.asset)}</div><div class="etfd-asset-name">${escHtml(h.name || '')}</div></td><td><div class="etfd-wt-bar-wrap"><div class="etfd-wt-bar" style="width:${barW}px;background:${color}"></div><span class="etfd-wt-val">${w.toFixed(2)}%</span></div></td><td class="etfd-mv">${_etfdFmtNum(h.market_value)}</td></tr>`;
    }).join('');
    if (othersPct > 0.01) {
        const remaining = totalCount - holdings.length;
        rows += `<tr class="etfd-others-row"><td colspan="2">+ ${remaining > 0 ? remaining + ' \u5176\u4ed6\u6301\u5009' : '\u5176\u4ed6'}</td><td class="etfd-mv">${othersPct.toFixed(2)}%</td></tr>`;
    }
    return `<table class="etfd-table"><thead><tr><th>\u6301\u5009\u80a1\u7968</th><th>\u6bd4\u91cd</th><th style="text-align:right">\u5e02\u5024</th></tr></thead><tbody>${rows}</tbody></table>`;
}

async function openEtfDetail(symbol) {
    const overlay = document.getElementById('etf-detail-overlay');
    if (!overlay) return;
    overlay.classList.remove('hidden');
    document.getElementById('etfd-symbol').textContent = symbol;
    document.getElementById('etfd-name').textContent   = '';
    document.getElementById('etfd-meta').innerHTML     = '';
    document.getElementById('etfd-chart-pane').innerHTML = '<div style="padding:40px;text-align:center;color:#aaa"><span class="loading loading-dots loading-sm"></span></div>';
    document.getElementById('etfd-table-pane').innerHTML = '';
    switchEtfTab(document.querySelector('.etf-dtab[data-tab="chart"]'), 'chart');
    try {
        const resp = await fetch(`/api/etf-detail/${encodeURIComponent(symbol)}`);
        const data = await resp.json();
        const etf  = data.etf || {}, holdings = data.holdings || [];
        document.getElementById('etfd-name').textContent = etf.name || symbol;
        const metaItems = [];
        if (etf.aum)           metaItems.push({ label: 'AUM',     val: _etfdFmtNum(etf.aum) });
        if (etf.expense_ratio) metaItems.push({ label: 'Expense', val: etf.expense_ratio.toFixed(2) + '%' });
        if (etf.asset_class)   metaItems.push({ label: 'Class',   val: etf.asset_class });
        document.getElementById('etfd-meta').innerHTML = metaItems.map(m =>
            `<div class="etf-meta-item"><span class="etf-meta-label">${escHtml(m.label)}</span><span class="etf-meta-val">${escHtml(m.val)}</span></div>`
        ).join('');
        if (holdings.length === 0) {
            const msg = '<div style="padding:40px;text-align:center;color:#aaa;font-size:13px">\u7121\u6301\u5009\u6578\u64da</div>';
            document.getElementById('etfd-chart-pane').innerHTML = msg;
            document.getElementById('etfd-table-pane').innerHTML = msg;
            return;
        }
        document.getElementById('etfd-chart-pane').innerHTML = _buildEtfdDonut(holdings, data.others_pct || 0);
        document.getElementById('etfd-table-pane').innerHTML = _buildEtfdTable(holdings, data.others_pct || 0, data.total_count || holdings.length);
    } catch (e) {
        document.getElementById('etfd-chart-pane').innerHTML = '<div style="padding:40px;text-align:center;color:#e74c3c;font-size:12px">\u8f09\u5165\u5931\u6557\uff0c\u8acb\u91cd\u8a66</div>';
    }
}

function closeEtfDetail(event) {
    if (event && event.target !== document.getElementById('etf-detail-overlay')) return;
    document.getElementById('etf-detail-overlay').classList.add('hidden');
}

function switchEtfTab(btn, tab) {
    document.querySelectorAll('.etf-dtab').forEach(b => b.classList.remove('active'));
    if (btn) btn.classList.add('active');
    document.getElementById('etfd-chart-pane').classList.toggle('hidden', tab !== 'chart');
    document.getElementById('etfd-table-pane').classList.toggle('hidden', tab !== 'table');
}

document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') { const o = document.getElementById('etf-detail-overlay'); if (o) o.classList.add('hidden'); }
});
