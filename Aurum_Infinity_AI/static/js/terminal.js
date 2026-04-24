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

function extractBusinessBriefFromReport(htmlReport) {
    if (!htmlReport) return [];

    var temp = document.createElement('div');
    temp.innerHTML = sanitizeHtml(htmlReport);

    var paragraphs = Array.from(temp.querySelectorAll('p'))
        .map(function(node) { return (node.textContent || '').trim(); })
        .filter(function(text) { return text.length >= 40; });

    if (paragraphs.length >= 2) {
        return paragraphs.slice(0, 2);
    }

    var textBlocks = (temp.innerText || '')
        .split(/\n{2,}/)
        .map(function(text) { return text.replace(/\s+/g, ' ').trim(); })
        .filter(function(text) { return text.length >= 40; });

    return textBlocks.slice(0, 2);
}

function renderBusinessBrief(ticker) {
    var container = document.getElementById('business-model-brief');
    if (!container) return;

    var normalizedTicker = String(ticker || '').toUpperCase();
    var paragraphs = BUSINESS_MODEL_BRIEFS[normalizedTicker] || [];
    var reportParagraphs = extractBusinessBriefFromReport(analysisCache.biz);
    var loadingCopy = [
        'AI report is generating, please wait.',
        normalizedTicker ? (normalizedTicker + ' 商業模式摘要生成中，完成後會自動顯示。') : '商業模式摘要生成中，完成後會自動顯示。'
    ];

    container.innerHTML = '';
    container.classList.remove('is-loading');

    if (!paragraphs.length && reportParagraphs.length) {
        paragraphs = reportParagraphs;
    }

    if (!paragraphs.length) {
        paragraphs = loadingCopy;
        container.classList.add('is-loading');
    }

    paragraphs.slice(0, 2).forEach(function(text) {
        var p = document.createElement('p');
        p.className = 'business-model-copy';
        p.textContent = text;
        container.appendChild(p);
    });

    container.hidden = false;
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

const BUSINESS_MODEL_BRIEFS = {
    NVDA: [
        'NVIDIA 專門設計圖形處理器（GPU），這是一種專門用來處理大量複雜計算的晶片，現在幾乎所有大型人工智慧系統都需要依靠這些晶片來運作。它把這些晶片賣給雲端服務商、大型科技公司以及各種研究機構，讓他們能夠訓練和執行像是 ChatGPT 那樣的 AI 模型。這間公司的業務範圍已經從單純的遊戲顯示卡，擴展到了數據中心、自駕車技術以及工業模擬平台。',
        'NVIDIA 最厲害的地方在於它不只是賣硬體，而是建立了一套完整的軟體生態系統。開發者習慣了使用它的 CUDA 軟體平台來編寫程式，這讓競爭對手很難搶走它的客戶。這種硬體加軟體的組合，讓它在 AI 領域幾乎沒有對手，也讓它掌握了極高的定價權。'
    ],
};

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
    var ratingLoadingState = document.getElementById('rating-loading-state');
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

        // 即時顯示基於基本面的暫時星級（AI 回來後會覆蓋）
        var prelimStars = Math.round((avg / 10 * 5) * 2) / 2;
        prelimStars = Math.max(0.5, Math.min(5.0, prelimStars));

        var verdictEl = document.getElementById('rating-verdict');
        _renderStars(prelimStars, null, null, null, 'quality_only', avg);

        if (ratingLoadingState) ratingLoadingState.classList.add('hidden');
        ratingResult.classList.remove('hidden');

        // 呼叫 AI 生成判讀摘要 + 公允價值 + 最終星級（非阻塞）
        _fetchAiVerdict(scoreMap, verdictEl);
    }
}


/**
 * 呼叫後端 AI 生成詳細判定語，附帶 section 摘要讓 AI 解釋原因
 */
function _fetchAiVerdict(scoreMap, verdictEl) {
    if (verdictEl) {
        verdictEl.classList.add('verdict-loading');
        verdictEl.textContent = '正在生成綜合判讀摘要...';
    }

    var myRequestId = _fetchRequestId;

    // 取得現價
    var priceEl = document.getElementById('hero-price');
    var currentPrice = null;
    if (priceEl) {
        var priceText = priceEl.textContent.replace(/[^0-9.]/g, '');
        if (priceText) currentPrice = parseFloat(priceText);
    }

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
            current_price: currentPrice,
            lang: LANG
        })
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (myRequestId !== _fetchRequestId) return;
        if (verdictEl) verdictEl.classList.remove('verdict-loading');
        if (verdictEl && data.success && data.verdict) {
            verdictEl.textContent = data.verdict;
        }
        if (data.success && data.stars != null) {
            _renderStars(
                data.stars,
                data.fair_value,
                data.fair_value_basis,
                data.discount_pct,
                data.star_source,
                data.quality_score
            );
        }
    })
    .catch(function() {
        if (myRequestId !== _fetchRequestId) return;
        if (verdictEl) verdictEl.classList.remove('verdict-loading');
    });
}

function _renderStars(stars, fairValue, fairValueBasis, discountPct, starSource, qualityScore) {
    var starsEl = document.getElementById('rating-stars');
    var starsMeta = document.getElementById('rating-stars-meta');
    var fairValueEl = document.getElementById('rating-fair-value');
    var watchZoneEl = document.getElementById('rating-watch-zone');
    if (!starsEl) return;

    // 生成星星 HTML（支援半星）
    var full = Math.floor(stars);
    var half = (stars - full) >= 0.5 ? 1 : 0;
    var empty = 5 - full - half;
    var html = '';
    for (var i = 0; i < full; i++)  html += '<span class="star star-full">&#9733;</span>';
    if (half)                        html += '<span class="star star-half">&#9733;</span>';
    for (var j = 0; j < empty; j++) html += '<span class="star star-empty">&#9734;</span>';
    starsEl.innerHTML = html;

    // 顏色 class
    starsEl.className = 'rating-stars';
    if      (stars >= 4.5) starsEl.classList.add('stars-5');
    else if (stars >= 3.5) starsEl.classList.add('stars-4');
    else if (stars >= 2.5) starsEl.classList.add('stars-3');
    else if (stars >= 1.5) starsEl.classList.add('stars-2');
    else                   starsEl.classList.add('stars-1');

    // 公允價值 meta
    if (starsMeta) {
        var metaText = stars.toFixed(1) + ' / 5.0';
        if (discountPct !== null && discountPct !== undefined) {
            var pct = Number(discountPct);
            if (!Number.isNaN(pct)) {
                var sign = pct >= 0 ? '折讓' : '溢價';
                metaText += '　' + sign + ' ' + Math.abs(pct).toFixed(1) + '%';
            }
        }
        starsMeta.textContent = metaText;
    }

    if (fairValueEl && fairValue) {
        var basis = fairValueBasis ? '（' + fairValueBasis + '）' : '';
        fairValueEl.textContent = '公允價值 ' + fairValue + basis;
        fairValueEl.style.display = '';
    } else if (fairValueEl) {
        fairValueEl.textContent = '';
        fairValueEl.style.display = 'none';
    }

    if (watchZoneEl) {
        var ticker = getCurrentTicker();
        var numericDiscount = Number(discountPct);
        var numericFairValue = Number(fairValue);
        var currentPrice = _readCurrentHeroPrice();
        watchZoneEl.classList.remove('watch-positive', 'watch-negative');
        if (!Number.isNaN(numericDiscount) && stars >= 5 && numericDiscount >= 20) {
            watchZoneEl.textContent = _buildFiveStarWatchText(ticker, currentPrice, numericFairValue, numericDiscount, true);
            watchZoneEl.classList.add('watch-positive');
            watchZoneEl.style.display = '';
        } else if (!Number.isNaN(numericDiscount) && stars <= 1.5 && numericDiscount <= -30) {
            watchZoneEl.textContent = (I18N.rating_watch_negative || '{ticker} 現價高於公允價值 30% 以上，星級下調')
                .replace('{ticker}', ticker);
            watchZoneEl.classList.add('watch-negative');
            watchZoneEl.style.display = '';
        } else if (!Number.isNaN(numericFairValue) && numericFairValue > 0 && currentPrice) {
            watchZoneEl.textContent = _buildFiveStarWatchText(ticker, currentPrice, numericFairValue, null, false);
            watchZoneEl.classList.add('watch-positive');
            watchZoneEl.style.display = '';
        } else {
            watchZoneEl.textContent = '';
            watchZoneEl.style.display = 'none';
        }
    }
    _latestFairValue = fairValue != null && !Number.isNaN(Number(fairValue)) ? Number(fairValue) : null;
    _syncChartAnnotations();
}

function _readCurrentHeroPrice() {
    var priceEl = document.getElementById('hero-price');
    if (!priceEl) return null;
    var priceText = priceEl.textContent.replace(/[^0-9.]/g, '');
    if (!priceText) return null;
    var price = Number(priceText);
    return Number.isNaN(price) || price <= 0 ? null : price;
}

function _buildFiveStarWatchText(ticker, currentPrice, fairValue, currentDiscountPct, isInZone) {
    if (!fairValue || Number.isNaN(fairValue)) {
        return (I18N.rating_watch_positive || '{ticker} 進入 5 星觀察區間').replace('{ticker}', ticker);
    }

    if (isInZone) {
        var activeDiscount = Number(currentDiscountPct);
        var activeDiscountText = Number.isNaN(activeDiscount) ? '' : '折讓公允價值 ' + Math.abs(activeDiscount).toFixed(1) + '% → ';
        return activeDiscountText + '系統提示「' + ticker + ' 進入 5 星觀察區間」';
    }

    var fiveStarPrice = fairValue * 0.8;
    var movePct = currentPrice ? ((fiveStarPrice - currentPrice) / currentPrice) * 100 : null;
    var roundedPrice = _formatWatchPrice(fiveStarPrice);
    var moveText = movePct === null || Number.isNaN(movePct)
        ? ''
        : '（' + (movePct >= 0 ? '+' : '') + movePct.toFixed(0) + '%）';
    var discountAtTarget = ((fairValue - fiveStarPrice) / fairValue) * 100;
    var direction = currentPrice && fiveStarPrice < currentPrice ? '下跌' : '回落';

    return '若股價' + direction + '至 ' + roundedPrice + moveText +
        ' → 折讓公允價值 ' + discountAtTarget.toFixed(0) + '% → 自動升至 5 星 | 系統提示「' +
        ticker + ' 進入 5 星觀察區間」';
}

function _formatWatchPrice(value) {
    if (!value || Number.isNaN(value)) return '—';
    return '$' + (value >= 100 ? value.toFixed(0) : value.toFixed(2).replace(/\.00$/, ''));
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
    var ratingLoadingState = document.getElementById('rating-loading-state');
    if (ratingPanel) ratingPanel.style.display = '';
    if (ratingResult) ratingResult.classList.add('hidden');
    if (ratingLoadingState) ratingLoadingState.classList.remove('hidden');
    var watchZoneEl = document.getElementById('rating-watch-zone');
    if (watchZoneEl) {
        watchZoneEl.textContent = '';
        watchZoneEl.style.display = 'none';
        watchZoneEl.classList.remove('watch-positive', 'watch-negative');
    }
    _latestFairValue = null;
    _syncChartAnnotations();
}

function _formatTapeMoney(value, currency) {
    if (value == null || Number.isNaN(Number(value))) return '—';
    var num = Number(value);
    var sym = {'USD':'$','HKD':'HK$','CNY':'¥','JPY':'¥','GBP':'£','EUR':'€'}[currency] || '$';
    return sym + num.toFixed(2);
}

function _buildSparklineSvg(values, isPositive) {
    if (!Array.isArray(values) || values.length < 2) return '';
    var width = 64;
    var height = 24;
    var pad = 2;
    var min = Math.min.apply(null, values);
    var max = Math.max.apply(null, values);
    var range = max - min || 1;
    var points = values.map(function(value, index) {
        var x = pad + ((width - pad * 2) * index / (values.length - 1));
        var y = height - pad - (((value - min) / range) * (height - pad * 2));
        return x.toFixed(2) + ',' + y.toFixed(2);
    }).join(' ');
    var lineColor = isPositive ? '#22c55e' : '#f87171';
    var fillTop = isPositive ? 'rgba(34,197,94,0.18)' : 'rgba(248,113,113,0.16)';
    var fillBottom = 'rgba(255,255,255,0)';
    var areaPoints = points + ' ' + (width - pad) + ',' + (height - pad) + ' ' + pad + ',' + (height - pad);
    return '' +
        '<svg class="related-ticker-chip-sparkline" viewBox="0 0 ' + width + ' ' + height + '" aria-hidden="true" focusable="false">' +
            '<defs>' +
                '<linearGradient id="spark-fill-' + (isPositive ? 'up' : 'down') + '" x1="0" x2="0" y1="0" y2="1">' +
                    '<stop offset="0%" stop-color="' + fillTop + '"></stop>' +
                    '<stop offset="100%" stop-color="' + fillBottom + '"></stop>' +
                '</linearGradient>' +
            '</defs>' +
            '<polygon points="' + areaPoints + '" fill="url(#spark-fill-' + (isPositive ? 'up' : 'down') + ')"></polygon>' +
            '<polyline points="' + points + '" fill="none" stroke="' + lineColor + '" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"></polyline>' +
        '</svg>';
}

function _renderRelatedTickerTapeLoading() {
    var shell = document.getElementById('related-ticker-tape-shell');
    var track = document.getElementById('related-ticker-tape-track');
    var meta = document.getElementById('related-ticker-tape-meta');
    if (!shell || !track || !meta) return;
    shell.classList.add('is-loading');
    shell.classList.remove('is-empty');
    track.classList.remove('is-animated');
    meta.textContent = '載入中...';
    track.innerHTML = '<span class="related-ticker-tape-placeholder">正在載入相關股票...</span>';
}

function renderRelatedTickerTape(payload) {
    var shell = document.getElementById('related-ticker-tape-shell');
    var label = document.getElementById('related-ticker-tape-label');
    var meta = document.getElementById('related-ticker-tape-meta');
    var track = document.getElementById('related-ticker-tape-track');
    if (!shell || !label || !meta || !track) return;

    var base = payload && payload.base ? payload.base : {};
    var items = payload && Array.isArray(payload.items) ? payload.items : [];
    var industry = base.industry || '';
    var sector = base.sector || '';
    var labelParts = [];
    if (industry) labelParts.push(industry);
    labelParts.push('同業股票');
    label.textContent = labelParts.join(' · ');
    meta.textContent = '';
    shell.classList.remove('is-loading');

    if (!items.length) {
        shell.classList.add('is-empty');
        track.classList.remove('is-animated');
        track.innerHTML = '<span class="related-ticker-tape-placeholder">暫無可顯示的相關股票</span>';
        return;
    }

    shell.classList.remove('is-empty');
    var cards = items.map(function(item) {
        var pct = item.change_pct;
        var changeClass = pct == null ? 'is-flat' : (pct >= 0 ? 'is-up' : 'is-down');
        var pctText = pct == null ? '—' : ((pct >= 0 ? '+' : '') + pct.toFixed(2) + '%');
        var spark = _buildSparklineSvg(item.sparkline || [], pct == null ? true : pct >= 0);
        return (
            '<button class="related-ticker-chip" type="button" data-ticker="' + _escapeHtml(item.ticker) + '" data-name="' + _escapeHtml(item.display_name || item.ticker) + '">' +
                '<span class="related-ticker-chip-spark">' + spark + '</span>' +
                '<span class="related-ticker-chip-symbol">' + _escapeHtml(item.ticker) + '</span>' +
                '<span class="related-ticker-chip-price">' + _escapeHtml(_formatTapeMoney(item.price, item.currency)) + '</span>' +
                '<span class="related-ticker-chip-change ' + changeClass + '">' + _escapeHtml(pctText) + '</span>' +
            '</button>'
        );
    }).join('');

    var shouldAnimate = items.length >= 5;
    track.innerHTML = shouldAnimate ? (cards + cards) : cards;
    track.classList.toggle('is-animated', shouldAnimate);
    track.querySelectorAll('.related-ticker-chip').forEach(function(chip) {
        chip.addEventListener('click', function() {
            navigateToStock(chip.dataset.ticker || '', chip.dataset.name || chip.dataset.ticker || '');
        });
    });
}

function loadRelatedTickerTape() {
    var ticker = getCurrentTicker();
    if (!ticker) return;
    _renderRelatedTickerTapeLoading();
    fetch('/api/related-ticker-tape?ticker=' + encodeURIComponent(ticker) + '&lang=' + encodeURIComponent(LANG))
        .then(function(r) {
            return r.json().then(function(data) {
                if (!r.ok || !data.success) throw new Error(data.error || 'load_failed');
                return data;
            });
        })
        .then(function(data) {
            if (ticker !== getCurrentTicker()) return;
            renderRelatedTickerTape(data);
        })
        .catch(function() {
            if (ticker !== getCurrentTicker()) return;
            renderRelatedTickerTape({ base: {}, items: [] });
        });
}

function initStockSubview() {
    setStockSubview(_stockSubview, { skipScroll: true });
}

function initAiDecisionTerminal() {
    return;
}

function setStockSubview(view, options) {
    options = options || {};
    _stockSubview = view || 'overview';

    document.querySelectorAll('.stock-subnav-btn').forEach(function(btn) {
        btn.classList.toggle('is-active', btn.dataset.view === _stockSubview);
    });

    var overviewSection = document.getElementById('overview-section');
    var metricsSection = document.getElementById('key-metrics-section');
    var analysisSection = document.getElementById('ai-analysis-section');
    var chartSection = document.getElementById('chart-section');
    var marketChartCard = document.getElementById('market-chart-card');
    var priceTargetCard = document.getElementById('price-target-card');
    var paPanel = document.getElementById('pa-panel');
    var paBtn = document.getElementById('pa-toggle-btn');
    var clearMarkersBtn = document.getElementById('chart-clear-markers');
    var showMarkersBtn = document.getElementById('chart-show-markers');
    var drawingToolbar = document.getElementById('chart-drawing-toolbar');
    var drawingOverlay = document.getElementById('chart-drawing-overlay');
    var isForecastOnly = _stockSubview === 'forecast';
    var isAiAnalysis = _stockSubview === 'ai-analysis';
    var isChartView = _stockSubview === 'chart';
    var isOverview = _stockSubview === 'overview';

    if (overviewSection) overviewSection.hidden = !isOverview;
    if (metricsSection) metricsSection.hidden = !isOverview;
    if (analysisSection) analysisSection.hidden = !isAiAnalysis;
    if (chartSection) chartSection.hidden = !(isOverview || isChartView || isForecastOnly);
    if (marketChartCard) marketChartCard.hidden = isForecastOnly;
    if (priceTargetCard) priceTargetCard.hidden = !isForecastOnly;
    if (drawingToolbar) drawingToolbar.hidden = !isChartView;
    if (drawingOverlay) drawingOverlay.classList.toggle('is-active', isChartView && !!_chartDrawingTool);

    // 切換到圖表 tab 時自動載入形態資料
    if (isChartView && Object.keys(_patternData).length === 0) {
        loadPatterns(_selectedChartDays || 365, _patternDojiScalar);
    }
    if (!isChartView) {
        var bar = document.getElementById('pattern-tag-bar');
        if (bar) bar.classList.add('hidden');
    }

    if (paBtn) paBtn.hidden = !isChartView;
    if (clearMarkersBtn) clearMarkersBtn.hidden = !isChartView || !_markersVisible || _periodEvents.length === 0;
    if (showMarkersBtn) showMarkersBtn.hidden = !isChartView || _markersVisible || _periodEvents.length === 0;

    if (paPanel && isChartView) {
        paPanel.hidden = false;
    } else if (paPanel) {
        paPanel.hidden = true;
        paPanel.classList.remove('open');
        if (paBtn) paBtn.classList.remove('open');
    }

    if (isOverview || isChartView) {
        _setMainChartMode(isChartView ? 'chart' : 'overview');
        _syncVolumeSeries();
        _syncChartAnnotations();
        if (isChartView) _loadChartDrawings();
        else _renderChartDrawings();
    }

    setTimeout(function() {
        if (!isForecastOnly && _chart) {
            var chartHost = document.getElementById('ohlc-chart');
            var chartWrap = chartHost ? chartHost.parentElement : null;
            var width = chartWrap && chartWrap.clientWidth ? chartWrap.clientWidth : (chartHost ? chartHost.clientWidth : 0);
            if (width > 0) {
                _chart.applyOptions({ width: width });
                _applyChartSeriesData();
                _syncVolumeSeries();
                _syncChartAnnotations();
                _focusChartRange(_selectedChartDays);
            }
        }
        if (_stockSubview === 'forecast' && _priceTargetPayload) {
            _scheduleForecastChartRender();
        }
    }, 60);

    if (!options.skipScroll) {
        window.scrollTo({ top: 0, behavior: 'instant' });
    }
}


/* ==========================================================
   頁面載入：自動觸發全部分析模組
   ========================================================== */
window.onload = function () {
    renderBusinessBrief(getCurrentTicker());
    _analyzeAllSections(id => fetchSection(id));
    initOhlcChart();
    loadKeyMetrics();
    loadRelatedTickerTape();
    initStockSubview();
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
            if (sectionId === 'biz') {
                renderBusinessBrief(getCurrentTicker());
            }
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

            // 更新左側評分色條
            if (typeof updateScoreBar === 'function') updateScoreBar(sectionId, extractedScore);

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

    delete analysisCache.biz;
    renderBusinessBrief(code);
    _renderRelatedTickerTapeLoading();

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
    loadRelatedTickerTape();

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

    loadRelatedTickerTape();

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
    var ratingLoadingLabel = document.querySelector('.rating-loading-label');
    if (ratingLoadingLabel) ratingLoadingLabel.textContent = t.rating_score || '綜合評分';
    var ratingDisclaimer = document.querySelector('.rating-disclaimer');
    if (ratingDisclaimer) ratingDisclaimer.textContent = t.rating_disclaimer || '估值參考指數，不構成投資建議';

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
        if (el) { el.textContent = '···'; }
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

            // Hero 股價區塊
            if (typeof _updateHeroPrice === 'function') {
                _updateHeroPrice(d.price, d.change, d.change_pct, d.currency);
            }

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
     - 蠟燭圖
     - Crosshair 圖例（OHLC）
     - 天數切換按鈕（30/90/180/365/730）
     - 期間分析（漲跌幅、最高、最低）
   ========================================================== */
let _chart = null;
let _chartSeries = null;
let _candleSeries = null;
let _volumeSeries = null;
let _chartData = [];
let _periodReportHtml = null;
let _periodStartDate = null;
let _periodEndDate = null;
let _periodEvents = [];
let _priceTargetPayload = null;
let _stockSubview = 'overview';
let _forecastChartRenderFrame = 0;
let _chartMode = 'overview';
let _chartPriceLines = [];
let _latestFairValue = null;
let _selectedChartDays = 180;
let _chartDrawings = [];
let _chartDrawingTool = null;
let _selectedChartDrawingId = null;
let _chartDrawingDraft = null;
let _chartDrawingDrag = null;

// K 線形態偵測
let _patternData = {};
let _patternVisible = false;
let _patternDojiScalar = 0.1;
let _patternFetchController = null;
var _dojiSliderTimer = null;
let _patternActiveFilter = null; // null = 全部顯示，string = 只顯示此形態 code

function _getRenderableCanvasWidth(canvas, horizontalPadding) {
    if (!canvas) return 0;
    var parent = canvas.parentElement;
    var rawWidth = parent ? parent.clientWidth : canvas.clientWidth;
    var width = rawWidth - (horizontalPadding || 0);
    return width > 120 ? width : 0;
}

function _scheduleForecastChartRender() {
    if (_forecastChartRenderFrame) {
        cancelAnimationFrame(_forecastChartRenderFrame);
    }
    _forecastChartRenderFrame = requestAnimationFrame(function() {
        _forecastChartRenderFrame = 0;
        if (_stockSubview !== 'forecast' || !_priceTargetPayload) return;
        renderPriceTargetChart(_chartData, _priceTargetPayload);
        renderGradesHistoricalChart(_priceTargetPayload);
    });
}

function _formatLegendVolume(value) {
    if (value == null || Number.isNaN(Number(value))) return '—';
    var num = Number(value);
    if (num >= 1e9) return (num / 1e9).toFixed(2) + 'B';
    if (num >= 1e6) return (num / 1e6).toFixed(2) + 'M';
    if (num >= 1e3) return (num / 1e3).toFixed(1) + 'K';
    return String(Math.round(num));
}

function _findChartRowByTime(time) {
    if (!time || !_chartData || _chartData.length === 0) return null;
    for (var i = _chartData.length - 1; i >= 0; i--) {
        if (_chartData[i].time === time) return _chartData[i];
    }
    return null;
}

function _clearChartPriceLines() {
    if (!_candleSeries || !_chartPriceLines.length) return;
    _chartPriceLines.forEach(function(line) {
        try { _candleSeries.removePriceLine(line); } catch (e) {}
    });
    _chartPriceLines = [];
}

function _addChartPriceLine(value, options) {
    if (!_candleSeries || value == null || Number.isNaN(Number(value))) return;
    var line = _candleSeries.createPriceLine({
        price: Number(value),
        color: options.color,
        lineWidth: options.lineWidth || 2,
        lineStyle: options.lineStyle || LightweightCharts.LineStyle.Dashed,
        axisLabelVisible: true,
        title: options.title || '',
    });
    _chartPriceLines.push(line);
}

function _syncChartAnnotations() {
    _clearChartPriceLines();
    return;
}

function _syncVolumeSeries() {
    if (!_volumeSeries) return;
    if (_stockSubview !== 'chart' || !_chartData || _chartData.length === 0) {
        _volumeSeries.setData([]);
        return;
    }

    _volumeSeries.setData(_chartData.map(function(d) {
        return {
            time: d.time,
            value: Number(d.volume) || 0,
            color: d.close >= d.open ? 'rgba(38, 166, 154, 0.45)' : 'rgba(239, 83, 80, 0.45)',
        };
    }));
}

function _focusChartRange(days) {
    if (!_chart || !_chartData || _chartData.length === 0) return;
    _selectedChartDays = days || _selectedChartDays;
    var timeScale = _chart.timeScale();
    if (timeScale && typeof timeScale.setVisibleLogicalRange === 'function') {
        var barCount = _chartData.length;
        var from = Math.max(-2, barCount - Math.min(barCount, _selectedChartDays) - 2);
        var to = barCount + 2;
        timeScale.setVisibleLogicalRange({ from: from, to: to });
    } else {
        timeScale.fitContent();
    }
}

function _chartDrawingsStorageKey() {
    return 'chart-drawings:' + getCurrentTicker();
}

function _saveChartDrawings() {
    try {
        window.localStorage.setItem(_chartDrawingsStorageKey(), JSON.stringify(_chartDrawings));
    } catch (e) {}
}

function _loadChartDrawings() {
    try {
        var raw = window.localStorage.getItem(_chartDrawingsStorageKey());
        _chartDrawings = raw ? JSON.parse(raw) : [];
    } catch (e) {
        _chartDrawings = [];
    }
    _selectedChartDrawingId = null;
    _chartDrawingDraft = null;
    _chartDrawingDrag = null;
    _renderChartDrawings();
    _syncDrawingToolbarState();
}

function _normalizeChartTimeValue(timeValue) {
    if (!timeValue) return null;
    if (typeof timeValue === 'string') return timeValue;
    if (typeof timeValue === 'number') {
        var date = new Date(timeValue * 1000);
        return date.getUTCFullYear() + '-' + String(date.getUTCMonth() + 1).padStart(2, '0') + '-' + String(date.getUTCDate()).padStart(2, '0');
    }
    if (typeof timeValue === 'object' && timeValue.year && timeValue.month && timeValue.day) {
        return timeValue.year + '-' + String(timeValue.month).padStart(2, '0') + '-' + String(timeValue.day).padStart(2, '0');
    }
    return null;
}

function _chartXToTime(x) {
    if (!_chart) return null;
    return _normalizeChartTimeValue(_chart.timeScale().coordinateToTime(x));
}

function _chartYToPrice(y) {
    if (!_candleSeries) return null;
    var price = _candleSeries.coordinateToPrice(y);
    return price == null || Number.isNaN(Number(price)) ? null : Number(price);
}

function _chartTimeToX(time) {
    if (!_chart) return null;
    var x = _chart.timeScale().timeToCoordinate(time);
    return x == null || Number.isNaN(Number(x)) ? null : Number(x);
}

function _chartPriceToY(price) {
    if (!_candleSeries) return null;
    var y = _candleSeries.priceToCoordinate(price);
    return y == null || Number.isNaN(Number(y)) ? null : Number(y);
}

function _syncDrawingToolbarState() {
    ['trend', 'hline'].forEach(function(tool) {
        var btn = document.getElementById('chart-tool-' + tool);
        if (btn) btn.classList.toggle('is-active', _chartDrawingTool === tool);
    });
    var delBtn = document.getElementById('chart-tool-delete');
    if (delBtn) delBtn.disabled = !_selectedChartDrawingId;
}

function setChartDrawingTool(tool) {
    if (_stockSubview !== 'chart') return;
    _chartDrawingTool = _chartDrawingTool === tool ? null : tool;
    _chartDrawingDraft = null;
    _chartDrawingDrag = null;
    _syncDrawingToolbarState();
    _renderChartDrawings();
}

function deleteSelectedChartDrawing() {
    if (!_selectedChartDrawingId) return;
    _chartDrawings = _chartDrawings.filter(function(item) { return item.id !== _selectedChartDrawingId; });
    _selectedChartDrawingId = null;
    _saveChartDrawings();
    _renderChartDrawings();
    _syncDrawingToolbarState();
}

function clearAllChartDrawings() {
    _chartDrawings = [];
    _selectedChartDrawingId = null;
    _chartDrawingDraft = null;
    _chartDrawingDrag = null;
    _saveChartDrawings();
    _renderChartDrawings();
    _syncDrawingToolbarState();
}

function _getChartOverlaySize() {
    var overlay = document.getElementById('chart-drawing-overlay');
    if (!overlay) return null;
    var rect = overlay.getBoundingClientRect();
    return { width: rect.width, height: rect.height };
}

function _findChartDrawingById(id) {
    for (var i = 0; i < _chartDrawings.length; i++) {
        if (_chartDrawings[i].id === id) return _chartDrawings[i];
    }
    return null;
}

function _renderChartDrawings() {
    var overlay = document.getElementById('chart-drawing-overlay');
    if (!overlay) return;
    var isInteractive = _stockSubview === 'chart';
    overlay.classList.toggle('is-active', isInteractive && !!_chartDrawingTool);

    var size = _getChartOverlaySize();
    if (!size || !size.width || !size.height || _stockSubview !== 'chart') {
        overlay.innerHTML = '';
        return;
    }

    overlay.setAttribute('viewBox', '0 0 ' + size.width + ' ' + size.height);
    var html = '';
    if (_chartDrawingTool) {
        html += '<rect class="chart-drawing-capture" x="0" y="0" width="' + size.width + '" height="' + size.height + '"></rect>';
    }

    function renderTrend(draw, isDraft) {
        var x1 = _chartTimeToX(draw.start.time);
        var y1 = _chartPriceToY(draw.start.price);
        var x2 = _chartTimeToX(draw.end.time);
        var y2 = _chartPriceToY(draw.end.price);
        if ([x1, y1, x2, y2].some(function(v) { return v == null; })) return '';
        var selected = !isDraft && draw.id === _selectedChartDrawingId;
        var line = '<line class="chart-drawing-line' + (selected ? ' is-selected' : '') + '" data-role="line" data-id="' + (draw.id || 'draft') + '" x1="' + x1 + '" y1="' + y1 + '" x2="' + x2 + '" y2="' + y2 + '"></line>';
        if (!selected) return line;
        return line +
            '<circle class="chart-drawing-handle is-selected" data-role="handle-start" data-id="' + draw.id + '" cx="' + x1 + '" cy="' + y1 + '" r="5"></circle>' +
            '<circle class="chart-drawing-handle is-selected" data-role="handle-end" data-id="' + draw.id + '" cx="' + x2 + '" cy="' + y2 + '" r="5"></circle>';
    }

    function renderHLine(draw, isDraft) {
        var y = _chartPriceToY(draw.price);
        if (y == null) return '';
        var selected = !isDraft && draw.id === _selectedChartDrawingId;
        var line = '<line class="chart-drawing-line' + (selected ? ' is-selected' : '') + '" data-role="line" data-id="' + (draw.id || 'draft') + '" x1="0" y1="' + y + '" x2="' + size.width + '" y2="' + y + '"></line>';
        if (!selected) return line;
        return line + '<circle class="chart-drawing-handle is-selected" data-role="handle-hline" data-id="' + draw.id + '" cx="' + (size.width - 18) + '" cy="' + y + '" r="5"></circle>';
    }

    _chartDrawings.forEach(function(draw) {
        html += draw.type === 'trend' ? renderTrend(draw, false) : renderHLine(draw, false);
    });

    if (_chartDrawingDraft) {
        html += _chartDrawingDraft.type === 'trend' ? renderTrend(_chartDrawingDraft, true) : renderHLine(_chartDrawingDraft, true);
    }

    overlay.innerHTML = html;
}

function _updateDrawingPointFromCoords(target, x, y) {
    var time = _chartXToTime(x);
    var price = _chartYToPrice(y);
    if (!time || price == null) return false;
    target.time = time;
    target.price = Number(price.toFixed(2));
    return true;
}

function _updateHLineFromCoord(target, y) {
    var price = _chartYToPrice(y);
    if (price == null) return false;
    target.price = Number(price.toFixed(2));
    return true;
}

function _onChartDrawingPointerDown(event) {
    if (_stockSubview !== 'chart') return;
    var target = event.target;
    var overlay = document.getElementById('chart-drawing-overlay');
    if (!overlay) return;
    var rect = overlay.getBoundingClientRect();
    var x = event.clientX - rect.left;
    var y = event.clientY - rect.top;

    if (target && target.dataset && target.dataset.role) {
        var drawingId = target.dataset.id;
        _selectedChartDrawingId = drawingId;
        _syncDrawingToolbarState();
        if (target.dataset.role === 'handle-start' || target.dataset.role === 'handle-end' || target.dataset.role === 'handle-hline') {
            if (overlay.setPointerCapture) overlay.setPointerCapture(event.pointerId);
            _chartDrawingDrag = { id: drawingId, role: target.dataset.role };
            event.preventDefault();
            return;
        }
        if (target.dataset.role === 'line') {
            _renderChartDrawings();
            event.preventDefault();
            return;
        }
    }

    _selectedChartDrawingId = null;
    _syncDrawingToolbarState();

    if (_chartDrawingTool === 'trend') {
        if (!_chartDrawingDraft) {
            _chartDrawingDraft = {
                type: 'trend',
                start: { time: _chartXToTime(x), price: _chartYToPrice(y) },
                end: { time: _chartXToTime(x), price: _chartYToPrice(y) },
            };
        } else {
            if (_updateDrawingPointFromCoords(_chartDrawingDraft.end, x, y)) {
                _chartDrawings.push({
                    id: 'd' + Date.now(),
                    type: 'trend',
                    start: _chartDrawingDraft.start,
                    end: _chartDrawingDraft.end,
                });
                _chartDrawingDraft = null;
                _chartDrawingTool = null;
                _saveChartDrawings();
                _syncDrawingToolbarState();
            }
        }
        _renderChartDrawings();
        event.preventDefault();
        return;
    }

    if (_chartDrawingTool === 'hline') {
        var price = _chartYToPrice(y);
        if (price != null) {
            _chartDrawings.push({
                id: 'd' + Date.now(),
                type: 'hline',
                price: Number(price.toFixed(2)),
            });
            _chartDrawingTool = null;
            _saveChartDrawings();
            _syncDrawingToolbarState();
            _renderChartDrawings();
        }
        event.preventDefault();
        return;
    }
}

function _onChartDrawingPointerMove(event) {
    if (_stockSubview !== 'chart') return;
    var overlay = document.getElementById('chart-drawing-overlay');
    if (!overlay) return;
    var rect = overlay.getBoundingClientRect();
    var x = event.clientX - rect.left;
    var y = event.clientY - rect.top;

    if (_chartDrawingDrag) {
        var item = _findChartDrawingById(_chartDrawingDrag.id);
        if (!item) return;
        if (_chartDrawingDrag.role === 'handle-start') _updateDrawingPointFromCoords(item.start, x, y);
        if (_chartDrawingDrag.role === 'handle-end') _updateDrawingPointFromCoords(item.end, x, y);
        if (_chartDrawingDrag.role === 'handle-hline') _updateHLineFromCoord(item, y);
        _saveChartDrawings();
        _renderChartDrawings();
        return;
    }

    if (_chartDrawingDraft && _chartDrawingTool === 'trend') {
        _updateDrawingPointFromCoords(_chartDrawingDraft.end, x, y);
        _renderChartDrawings();
    }
}

function _onChartDrawingPointerUp(event) {
    var overlay = document.getElementById('chart-drawing-overlay');
    if (overlay && overlay.releasePointerCapture && event && event.pointerId != null) {
        try { overlay.releasePointerCapture(event.pointerId); } catch (e) {}
    }
    if (_chartDrawingDrag) {
        _chartDrawingDrag = null;
        _saveChartDrawings();
    }
}

function initOhlcChart() {
    const container = document.getElementById('ohlc-chart');
    if (!container || typeof LightweightCharts === 'undefined') return;
    const chartWrap = container.parentElement;
    const getChartWidth = function() {
        return Math.max(0, (chartWrap && chartWrap.clientWidth) ? chartWrap.clientWidth : container.clientWidth);
    };

    const _isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    _chart = LightweightCharts.createChart(container, {
        width: getChartWidth(),
        layout: {
            background: { type: 'solid', color: _isDark ? '#242428' : '#ffffff' },
            textColor: _isDark ? '#8b949e' : '#8a94a6',
            fontFamily: "'JetBrains Mono', monospace",
            fontSize: 11,
        },
        grid: {
            vertLines: { color: _isDark ? 'rgba(255,255,255,0.035)' : 'rgba(148,163,184,0.07)' },
            horzLines: { color: _isDark ? 'rgba(255,255,255,0.045)' : 'rgba(148,163,184,0.09)' },
        },
        crosshair: {
            mode: LightweightCharts.CrosshairMode.Normal,
            vertLine: {
                color: _isDark ? 'rgba(148,163,184,0.32)' : 'rgba(100,116,139,0.22)',
                labelBackgroundColor: _isDark ? '#334155' : '#64748b',
            },
            horzLine: {
                color: _isDark ? 'rgba(148,163,184,0.22)' : 'rgba(100,116,139,0.16)',
                labelBackgroundColor: _isDark ? '#334155' : '#64748b',
            },
        },
        rightPriceScale: {
            borderColor: 'transparent',
            scaleMargins: {
                top: 0.12,
                bottom: 0.16,
            },
        },
        timeScale: {
            borderColor: 'transparent',
            timeVisible: false,
            rightOffset: 5,
            fixLeftEdge: true,
            fixRightEdge: true,
        },
        handleScroll: { vertTouchDrag: false },
    });
    window._chart = _chart;

    _volumeSeries = _chart.addHistogramSeries({
        priceScaleId: '',
        priceFormat: { type: 'volume' },
        lastValueVisible: false,
        priceLineVisible: false,
    });
    _volumeSeries.priceScale().applyOptions({
        scaleMargins: {
            top: 0.74,
            bottom: 0.02,
        },
    });

    _setMainChartMode(_stockSubview === 'chart' ? 'chart' : 'overview');
    _loadChartDrawings();

    // Crosshair 圖例
    const legendEl = document.getElementById('chart-legend');
    _chart.subscribeCrosshairMove(function(param) {
        if (!legendEl) return;
        if (!param.time || !param.seriesData || param.seriesData.size === 0) {
            legendEl.classList.add('hidden');
            return;
        }
        const point = param.seriesData.get(_chartSeries);
        if (!point) { legendEl.classList.add('hidden'); return; }

        legendEl.classList.remove('hidden');
        document.getElementById('legend-date').textContent = param.time;
        var row = _findChartRowByTime(param.time);
        var changeEl = document.getElementById('legend-change');
        var volumeEl = document.getElementById('legend-volume');
        if (_chartMode === 'chart') {
            document.getElementById('legend-open').textContent = point.open.toFixed(2);
            document.getElementById('legend-high').textContent = point.high.toFixed(2);
            document.getElementById('legend-low').textContent = point.low.toFixed(2);
            document.getElementById('legend-close').textContent = point.close.toFixed(2);
            if (changeEl && row && row.open) {
                var candlePct = ((row.close - row.open) / row.open) * 100;
                changeEl.textContent = (candlePct >= 0 ? '+' : '') + candlePct.toFixed(2) + '%';
                changeEl.classList.toggle('is-up', candlePct >= 0);
                changeEl.classList.toggle('is-down', candlePct < 0);
            }
        } else {
            var priceText = point.value.toFixed(2);
            document.getElementById('legend-open').textContent = priceText;
            document.getElementById('legend-high').textContent = priceText;
            document.getElementById('legend-low').textContent = priceText;
            document.getElementById('legend-close').textContent = priceText;
            if (changeEl && row && row.open) {
                var linePct = ((row.close - row.open) / row.open) * 100;
                changeEl.textContent = (linePct >= 0 ? '+' : '') + linePct.toFixed(2) + '%';
                changeEl.classList.toggle('is-up', linePct >= 0);
                changeEl.classList.toggle('is-down', linePct < 0);
            }
        }
        if (volumeEl) volumeEl.textContent = _formatLegendVolume(row ? row.volume : null);
        var patternLegendEl = document.getElementById('legend-pattern');
        if (patternLegendEl) {
            var dateStr = (typeof param.time === 'string') ? param.time : null;
            if (dateStr && _patternVisible && _patternData[dateStr] && _patternData[dateStr].length > 0) {
                patternLegendEl.textContent = _patternData[dateStr].map(function(p) { return p.name_zh; }).join(' · ');
                patternLegendEl.classList.remove('hidden');
            } else {
                patternLegendEl.textContent = '';
                patternLegendEl.classList.add('hidden');
            }
        }
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
        if (_stockSubview !== 'chart' || !param.time) return;
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

    var drawingOverlay = document.getElementById('chart-drawing-overlay');
    if (drawingOverlay) {
        drawingOverlay.addEventListener('pointerdown', _onChartDrawingPointerDown);
        drawingOverlay.addEventListener('pointermove', _onChartDrawingPointerMove);
        drawingOverlay.addEventListener('pointerup', _onChartDrawingPointerUp);
        drawingOverlay.addEventListener('pointerleave', _onChartDrawingPointerUp);
    }
    if (_chart.timeScale() && typeof _chart.timeScale().subscribeVisibleTimeRangeChange === 'function') {
        _chart.timeScale().subscribeVisibleTimeRangeChange(function() {
            _renderChartDrawings();
        });
    }

    // Resize
    var resizeTimer;
    window.addEventListener('resize', function() {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(function() {
            const width = getChartWidth();
            if (_chart && width > 0) {
                _chart.applyOptions({ width: width });
                _focusChartRange(_selectedChartDays);
                _renderChartDrawings();
            }
            if (_priceTargetPayload) {
                renderPriceTargetChart(_chartData, _priceTargetPayload);
            }
        }, 100);
    });

    // 初始載入
    loadOhlcChart(180);
}

function loadOhlcChart(days) {
    var ticker = getCurrentTicker();
    if (!ticker) return;
    _selectedChartDays = days || _selectedChartDays;

    fetch('/api/ohlc?symbol=' + encodeURIComponent(ticker) + '&days=' + days)
        .then(function(r) { return r.json(); })
        .then(function(data) {
            var emptyEl = document.getElementById('chart-empty');
            if (!Array.isArray(data) || data.length === 0) {
                _chartData = [];
                if (_chartSeries) _chartSeries.setData([]);
                _syncVolumeSeries();
                _syncChartAnnotations();
                _updatePeriodInfo([]);
                loadPriceTargetChart([]);
                if (emptyEl) emptyEl.classList.remove('hidden');
                return;
            }
            if (emptyEl) emptyEl.classList.add('hidden');

            _chartData = data;
            _patternData = {};
            _patternActiveFilter = null;
            _patternVisible = false;
            _renderPatternTagBar();
            if (_stockSubview === 'chart') loadPatterns(_selectedChartDays, _patternDojiScalar);
            _loadChartDrawings();
            _setMainChartMode(_stockSubview === 'chart' ? 'chart' : 'overview');
            _applyChartSeriesData();
            _syncVolumeSeries();
            _syncChartAnnotations();

            _focusChartRange(_selectedChartDays);
            _updatePeriodInfo(data);
            loadPriceTargetChart(data);
        })
        .catch(function(err) {
            console.error('[Chart] Load error:', err);
            _renderPriceTargetEmpty(_translateForecastError('Unable to load price history'));
        });
}

function _setMainChartMode(mode) {
    if (!_chart) return;
    var nextMode = mode === 'chart' ? 'chart' : 'overview';
    if (_chartMode === nextMode && _chartSeries) {
        _applyChartSeriesData();
        _updateChartLegendVisibility();
        _syncVolumeSeries();
        _syncChartAnnotations();
        return;
    }

    if (_chartSeries) {
        _chart.removeSeries(_chartSeries);
    }

    _chartMode = nextMode;
    _candleSeries = null;

    if (_chartMode === 'chart') {
        _chartSeries = _chart.addCandlestickSeries({
            upColor: '#26a69a',
            downColor: '#ef5350',
            borderDownColor: '#ef5350',
            borderUpColor: '#26a69a',
            wickDownColor: '#ef5350',
            wickUpColor: '#26a69a',
        });
        _chartSeries.priceScale().applyOptions({
            scaleMargins: {
                top: 0.08,
                bottom: 0.30,
            },
        });
        _candleSeries = _chartSeries;
    } else {
        _chartSeries = _chart.addAreaSeries({
            lineColor: '#22c55e',
            lineWidth: 2,
            topColor: 'rgba(34, 197, 94, 0.18)',
            bottomColor: 'rgba(34, 197, 94, 0.01)',
            crosshairMarkerVisible: true,
            crosshairMarkerRadius: 3,
            crosshairMarkerBorderColor: '#22c55e',
            crosshairMarkerBackgroundColor: '#ffffff',
        });
        _chartSeries.priceScale().applyOptions({
            scaleMargins: {
                top: 0.12,
                bottom: 0.16,
            },
        });
    }

    _applyChartSeriesData();
    _updateChartLegendVisibility();
    _syncVolumeSeries();
    _syncChartAnnotations();
}

function _applyChartSeriesData() {
    if (!_chartSeries) return;
    if (!_chartData || _chartData.length === 0) {
        _chartSeries.setData([]);
        return;
    }
    if (_chartMode === 'chart') {
        _chartSeries.setData(_chartData.map(function(d) {
            return { time: d.time, open: d.open, high: d.high, low: d.low, close: d.close };
        }));
    } else {
        _chartSeries.setData(_chartData.map(function(d) {
            return { time: d.time, value: d.close };
        }));
    }
}

function _updateChartLegendVisibility() {
    var openVal = document.getElementById('legend-open');
    var highVal = document.getElementById('legend-high');
    var lowVal = document.getElementById('legend-low');
    var closeVal = document.getElementById('legend-close');
    var changeEl = document.getElementById('legend-change');
    var volumeEl = document.getElementById('legend-volume');
    if (openVal) openVal.textContent = '—';
    if (highVal) highVal.textContent = '—';
    if (lowVal) lowVal.textContent = '—';
    if (closeVal) closeVal.textContent = '—';
    if (changeEl) {
        changeEl.textContent = '—';
        changeEl.classList.remove('is-up', 'is-down');
    }
    if (volumeEl) volumeEl.textContent = '—';
}

function loadPriceTargetChart(priceHistory) {
    var ticker = getCurrentTicker();
    var summaryEl = document.getElementById('price-target-summary');
    var emptyEl = document.getElementById('price-target-empty');
    if (!ticker || !document.getElementById('price-target-chart')) return;

    var requestTicker = ticker;
    if (summaryEl) summaryEl.textContent = I18N.forecast_loading || '正在載入分析師預測...';
    if (emptyEl) emptyEl.classList.add('hidden');

    fetch('/api/analyst-forecast?symbol=' + encodeURIComponent(ticker) + '&lang=' + encodeURIComponent(_getCurrentLang()))
        .then(function(resp) {
            return resp.json().then(function(payload) {
                if (!resp.ok || !payload.success) {
                    throw new Error(payload.error || (I18N.forecast_empty || '暫無分析師預測數據'));
                }
                return payload;
            });
        })
        .then(function(payload) {
            if (requestTicker !== getCurrentTicker()) return;
            _priceTargetPayload = payload;
            renderPriceTargetChart(priceHistory || _chartData, payload);
            renderGradesHistoricalChart(payload);
            renderAnalystConsensus(payload);
            renderAnalystGradesList(payload);
            _syncChartAnnotations();
            if (_stockSubview === 'forecast') {
                _scheduleForecastChartRender();
            }
        })
        .catch(function(err) {
            if (requestTicker !== getCurrentTicker()) return;
            console.warn('[Analyst Forecast] Load error:', err);
            _priceTargetPayload = null;
            _syncChartAnnotations();
            _renderPriceTargetEmpty(_translateForecastError(err.message));
        });
}

function _renderPriceTargetEmpty(message) {
    var canvas = document.getElementById('price-target-chart');
    var gradesCanvas = document.getElementById('grades-history-chart');
    var summaryEl = document.getElementById('price-target-summary');
    var emptyEl = document.getElementById('price-target-empty');
    var gradesList = document.getElementById('analyst-grades-list');
    if (summaryEl) summaryEl.textContent = message || (I18N.forecast_empty || '暫無分析師預測數據');
    if (emptyEl) {
        emptyEl.textContent = message || (I18N.forecast_empty || '暫無分析師預測數據');
        emptyEl.classList.remove('hidden');
    }
    _setText('analyst-consensus-text', '—');
    _setText('analyst-strong-buy', '—');
    _setText('analyst-buy', '—');
    _setText('analyst-hold', '—');
    _setText('analyst-sell', '—');
    if (gradesList) gradesList.innerHTML = '<div class="analyst-grade-empty">' + (message || (I18N.forecast_empty || '暫無分析師預測數據')) + '</div>';
    if (!canvas) return;
    var ctx = canvas.getContext('2d');
    if (!ctx) return;
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    if (gradesCanvas) {
        var gradesCtx = gradesCanvas.getContext('2d');
        if (gradesCtx) gradesCtx.clearRect(0, 0, gradesCanvas.width, gradesCanvas.height);
    }
}

function renderPriceTargetChart(priceHistory, payload) {
    var canvas = document.getElementById('price-target-chart');
    var summaryEl = document.getElementById('price-target-summary');
    var emptyEl = document.getElementById('price-target-empty');
    if (!canvas || !payload) return;
    if (emptyEl) emptyEl.classList.add('hidden');

    var targetPayload = payload.price_targets || payload;
    var cssWidth = _getRenderableCanvasWidth(canvas, 0);
    if (!cssWidth) return;
    var dpr = window.devicePixelRatio || 1;
    var cssHeight = 250;
    canvas.style.width = '100%';
    canvas.style.height = cssHeight + 'px';
    canvas.width = Math.max(320, Math.floor(cssWidth * dpr));
    canvas.height = Math.floor(cssHeight * dpr);

    var ctx = canvas.getContext('2d');
    if (!ctx) return;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, cssWidth, cssHeight);

    var history = Array.isArray(priceHistory) ? priceHistory.slice(-252) : [];
    var closes = history.map(function(row) { return Number(row.close); }).filter(function(v) { return Number.isFinite(v); });
    var lastClose = Number(payload.last_close) || (closes.length ? closes[closes.length - 1] : null);
    var targets = [
        { key: 'target_high', label: I18N.forecast_label_high || '最高', value: Number(targetPayload.target_high), color: '#2563eb' },
        { key: 'target_avg', label: I18N.forecast_label_avg || '平均', value: Number(targetPayload.target_avg), color: '#64748b' },
        { key: 'target_low', label: I18N.forecast_label_low || '最低', value: Number(targetPayload.target_low), color: '#ef4444' },
    ].filter(function(item) { return Number.isFinite(item.value); });

    if (!targets.length || !Number.isFinite(lastClose)) {
        _renderPriceTargetEmpty(I18N.forecast_empty || '暫無分析師預測數據');
        return;
    }

    var values = closes.concat(targets.map(function(t) { return t.value; })).concat([lastClose]);
    var minVal = Math.min.apply(null, values);
    var maxVal = Math.max.apply(null, values);
    var padVal = Math.max((maxVal - minVal) * 0.12, maxVal * 0.03, 1);
    minVal -= padVal;
    maxVal += padVal;

    var pad = { left: 54, right: 126, top: 28, bottom: 34 };
    var chartW = Math.max(10, cssWidth - pad.left - pad.right);
    var chartH = cssHeight - pad.top - pad.bottom;
    var histW = chartW * 0.52;
    var forecastW = chartW - histW;
    var lastX = pad.left + histW;
    var yFor = function(value) {
        return pad.top + (maxVal - value) / (maxVal - minVal) * chartH;
    };
    var money = function(value) {
        if (!Number.isFinite(value)) return '--';
        return '$' + (Math.abs(value) >= 100 ? value.toFixed(0) : value.toFixed(2));
    };
    var pct = function(value) {
        if (!Number.isFinite(value) || !lastClose) return '';
        var p = ((value - lastClose) / lastClose) * 100;
        return (p >= 0 ? '+' : '') + p.toFixed(1) + '%';
    };

    ctx.font = '11px JetBrains Mono, monospace';
    ctx.lineWidth = 1;
    ctx.strokeStyle = 'rgba(148, 163, 184, 0.22)';
    ctx.fillStyle = 'rgba(100, 116, 139, 0.72)';
    for (var i = 0; i < 5; i++) {
        var y = pad.top + chartH * i / 4;
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(pad.left + chartW, y);
        ctx.stroke();
        var labelVal = maxVal - (maxVal - minVal) * i / 4;
        ctx.fillText(labelVal.toFixed(0), 14, y + 4);
    }

    ctx.strokeStyle = 'rgba(148, 163, 184, 0.28)';
    ctx.beginPath();
    ctx.moveTo(lastX, pad.top);
    ctx.lineTo(lastX, pad.top + chartH);
    ctx.stroke();

    if (history.length > 1) {
        ctx.beginPath();
        history.forEach(function(row, idx) {
            var x = pad.left + (idx / (history.length - 1)) * histW;
            var y = yFor(Number(row.close));
            if (idx === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });
        ctx.strokeStyle = '#1d4ed8';
        ctx.lineWidth = 2;
        ctx.stroke();
    }

    var lastY = yFor(lastClose);
    targets.forEach(function(target) {
        ctx.beginPath();
        ctx.setLineDash([6, 5]);
        ctx.moveTo(lastX, lastY);
        ctx.lineTo(lastX + forecastW, yFor(target.value));
        ctx.strokeStyle = target.color;
        ctx.lineWidth = 2;
        ctx.stroke();
        ctx.setLineDash([]);

        var y = yFor(target.value);
        ctx.fillStyle = target.color;
        ctx.font = '700 12px JetBrains Mono, monospace';
        ctx.fillText(target.label + ' ' + money(target.value), lastX + forecastW + 10, y - 4);
        ctx.font = '11px JetBrains Mono, monospace';
        ctx.fillText(pct(target.value), lastX + forecastW + 10, y + 12);
    });

    ctx.fillStyle = '#0f172a';
    ctx.beginPath();
    ctx.arc(lastX, lastY, 4, 0, Math.PI * 2);
    ctx.fill();

    ctx.fillStyle = 'rgba(100, 116, 139, 0.86)';
    ctx.font = '11px JetBrains Mono, monospace';
    ctx.fillText(I18N.forecast_past_12m || '過去 12 個月', pad.left + histW * 0.32, pad.top - 8);
    ctx.fillText(I18N.forecast_next_12m || '未來 12 個月預測', lastX + forecastW * 0.22, pad.top - 8);

    var count = targetPayload.analyst_count;
    var countText = Number.isFinite(Number(count))
        ? (I18N.forecast_analyst_count || '{count} 位分析師').replace('{count}', String(count))
        : (I18N.forecast_analyst_count || '{count} 位分析師').replace('{count}', '—');
    var asOf = payload.as_of
        ? (I18N.forecast_as_of || '（截至 {date}）').replace('{date}', payload.as_of)
        : '';
    var avg = Number(targetPayload.target_avg);
    var high = Number(targetPayload.target_high);
    var low = Number(targetPayload.target_low);
    if (summaryEl) {
        summaryEl.textContent = (I18N.forecast_summary || '基於 {count_text}{as_of} 的分析師預測。平均目標價 {avg}，最高 {high}，最低 {low}，相對最新收市價 {last_close} 為 {pct}。')
            .replace('{count_text}', countText)
            .replace('{as_of}', asOf)
            .replace('{avg}', money(avg))
            .replace('{high}', money(high))
            .replace('{low}', money(low))
            .replace('{last_close}', money(lastClose))
            .replace('{pct}', pct(avg));
    }
}

function renderAnalystConsensus(payload) {
    var consensus = (payload.grades && payload.grades.consensus) || {};
    _setText('analyst-consensus-text', consensus.consensus || '—');
    _setText('analyst-strong-buy', _formatCount(consensus.strong_buy));
    _setText('analyst-buy', _formatCount(consensus.buy));
    _setText('analyst-hold', _formatCount(consensus.hold));
    _setText('analyst-sell', _formatCount((consensus.sell || 0) + (consensus.strong_sell || 0)));
}

function renderGradesHistoricalChart(payload) {
    var canvas = document.getElementById('grades-history-chart');
    var tooltip = document.getElementById('grades-history-tooltip');
    var rows = payload.grades && Array.isArray(payload.grades.historical) ? payload.grades.historical : [];
    if (!canvas) return;
    var parent = canvas.parentElement;
    var cssWidth = _getRenderableCanvasWidth(canvas, 20);
    if (!cssWidth) return;
    var dpr = window.devicePixelRatio || 1;
    var cssHeight = 220;
    canvas.style.width = '100%';
    canvas.style.height = cssHeight + 'px';
    canvas.width = Math.max(320, Math.floor(cssWidth * dpr));
    canvas.height = Math.floor(cssHeight * dpr);
    var ctx = canvas.getContext('2d');
    if (!ctx) return;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, cssWidth, cssHeight);
    if (tooltip) tooltip.classList.add('hidden');
    if (!rows.length) {
        canvas.onmousemove = null;
        canvas.onmouseleave = null;
        return;
    }

    var pad = { left: 38, right: 10, top: 18, bottom: 28 };
    var chartW = cssWidth - pad.left - pad.right;
    var chartH = cssHeight - pad.top - pad.bottom;
    var keys = ['strong_buy', 'buy', 'hold', 'sell', 'strong_sell'];
    var maxTotal = Math.max.apply(null, rows.map(function(row) {
        return row.strong_buy + row.buy + row.hold + row.sell + row.strong_sell;
    })) || 1;
    var barW = Math.max(14, Math.min(38, chartW / rows.length - 10));
    var gap = rows.length > 1 ? (chartW - barW * rows.length) / (rows.length - 1) : 0;
    var colors = {
        strong_buy: '#16a34a',
        buy: '#4ade80',
        hold: '#f59e0b',
        sell: '#fb7185',
        strong_sell: '#dc2626'
    };
    var labels = {
        strong_buy: I18N.forecast_strong_buy || '強力買入',
        buy: I18N.forecast_buy || '買入',
        hold: I18N.forecast_hold || '持有',
        sell: I18N.forecast_sell || '賣出',
        strong_sell: I18N.forecast_strong_sell || '強力賣出'
    };
    var bars = rows.map(function(row, idx) {
        var x = pad.left + idx * (barW + gap);
        var total = keys.reduce(function(sum, key) { return sum + (Number(row[key]) || 0); }, 0);
        return { index: idx, x: x, width: barW, total: total, row: row, top: pad.top + chartH, bottom: pad.top + chartH };
    });

    function drawChart(hoverIndex) {
        ctx.clearRect(0, 0, cssWidth, cssHeight);

        for (var i = 0; i < 4; i++) {
            var y = pad.top + chartH * i / 3;
            ctx.beginPath();
            ctx.moveTo(pad.left, y);
            ctx.lineTo(pad.left + chartW, y);
            ctx.strokeStyle = 'rgba(148,163,184,0.18)';
            ctx.stroke();
        }

        rows.forEach(function(row, idx) {
            var bar = bars[idx];
            var isHover = hoverIndex === idx;
            var yBase = pad.top + chartH;
            bar.bottom = yBase;
            bar.top = yBase;

            if (isHover) {
                ctx.fillStyle = 'rgba(15, 23, 42, 0.06)';
                ctx.fillRect(bar.x - 4, pad.top - 4, bar.width + 8, chartH + 8);
            }

            keys.forEach(function(key) {
                var value = Number(row[key]) || 0;
                if (!value) return;
                var h = chartH * (value / maxTotal);
                yBase -= h;
                ctx.fillStyle = colors[key];
                ctx.fillRect(bar.x, yBase, bar.width, h);
                bar.top = Math.min(bar.top, yBase);
            });

            if (isHover && bar.total > 0) {
                ctx.strokeStyle = 'rgba(15, 23, 42, 0.28)';
                ctx.lineWidth = 1.5;
                ctx.strokeRect(bar.x - 0.5, bar.top - 0.5, bar.width + 1, (bar.bottom - bar.top) + 1);
                ctx.lineWidth = 1;
            }

            ctx.fillStyle = 'rgba(100,116,139,0.85)';
            ctx.font = '10px JetBrains Mono, monospace';
            ctx.textAlign = 'center';
            ctx.fillText((row.date || '').slice(5, 7) + '/' + (row.date || '').slice(2, 4), bar.x + bar.width / 2, cssHeight - 8);
        });
        ctx.textAlign = 'start';
    }

    function showTooltip(bar, clientX, clientY) {
        if (!tooltip) return;
        var row = bar.row || {};
        tooltip.innerHTML =
            '<div class="grades-tooltip-title">' + _escapeHtml(row.date || '—') + '</div>' +
            keys.map(function(key) {
                return '<div class="grades-tooltip-row">' +
                    '<span class="grades-tooltip-label"><i class="grades-legend-dot ' + key.replace('_', '-') + '"></i>' + _escapeHtml(labels[key]) + '</span>' +
                    '<strong class="grades-tooltip-value">' + _escapeHtml(String(Number(row[key]) || 0)) + '</strong>' +
                    '</div>';
            }).join('');
        tooltip.classList.remove('hidden');

        var wrapRect = parent.getBoundingClientRect();
        var left = clientX - wrapRect.left + 14;
        var top = clientY - wrapRect.top + 14;
        var tooltipWidth = tooltip.offsetWidth || 180;
        var tooltipHeight = tooltip.offsetHeight || 140;
        if (left + tooltipWidth > wrapRect.width - 8) left = wrapRect.width - tooltipWidth - 8;
        if (top + tooltipHeight > wrapRect.height - 8) top = wrapRect.height - tooltipHeight - 8;
        if (left < 8) left = 8;
        if (top < 8) top = 8;
        tooltip.style.left = left + 'px';
        tooltip.style.top = top + 'px';
    }

    function findHoveredBar(offsetX, offsetY) {
        if (offsetY < pad.top || offsetY > pad.top + chartH) return -1;
        for (var i = 0; i < bars.length; i++) {
            var bar = bars[i];
            if (offsetX >= bar.x - 4 && offsetX <= bar.x + bar.width + 4) {
                return i;
            }
        }
        return -1;
    }

    drawChart(-1);
    canvas._gradesHoverIndex = -1;
    canvas.onmousemove = function(event) {
        var rect = canvas.getBoundingClientRect();
        var hoverIndex = findHoveredBar(event.clientX - rect.left, event.clientY - rect.top);
        if (canvas._gradesHoverIndex !== hoverIndex) {
            canvas._gradesHoverIndex = hoverIndex;
            drawChart(hoverIndex);
        }
        if (hoverIndex >= 0) {
            showTooltip(bars[hoverIndex], event.clientX, event.clientY);
        } else if (tooltip) {
            tooltip.classList.add('hidden');
        }
    };
    canvas.onmouseleave = function() {
        canvas._gradesHoverIndex = -1;
        drawChart(-1);
        if (tooltip) tooltip.classList.add('hidden');
    };
}

function renderAnalystGradesList(payload) {
    var container = document.getElementById('analyst-grades-list');
    var rows = payload.grades && Array.isArray(payload.grades.latest) ? payload.grades.latest : [];
    if (!container) return;
    if (!rows.length) {
        container.innerHTML = '<div class="analyst-grade-empty">No analyst grades available.</div>';
        return;
    }
    container.innerHTML = rows.map(function(row) {
        var action = String(row.action || '').toLowerCase();
        var previous = row.previous_grade || '—';
        var next = row.new_grade || '—';
        return '<div class="analyst-grade-row">' +
            '<div class="analyst-grade-date">' + _escapeHtml(row.date || '—') + '</div>' +
            '<div class="analyst-grade-firm">' + _escapeHtml(row.grading_company || 'Unknown') + '</div>' +
            '<div class="analyst-grade-change">' + _escapeHtml(previous) + ' → ' + _escapeHtml(next) + '</div>' +
            '<div class="analyst-grade-action ' + _escapeHtml(action) + '">' + _escapeHtml(action || 'update') + '</div>' +
            '</div>';
    }).join('');
}

function _setText(id, value) {
    var el = document.getElementById(id);
    if (el) el.textContent = value;
}

function _formatCount(value) {
    return Number.isFinite(Number(value)) ? String(value) : '—';
}

function _escapeHtml(value) {
    return String(value == null ? '' : value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function _getCurrentLang() {
    return document.body.dataset.lang || 'zh_hk';
}

function _translateForecastError(message) {
    var text = String(message || '').trim();
    if (!text) return I18N.forecast_empty || '暫無分析師預測數據';
    if (text === 'No analyst forecast data') return I18N.forecast_empty || '暫無分析師預測數據';
    if (text === 'Failed to fetch analyst forecast data') return I18N.forecast_load_error || '無法載入分析師預測';
    if (text === 'Unable to load price history') return I18N.forecast_load_error || '無法載入分析師預測';
    return text;
}

function _localizeConsensus(value) {
    var key = String(value || '').trim().toLowerCase();
    if (!key) return '—';
    if (key === 'strong buy') return I18N.forecast_strong_buy || '強力買入';
    if (key === 'buy') return I18N.forecast_buy || '買入';
    if (key === 'hold') return I18N.forecast_hold || '持有';
    if (key === 'sell' || key === 'strong sell') return I18N.forecast_sell || '賣出';
    return value;
}

function _localizeAnalystAction(action) {
    var key = String(action || '').trim().toLowerCase();
    if (!key) return I18N.forecast_action_update || '更新';
    if (key === 'upgrade') return I18N.forecast_action_upgrade || '上調';
    if (key === 'downgrade') return I18N.forecast_action_downgrade || '下調';
    if (key === 'maintain') return I18N.forecast_action_maintain || '維持';
    if (key === 'reiterate') return I18N.forecast_action_reiterate || '重申';
    if (key === 'initiated') return I18N.forecast_action_initiated || '首次覆蓋';
    if (key === 'resumed') return I18N.forecast_action_resumed || '恢復覆蓋';
    return I18N.forecast_action_update || '更新';
}

function renderAnalystConsensus(payload) {
    var consensus = (payload.grades && payload.grades.consensus) || {};
    _setText('analyst-consensus-text', _localizeConsensus(consensus.consensus));
    _setText('analyst-strong-buy', _formatCount(consensus.strong_buy));
    _setText('analyst-buy', _formatCount(consensus.buy));
    _setText('analyst-hold', _formatCount(consensus.hold));
    _setText('analyst-sell', _formatCount((consensus.sell || 0) + (consensus.strong_sell || 0)));
}

function renderAnalystGradesList(payload) {
    var container = document.getElementById('analyst-grades-list');
    var rows = payload.grades && Array.isArray(payload.grades.latest) ? payload.grades.latest : [];
    if (!container) return;
    if (!rows.length) {
        container.innerHTML = '<div class="analyst-grade-empty">' + (I18N.forecast_no_grades || '暫無分析師評級資料') + '</div>';
        return;
    }
    container.innerHTML = rows.map(function(row) {
        var action = String(row.action || '').toLowerCase();
        var previous = row.previous_grade || '—';
        var next = row.new_grade || '—';
        return '<div class="analyst-grade-row">' +
            '<div class="analyst-grade-date">' + _escapeHtml(row.date || '—') + '</div>' +
            '<div class="analyst-grade-firm">' + _escapeHtml(row.grading_company || (I18N.forecast_unknown_firm || '未知機構')) + '</div>' +
            '<div class="analyst-grade-change">' + _escapeHtml(previous) + ' → ' + _escapeHtml(next) + '</div>' +
            '<div class="analyst-grade-action ' + _escapeHtml(action) + '">' + _escapeHtml(_localizeAnalystAction(action)) + '</div>' +
            '</div>';
    }).join('');
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
    if (_chartMode !== 'chart' || !_candleSeries || !events || events.length === 0) return;
    _markersVisible = true;
    // 顯示清除按鈕
    var clearBtn = document.getElementById('chart-clear-markers');
    if (clearBtn) clearBtn.classList.remove('hidden');
    _syncAllMarkers();
}

function clearChartMarkers() {
    if (!_candleSeries) return;
    _markersVisible = false;
    var clearBtn = document.getElementById('chart-clear-markers');
    if (clearBtn) clearBtn.classList.add('hidden');
    var showBtn = document.getElementById('chart-show-markers');
    if (showBtn && _periodEvents.length > 0) showBtn.classList.remove('hidden');
    _syncAllMarkers();
}

function showChartMarkers() {
    _markersVisible = true;
    var showBtn = document.getElementById('chart-show-markers');
    if (showBtn) showBtn.classList.add('hidden');
    _syncAllMarkers();
}

function _syncAllMarkers() {
    if (!_candleSeries) return;
    var all = [];

    // Period event markers
    if (_markersVisible && _periodEvents && _periodEvents.length > 0) {
        var sorted = _periodEvents.slice().sort(function(a, b) {
            return a.date < b.date ? -1 : a.date > b.date ? 1 : 0;
        });
        sorted.forEach(function(evt, idx) {
            var isUp = evt.type === 'up';
            all.push({
                time: evt.date,
                position: isUp ? 'belowBar' : 'aboveBar',
                color: isUp ? '#26a69a' : '#ef5350',
                shape: isUp ? 'arrowUp' : 'arrowDown',
                text: String(idx + 1),
                size: 2.5,
            });
        });
    }

    // Pattern markers
    if (_patternVisible && _patternData) {
        Object.keys(_patternData).forEach(function(date) {
            var patterns = _patternData[date];
            if (_patternActiveFilter) {
                patterns = patterns.filter(function(p) { return p.code === _patternActiveFilter; });
            }
            var m = _buildPatternMarker(date, patterns);
            if (m) all.push(m);
        });
    }

    // LightweightCharts requires markers sorted by time
    all.sort(function(a, b) { return a.time < b.time ? -1 : a.time > b.time ? 1 : 0; });
    _candleSeries.setMarkers(all);
}

function _buildPatternMarker(date, patterns) {
    if (!patterns || patterns.length === 0) return null;
    var hasBearish = patterns.some(function(p) { return p.direction === 'bearish'; });
    var hasBullish = patterns.some(function(p) { return p.direction === 'bullish'; });
    var direction = hasBearish ? 'bearish' : hasBullish ? 'bullish' : 'neutral';
    var text = patterns.map(function(p) { return _patternShortLabel(p.code); }).join('/');
    return {
        time: date,
        position: direction === 'bullish' ? 'belowBar' : 'aboveBar',
        color: direction === 'bearish' ? '#ef5350' : direction === 'bullish' ? '#26a69a' : '#C9A84C',
        shape: direction === 'bullish' ? 'arrowUp' : direction === 'bearish' ? 'arrowDown' : 'circle',
        text: text,
        size: 1.5,
    };
}

function _patternShortLabel(code) {
    var map = {
        doji: 'DJ', hammer: 'HM', inverted_hammer: 'IH',
        hanging_man: 'HN', shooting_star: 'SS',
        bullish_engulfing: 'BE↑', bearish_engulfing: 'BE↓',
        morning_star: 'MS', evening_star: 'ES',
    };
    return map[code] || code;
}

function loadPatterns(days, dojiScalar) {
    if (_patternFetchController) _patternFetchController.abort();
    _patternFetchController = new AbortController();
    var symbol = (typeof getCurrentTicker === 'function') ? getCurrentTicker() : '';
    if (!symbol) return;
    var url = '/api/ohlc-patterns?symbol=' + encodeURIComponent(symbol) +
              '&days=' + (days || 365) + '&doji_scalar=' + (dojiScalar || 0.1);
    fetch(url, { signal: _patternFetchController.signal })
        .then(function(r) { return r.json(); })
        .then(function(data) {
            _patternData = {};
            if (data && data.patterns) {
                Object.keys(data.patterns).forEach(function(date) {
                    _patternData[date] = data.patterns[date];
                });
            }
            _patternActiveFilter = null;
            _patternVisible = false;
            _renderPatternTagBar();
        })
        .catch(function(err) {
            if (err.name !== 'AbortError') console.error('loadPatterns error', err);
        });
}

function togglePatternDetection() {
    // 已由 tag bar 取代，保留函式避免殘留呼叫報錯
}

function onDojiScalarInput(rawValue) {
    var val = parseInt(rawValue);
    var display = document.getElementById('doji-scalar-val');
    if (display) display.textContent = val + '%';
    _patternDojiScalar = val / 100;
    clearTimeout(_dojiSliderTimer);
    _dojiSliderTimer = setTimeout(function() {
        _patternData = {};
        _patternActiveFilter = null;
        loadPatterns(_selectedChartDays || 365, _patternDojiScalar);
    }, 300);
}

var _PATTERN_TIPS = {
    doji: {
        signal: '中性',
        body: '開盤價與收盤價幾乎相同，K 線幾乎沒有實體。',
        meaning: '市場多空力量均衡，買賣雙方都無法主導，代表猶豫與不確定，通常出現在趨勢轉折前。',
    },
    hammer: {
        signal: '看漲',
        body: '下跌趨勢中出現，實體在上方，下影線很長（≥ 2 倍實體），上影線極短。',
        meaning: '盤中雖然大幅下跌，但收盤前買方強力反攻收回，顯示下方支撐強勁，可能止跌反彈。',
    },
    inverted_hammer: {
        signal: '看漲',
        body: '下跌趨勢中出現，實體在下方，上影線很長（≥ 2 倍實體），下影線極短。',
        meaning: '盤中買方嘗試拉高但失敗，但整體顯示多方開始試探，若隔日確認上漲則反轉訊號成立。',
    },
    hanging_man: {
        signal: '看跌',
        body: '上漲趨勢中出現，外形與錘子線相同——實體在上，下影線長。',
        meaning: '上漲過程中盤中大幅跳水，雖然最終收回，但顯示多方動能正在減弱，可能即將下跌。',
    },
    shooting_star: {
        signal: '看跌',
        body: '上漲趨勢中出現，實體在下方，上影線很長（≥ 2 倍實體），下影線極短。',
        meaning: '盤中大幅拉高但最終被空方壓回，顯示上方阻力強大，買方無力維持高價，可能見頂回落。',
    },
    bullish_engulfing: {
        signal: '看漲',
        body: '兩根 K 線：第一根為陰線，第二根為陽線，且陽線實體完全包住陰線實體。',
        meaning: '多方一舉吞噬前一日的跌幅，力道強勁，是下跌趨勢中常見的反轉訊號。',
    },
    bearish_engulfing: {
        signal: '看跌',
        body: '兩根 K 線：第一根為陽線，第二根為陰線，且陰線實體完全包住陽線實體。',
        meaning: '空方一舉吞噬前一日的漲幅，壓力巨大，是上漲趨勢中常見的反轉訊號。',
    },
    morning_star: {
        signal: '看漲',
        body: '三根 K 線：大陰線 → 小實體（十字或小蠟燭）→ 大陽線，第三根收盤超過第一根實體中點。',
        meaning: '下跌後市場進入猶豫，再由多方強力接手，是跌勢中最可靠的底部反轉訊號之一。',
    },
    evening_star: {
        signal: '看跌',
        body: '三根 K 線：大陽線 → 小實體（十字或小蠟燭）→ 大陰線，第三根收盤低於第一根實體中點。',
        meaning: '上漲後市場開始猶豫，再被空方強力壓制，是漲勢中最可靠的頂部反轉訊號之一。',
    },
};

function _renderPatternTagBar() {
    var bar = document.getElementById('pattern-tag-bar');
    if (!bar) return;

    // 若沒有資料，隱藏 bar
    if (!_patternData || Object.keys(_patternData).length === 0) {
        bar.classList.add('hidden');
        bar.innerHTML = '';
        return;
    }

    // 統計每個形態出現次數與方向
    var counts = {}; // code -> { count, direction, name_zh }
    Object.keys(_patternData).forEach(function(date) {
        _patternData[date].forEach(function(p) {
            if (!counts[p.code]) {
                counts[p.code] = { count: 0, direction: p.direction, name_zh: p.name_zh };
            }
            counts[p.code].count++;
        });
    });

    var codes = Object.keys(counts);
    if (codes.length === 0) {
        bar.classList.add('hidden');
        bar.innerHTML = '';
        return;
    }

    // 排序：bearish → bullish → neutral
    var dirOrder = { bearish: 0, bullish: 1, neutral: 2 };
    codes.sort(function(a, b) {
        return (dirOrder[counts[a].direction] || 2) - (dirOrder[counts[b].direction] || 2);
    });

    bar.innerHTML = '';
    codes.forEach(function(code) {
        var info = counts[code];
        var tip = _PATTERN_TIPS[code];
        var tag = document.createElement('div');
        tag.className = 'pattern-tag-wrap';

        var btn = document.createElement('button');
        btn.className = 'pattern-tag ' + info.direction;
        btn.dataset.code = code;
        if (_patternActiveFilter === code) btn.classList.add('is-active');
        btn.innerHTML =
            '<span class="pattern-tag-dot"></span>' +
            '<span>' + info.name_zh + '</span>' +
            '<span class="pattern-tag-count">×' + info.count + '</span>';
        btn.addEventListener('click', function() { _setPatternFilter(code); });

        if (tip) {
            var sigClass = info.direction === 'bearish' ? 'bearish' : info.direction === 'bullish' ? 'bullish' : 'neutral';
            var tooltip = document.createElement('div');
            tooltip.className = 'pattern-tooltip';
            tooltip.innerHTML =
                '<div class="pt-signal ' + sigClass + '">' + tip.signal + '</div>' +
                '<div class="pt-section">' +
                    '<div class="pt-label">K線形態</div>' +
                    '<div class="pt-text">' + tip.body + '</div>' +
                '</div>' +
                '<div class="pt-section">' +
                    '<div class="pt-label">代表意義</div>' +
                    '<div class="pt-text">' + tip.meaning + '</div>' +
                '</div>';
            tag.appendChild(btn);
            tag.appendChild(tooltip);
        } else {
            tag.appendChild(btn);
        }

        bar.appendChild(tag);
    });

    bar.classList.remove('hidden');
}

function _setPatternFilter(code) {
    var prev = _patternActiveFilter;
    // 點同一個 tag → 取消（隱藏 marker）；點不同 tag → 切換
    _patternActiveFilter = (prev === code) ? null : code;
    _patternVisible = (_patternActiveFilter !== null);

    // 更新 tag active 狀態
    var bar = document.getElementById('pattern-tag-bar');
    if (bar) {
        bar.querySelectorAll('.pattern-tag').forEach(function(tag) {
            tag.classList.toggle('is-active', tag.dataset.code === _patternActiveFilter);
        });
    }

    _syncAllMarkers();
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

/* ================================================================
   FOCUSED FLOW — New Layout Functions
   ================================================================ */

// PA Panel toggle (collapsible below chart)
function togglePaPanel() {
    var panel = document.getElementById('pa-panel');
    var btn   = document.getElementById('pa-toggle-btn');
    if (!panel) return;
    var isOpen = panel.classList.toggle('open');
    if (btn) btn.classList.toggle('open', isOpen);
    if (isOpen) {
        panel.classList.remove('hidden');
    }
}

// Update arc-score-bar color based on score value
function updateScoreBar(id, score) {
    var bar = document.getElementById('arc-bar-' + id);
    if (!bar) return;
    bar.className = 'arc-score-bar';
    if (score == null) return;
    var n = parseFloat(score);
    if (isNaN(n)) return;
    if (n >= 80) bar.classList.add('grade-a');
    else if (n >= 65) bar.classList.add('grade-b');
    else if (n >= 50) bar.classList.add('grade-c');
    else if (n >= 35) bar.classList.add('grade-d');
    else bar.classList.add('grade-f');
}

// Populate hero price from metrics data (called after loadKeyMetrics resolves)
function _updateHeroPrice(price, change, changePct, currency) {
    var csym = {'USD':'$','HKD':'HK$','CNY':'¥','JPY':'¥','GBP':'£','EUR':'€'}[currency] || '$';
    var priceEl  = document.getElementById('hero-price');
    var changeEl = document.getElementById('hero-change');
    if (priceEl && price != null) {
        priceEl.textContent = csym + parseFloat(price).toFixed(2);
    }
    if (changeEl) {
        var pct = changePct != null ? parseFloat(changePct) : null;
        if ((pct == null || Number.isNaN(pct)) && change != null && price != null) {
            var currentPrice = parseFloat(price);
            var dailyChange = parseFloat(change);
            var previousPrice = currentPrice - dailyChange;
            if (previousPrice) {
                pct = dailyChange / Math.abs(previousPrice) * 100;
            }
        }

        if (pct != null && !Number.isNaN(pct)) {
            var sign = pct >= 0 ? '+' : '';
            changeEl.textContent = sign + pct.toFixed(2) + '%';
            changeEl.style.color = pct >= 0 ? 'var(--sentiment-up, #2D9160)' : 'var(--sentiment-down, #c45542)';
        } else {
            changeEl.textContent = '';
            changeEl.removeAttribute('style');
        }
    }
}
