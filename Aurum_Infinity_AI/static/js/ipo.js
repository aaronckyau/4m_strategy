/* ================================================================
   ipo.js - IPO 追蹤頁面互動邏輯
   ================================================================
   函數索引：
     1. openIpoReport()       — 開啟分析報告彈窗
     2. closeIpoReport()      — 關閉彈窗
     3. toggleIpoItem()       — 手機列表展開/收合
     4. toggleListedSections() — 桌面半新股報告行展開/收合
     5. openListedDetail()    — 手機半新股詳情 Modal
     6. _initListedSort()     — 半新股桌面表格排序
   ================================================================ */


/* ==========================================================
   6. 半新股桌面表格排序
   ========================================================== */
(function _initListedSort() {
    var table = document.getElementById('listed-table');
    if (!table) return;

    var headers = table.querySelectorAll('th.sortable');
    var _sortCol = -1;
    var _sortAsc = true;

    headers.forEach(function(th) {
        th.style.cursor = 'pointer';
        th.addEventListener('click', function() {
            var col  = parseInt(th.getAttribute('data-col'));
            var type = th.getAttribute('data-type') || 'text';

            // 切換方向
            if (_sortCol === col) {
                _sortAsc = !_sortAsc;
            } else {
                _sortCol = col;
                _sortAsc = true;
            }

            // 更新箭頭
            headers.forEach(function(h) {
                h.querySelector('.sort-arrow').textContent = '';
            });
            th.querySelector('.sort-arrow').textContent = _sortAsc ? ' ▲' : ' ▼';

            // 收集資料行（跳過 sections-row）
            var tbody = table.querySelector('tbody');
            var dataRows = tbody.querySelectorAll('tr.listed-data-row');
            var pairs = []; // [{dataRow, sectionRow}]

            dataRows.forEach(function(row) {
                var idx = row.getAttribute('data-idx');
                var secRow = document.getElementById('listed-row-' + idx);
                pairs.push({ dr: row, sr: secRow });
            });

            // 排序
            pairs.sort(function(a, b) {
                var cellA = a.dr.children[col];
                var cellB = b.dr.children[col];
                var va = (cellA.getAttribute('data-sv') || cellA.textContent).trim();
                var vb = (cellB.getAttribute('data-sv') || cellB.textContent).trim();

                if (type === 'num') {
                    va = parseFloat(va) || -9999;
                    vb = parseFloat(vb) || -9999;
                    return _sortAsc ? va - vb : vb - va;
                }
                if (type === 'date') {
                    // YYYY/MM/DD → 字串比較即可
                    if (va < vb) return _sortAsc ? -1 : 1;
                    if (va > vb) return _sortAsc ? 1 : -1;
                    return 0;
                }
                // text
                return _sortAsc ? va.localeCompare(vb, 'zh-Hant') : vb.localeCompare(va, 'zh-Hant');
            });

            // 重新插入 DOM
            pairs.forEach(function(p) {
                tbody.appendChild(p.dr);
                if (p.sr) tbody.appendChild(p.sr);
            });
        });
    });
})();


/* ==========================================================
   1. openIpoReport — 開啟 IPO 分析報告
   ========================================================== */
function openIpoReport(ticker, companyName, sectionKey, sectionName) {
    var ipoData = window.IPO_DATA || {};
    var stock   = ipoData[ticker];
    if (!stock || !stock.sections[sectionKey]) return;

    // 優先使用預渲染的 HTML，fallback 到純文字
    var htmlContent = (stock.sections_html || {})[sectionKey];
    var isHtml = !!htmlContent;
    var content = htmlContent || stock.sections[sectionKey];
    var title   = companyName + ' — ' + sectionName;
    var tag     = '// ' + ticker;

    var isMobile = window.matchMedia('(max-width: 640px)').matches;

    if (isMobile) {
        _openIpoMobileModal(title, tag, content, isHtml);
    } else {
        _openIpoDesktopWindow(title, tag, content, isHtml);
    }
}


/* ── 桌面：彈出窗 ── */
function _openIpoDesktopWindow(title, tag, content, isHtml) {
    // 移除已有的彈窗
    closeIpoReport();

    // 背景遮罩
    var overlay = document.createElement('div');
    overlay.className = 'ipo-overlay';
    overlay.id = 'ipo-overlay';
    overlay.onclick = closeIpoReport;
    document.body.appendChild(overlay);

    // 彈窗
    var win = document.createElement('div');
    win.className = 'ipo-report-window';
    win.id = 'ipo-report-window';
    win.innerHTML =
        '<div class="ipo-report-header">' +
            '<div>' +
                '<span class="ipo-report-title">' + _escHtml(title) + '</span>' +
                '<span class="ipo-report-tag">' + _escHtml(tag) + '</span>' +
            '</div>' +
            '<button class="ipo-report-close" onclick="closeIpoReport()">' +
                '<svg viewBox="0 0 16 16" width="16" height="16"><line x1="3" y1="3" x2="13" y2="13" stroke="currentColor" stroke-width="2"/><line x1="13" y1="3" x2="3" y2="13" stroke="currentColor" stroke-width="2"/></svg>' +
            '</button>' +
        '</div>' +
        '<div class="ipo-report-body window-body"></div>';

    var bodyEl = win.querySelector('.ipo-report-body');
    if (isHtml) {
        bodyEl.innerHTML = sanitizeHtml(content);
    } else {
        bodyEl.textContent = content;
    }
    document.body.appendChild(win);

    // ESC 關閉
    document.addEventListener('keydown', _ipoEscHandler);
}


/* ── 手機：DaisyUI Modal ── */
function _openIpoMobileModal(title, tag, content, isHtml) {
    var modal = document.getElementById('ipo-mobile-modal');
    var titleEl = document.getElementById('ipo-modal-title');
    var tagEl   = document.getElementById('ipo-modal-tag');
    var bodyEl  = document.getElementById('ipo-modal-body');

    titleEl.textContent = title;
    tagEl.textContent   = tag;

    if (isHtml) {
        bodyEl.innerHTML = sanitizeHtml(content);
        bodyEl.classList.add('window-body');
        bodyEl.style.whiteSpace = '';
    } else {
        bodyEl.textContent = content;
        bodyEl.classList.remove('window-body');
        bodyEl.style.whiteSpace = 'pre-wrap';
    }

    modal.showModal();
}


/* ==========================================================
   2. closeIpoReport — 關閉桌面彈窗
   ========================================================== */
function closeIpoReport() {
    var win     = document.getElementById('ipo-report-window');
    var overlay = document.getElementById('ipo-overlay');
    if (win)     win.remove();
    if (overlay) overlay.remove();
    document.removeEventListener('keydown', _ipoEscHandler);
}

function _ipoEscHandler(e) {
    if (e.key === 'Escape') closeIpoReport();
}


/* ==========================================================
   3. toggleIpoItem — 手機列表展開/收合
   ========================================================== */
function toggleIpoItem(id) {
    var item = document.getElementById('ipo-' + id);
    if (!item) return;
    item.classList.toggle('expanded');
}


/* ==========================================================
   4. toggleListedSections — 桌面半新股報告行展開/收合
   ========================================================== */
function toggleListedSections(rowId) {
    var row = document.getElementById(rowId);
    if (!row) return;
    row.style.display = row.style.display === 'none' ? '' : 'none';
}


/* ==========================================================
   5. openListedDetail — 手機半新股晶片卡 → 底部 Modal
   ========================================================== */
function openListedDetail(index) {
    var data = (window.LISTED_DATA || [])[index];
    if (!data) return;

    var modal   = document.getElementById('ipo-mobile-modal');
    var titleEl = document.getElementById('ipo-modal-title');
    var tagEl   = document.getElementById('ipo-modal-tag');
    var bodyEl  = document.getElementById('ipo-modal-body');

    titleEl.textContent = data.company_name;
    tagEl.textContent   = '// ' + data.ticker;

    // 組裝內容
    var perfClass = '';
    if (data.first_day_performance.charAt(0) === '+') perfClass = ' style="color:#16a34a"';
    else if (data.first_day_performance.charAt(0) === '-') perfClass = ' style="color:#dc2626"';

    var html = '<div class="listed-detail-grid">' +
        _detailRow('上市日', data.listing_date) +
        _detailRow('招股價', data.offer_price) +
        _detailRow('上市價', data.listing_price) +
        _detailRow('超額倍數', data.oversubscription) +
        _detailRow('穩中一手', data.one_lot_chance) +
        _detailRow('中籤率', data.ballot_rate) +
        '<div class="listed-detail-row"><span>首日表現</span><span' + perfClass + '>' + _escHtml(data.first_day_performance) + '</span></div>' +
        '</div>';

    // Section 按鈕
    html += '<div class="listed-detail-sections">';
    var keys = Object.keys(data.sections);
    for (var i = 0; i < keys.length; i++) {
        var k = keys[i];
        var s = data.sections[k];
        var cls = s.has ? ' has-data' : '';
        var dis = s.has ? '' : ' disabled';
        var scoreText = s.score !== null ? s.score.toFixed(1) : '—';
        html += '<button class="ipo-section-btn' + cls + '"' + dis +
                ' onclick="openIpoReport(\'' + _escAttr(data.ticker) + '\',\'' + _escAttr(data.company_name) + '\',\'' + _escAttr(k) + '\',\'' + _escAttr(s.name) + '\')">' +
                '<span class="ipo-section-name">' + _escHtml(s.name) + '</span>' +
                '<span class="ipo-section-score">' + scoreText + '</span></button>';
    }
    html += '</div>';

    bodyEl.innerHTML = html;
    modal.showModal();
}

function _detailRow(label, value) {
    return '<div class="listed-detail-row"><span>' + label + '</span><span>' + _escHtml(value) + '</span></div>';
}


// HTML 跳脫 — 已移至 utils.js（escHtml, escAttr, sanitizeHtml）
// 保留別名以相容現有呼叫
var _escHtml = escHtml;
var _escAttr = escAttr;
