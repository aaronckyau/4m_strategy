/**
 * news_radar.js — 新聞投資雷達前端邏輯
 */
(function () {
    'use strict';

    var _lang = window._RADAR_LANG || 'zh_hk';
    var _t    = window._RADAR_T    || {};

    var _currentEvent = '';
    var _loadingTimer = null;
    var _loadingStep  = 0;
    var _loadingMessages = [
        _t.loading1 || '正在搜尋最新新聞…',
        _t.loading2 || '分析投資邏輯…',
        _t.loading3 || '整理投資建議…',
    ];

    // ── DOM refs ──────────────────────────────────────────────
    function el(id) { return document.getElementById(id); }

    // ── 設定熱門話題 ──────────────────────────────────────────
    window._radarSetTopic = function (text) {
        var input = el('radar-input');
        if (!input) return;
        input.value = text.trim();
        input.focus();
        window._radarAnalyze();
    };

    // ── 重新分析 ──────────────────────────────────────────────
    window._radarReanalyze = function () {
        if (!_currentEvent) return;
        el('radar-input').value = _currentEvent;
        _doAnalyze(_currentEvent, true);
    };

    // ── 主分析入口 ────────────────────────────────────────────
    window._radarAnalyze = function () {
        var input = el('radar-input');
        if (!input) return;
        var text = input.value.trim();
        if (!text) return;
        _doAnalyze(text, false);
    };

    function _doAnalyze(eventText, force) {
        _currentEvent = eventText;
        _setState('loading');
        _startLoadingCycle();

        fetch('/api/news-radar/analyze', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ event: eventText, lang: _lang, force_update: force }),
        })
        .then(function (res) { return res.json(); })
        .then(function (data) {
            _stopLoadingCycle();
            if (!data.success) {
                _setState('error', data.error || _t.error || '分析失敗，請稍後再試');
                return;
            }
            _renderResult(eventText, data);
            _setState('result');
        })
        .catch(function (err) {
            _stopLoadingCycle();
            _setState('error', _t.error || '分析失敗，請稍後再試');
            console.error('radar analyze error:', err);
        });
    }

    // ── 加載動畫循環 ──────────────────────────────────────────
    function _startLoadingCycle() {
        _loadingStep = 0;
        _setLoadingText(_loadingMessages[0]);
        _loadingTimer = setInterval(function () {
            _loadingStep = (_loadingStep + 1) % _loadingMessages.length;
            _setLoadingText(_loadingMessages[_loadingStep]);
        }, 2200);
    }

    function _stopLoadingCycle() {
        if (_loadingTimer) {
            clearInterval(_loadingTimer);
            _loadingTimer = null;
        }
    }

    function _setLoadingText(text) {
        var ltEl = el('radar-loading-text');
        if (ltEl) ltEl.textContent = text;
    }

    // ── 狀態切換 ──────────────────────────────────────────────
    function _setState(state, msg) {
        var emptyEl   = el('radar-empty');
        var loadingEl = el('radar-loading');
        var resultEl  = el('radar-result');
        var errorEl   = el('radar-error');
        var btnEl     = el('radar-submit-btn');

        _hide(emptyEl);
        _hide(loadingEl);
        _hide(resultEl);
        _hide(errorEl);

        if (btnEl) btnEl.disabled = (state === 'loading');

        if (state === 'loading')  { _show(loadingEl); }
        if (state === 'result')   { _show(resultEl); }
        if (state === 'error')    {
            _show(errorEl);
            var msgEl = el('radar-error-msg');
            if (msgEl && msg) msgEl.textContent = msg;
        }
    }

    function _show(el) { if (el) el.style.display = ''; }
    function _hide(el) { if (el) el.style.display = 'none'; }

    // ── 渲染結果 ──────────────────────────────────────────────
    function _renderResult(eventText, data) {
        var radar = data.radar;

        // 標題欄
        var labelEl = el('radar-event-label');
        if (labelEl) {
            labelEl.textContent = (radar && radar.event_title) ? radar.event_title : eventText;
        }

        // 事件摘要
        var summaryEl = el('radar-event-summary');
        if (summaryEl) {
            summaryEl.textContent = (radar && radar.event_summary) ? radar.event_summary
                : (data.summary || '');
        }

        // 影響力評分
        var scoreBadge = el('radar-score-badge');
        var scoreVal   = el('radar-score-value');
        if (radar && radar.score != null) {
            if (scoreVal) scoreVal.textContent = String(radar.score);
            if (scoreBadge) _show(scoreBadge);
        }

        if (!radar) {
            // fallback：只顯示純文字報告
            _renderReport(data.report_html);
            _hideScenarios();
            return;
        }

        // 情境 A
        _renderScenario('a', radar.scenario_a);
        // 情境 B
        _renderScenario('b', radar.scenario_b);

        // 時間軸 + 風險
        var metaRow = el('radar-meta-row');
        if (radar.timeline || radar.risk_note) {
            var timelineEl = el('radar-timeline');
            var riskEl     = el('radar-risk');
            if (timelineEl) timelineEl.textContent = radar.timeline || '—';
            if (riskEl)     riskEl.textContent     = radar.risk_note || '—';
            if (metaRow) _show(metaRow);
        }

        // 報告
        _renderReport(data.report_html);
    }

    function _renderScenario(side, scenario) {
        if (!scenario) return;

        var labelEl   = el('radar-label-'   + side);
        var sectorsEl = el('radar-sectors-' + side);
        var picksEl   = el('radar-picks-'   + side);

        if (labelEl)   labelEl.textContent   = scenario.label || '—';
        if (sectorsEl) sectorsEl.textContent  = (scenario.sectors || []).join(' · ');

        if (picksEl) {
            picksEl.innerHTML = '';
            (scenario.picks || []).forEach(function (pick) {
                picksEl.appendChild(_buildPickCard(pick));
            });
        }
    }

    function _buildPickCard(pick) {
        var card = document.createElement('a');
        card.className  = 'radar-pick-card';
        card.href       = '/' + encodeURIComponent(pick.ticker);
        card.title      = pick.name + ' — ' + pick.reason;

        var tickerEl = document.createElement('span');
        tickerEl.className   = 'radar-pick-ticker';
        tickerEl.textContent = pick.ticker;

        var infoEl = document.createElement('div');
        infoEl.className = 'radar-pick-info';

        var nameEl = document.createElement('div');
        nameEl.className   = 'radar-pick-name';
        nameEl.textContent = pick.name;

        var reasonEl = document.createElement('div');
        reasonEl.className   = 'radar-pick-reason';
        reasonEl.textContent = pick.reason;

        infoEl.appendChild(nameEl);
        infoEl.appendChild(reasonEl);

        var arrowEl = document.createElement('span');
        arrowEl.className = 'radar-pick-arrow';
        arrowEl.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>';

        card.appendChild(tickerEl);
        card.appendChild(infoEl);
        card.appendChild(arrowEl);

        return card;
    }

    function _renderReport(html) {
        var sectionEl  = el('radar-report-section');
        var contentEl  = el('radar-report-content');
        var bodyEl     = el('radar-report-body');
        var toggleEl   = el('radar-report-toggle');

        if (!html || !sectionEl) return;
        if (contentEl) contentEl.innerHTML = html;
        _show(sectionEl);

        // 預設折疊
        if (bodyEl)   bodyEl.classList.remove('open');
        if (toggleEl) toggleEl.classList.remove('open');
    }

    function _hideScenarios() {
        var scenariosEl = document.querySelector('.radar-scenarios');
        if (scenariosEl) _hide(scenariosEl);
    }

    // ── 折疊報告 ──────────────────────────────────────────────
    window._radarToggleReport = function () {
        var bodyEl     = el('radar-report-body');
        var toggleEl   = el('radar-report-toggle');
        var labelEl    = el('radar-report-toggle-label');

        if (!bodyEl) return;
        var isOpen = bodyEl.classList.toggle('open');
        if (toggleEl) toggleEl.classList.toggle('open', isOpen);
        if (labelEl) {
            labelEl.textContent = isOpen
                ? (_t.reportHide || '收起報告')
                : (_t.report || '完整分析報告');
        }
    };

    // ── Enter 鍵提交 ──────────────────────────────────────────
    document.addEventListener('DOMContentLoaded', function () {
        var input = el('radar-input');
        if (input) {
            input.addEventListener('keydown', function (e) {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    window._radarAnalyze();
                }
            });
        }
    });

}());
