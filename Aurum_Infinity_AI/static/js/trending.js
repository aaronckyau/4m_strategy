(function () {
    if (document.body.dataset.page !== "trending") return;

    const state = {
        activeSymbol: null,
        activeSource: "trending",
        trending: [],
        searchResults: [],
    };

    function formatNumber(value) {
        if (typeof value !== "number" || Number.isNaN(value)) return "--";
        return new Intl.NumberFormat("zh-Hant").format(value);
    }

    function formatSignedNumber(value) {
        if (typeof value !== "number" || Number.isNaN(value)) return "--";
        return `${value > 0 ? "+" : ""}${formatNumber(value)}`;
    }

    function formatPercent(value) {
        if (typeof value !== "number" || Number.isNaN(value)) return null;
        const rounded = Math.round(value * 10) / 10;
        return `${rounded > 0 ? "+" : ""}${rounded}%`;
    }

    function escapeHtml(value = "") {
        return String(value)
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;")
            .replaceAll("'", "&#39;");
    }

    function formatTime(value) {
        if (!value) return "時間未知";
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return value;
        return new Intl.DateTimeFormat("zh-Hant", {
            month: "numeric",
            day: "numeric",
            hour: "2-digit",
            minute: "2-digit",
        }).format(date);
    }

    async function fetchJson(url) {
        const response = await fetch(url);
        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.detail || data.error || "API request failed");
        }
        return data;
    }

    function setMetric(key, value) {
        const node = document.querySelector(`[data-metric="${key}"]`);
        if (node) node.textContent = value;
    }

    function setCoverageNote(value) {
        const node = document.querySelector('[data-metric="coverage-note"]');
        if (node) node.textContent = value;
    }

    function setStatus(id, text) {
        const node = document.getElementById(id);
        if (node) node.textContent = text;
    }

    function avatarMarkup(user) {
        const avatar = user?.avatar_url || "";
        const name = user?.name || user?.username || "Unknown";
        if (avatar) {
            return `<img src="${escapeHtml(avatar)}" alt="${escapeHtml(name)}" loading="lazy" referrerpolicy="no-referrer">`;
        }
        return `<span class="trending-avatar-fallback" aria-hidden="true">${escapeHtml(name.slice(0, 1).toUpperCase())}</span>`;
    }

    function getHistoryBadgeMarkup(history, key) {
        const entry = history?.[key];
        if (!entry || !entry.has_data) {
            return `<span class="trending-history-badge muted">${key.toUpperCase()} --</span>`;
        }
        const delta = entry.delta ?? 0;
        const tone = delta > 0 ? "up" : delta < 0 ? "down" : "flat";
        return `<span class="trending-history-badge ${tone}">${key.toUpperCase()} ${escapeHtml(formatSignedNumber(delta))}</span>`;
    }

    function renderHistoryCards(history) {
        ["1d", "7d", "10d"].forEach((key) => {
            const entry = history?.[key];
            const valueNode = document.getElementById(`history-${key}-value`);
            const noteNode = document.getElementById(`history-${key}-note`);
            if (!valueNode || !noteNode) return;

            if (!entry || !entry.has_data) {
                valueNode.textContent = "--";
                valueNode.className = "";
                noteNode.textContent = "資料不足";
                return;
            }

            const delta = entry.delta ?? 0;
            const percent = formatPercent(entry.percent_change);
            valueNode.textContent = formatSignedNumber(delta);
            valueNode.className = delta > 0 ? "trending-history-positive" : delta < 0 ? "trending-history-negative" : "trending-history-flat";
            noteNode.textContent = percent ? `對比 ${entry.compare_date}（${percent}）` : `對比 ${entry.compare_date}`;
        });
    }

    function renderSymbolCards(containerId, items, type) {
        const container = document.getElementById(containerId);
        if (!container) return;

        if (!items.length) {
            container.innerHTML = '<div class="trending-empty-state">目前沒有可顯示的股票。</div>';
            return;
        }

        container.innerHTML = items.map((item, index) => {
            const symbol = item.symbol || "--";
            const title = item.title || "未命名公司";
            const exchange = item.exchange || "Unknown";
            const isActive = state.activeSymbol === symbol && state.activeSource === type;
            const badge = type === "trending"
                ? `<span class="trending-card-rank">#${index + 1}</span>`
                : '<span class="trending-card-rank subtle">Search</span>';
            const meta = type === "trending"
                ? `
                    <span>${escapeHtml(exchange)}</span>
                    <span>${formatNumber(item.watchlist_count)} watchlists</span>
                `
                : `<span>${escapeHtml(exchange)}</span>`;
            const historyMarkup = type === "trending"
                ? `
                    <div class="trending-symbol-history-row">
                        ${getHistoryBadgeMarkup(item.history, "1d")}
                        ${getHistoryBadgeMarkup(item.history, "7d")}
                        ${getHistoryBadgeMarkup(item.history, "10d")}
                    </div>
                `
                : "";

            return `
                <button
                    class="trending-symbol-card ${isActive ? "active" : ""}"
                    type="button"
                    data-symbol="${escapeHtml(symbol)}"
                    data-source="${type}"
                >
                    <div class="trending-symbol-card-top">
                        ${badge}
                        <span class="trending-symbol-chip">$${escapeHtml(symbol)}</span>
                    </div>
                    <div class="trending-symbol-main">
                        <strong>${escapeHtml(symbol)}</strong>
                        <span>${escapeHtml(title)}</span>
                    </div>
                    <div class="trending-symbol-meta">
                        ${meta}
                    </div>
                    ${historyMarkup}
                </button>
            `;
        }).join("");
    }

    function renderMessages(messages) {
        const messageList = document.getElementById("message-list");
        if (!messageList) return;

        if (!messages.length) {
            messageList.innerHTML = '<div class="trending-empty-state">目前沒有可顯示的訊息。</div>';
            return;
        }

        messageList.innerHTML = messages.map((message) => {
            const user = message.user || {};
            const sentiment = message.entities?.sentiment?.basic || "Unlabeled";
            const likeCount = message.likes?.total ?? 0;

            return `
                <article class="trending-message-card">
                    <div class="trending-message-head">
                        <div class="trending-message-user">
                            ${avatarMarkup(user)}
                            <div>
                                <strong>${escapeHtml(user.name || user.username || "Unknown")}</strong>
                                <span>@${escapeHtml(user.username || "unknown")}</span>
                            </div>
                        </div>
                        <div class="trending-message-meta">
                            <span class="trending-pill ${sentiment.toLowerCase()}">${escapeHtml(sentiment)}</span>
                            <time>${escapeHtml(formatTime(message.created_at))}</time>
                        </div>
                    </div>
                    <p>${escapeHtml(message.body || "")}</p>
                    <div class="trending-message-foot">
                        <span>${formatNumber(likeCount)} likes</span>
                        <span>Source: ${escapeHtml(message.source?.title || "Stocktwits")}</span>
                    </div>
                </article>
            `;
        }).join("");
    }

    function updateSentiment(messages) {
        const summary = { Bullish: 0, Bearish: 0, Unlabeled: 0 };
        messages.forEach((message) => {
            const sentiment = message.entities?.sentiment?.basic;
            if (sentiment === "Bullish" || sentiment === "Bearish") {
                summary[sentiment] += 1;
            } else {
                summary.Unlabeled += 1;
            }
        });

        document.getElementById("bullish-count").textContent = summary.Bullish;
        document.getElementById("bearish-count").textContent = summary.Bearish;
        document.getElementById("neutral-count").textContent = summary.Unlabeled;

        const totalLabeled = summary.Bullish + summary.Bearish;
        const ratio = totalLabeled ? `${Math.round((summary.Bullish / totalLabeled) * 100)}% Bullish` : "No labeled sentiment";
        setMetric("sentiment-ratio", ratio);
    }

    function setSelectedHero(info = {}) {
        const symbol = info.symbol || "--";
        const title = info.title || "從左側熱門榜或搜尋結果選一檔股票。";
        const exchange = info.exchange || "等待載入";
        const watchlists = typeof info.watchlist_count === "number"
            ? `${formatNumber(info.watchlist_count)} 人追蹤`
            : "尚未載入 watchlist 數";

        document.getElementById("selected-symbol-chip").textContent = `$ ${symbol}`;
        document.getElementById("selected-exchange").textContent = exchange;
        document.getElementById("selected-company").textContent = title;
        document.getElementById("stream-meta").textContent = watchlists;
    }

    async function loadTrending() {
        setStatus("trending-status", "載入本地股票快照中...");
        try {
            const data = await fetchJson("/api/trending");
            state.trending = data.symbols || [];
            const meta = data.meta || {};

            renderSymbolCards("trending-list", state.trending, "trending");
            setMetric("trending-count", formatNumber(state.trending.length));
            setCoverageNote(
                meta.total_symbols
                    ? `已同步 ${meta.synced_symbols || 0} / ${meta.total_symbols} 檔 S&P 500（最新日期 ${meta.latest_snapshot_date || "--"}）`
                    : "來自本地 S&P 500 快照"
            );
            setMetric(
                "history-readiness",
                meta.latest_snapshot_date ? `最新快照 ${meta.latest_snapshot_date}` : "等待第一批快照"
            );
            setStatus(
                "trending-status",
                meta.total_symbols
                    ? `目前顯示 ${state.trending.length} 檔已同步股票，總覆蓋 ${meta.synced_symbols || 0} / ${meta.total_symbols}`
                    : `已載入 ${state.trending.length} 檔股票`
            );

            if (!state.activeSymbol && state.trending.length) {
                await selectSymbol(state.trending[0].symbol, "trending");
            }
        } catch (error) {
            setStatus("trending-status", `熱門股票載入失敗：${error.message}`);
        }
    }

    async function searchSymbols(query) {
        setStatus("search-status", `搜尋「${query}」中...`);
        try {
            const data = await fetchJson(`/api/trending/search?q=${encodeURIComponent(query)}`);
            state.searchResults = (data.results || []).filter((item) => item.type === "symbol");
            renderSymbolCards("search-results", state.searchResults, "search");
            setStatus("search-status", state.searchResults.length ? `找到 ${state.searchResults.length} 筆結果` : "找不到符合條件的 symbol");
        } catch (error) {
            setStatus("search-status", `搜尋失敗：${error.message}`);
        }
    }

    async function selectSymbol(symbol, source) {
        state.activeSymbol = symbol;
        state.activeSource = source;

        setMetric("active-symbol", symbol);
        renderSymbolCards("trending-list", state.trending, "trending");
        renderSymbolCards("search-results", state.searchResults, "search");

        setSelectedHero({ symbol, title: "載入中...", exchange: "讀取資料中..." });
        renderHistoryCards(null);
        setStatus("stream-status", `正在抓取 ${symbol} 的最新 30 則訊息`);

        try {
            const data = await fetchJson(`/api/trending/stream/${encodeURIComponent(symbol)}?limit=30`);
            const messages = data.messages || [];
            const info = data.symbol || {};

            setSelectedHero(info);
            document.getElementById("stream-title").textContent = `${info.symbol || symbol} · ${info.title || "未知公司"}`;
            document.getElementById("stream-meta").textContent = `${info.exchange || "未知交易所"} · ${formatNumber(info.watchlist_count)} 人追蹤`;
            renderHistoryCards(info.history);
            setStatus("stream-status", messages.length ? `顯示 ${messages.length} 則最新訊息` : "此股票尚未同步到本地資料庫，或今日尚未抓到訊息");
            renderMessages(messages);
            updateSentiment(messages);
        } catch (error) {
            document.getElementById("stream-title").textContent = symbol;
            document.getElementById("stream-meta").textContent = "載入失敗";
            setSelectedHero({ symbol, title: "無法載入公司資訊", exchange: "Error" });
            renderHistoryCards(null);
            setStatus("stream-status", `訊息流載入失敗：${error.message}`);
            renderMessages([]);
            updateSentiment([]);
        }
    }

    function bindEvents() {
        document.getElementById("refresh-trending")?.addEventListener("click", () => loadTrending());

        document.getElementById("symbol-search-form")?.addEventListener("submit", (event) => {
            event.preventDefault();
            const input = document.getElementById("symbol-search-input");
            const query = input.value.trim();
            if (!query) {
                setStatus("search-status", "請輸入股票代號或公司名稱");
                return;
            }
            searchSymbols(query);
        });

        document.addEventListener("click", (event) => {
            const button = event.target.closest("[data-symbol]");
            if (!button) return;
            selectSymbol(button.dataset.symbol, button.dataset.source);
        });
    }

    bindEvents();
    loadTrending();
})();
