(function () {
    if (document.body.dataset.page !== "market-guide") return;

    var dataNode = document.getElementById("market-guide-data");
    var moodEl = document.getElementById("guide-mood");
    var summaryEl = document.getElementById("guide-summary");
    var moodBadge = document.getElementById("guide-mood-badge");
    var breadthConclusion = document.getElementById("breadth-conclusion");
    var breadthDate = document.getElementById("breadth-date");
    var breadthGrid = document.getElementById("breadth-grid");
    var breadthAdvancers = document.getElementById("breadth-advancers");
    var breadthDecliners = document.getElementById("breadth-decliners");
    var breadthAdvancersPct = document.getElementById("breadth-advancers-pct");
    var breadthDeclinersPct = document.getElementById("breadth-decliners-pct");
    var breadthAdvancersBar = document.getElementById("breadth-advancers-bar");
    var flowConclusion = document.getElementById("flow-conclusion");
    var flowDate = document.getElementById("flow-date");
    var flowMarketVolume = document.getElementById("flow-market-volume");
    var flowInflows = document.getElementById("flow-inflows");
    var flowOutflows = document.getElementById("flow-outflows");
    var sectorList = document.getElementById("guide-sector-list");
    var sectorDate = document.getElementById("guide-sector-date");
    var searchForm = document.getElementById("guide-stock-search-form");
    var searchInput = document.getElementById("guide-stock-search-input");
    var searchResults = document.getElementById("guide-stock-search-results");
    var tapeRefs = {};
    var searchTimer = null;
    var activeSearchIndex = -1;
    var latestSearchResults = [];

    var state = {
        pulse: [],
        gainers: [],
        losers: [],
        most_active: [],
        sectors: []
    };

    try {
        var initial = JSON.parse(dataNode ? dataNode.textContent || "{}" : "{}");
        state.pulse = initial.pulse || [];
        state.gainers = initial.gainers || [];
        state.losers = initial.losers || [];
        state.most_active = initial.most_active || [];
    } catch (error) {
        console.error("market guide data parse failed:", error);
    }

    function escapeHtml(value) {
        return String(value === null || value === undefined ? "" : value)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function parseChange(item) {
        if (!item) return 0;
        if (typeof item.raw_chg === "number") return item.raw_chg;
        if (typeof item.changes_percentage === "number") return item.changes_percentage;
        if (typeof item.changesPercentage === "number") return item.changesPercentage;
        var raw = item.change || item.change_pct || item.changes_percentage || "";
        var value = Number(String(raw).replace("%", "").replace("+", "").trim());
        return Number.isFinite(value) ? value : 0;
    }

    function bySymbol(symbol) {
        return state.pulse.find(function (item) { return item.symbol === symbol || item.label === symbol; }) || null;
    }

    function formatChange(value) {
        if (value === null || value === undefined || value === "") return "--";
        var num = Number(value);
        if (!Number.isFinite(num)) return "--";
        return (num > 0 ? "+" : "") + num.toFixed(2) + "%";
    }

    function formatRatio(value) {
        var num = Number(value);
        if (!Number.isFinite(num)) return "--";
        return num.toFixed(2) + "x";
    }

    function formatPercentValue(value) {
        var num = Number(value);
        if (!Number.isFinite(num)) return "--";
        return num.toFixed(1) + "%";
    }

    function changeTone(value) {
        var num = Number(value);
        if (!Number.isFinite(num)) return "";
        return num >= 0 ? "is-positive" : "is-negative";
    }

    function formatMoneyValue(value) {
        var num = Number(value);
        if (!Number.isFinite(num)) return "--";
        if (num >= 1000000000) return "US$" + (num / 1000000000).toFixed(2) + "B";
        if (num >= 1000000) return "US$" + (num / 1000000).toFixed(1) + "M";
        if (num >= 1000) return "US$" + (num / 1000).toFixed(0) + "K";
        return "US$" + Math.round(num);
    }

    function stockUrl(symbol) {
        return "/" + encodeURIComponent(String(symbol || "").trim().toUpperCase());
    }

    function renderSparkPoints(points) {
        if (!Array.isArray(points) || points.length < 2) return "";
        var nums = points.map(Number).filter(Number.isFinite);
        if (nums.length < 2) return "";
        var min = Math.min.apply(null, nums);
        var max = Math.max.apply(null, nums);
        var range = max - min || 1;
        return nums.map(function (value, index) {
            var x = (index / (nums.length - 1) * 70).toFixed(1);
            var y = (26 - ((value - min) / range * 22)).toFixed(1);
            return x + "," + y;
        }).join(" ");
    }

    function initTapeRefs() {
        Array.prototype.slice.call(document.querySelectorAll(".guide-tape-item")).forEach(function (item) {
            var symbol = item.dataset.symbol;
            if (!symbol) return;
            if (!tapeRefs[symbol]) tapeRefs[symbol] = [];
            tapeRefs[symbol].push({
                item: item,
                price: item.querySelector(".guide-tape-price"),
                change: item.querySelector(".guide-tape-change"),
                spark: item.querySelector(".guide-tape-spark"),
                line: item.querySelector(".guide-tape-spark polyline")
            });
        });
    }

    function applyTapeItem(item) {
        var refs = tapeRefs[item.symbol] || [];
        var price = item.price || "--";
        var change = item.change || "--";
        var dir = item.dir || "flat";
        var points = renderSparkPoints(item.sparkline || []);
        refs.forEach(function (ref) {
            if (ref.price) ref.price.textContent = price;
            if (ref.change) {
                ref.change.textContent = change;
                ref.change.classList.remove("is-up", "is-down", "is-flat");
                ref.change.classList.add("is-" + dir);
            }
            if (ref.spark) {
                ref.spark.classList.remove("is-up", "is-down", "is-flat");
                ref.spark.classList.add("is-" + dir);
            }
            if (ref.line && points) ref.line.setAttribute("points", points);
        });
    }

    function renderSearchResults(results) {
        latestSearchResults = results || [];
        activeSearchIndex = -1;
        if (!searchResults) return;
        if (!latestSearchResults.length) {
            searchResults.hidden = true;
            searchResults.innerHTML = "";
            return;
        }
        searchResults.innerHTML = latestSearchResults.slice(0, 8).map(function (item, index) {
            var symbol = item.code || item.ticker || item.symbol || "";
            var name = item.display_name || item.name || item.name_zh_hk || item.name_eng || "";
            return [
                '<button class="guide-search-result" type="button" data-index="' + index + '" data-symbol="' + escapeHtml(symbol) + '">',
                "  <strong>" + escapeHtml(symbol) + "</strong>",
                "  <span>" + escapeHtml(name) + "</span>",
                "</button>"
            ].join("");
        }).join("");
        searchResults.hidden = false;
    }

    async function searchStocks(query) {
        if (!query || query.length < 1) {
            renderSearchResults([]);
            return;
        }
        try {
            var response = await fetch("/api/search_stock?q=" + encodeURIComponent(query), { cache: "no-store" });
            if (!response.ok) throw new Error("HTTP " + response.status);
            var payload = await response.json();
            renderSearchResults(Array.isArray(payload) ? payload : (payload.results || []));
        } catch (error) {
            console.warn("market guide search failed:", error);
            renderSearchResults([]);
        }
    }

    function submitSearch() {
        var query = searchInput ? searchInput.value.trim() : "";
        var selected = latestSearchResults[activeSearchIndex];
        var symbol = selected ? (selected.code || selected.ticker || selected.symbol) : query;
        if (symbol) window.location.href = stockUrl(symbol);
    }

    async function refreshPulseTape() {
        try {
            var response = await fetch("/api/markets/pulse", { cache: "no-store" });
            if (!response.ok) throw new Error("HTTP " + response.status);
            var payload = await response.json();
            state.pulse = payload.pulse || state.pulse;
            state.pulse.forEach(applyTapeItem);
            renderMood();
        } catch (error) {
            console.warn("market guide pulse refresh failed:", error);
        }
    }

    function classifyMarket() {
        var spx = parseChange(bySymbol("^GSPC"));
        var nasdaq = parseChange(bySymbol("^IXIC"));
        var vix = parseChange(bySymbol("^VIX"));
        var tnx = parseChange(bySymbol("^TNX"));
        var dxy = parseChange(bySymbol("DX-Y.NYB"));
        var score = spx * 0.42 + nasdaq * 0.36 - vix * 0.12 - Math.max(tnx, 0) * 0.05 - Math.max(dxy, 0) * 0.05;

        if (score >= 0.55) {
            return {
                key: "risk_on",
                angle: 35,
                title: "偏向風險偏好",
                summary: "大盤與成長股同步偏強，可以優先看強勢板塊和成交活躍股票，但不宜追太遠的急升。",
                badge: "風險偏好模式 · Risk-On"
            };
        }
        if (score <= -0.55) {
            return {
                key: "defensive",
                angle: 195,
                title: "先控制風險",
                summary: "大盤或波動指標轉弱，交易上應先縮短觀察名單，等待價格站穩再行動。",
                badge: "防守模式 · Risk-Off"
            };
        }
        return {
            key: "mixed",
            angle: 105,
            title: "市場方向未明",
            summary: "指數訊號分歧，較適合看板塊輪動和相對強弱，不急於單邊押注。",
            badge: "分化觀察 · Mixed"
        };
    }

    function renderMood() {
        var mood = classifyMarket();
        moodEl.textContent = mood.title;
        summaryEl.textContent = mood.summary;
        if (moodBadge) {
            moodBadge.textContent = mood.badge;
            moodBadge.className = "guide-mood-badge is-" + mood.key.replace("_", "-");
        }
    }

    function renderSectors(payload) {
        var periods = payload && payload.periods ? payload.periods : [];
        var oneDay = periods.find(function (period) { return period.id === "1d"; }) || periods[0];
        var items = oneDay && oneDay.items ? oneDay.items.slice() : [];
        items = items.filter(function (item) { return Number.isFinite(Number(item.performance)); })
            .sort(function (a, b) { return Number(b.performance) - Number(a.performance); });

        if (sectorDate) {
            sectorDate.textContent = payload && payload.latest_price_date ? ("價格日 " + payload.latest_price_date) : "暫無價格日";
        }
        if (!items.length) {
            sectorList.innerHTML = '<p class="empty-text">暫無板塊資料</p>';
            return;
        }

        var top = items.slice(0, 8);
        sectorList.innerHTML = top.map(function (item) {
            var perf = Number(item.performance);
            var width = Math.min(100, Math.max(8, Math.abs(perf) * 18));
            return [
                '<div class="sector-item ' + (perf >= 0 ? "is-up" : "is-down") + ' guide-enter">',
                "  <strong>" + escapeHtml(item.sector || item.symbol || "Sector") + "</strong>",
                '  <div class="sector-meter"><span style="width:' + width.toFixed(0) + '%"></span></div>',
                '  <span class="sector-change ' + (perf >= 0 ? "is-up" : "is-down") + '">' + formatChange(perf) + "</span>",
                "</div>"
            ].join("");
        }).join("");
    }

    async function refreshMovers() {
        try {
            var response = await fetch("/api/markets/movers", { cache: "no-store" });
            if (!response.ok) throw new Error("HTTP " + response.status);
            var payload = await response.json();
            state.gainers = payload.gainers || state.gainers;
            state.losers = payload.losers || state.losers;
            state.most_active = payload.most_active || state.most_active;
        } catch (error) {
            console.warn("market guide movers refresh failed:", error);
        }
    }

    async function refreshSectors() {
        try {
            var response = await fetch("/api/sector-performance", { cache: "no-store" });
            if (!response.ok) throw new Error("HTTP " + response.status);
            renderSectors(await response.json());
        } catch (error) {
            console.warn("market guide sector refresh failed:", error);
            sectorList.innerHTML = '<p class="empty-text">暫時無法取得板塊資料</p>';
        }
    }

    function breadthMetric(label, value, detail, tone, tooltip) {
        var labelHtml = tooltip
            ? '<span class="metric-label-with-tip">' + escapeHtml(label) + '<button class="info-tip" type="button" aria-label="' + escapeHtml(label) + '說明" data-tooltip="' + escapeHtml(tooltip) + '">?</button></span>'
            : "<span>" + escapeHtml(label) + "</span>";
        return [
            '<div class="breadth-metric ' + (tone || "") + '">',
            "  " + labelHtml,
            "  <strong>" + escapeHtml(value) + "</strong>",
            detail ? ("  <em>" + escapeHtml(detail) + "</em>") : "",
            "</div>"
        ].join("");
    }

    function renderBreadth(payload) {
        if (!payload) return;
        var summary = breadthConclusion ? breadthConclusion.closest(".breadth-summary") : null;
        if (summary) {
            summary.classList.remove("is-healthy", "is-mixed", "is-narrow", "is-weak", "is-empty");
        }
        if (breadthConclusion) {
            breadthConclusion.textContent = payload.conclusion || "資料不足";
            breadthConclusion.className = "";
            if (payload.conclusion === "廣度健康") {
                breadthConclusion.classList.add("is-up");
                if (summary) summary.classList.add("is-healthy");
            } else if (payload.conclusion === "市場分化") {
                if (summary) summary.classList.add("is-mixed");
            } else if (payload.conclusion === "少數權值股撐市") {
                breadthConclusion.classList.add("is-warning");
                if (summary) summary.classList.add("is-narrow");
            } else if (payload.conclusion === "廣度轉弱") {
                breadthConclusion.classList.add("is-down");
                if (summary) summary.classList.add("is-weak");
            } else if (summary) {
                summary.classList.add("is-empty");
            }
        }
        if (breadthDate) {
            breadthDate.textContent = payload.latest_date ? ("價格日 " + payload.latest_date) : "--";
        }
        if (breadthAdvancers) breadthAdvancers.textContent = String(payload.advancers || 0);
        if (breadthDecliners) breadthDecliners.textContent = String(payload.decliners || 0);
        if (breadthAdvancersPct) breadthAdvancersPct.textContent = formatPercentValue(payload.advancers_pct);
        if (breadthDeclinersPct) breadthDeclinersPct.textContent = formatPercentValue(payload.decliners_pct);
        if (breadthAdvancersBar) {
            var advPct = Number(payload.advancers_pct);
            breadthAdvancersBar.style.width = (Number.isFinite(advPct) ? Math.max(0, Math.min(100, advPct)) : 0) + "%";
        }
        if (!breadthGrid) return;

        breadthGrid.innerHTML = [
            breadthMetric("上升 / 下跌比率", formatRatio(payload.adv_decl_ratio), "A/D 比率", "", "S&P 500 上升家數除以下跌家數。高於 1 代表上升股票較多，低於 1 代表下跌股票較多。"),
            breadthMetric("高於 20 日均線", formatPercentValue(payload.above_20dma_pct), (payload.above_20dma_count || 0) + " / " + (payload.above_20dma_eligible || 0), ""),
            breadthMetric("高於 50 日均線", formatPercentValue(payload.above_50dma_pct), (payload.above_50dma_count || 0) + " / " + (payload.above_50dma_eligible || 0), ""),
            breadthMetric("新高 / 新低", (payload.new_highs || 0) + " / " + (payload.new_lows || 0), "52 週", "")
        ].join("");
    }

    async function refreshBreadth() {
        try {
            var response = await fetch("/api/markets/breadth", { cache: "no-store" });
            if (!response.ok) throw new Error("HTTP " + response.status);
            renderBreadth(await response.json());
        } catch (error) {
            console.warn("market breadth load failed:", error);
            if (breadthConclusion) breadthConclusion.textContent = "暫時無法讀取";
            if (breadthGrid) breadthGrid.innerHTML = '<p class="empty-text">暫時無法取得市場廣度資料</p>';
        }
    }

    function flowMetric(label, value, tone) {
        return [
            '<div class="flow-volume-metric ' + (tone || "") + '">',
            "  <span>" + escapeHtml(label) + "</span>",
            "  <strong>" + escapeHtml(value) + "</strong>",
            "</div>"
        ].join("");
    }

    function renderFlowRows(items) {
        if (!items || !items.length) {
            return '<p class="empty-text">暫無明顯板塊</p>';
        }
        return items.map(function (item) {
            var change = Number(item.change_pct);
            var value5 = item.value_vs_5d_pct;
            var advancers = Number(item.advancers_pct);
            var tone = change >= 0 ? "is-positive" : "is-negative";
            return [
                '<div class="flow-row ' + tone + '">',
                '  <div class="flow-row-main">',
                "    <strong>" + escapeHtml(item.sector || "未分類") + '<span class="flow-state-badge is-' + escapeHtml(item.flow_state_tone || "neutral") + '">' + escapeHtml(item.flow_state || "資料不足") + "</span></strong>",
                "    <span>成交額 vs 5日 " + escapeHtml(formatChange(value5)) + " · 上升比例 " + escapeHtml(formatPercentValue(advancers)) + "</span>",
                "  </div>",
                '  <div class="flow-row-side">',
                "    <strong>" + escapeHtml(formatChange(change)) + "</strong>",
                "    <em>" + escapeHtml(formatMoneyValue(item.total_traded_value)) + "</em>",
                "  </div>",
                "</div>"
            ].join("");
        }).join("");
    }

    function renderFlow(payload) {
        if (!payload) return;
        if (flowConclusion) flowConclusion.textContent = payload.conclusion || "資料不足";
        if (flowDate) flowDate.textContent = payload.latest_date ? ("價格日 " + payload.latest_date) : "--";

        var market = payload.market || {};
        if (flowMarketVolume) {
            var vsYesterday = market.value_vs_yesterday_pct;
            var vs5 = market.value_vs_5d_pct;
            var vs20 = market.value_vs_20d_pct;
            flowMarketVolume.innerHTML = [
                flowMetric("S&P 500 總成交額", formatMoneyValue(market.total_traded_value), ""),
                flowMetric("vs 昨日", formatChange(vsYesterday), changeTone(vsYesterday)),
                flowMetric("vs 5日均額", formatChange(vs5), changeTone(vs5)),
                flowMetric("vs 20日均額", formatChange(vs20), changeTone(vs20))
            ].join("");
        }
        if (flowInflows) flowInflows.innerHTML = renderFlowRows(payload.inflows || []);
        if (flowOutflows) flowOutflows.innerHTML = renderFlowRows(payload.outflows || []);
    }

    async function refreshFlow() {
        try {
            var response = await fetch("/api/markets/flow", { cache: "no-store" });
            if (!response.ok) throw new Error("HTTP " + response.status);
            renderFlow(await response.json());
        } catch (error) {
            console.warn("market flow load failed:", error);
            if (flowConclusion) flowConclusion.textContent = "暫時無法讀取";
            if (flowMarketVolume) flowMarketVolume.innerHTML = '<p class="empty-text">暫時無法取得成交額資料</p>';
            if (flowInflows) flowInflows.innerHTML = '<p class="empty-text">暫時無法取得流入板塊</p>';
            if (flowOutflows) flowOutflows.innerHTML = '<p class="empty-text">暫時無法取得流出板塊</p>';
        }
    }

    if (searchInput) {
        searchInput.addEventListener("input", function () {
            window.clearTimeout(searchTimer);
            var query = searchInput.value.trim();
            searchTimer = window.setTimeout(function () { searchStocks(query); }, 180);
        });

        searchInput.addEventListener("keydown", function (event) {
            var items = searchResults ? Array.prototype.slice.call(searchResults.querySelectorAll(".guide-search-result")) : [];
            if (event.key === "ArrowDown" && items.length) {
                event.preventDefault();
                activeSearchIndex = Math.min(activeSearchIndex + 1, items.length - 1);
            } else if (event.key === "ArrowUp" && items.length) {
                event.preventDefault();
                activeSearchIndex = Math.max(activeSearchIndex - 1, 0);
            } else if (event.key === "Escape") {
                renderSearchResults([]);
            } else {
                return;
            }
            items.forEach(function (item, index) {
                item.classList.toggle("is-active", index === activeSearchIndex);
            });
        });
    }

    if (searchForm) {
        searchForm.addEventListener("submit", function (event) {
            event.preventDefault();
            submitSearch();
        });
    }

    if (searchResults) {
        searchResults.addEventListener("click", function (event) {
            var button = event.target.closest(".guide-search-result");
            if (!button) return;
            window.location.href = stockUrl(button.dataset.symbol);
        });
    }

    document.addEventListener("click", function (event) {
        if (!searchForm || searchForm.contains(event.target)) return;
        renderSearchResults([]);
    });

    initTapeRefs();
    renderMood();
    refreshPulseTape();
    refreshMovers();
    refreshSectors();
    refreshBreadth();
    refreshFlow();
    window.setInterval(refreshPulseTape, 15000);
    window.setInterval(refreshMovers, 10 * 60 * 1000);
    window.setInterval(refreshSectors, 60 * 60 * 1000);
    window.setInterval(refreshBreadth, 15 * 60 * 1000);
    window.setInterval(refreshFlow, 15 * 60 * 1000);
})();
