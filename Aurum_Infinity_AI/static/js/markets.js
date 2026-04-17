(function () {
    var page = document.body.dataset.page;
    if (page !== "markets") return;

    var grid = document.getElementById("markets-grid");
    var heatmapBoard = document.getElementById("heatmap-board");
    var heatmapEmpty = document.getElementById("heatmap-empty");
    var heatmapSurface = document.getElementById("heatmap-surface");
    var inspector = document.getElementById("heatmap-inspector");
    var inspectorClose = document.getElementById("heatmap-inspector-close");
    var inspectorTitle = document.getElementById("inspector-title");
    var inspectorSubtitle = document.getElementById("inspector-subtitle");
    var inspectorCount = document.getElementById("inspector-count");
    var inspectorMarketCap = document.getElementById("inspector-market-cap");
    var inspectorChange = document.getElementById("inspector-change");
    var inspectorScope = document.getElementById("inspector-scope");
    var inspectorList = document.getElementById("inspector-list");
    var metaCount = document.getElementById("heatmap-meta-count");
    var metaDate = document.getElementById("heatmap-meta-date");
    var metaSync = document.getElementById("heatmap-meta-sync");
    var sectorPerformanceGrid = document.getElementById("sector-performance-grid");
    var sectorPerformanceDate = document.getElementById("sector-performance-date");
    var sectorPerformanceSync = document.getElementById("sector-performance-sync");

    var marketRefreshMs = 5000;
    var heatmapRefreshMs = 300000;
    var sectorPerformanceRefreshMs = 3600000;
    var currentHeatmapPayload = null;
    var currentInspectorSelection = null;
    var resizeTimer = null;

    var tooltip = document.createElement("div");
    tooltip.className = "heatmap-tooltip";
    heatmapSurface.appendChild(tooltip);

    function formatNumber(value, digits) {
        if (value === null || value === undefined || value === "") return "—";
        var num = Number(value);
        if (!Number.isFinite(num)) return "—";
        return num.toLocaleString("en-US", {
            minimumFractionDigits: digits,
            maximumFractionDigits: digits
        });
    }

    function formatSigned(value, digits) {
        if (value === null || value === undefined || value === "") return "—";
        var num = Number(value);
        if (!Number.isFinite(num)) return "—";
        var sign = num > 0 ? "+" : "";
        return sign + num.toLocaleString("en-US", {
            minimumFractionDigits: digits,
            maximumFractionDigits: digits
        });
    }

    function formatPercent(value) {
        return formatSigned(value, 2) + (value === null || value === undefined || value === "" ? "" : "%");
    }

    function formatAxisPercent(value) {
        if (Math.abs(value) >= 1) return value.toFixed(0) + "%";
        return value.toFixed(1) + "%";
    }

    function formatMarketCap(value) {
        if (value === null || value === undefined || value === "") return "—";
        var num = Number(value);
        if (!Number.isFinite(num)) return "—";
        var abs = Math.abs(num);
        if (abs >= 1e12) return "$" + (num / 1e12).toFixed(2) + "T";
        if (abs >= 1e9) return "$" + (num / 1e9).toFixed(1) + "B";
        if (abs >= 1e6) return "$" + (num / 1e6).toFixed(1) + "M";
        return "$" + num.toLocaleString("en-US", { maximumFractionDigits: 0 });
    }

    function formatDateTime(value) {
        if (!value) return "—";
        var date = new Date(value);
        if (Number.isNaN(date.getTime())) return value;
        return date.toLocaleString("zh-HK", {
            hour12: false,
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
            hour: "2-digit",
            minute: "2-digit"
        });
    }

    function changeClass(change) {
        var num = Number(change);
        if (!Number.isFinite(num) || num === 0) return "is-flat";
        return num > 0 ? "is-up" : "is-down";
    }

    function renderCard(item) {
        return [
            '<article class="market-card market-card-compact">',
            '  <p class="market-name">' + item.label + "</p>",
            '  <div class="market-price">' + formatNumber(item.price, 2) + "</div>",
            '  <div class="market-change ' + changeClass(item.change) + '">',
            "    <span>" + formatSigned(item.change, 2) + "</span>",
            "  </div>",
            "</article>"
        ].join("");
    }

    function setMarketError(message) {
        grid.innerHTML = '<div class="markets-error">' + message + "</div>";
    }

    function setSectorPerformanceError(message) {
        sectorPerformanceGrid.innerHTML = '<div class="markets-error">' + message + "</div>";
    }

    async function loadMarkets() {
        try {
            var response = await fetch("/api/market-indices", { cache: "no-store" });
            if (!response.ok) throw new Error("HTTP " + response.status);
            var payload = await response.json();
            if (!payload.indices || !payload.indices.length) throw new Error("Empty payload");
            grid.innerHTML = payload.indices.map(renderCard).join("");
        } catch (error) {
            console.error("market indices load failed:", error);
            setMarketError("暫時無法取得市場即時資料。");
        }
    }

    function renderPerformancePeriod(period) {
        var items = (period.items || []).filter(function (item) {
            return item.performance !== null && item.performance !== undefined;
        });
        if (!items.length) {
            return '<div class="performance-card"><h3 class="performance-card-title">' + period.label + '</h3><div class="markets-error">暫時無法取得區塊資料。</div></div>';
        }

        var minVal = Math.min.apply(null, items.map(function (item) { return item.performance; }));
        var maxVal = Math.max.apply(null, items.map(function (item) { return item.performance; }));
        var absMax = Math.max(0.5, Math.ceil(Math.max(Math.abs(minVal), Math.abs(maxVal)) * 1.15 * 2) / 2);
        var domainMin = -absMax;
        var domainMax = absMax;
        var domainSpan = domainMax - domainMin;
        var zeroPct = ((0 - domainMin) / domainSpan) * 100;

        var rowsHtml = items.map(function (item) {
            var perf = Number(item.performance);
            var className = perf > 0 ? "is-up" : perf < 0 ? "is-down" : "is-flat";
            var startPct = ((Math.min(perf, 0) - domainMin) / domainSpan) * 100;
            var widthPct = (Math.abs(perf) / domainSpan) * 100;
            var valueLeft = perf >= 0 ? Math.min(startPct + widthPct + 1, 95) : Math.max(startPct - 9, 1);
            return [
                '<div class="performance-row">',
                '  <div class="performance-sector">' + item.sector + "</div>",
                '  <div class="performance-bar-track">',
                '    <span class="performance-zero-line" style="left:' + zeroPct + '%"></span>',
                '    <span class="performance-bar ' + className + '" style="left:' + startPct + '%;width:' + widthPct + '%"></span>',
                '    <span class="performance-value" style="left:' + valueLeft + '%">' + formatSigned(perf, 2) + "</span>",
                "  </div>",
                "</div>"
            ].join("");
        }).join("");

        var axisHtml = [
            '<div class="performance-axis">',
            '  <span>' + formatAxisPercent(domainMin) + "</span>",
            '  <span>0%</span>',
            '  <span>' + formatAxisPercent(domainMax) + "</span>",
            "</div>"
        ].join("");

        return [
            '<section class="performance-card">',
            '  <h3 class="performance-card-title">' + period.label + "</h3>",
            '  <div class="performance-list">' + rowsHtml + "</div>",
            axisHtml,
            "</section>"
        ].join("");
    }

    function renderSectorPerformance(payload) {
        if (!payload || !payload.periods || !payload.periods.length) {
            setSectorPerformanceError("暫時無法取得 sector ETF 表現。");
            return;
        }
        sectorPerformanceGrid.innerHTML = payload.periods.map(renderPerformancePeriod).join("");
        sectorPerformanceDate.textContent = "價格日 " + (payload.latest_price_date || "—");
        sectorPerformanceSync.textContent = "更新時間 " + formatDateTime(payload.updated_at);
    }

    async function loadSectorPerformance() {
        try {
            var response = await fetch("/api/sector-performance", { cache: "no-store" });
            if (!response.ok) throw new Error("HTTP " + response.status);
            var payload = await response.json();
            renderSectorPerformance(payload);
        } catch (error) {
            console.error("sector performance load failed:", error);
            setSectorPerformanceError("暫時無法取得 sector ETF 表現。");
        }
    }

    function getHeatColor(changePct) {
        if (changePct === null || changePct === undefined || !Number.isFinite(Number(changePct))) {
            return "#5f564f";
        }
        var value = Math.max(-3, Math.min(3, Number(changePct)));
        return d3.scaleLinear()
            .domain([-3, -1, 0, 1, 3])
            .range(["#5c0b05", "#d43d2d", "#b7ada2", "#16a34a", "#0b5d2a"])(value);
    }

    function setHeatmapEmpty(message) {
        closeInspector();
        heatmapEmpty.hidden = false;
        heatmapEmpty.textContent = message;
        heatmapBoard.innerHTML = "";
        heatmapBoard.classList.remove("is-loading");
    }

    function hideHeatmapEmpty() {
        heatmapEmpty.hidden = true;
        heatmapBoard.classList.remove("is-loading");
    }

    function openInspector() {
        inspector.classList.add("is-open");
        heatmapBoard.classList.add("with-inspector");
    }

    function closeInspector() {
        inspector.classList.remove("is-open");
        heatmapBoard.classList.remove("with-inspector");
        currentInspectorSelection = null;
    }

    function updateHeatmapMeta(payload) {
        var renderedCount = payload.rendered_count || 0;
        var constituentCount = payload.constituent_count || 0;
        metaCount.textContent = "熱圖 " + renderedCount + " / 成分股 " + constituentCount;
        metaDate.textContent = "價格日 " + (payload.latest_ohlc_date || "—");
        metaSync.textContent = "同步時間 " + formatDateTime(payload.synced_at);
    }

    function buildHierarchy(stocks) {
        var bySector = {};
        stocks.forEach(function (item) {
            var sector = item.sector || "Unknown";
            var industry = item.industry || "Unknown";
            if (!bySector[sector]) bySector[sector] = {};
            if (!bySector[sector][industry]) bySector[sector][industry] = [];
            bySector[sector][industry].push({
                name: item.ticker,
                ticker: item.ticker,
                companyName: item.name,
                sector: sector,
                industry: industry,
                market_cap: item.market_cap,
                price: item.price,
                change_pct: item.change_pct,
                change_value: item.change_value,
                latest_date: item.latest_date,
                previous_date: item.previous_date
            });
        });

        return {
            name: "S&P 500",
            children: Object.keys(bySector).sort().map(function (sector) {
                var industries = bySector[sector];
                return {
                    name: sector,
                    children: Object.keys(industries).sort().map(function (industry) {
                        return {
                            name: industry,
                            children: industries[industry]
                        };
                    })
                };
            })
        };
    }

    function summarizeGroup(items) {
        var totalMarketCap = 0;
        var weightedChange = 0;
        var validWeight = 0;
        items.forEach(function (item) {
            var cap = Number(item.market_cap) || 0;
            var change = Number(item.change_pct);
            totalMarketCap += cap;
            if (cap > 0 && Number.isFinite(change)) {
                weightedChange += cap * change;
                validWeight += cap;
            }
        });
        return {
            count: items.length,
            totalMarketCap: totalMarketCap,
            avgChange: validWeight > 0 ? weightedChange / validWeight : null,
            topHoldings: items.slice().sort(function (a, b) {
                return (Number(b.market_cap) || 0) - (Number(a.market_cap) || 0);
            }).slice(0, 6)
        };
    }

    function renderInspector(selection) {
        if (!selection || !selection.items || !selection.items.length) {
            closeInspector();
            return;
        }

        currentInspectorSelection = selection;
        var summary = summarizeGroup(selection.items);

        inspectorTitle.textContent = selection.title;
        inspectorSubtitle.textContent = selection.subtitle;
        inspectorCount.textContent = String(summary.count);
        inspectorMarketCap.textContent = formatMarketCap(summary.totalMarketCap);
        inspectorChange.textContent = formatPercent(summary.avgChange);
        inspectorChange.className = "inspector-value " + changeClass(summary.avgChange);
        inspectorScope.textContent = selection.scopeLabel;
        inspectorList.innerHTML = summary.topHoldings.map(function (item) {
            return [
                '<div class="inspector-row">',
                '  <div class="inspector-row-main">',
                '    <span class="inspector-row-ticker">' + item.ticker + '</span>',
                '    <span class="inspector-row-name">' + item.companyName + '</span>',
                "  </div>",
                '  <div class="inspector-row-side">',
                '    <span class="inspector-row-cap">' + formatMarketCap(item.market_cap) + "</span>",
                '    <span class="inspector-row-change ' + changeClass(item.change_pct) + '">' + formatPercent(item.change_pct) + "</span>",
                "  </div>",
                "</div>"
            ].join("");
        }).join("");
        openInspector();
    }

    function showTooltip(event, data) {
        tooltip.innerHTML = [
            '<p class="tooltip-ticker">' + data.ticker + "</p>",
            '<p class="tooltip-name">' + data.companyName + "</p>",
            '<div class="tooltip-grid">',
            '  <div><span class="tooltip-label">SECTOR</span><span class="tooltip-value">' + data.sector + "</span></div>",
            '  <div><span class="tooltip-label">PRICE</span><span class="tooltip-value">' + formatNumber(data.price, 2) + "</span></div>",
            '  <div><span class="tooltip-label">CHANGE</span><span class="tooltip-value ' + changeClass(data.change_pct) + '">' + formatPercent(data.change_pct) + "</span></div>",
            '  <div><span class="tooltip-label">MKT CAP</span><span class="tooltip-value">' + formatMarketCap(data.market_cap) + "</span></div>",
            "</div>"
        ].join("");
        tooltip.classList.add("is-visible");
        moveTooltip(event);
    }

    function moveTooltip(event) {
        var surfaceRect = heatmapSurface.getBoundingClientRect();
        var tooltipRect = tooltip.getBoundingClientRect();
        var left = event.clientX - surfaceRect.left + 16;
        var top = event.clientY - surfaceRect.top + 16;

        if (left + tooltipRect.width > surfaceRect.width - 8) {
            left = surfaceRect.width - tooltipRect.width - 8;
        }
        if (top + tooltipRect.height > surfaceRect.height - 8) {
            top = surfaceRect.height - tooltipRect.height - 8;
        }

        tooltip.style.left = Math.max(8, left) + "px";
        tooltip.style.top = Math.max(8, top) + "px";
    }

    function hideTooltip() {
        tooltip.classList.remove("is-visible");
    }

    function renderHeatmap(payload) {
        if (!window.d3 || !payload || !payload.stocks || !payload.stocks.length) {
            setHeatmapEmpty("暫時無法建立 S&P 500 heatmap。");
            return;
        }

        hideHeatmapEmpty();
        updateHeatmapMeta(payload);

        var width = Math.max(heatmapBoard.clientWidth, 320);
        var height = Math.max(Math.round(width * 0.58), 520);
        heatmapBoard.style.height = height + "px";
        heatmapBoard.innerHTML = "";

        var root = d3.hierarchy(buildHierarchy(payload.stocks))
            .sum(function (d) { return d.market_cap || 0; })
            .sort(function (a, b) { return (b.value || 0) - (a.value || 0); });

        d3.treemap()
            .tile(d3.treemapSquarify.ratio(1.15))
            .size([width, height])
            .paddingOuter(6)
            .paddingInner(2)
            .paddingTop(function (node) {
                if (node.depth === 1) return 22;
                if (node.depth === 2) return 16;
                return 0;
            })(root);

        var svg = d3.select(heatmapBoard)
            .append("svg")
            .attr("class", "heatmap-svg")
            .attr("viewBox", "0 0 " + width + " " + height)
            .attr("preserveAspectRatio", "xMidYMid meet");

        var sectors = root.children || [];
        var industries = [];
        sectors.forEach(function (sectorNode) {
            (sectorNode.children || []).forEach(function (industryNode) {
                industries.push(industryNode);
            });
        });
        var leaves = root.leaves();

        var sectorGroup = svg.append("g").attr("class", "sector-layer");
        sectorGroup.selectAll("rect.sector-frame")
            .data(sectors)
            .enter()
            .append("rect")
            .attr("class", "sector-frame")
            .attr("x", function (d) { return d.x0; })
            .attr("y", function (d) { return d.y0; })
            .attr("width", function (d) { return Math.max(0, d.x1 - d.x0); })
            .attr("height", function (d) { return Math.max(0, d.y1 - d.y0); });

        var visibleSectors = sectors.filter(function (d) {
            return (d.x1 - d.x0) > 92 && (d.y1 - d.y0) > 28;
        });

        sectorGroup.selectAll("rect.sector-label-bar")
            .data(visibleSectors)
            .enter()
            .append("rect")
            .attr("class", "sector-label-bar")
            .attr("x", function (d) { return d.x0 + 1; })
            .attr("y", function (d) { return d.y0 + 1; })
            .attr("width", function (d) { return Math.max(0, d.x1 - d.x0 - 2); })
            .attr("height", 18)
            .attr("rx", 4)
            .attr("ry", 4)
            .on("click", function (event, d) {
                event.stopPropagation();
                var items = d.leaves().map(function (leaf) { return leaf.data; });
                renderInspector({
                    title: d.data.name,
                    subtitle: "Sector 摘要與主要成分股，依市值排序。",
                    scopeLabel: "Sector",
                    items: items
                });
            });

        sectorGroup.selectAll("text")
            .data(visibleSectors)
            .enter()
            .append("text")
            .attr("class", "sector-label")
            .attr("x", function (d) { return d.x0 + 8; })
            .attr("y", function (d) { return d.y0 + 13; })
            .text(function (d) { return d.data.name; });

        var industryGroup = svg.append("g").attr("class", "industry-layer");
        industryGroup.selectAll("rect.industry-frame")
            .data(industries)
            .enter()
            .append("rect")
            .attr("class", "industry-frame")
            .attr("x", function (d) { return d.x0; })
            .attr("y", function (d) { return d.y0; })
            .attr("width", function (d) { return Math.max(0, d.x1 - d.x0); })
            .attr("height", function (d) { return Math.max(0, d.y1 - d.y0); });

        var visibleIndustries = industries.filter(function (d) {
            return (d.x1 - d.x0) > 92 && (d.y1 - d.y0) > 22;
        });

        industryGroup.selectAll("rect.industry-label-bar")
            .data(visibleIndustries)
            .enter()
            .append("rect")
            .attr("class", "industry-label-bar")
            .attr("x", function (d) { return d.x0 + 1; })
            .attr("y", function (d) { return d.y0 + 1; })
            .attr("width", function (d) { return Math.max(0, d.x1 - d.x0 - 2); })
            .attr("height", 12)
            .attr("rx", 3)
            .attr("ry", 3)
            .on("click", function (event, d) {
                event.stopPropagation();
                var items = d.leaves().map(function (leaf) { return leaf.data; });
                renderInspector({
                    title: d.data.name,
                    subtitle: d.parent ? (d.parent.data.name + " 內的 Industry / Sub-sector 摘要。") : "Industry 摘要。",
                    scopeLabel: d.parent ? d.parent.data.name : "Industry",
                    items: items
                });
            });

        industryGroup.selectAll("text")
            .data(visibleIndustries)
            .enter()
            .append("text")
            .attr("class", "industry-label")
            .attr("x", function (d) { return d.x0 + 6; })
            .attr("y", function (d) { return d.y0 + 9; })
            .text(function (d) { return d.data.name; });

        var tileGroup = svg.append("g")
            .attr("class", "tile-layer")
            .selectAll("g")
            .data(leaves)
            .enter()
            .append("g")
            .attr("class", "tile-group")
            .attr("transform", function (d) { return "translate(" + d.x0 + "," + d.y0 + ")"; });

        tileGroup.append("rect")
            .attr("class", "tile-rect")
            .attr("width", function (d) { return Math.max(0, d.x1 - d.x0); })
            .attr("height", function (d) { return Math.max(0, d.y1 - d.y0); })
            .attr("fill", function (d) { return getHeatColor(d.data.change_pct); })
            .on("mouseenter", function (event, d) { showTooltip(event, d.data); })
            .on("mousemove", function (event) { moveTooltip(event); })
            .on("mouseleave", hideTooltip);

        tileGroup.each(function (d) {
            var group = d3.select(this);
            var widthBox = d.x1 - d.x0;
            var heightBox = d.y1 - d.y0;
            var canShowTicker = widthBox > 48 && heightBox > 28;
            var canShowChange = widthBox > 68 && heightBox > 46;

            if (canShowTicker) {
                group.append("text")
                    .attr("class", "tile-label")
                    .attr("x", 7)
                    .attr("y", 16)
                    .text(d.data.ticker);
            }

            if (canShowChange) {
                group.append("text")
                    .attr("class", "tile-sub")
                    .attr("x", 7)
                    .attr("y", 32)
                    .text(formatPercent(d.data.change_pct));
            } else if (widthBox > 72 && heightBox > 32) {
                group.append("text")
                    .attr("class", "tile-sub is-muted")
                    .attr("x", 7)
                    .attr("y", 31)
                    .text(formatMarketCap(d.data.market_cap));
            }
        });

        svg.on("click", function () {
            hideTooltip();
        });

        if (currentInspectorSelection) {
            var matches = [];
            if (currentInspectorSelection.scopeLabel === "Sector") {
                matches = leaves
                    .map(function (leaf) { return leaf.data; })
                    .filter(function (item) { return item.sector === currentInspectorSelection.title; });
            } else {
                matches = leaves
                    .map(function (leaf) { return leaf.data; })
                    .filter(function (item) {
                        return item.industry === currentInspectorSelection.title &&
                            item.sector === currentInspectorSelection.scopeLabel;
                    });
            }
            if (matches.length) {
                renderInspector({
                    title: currentInspectorSelection.title,
                    subtitle: currentInspectorSelection.subtitle,
                    scopeLabel: currentInspectorSelection.scopeLabel,
                    items: matches
                });
            }
        }
    }

    async function loadHeatmap() {
        try {
            var response = await fetch("/api/sp500-heatmap", { cache: "no-store" });
            if (!response.ok) throw new Error("HTTP " + response.status);
            currentHeatmapPayload = await response.json();
            renderHeatmap(currentHeatmapPayload);
        } catch (error) {
            console.error("sp500 heatmap load failed:", error);
            setHeatmapEmpty("暫時無法建立 S&P 500 heatmap。");
        }
    }

    window.addEventListener("resize", function () {
        if (!currentHeatmapPayload) return;
        window.clearTimeout(resizeTimer);
        resizeTimer = window.setTimeout(function () {
            renderHeatmap(currentHeatmapPayload);
        }, 120);
    });

    loadMarkets();
    loadHeatmap();
    loadSectorPerformance();
    window.setInterval(loadMarkets, marketRefreshMs);
    window.setInterval(loadHeatmap, heatmapRefreshMs);
    window.setInterval(loadSectorPerformance, sectorPerformanceRefreshMs);

    inspectorClose.addEventListener("click", function () {
        closeInspector();
    });
})();
