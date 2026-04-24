(function () {
    if (document.body.dataset.page !== "markets") return;

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
    var sectorPerformanceGrid = document.getElementById("sector-performance-grid");
    var sectorPerformanceDate = document.getElementById("sector-performance-date");
    var sectorPerformanceSync = document.getElementById("sector-performance-sync");
    var performanceTabs = Array.prototype.slice.call(document.querySelectorAll(".performance-tab"));
    var i18nNode = document.getElementById("markets-i18n");
    var i18n = {};

    var displayTimeZone = "Asia/Hong_Kong";
    var activePerformanceIndex = 0;
    var currentHeatmapPayload = null;
    var currentInspectorSelection = null;
    var resizeTimer = null;
    var EMPTY_VALUE = "--";

    if (i18nNode) {
        try { i18n = JSON.parse(i18nNode.textContent || "{}"); } catch (error) { console.error(error); }
    }

    function tr(key) { return i18n[key] || ""; }
    function interpolate(template, values) {
        return String(template || "").replace(/\{(\w+)\}/g, function (_, key) {
            return values[key] !== undefined ? values[key] : "";
        });
    }

    var tooltip = document.createElement("div");
    tooltip.className = "heatmap-tooltip";
    heatmapSurface.appendChild(tooltip);

    function formatNumber(value, digits) {
        var num = Number(value);
        if (!Number.isFinite(num)) return EMPTY_VALUE;
        return num.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
    }
    function formatSigned(value, digits) {
        var num = Number(value);
        if (!Number.isFinite(num)) return EMPTY_VALUE;
        return (num > 0 ? "+" : "") + formatNumber(num, digits);
    }
    function formatPercent(value) { return Number.isFinite(Number(value)) ? formatSigned(value, 2) + "%" : EMPTY_VALUE; }
    function formatAxisPercent(value) { return Number.isFinite(Number(value)) ? ((Math.abs(value) >= 1 ? value.toFixed(0) : value.toFixed(1)) + "%") : EMPTY_VALUE; }
    function formatMarketCap(value) {
        var num = Number(value);
        if (!Number.isFinite(num)) return EMPTY_VALUE;
        var abs = Math.abs(num);
        if (abs >= 1e12) return "$" + (num / 1e12).toFixed(2) + "T";
        if (abs >= 1e9) return "$" + (num / 1e9).toFixed(1) + "B";
        if (abs >= 1e6) return "$" + (num / 1e6).toFixed(1) + "M";
        return "$" + num.toLocaleString("en-US", { maximumFractionDigits: 0 });
    }
    function formatDateTime(value) {
        if (!value) return EMPTY_VALUE;
        var date = new Date(value);
        if (Number.isNaN(date.getTime())) return value;
        return date.toLocaleString(i18n.locale || "zh-HK", {
            timeZone: displayTimeZone, hour12: false, year: "numeric", month: "2-digit",
            day: "2-digit", hour: "2-digit", minute: "2-digit"
        }) + " " + tr("timezone_suffix");
    }
    function changeClass(change) {
        var num = Number(change);
        if (!Number.isFinite(num) || num === 0) return "is-flat";
        return num > 0 ? "is-up" : "is-down";
    }

    function setMarketError(message) { grid.innerHTML = '<div class="markets-error">' + message + "</div>"; }
    function setSectorPerformanceError(message) { sectorPerformanceGrid.innerHTML = '<div class="markets-error">' + message + "</div>"; }
    function setHeatmapEmpty(message) {
        heatmapEmpty.hidden = false;
        heatmapEmpty.textContent = message;
        heatmapBoard.innerHTML = "";
        heatmapBoard.classList.remove("is-loading");
        tooltip.classList.remove("is-visible");
        closeInspector();
    }
    function hideHeatmapEmpty() {
        heatmapEmpty.hidden = true;
        heatmapBoard.classList.remove("is-loading");
    }

    function renderMarketChip(item) {
        return '<article class="ticker-chip"><p class="ticker-name">' + item.label +
            '</p><div class="ticker-price">' + formatNumber(item.price, 2) +
            '</div><div class="ticker-change ' + changeClass(item.change) + '">' +
            formatSigned(item.change, 2) + "</div></article>";
    }

    async function loadMarkets() {
        try {
            var response = await fetch("/api/market-indices", { cache: "no-store" });
            if (!response.ok) throw new Error("HTTP " + response.status);
            var payload = await response.json();
            if (!payload.indices || !payload.indices.length) throw new Error("Empty payload");
            grid.innerHTML = payload.indices.map(renderMarketChip).join("");
        } catch (error) {
            console.error("market indices load failed:", error);
            setMarketError(tr("indices_error"));
        }
    }

    function updatePerformanceTabs() {
        performanceTabs.forEach(function (button, index) {
            var active = index === activePerformanceIndex;
            button.classList.toggle("is-active", active);
            button.setAttribute("aria-selected", active ? "true" : "false");
        });
        Array.prototype.slice.call(sectorPerformanceGrid.querySelectorAll(".performance-panel")).forEach(function (panel, index) {
            panel.classList.toggle("is-active", index === activePerformanceIndex);
        });
    }

    function renderPerformancePeriod(period, index) {
        var items = (period.items || []).filter(function (item) { return item.performance !== null && item.performance !== undefined; });
        var title = (i18n.period_labels && i18n.period_labels[period.id]) || period.label || "";
        if (!items.length) {
            return '<section class="performance-panel ' + (index === activePerformanceIndex ? "is-active" : "") +
                '"><h3 class="performance-panel-title">' + title + '</h3><div class="markets-error">' +
                tr("sector_error") + "</div></section>";
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
            var startPct = ((Math.min(perf, 0) - domainMin) / domainSpan) * 100;
            var widthPct = (Math.abs(perf) / domainSpan) * 100;
            var valueLeft = perf >= 0 ? Math.min(startPct + widthPct + 1, 95) : Math.max(startPct - 9, 1);
            return '<div class="performance-row"><div class="performance-sector">' + item.sector +
                '</div><div class="performance-bar-track"><span class="performance-zero-line" style="left:' + zeroPct +
                '%"></span><span class="performance-bar ' + changeClass(perf) + '" style="left:' + startPct +
                "%;width:" + widthPct + '%"></span><span class="performance-value" style="left:' + valueLeft +
                '%">' + formatSigned(perf, 2) + "</span></div></div>";
        }).join("");
        return '<section class="performance-panel ' + (index === activePerformanceIndex ? "is-active" : "") +
            '"><h3 class="performance-panel-title">' + title + '</h3><div class="performance-list">' + rowsHtml +
            '</div><div class="performance-axis"><span>' + formatAxisPercent(domainMin) + "</span><span>0%</span><span>" +
            formatAxisPercent(domainMax) + "</span></div></section>";
    }

    function renderSectorPerformance(payload) {
        if (!payload || !payload.periods || !payload.periods.length) {
            setSectorPerformanceError(tr("sector_error"));
            return;
        }
        sectorPerformanceGrid.innerHTML = payload.periods.map(renderPerformancePeriod).join("");
        sectorPerformanceDate.textContent = tr("price_date") + " " + (payload.latest_price_date || EMPTY_VALUE);
        sectorPerformanceSync.textContent = tr("sync_time") + " " + formatDateTime(payload.updated_at);
        updatePerformanceTabs();
    }

    async function loadSectorPerformance() {
        try {
            var response = await fetch("/api/sector-performance", { cache: "no-store" });
            if (!response.ok) throw new Error("HTTP " + response.status);
            renderSectorPerformance(await response.json());
        } catch (error) {
            console.error("sector performance load failed:", error);
            setSectorPerformanceError(tr("sector_error"));
        }
    }

    function getHeatColor(changePct) {
        if (!Number.isFinite(Number(changePct))) return "#5f564f";
        return d3.scaleLinear().domain([-3, -1, 0, 1, 3]).range(["#5c0b05", "#d43d2d", "#b7ada2", "#16a34a", "#0b5d2a"])(Math.max(-3, Math.min(3, Number(changePct))));
    }
    function openInspector() { inspector.classList.add("is-open"); }
    function closeInspector() { inspector.classList.remove("is-open"); currentInspectorSelection = null; }
    function updateHeatmapMeta(payload) {
        metaCount.textContent = interpolate(tr("heatmap_meta"), {
            rendered: payload.rendered_count || 0,
            count: payload.constituent_count || 0
        });
    }
    function buildHierarchy(stocks) {
        var bySector = {};
        stocks.forEach(function (item) {
            var sector = item.sector || tr("unknown");
            var industry = item.industry || tr("unknown");
            bySector[sector] = bySector[sector] || {};
            bySector[sector][industry] = bySector[sector][industry] || [];
            bySector[sector][industry].push(item);
        });
        return { name: tr("root_label"), children: Object.keys(bySector).sort().map(function (sector) {
            return { name: sector, children: Object.keys(bySector[sector]).sort().map(function (industry) {
                return { name: industry, children: bySector[sector][industry] };
            }) };
        }) };
    }
    function summarizeGroup(items) {
        var totalMarketCap = 0, weightedChange = 0, validWeight = 0;
        items.forEach(function (item) {
            var cap = Number(item.market_cap) || 0;
            var change = Number(item.change_pct);
            totalMarketCap += cap;
            if (cap > 0 && Number.isFinite(change)) { weightedChange += cap * change; validWeight += cap; }
        });
        return {
            count: items.length,
            totalMarketCap: totalMarketCap,
            avgChange: validWeight > 0 ? weightedChange / validWeight : null,
            topHoldings: items.slice().sort(function (a, b) { return (Number(b.market_cap) || 0) - (Number(a.market_cap) || 0); }).slice(0, 6)
        };
    }

    function renderInspector(selection) {
        if (!selection || !selection.items || !selection.items.length) { closeInspector(); return; }
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
            return '<div class="inspector-row"><div class="inspector-row-main"><span class="inspector-row-ticker">' +
                item.ticker + '</span><span class="inspector-row-name">' + item.name +
                '</span></div><div class="inspector-row-side"><span class="inspector-row-cap">' +
                formatMarketCap(item.market_cap) + '</span><span class="inspector-row-change ' +
                changeClass(item.change_pct) + '">' + formatPercent(item.change_pct) + "</span></div></div>";
        }).join("");
        openInspector();
    }

    function showTooltip(event, data) {
        tooltip.innerHTML = '<p class="tooltip-ticker">' + data.ticker + '</p><p class="tooltip-name">' + data.name +
            '</p><div class="tooltip-grid"><div><span class="tooltip-label">' + tr("tooltip_sector") +
            '</span><span class="tooltip-value">' + data.sector + '</span></div><div><span class="tooltip-label">' +
            tr("tooltip_price") + '</span><span class="tooltip-value">' + formatNumber(data.price, 2) +
            '</span></div><div><span class="tooltip-label">' + tr("tooltip_change") +
            '</span><span class="tooltip-value ' + changeClass(data.change_pct) + '">' + formatPercent(data.change_pct) +
            '</span></div><div><span class="tooltip-label">' + tr("tooltip_market_cap") +
            '</span><span class="tooltip-value">' + formatMarketCap(data.market_cap) + "</span></div></div>";
        tooltip.classList.add("is-visible");
        moveTooltip(event);
    }

    function moveTooltip(event) {
        var surfaceRect = heatmapSurface.getBoundingClientRect();
        var tooltipRect = tooltip.getBoundingClientRect();
        var left = event.clientX - surfaceRect.left + 16;
        var top = event.clientY - surfaceRect.top + 16;
        if (left + tooltipRect.width > surfaceRect.width - 8) left = surfaceRect.width - tooltipRect.width - 8;
        if (top + tooltipRect.height > surfaceRect.height - 8) top = surfaceRect.height - tooltipRect.height - 8;
        tooltip.style.left = Math.max(8, left) + "px";
        tooltip.style.top = Math.max(8, top) + "px";
    }

    function hideTooltip() { tooltip.classList.remove("is-visible"); }

    function reselectInspector(leaves) {
        if (!currentInspectorSelection) return;
        var matches = leaves.map(function (leaf) { return leaf.data; }).filter(function (item) {
            if (currentInspectorSelection.kind === "sector") return item.sector === currentInspectorSelection.title;
            return item.sector === currentInspectorSelection.sector && item.industry === currentInspectorSelection.title;
        });
        if (!matches.length) { closeInspector(); return; }
        renderInspector({
            kind: currentInspectorSelection.kind,
            title: currentInspectorSelection.title,
            subtitle: currentInspectorSelection.subtitle,
            scopeLabel: currentInspectorSelection.scopeLabel,
            sector: currentInspectorSelection.sector,
            items: matches
        });
    }

    function renderHeatmap(payload) {
        if (!window.d3 || !payload || !payload.stocks || !payload.stocks.length) {
            setHeatmapEmpty(tr("heatmap_empty"));
            return;
        }
        hideHeatmapEmpty();
        updateHeatmapMeta(payload);
        var width = Math.max(heatmapBoard.clientWidth, 320);
        var height = Math.max(Math.round(width * 0.6), 520);
        heatmapBoard.style.height = height + "px";
        heatmapBoard.innerHTML = "";

        var root = d3.hierarchy(buildHierarchy(payload.stocks))
            .sum(function (d) { return d.market_cap || 0; })
            .sort(function (a, b) { return (b.value || 0) - (a.value || 0); });
        d3.treemap().tile(d3.treemapSquarify.ratio(1.15)).size([width, height]).paddingOuter(6).paddingInner(2).paddingTop(function (node) {
            if (node.depth === 1) return 22;
            if (node.depth === 2) return 16;
            return 0;
        })(root);

        var svg = d3.select(heatmapBoard).append("svg").attr("class", "heatmap-svg").attr("viewBox", "0 0 " + width + " " + height).attr("preserveAspectRatio", "xMidYMid meet");
        var sectors = root.children || [];
        var industries = [];
        sectors.forEach(function (sectorNode) { (sectorNode.children || []).forEach(function (industryNode) { industries.push(industryNode); }); });
        var leaves = root.leaves();

        var sectorGroup = svg.append("g").attr("class", "sector-layer");
        sectorGroup.selectAll("rect.sector-frame").data(sectors).enter().append("rect")
            .attr("class", "sector-frame").attr("x", function (d) { return d.x0; }).attr("y", function (d) { return d.y0; })
            .attr("width", function (d) { return Math.max(0, d.x1 - d.x0); }).attr("height", function (d) { return Math.max(0, d.y1 - d.y0); });

        var visibleSectors = sectors.filter(function (d) { return (d.x1 - d.x0) > 96 && (d.y1 - d.y0) > 30; });
        sectorGroup.selectAll("rect.sector-label-bar").data(visibleSectors).enter().append("rect")
            .attr("class", "sector-label-bar").attr("x", function (d) { return d.x0 + 1; }).attr("y", function (d) { return d.y0 + 1; })
            .attr("width", function (d) { return Math.max(0, d.x1 - d.x0 - 2); }).attr("height", 18).attr("rx", 4).attr("ry", 4)
            .on("click", function (event, d) {
                event.stopPropagation();
                renderInspector({
                    kind: "sector",
                    title: d.data.name,
                    subtitle: tr("sector_summary_subtitle"),
                    scopeLabel: tr("scope_sector"),
                    sector: d.data.name,
                    items: d.leaves().map(function (leaf) { return leaf.data; })
                });
            });
        sectorGroup.selectAll("text.sector-label").data(visibleSectors).enter().append("text")
            .attr("class", "sector-label").attr("x", function (d) { return d.x0 + 8; }).attr("y", function (d) { return d.y0 + 13; })
            .text(function (d) { return d.data.name; });

        var industryGroup = svg.append("g").attr("class", "industry-layer");
        industryGroup.selectAll("rect.industry-frame").data(industries).enter().append("rect")
            .attr("class", "industry-frame").attr("x", function (d) { return d.x0; }).attr("y", function (d) { return d.y0; })
            .attr("width", function (d) { return Math.max(0, d.x1 - d.x0); }).attr("height", function (d) { return Math.max(0, d.y1 - d.y0); });

        var visibleIndustries = industries.filter(function (d) { return (d.x1 - d.x0) > 92 && (d.y1 - d.y0) > 22; });
        industryGroup.selectAll("rect.industry-label-bar").data(visibleIndustries).enter().append("rect")
            .attr("class", "industry-label-bar").attr("x", function (d) { return d.x0 + 1; }).attr("y", function (d) { return d.y0 + 1; })
            .attr("width", function (d) { return Math.max(0, d.x1 - d.x0 - 2); }).attr("height", 12).attr("rx", 3).attr("ry", 3)
            .on("click", function (event, d) {
                event.stopPropagation();
                renderInspector({
                    kind: "industry",
                    title: d.data.name,
                    subtitle: d.parent ? interpolate(tr("industry_summary_subtitle"), { sector: d.parent.data.name }) : tr("industry_summary_fallback"),
                    scopeLabel: d.parent ? d.parent.data.name : tr("scope_industry"),
                    sector: d.parent ? d.parent.data.name : null,
                    items: d.leaves().map(function (leaf) { return leaf.data; })
                });
            });
        industryGroup.selectAll("text.industry-label").data(visibleIndustries).enter().append("text")
            .attr("class", "industry-label").attr("x", function (d) { return d.x0 + 6; }).attr("y", function (d) { return d.y0 + 9; })
            .text(function (d) { return d.data.name; });

        var tileGroup = svg.append("g").attr("class", "tile-layer").selectAll("g").data(leaves).enter().append("g")
            .attr("class", "tile-group").attr("transform", function (d) { return "translate(" + d.x0 + "," + d.y0 + ")"; });
        tileGroup.append("rect").attr("class", "tile-rect").attr("width", function (d) { return Math.max(0, d.x1 - d.x0); })
            .attr("height", function (d) { return Math.max(0, d.y1 - d.y0); }).attr("fill", function (d) { return getHeatColor(d.data.change_pct); })
            .on("mouseenter", function (event, d) { showTooltip(event, d.data); })
            .on("mousemove", function (event) { moveTooltip(event); })
            .on("mouseleave", hideTooltip);

        tileGroup.each(function (d) {
            var group = d3.select(this);
            var widthBox = d.x1 - d.x0;
            var heightBox = d.y1 - d.y0;
            if (widthBox > 48 && heightBox > 28) {
                group.append("text").attr("class", "tile-label").attr("x", 7).attr("y", 16).text(d.data.ticker);
            }
            if (widthBox > 68 && heightBox > 46) {
                group.append("text").attr("class", "tile-sub").attr("x", 7).attr("y", 32).text(formatPercent(d.data.change_pct));
            } else if (widthBox > 72 && heightBox > 32) {
                group.append("text").attr("class", "tile-sub is-muted").attr("x", 7).attr("y", 31).text(formatMarketCap(d.data.market_cap));
            }
        });

        svg.on("click", function () { hideTooltip(); closeInspector(); });
        reselectInspector(leaves);
    }

    async function loadHeatmap() {
        try {
            var response = await fetch("/api/sp500-heatmap", { cache: "no-store" });
            if (!response.ok) throw new Error("HTTP " + response.status);
            currentHeatmapPayload = await response.json();
            renderHeatmap(currentHeatmapPayload);
        } catch (error) {
            console.error("sp500 heatmap load failed:", error);
            setHeatmapEmpty(tr("heatmap_empty"));
        }
    }

    performanceTabs.forEach(function (button) {
        button.addEventListener("click", function () {
            activePerformanceIndex = Number(button.dataset.periodIndex || 0);
            updatePerformanceTabs();
        });
    });
    window.addEventListener("resize", function () {
        if (!currentHeatmapPayload) return;
        window.clearTimeout(resizeTimer);
        resizeTimer = window.setTimeout(function () { renderHeatmap(currentHeatmapPayload); }, 120);
    });
    inspectorClose.addEventListener("click", function (event) { event.stopPropagation(); closeInspector(); });
    heatmapSurface.addEventListener("click", function (event) {
        if (event.target === heatmapSurface || event.target === heatmapBoard) { closeInspector(); hideTooltip(); }
    });

    // ── Ticker Tape ────────────────────────────────────────────
    // symbol → [{chip, priceEl, changeEl, sparkEl}, ...]  (2 copies for loop)
    var tapeChips = {};
    var tapeUpdatedEl = document.querySelector(".tape-updated-text");

    function initTapeRefs() {
        document.querySelectorAll(".tape-chip[data-symbol]").forEach(function (chip) {
            var sym = chip.dataset.symbol;
            if (!tapeChips[sym]) tapeChips[sym] = [];
            tapeChips[sym].push({
                chip:     chip,
                priceEl:  chip.querySelector(".tape-price"),
                changeEl: chip.querySelector(".tape-change"),
                sparkEl:  chip.querySelector(".tape-spark polyline"),
            });
        });
    }

    function flashTapeChip(chip, dir) {
        chip.classList.remove("tape-flash-up", "tape-flash-down", "tape-flash-flat");
        void chip.offsetWidth;
        chip.classList.add(dir === "up" ? "tape-flash-up" : dir === "down" ? "tape-flash-down" : "tape-flash-flat");
    }

    function renderSparkPoints(closes) {
        if (!closes || closes.length < 2) return "";
        var mn = Math.min.apply(null, closes);
        var mx = Math.max.apply(null, closes);
        var rng = mx - mn || 1;
        var n = closes.length;
        return closes.map(function (v, i) {
            var x = (i / (n - 1) * 60).toFixed(1);
            var y = (24 - ((v - mn) / rng * 20) - 2).toFixed(1);
            return x + "," + y;
        }).join(" ");
    }

    function applyTapeItem(item) {
        var refs = tapeChips[item.symbol];
        if (!refs) return;

        var newPrice  = item.price  || "\u2014";
        var newChange = item.change || "\u2014";
        var newDir    = item.dir    || "flat";
        var sparkPts  = renderSparkPoints(item.sparkline || []);

        refs.forEach(function (ref) {
            var priceChanged  = ref.priceEl  && ref.priceEl.textContent  !== newPrice;
            var changeChanged = ref.changeEl && ref.changeEl.textContent !== newChange;

            if (priceChanged  && ref.priceEl)  ref.priceEl.textContent  = newPrice;
            if (changeChanged && ref.changeEl) ref.changeEl.textContent = newChange;

            if (ref.changeEl) {
                ref.changeEl.classList.remove("is-up", "is-down", "is-flat");
                ref.changeEl.classList.add("is-" + newDir);
            }
            if (ref.sparkEl && sparkPts) {
                ref.sparkEl.setAttribute("points", sparkPts);
                var svg = ref.sparkEl.closest("svg");
                if (svg) {
                    svg.classList.remove("is-up", "is-down", "is-flat");
                    svg.classList.add("is-" + newDir);
                }
            }
            if (priceChanged || changeChanged) flashTapeChip(ref.chip, newDir);
        });
    }

    async function loadPulse() {
        try {
            var resp = await fetch("/api/markets/pulse", { cache: "no-store" });
            if (!resp.ok) throw new Error("HTTP " + resp.status);
            var payload = await resp.json();
            (payload.pulse || []).forEach(applyTapeItem);
            if (tapeUpdatedEl && payload.updated_at) {
                tapeUpdatedEl.textContent = payload.updated_at;
            }
        } catch (err) {
            console.warn("pulse load failed:", err);
        }
    }

    initTapeRefs();
    loadMarkets();
    loadHeatmap();
    loadSectorPerformance();
    loadPulse();
    window.setInterval(loadPulse, 15000);
    window.setInterval(loadMarkets, 5000);
    window.setInterval(loadHeatmap, 900000);
    window.setInterval(loadSectorPerformance, 3600000);
})();

