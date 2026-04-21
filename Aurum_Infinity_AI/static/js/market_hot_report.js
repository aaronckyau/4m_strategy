(function () {
    "use strict";

    var marketReportModal = document.getElementById("market-report-modal");
    var marketReportDataNode = document.getElementById("market-report-data");
    if (!marketReportModal || !marketReportDataNode) return;

    var marketReportOpeners = Array.prototype.slice.call(document.querySelectorAll("[data-market-report-open]"));
    var marketReportClosers = Array.prototype.slice.call(document.querySelectorAll("[data-market-report-close]"));
    var marketReportTitle = document.getElementById("market-report-modal-title");
    var marketReportLabel = document.getElementById("market-report-label");
    var marketReportBody = document.getElementById("market-report-body");
    var marketBullishTitle = document.getElementById("market-bullish-title");
    var marketBullishAnalysis = document.getElementById("market-bullish-analysis");
    var marketBullishStocks = document.getElementById("market-bullish-stocks");
    var marketRiskTitle = document.getElementById("market-risk-title");
    var marketRiskAnalysis = document.getElementById("market-risk-analysis");
    var marketRiskStocks = document.getElementById("market-risk-stocks");
    var marketReportData = null;
    var lastMarketReportTrigger = null;
    var lang = window._MARKET_REPORT_LANG || "zh_hk";

    try {
        marketReportData = JSON.parse(marketReportDataNode.textContent || "{}");
    } catch (error) {
        console.error("market report data parse failed:", error);
        return;
    }

    function clearNode(node) {
        if (node) node.innerHTML = "";
    }

    function renderParagraphs(node, text) {
        if (!node) return;
        clearNode(node);
        String(text || "")
            .split(/\n\s*\n/)
            .map(function (paragraph) { return paragraph.trim(); })
            .filter(Boolean)
            .forEach(function (paragraph) {
                var element = document.createElement("p");
                element.textContent = paragraph;
                node.appendChild(element);
            });
    }

    function stockHref(symbol) {
        var cleanSymbol = encodeURIComponent(symbol || "");
        return "/" + cleanSymbol + "?lang=" + encodeURIComponent(lang);
    }

    function renderScenarioStocks(node, stocks) {
        if (!node) return;
        clearNode(node);
        (stocks || []).forEach(function (stock) {
            var article = document.createElement("article");
            article.className = "scenario-stock";

            var symbol = document.createElement("a");
            symbol.href = stockHref(stock.symbol || "");
            symbol.textContent = stock.symbol || "N/A";
            article.appendChild(symbol);

            var reason = document.createElement("span");
            reason.textContent = stock.reason || "暫未有明確原因。";
            article.appendChild(reason);

            if (stock.risk) {
                var risk = document.createElement("small");
                risk.textContent = stock.risk;
                article.appendChild(risk);
            }

            node.appendChild(article);
        });
    }

    function normalizeScenario(scenario, stockKey) {
        scenario = scenario || {};
        return {
            title: scenario.title || "",
            analysis: scenario.analysis || "",
            stocks: scenario[stockKey] || []
        };
    }

    function getMarketReportPayload(trigger) {
        if (!marketReportData) return null;
        var topicIndex = trigger && trigger.dataset ? trigger.dataset.topicIndex : "";
        var topics = marketReportData.market_topics || [];

        if (topicIndex !== "" && topicIndex !== undefined) {
            var topic = topics[Number(topicIndex)];
            if (topic) {
                return {
                    title: topic.topic || marketReportData.title || "市場熱話報告",
                    label: topic.type || "Topic Report",
                    report: topic.detail_report || topic.summary || "",
                    bullish: normalizeScenario(topic.bullish_scenario || marketReportData.bullish_scenario, "suggested_stocks"),
                    risk: normalizeScenario(topic.risk_scenario || marketReportData.risk_scenario, "watchlist_stocks")
                };
            }
        }

        return {
            title: marketReportData.title || "市場熱話報告",
            label: "Report",
            report: marketReportData.report || marketReportData.executive_summary || "",
            bullish: normalizeScenario(marketReportData.bullish_scenario, "suggested_stocks"),
            risk: normalizeScenario(marketReportData.risk_scenario, "watchlist_stocks")
        };
    }

    function renderMarketReport(trigger) {
        var payload = getMarketReportPayload(trigger);
        if (!payload) return;

        if (marketReportTitle) marketReportTitle.textContent = payload.title;
        if (marketReportLabel) marketReportLabel.textContent = payload.label;
        renderParagraphs(marketReportBody, payload.report);

        if (marketBullishTitle) marketBullishTitle.textContent = payload.bullish.title || "情境A 看漲";
        if (marketBullishAnalysis) marketBullishAnalysis.textContent = payload.bullish.analysis || "";
        renderScenarioStocks(marketBullishStocks, payload.bullish.stocks);

        if (marketRiskTitle) marketRiskTitle.textContent = payload.risk.title || "情境B 風險";
        if (marketRiskAnalysis) marketRiskAnalysis.textContent = payload.risk.analysis || "";
        renderScenarioStocks(marketRiskStocks, payload.risk.stocks);
    }

    function openMarketReport(trigger) {
        lastMarketReportTrigger = trigger || document.activeElement;
        marketReportModal.hidden = false;
        document.body.classList.add("market-report-lock");
        var closeButton = marketReportModal.querySelector(".market-report-close");
        if (closeButton) closeButton.focus();
    }

    function closeMarketReport() {
        if (marketReportModal.hidden) return;
        marketReportModal.hidden = true;
        document.body.classList.remove("market-report-lock");
        if (lastMarketReportTrigger && typeof lastMarketReportTrigger.focus === "function") {
            lastMarketReportTrigger.focus();
        }
        lastMarketReportTrigger = null;
    }

    marketReportOpeners.forEach(function (button) {
        button.addEventListener("click", function () {
            renderMarketReport(button);
            openMarketReport(button);
        });
    });

    marketReportClosers.forEach(function (button) {
        button.addEventListener("click", closeMarketReport);
    });

    document.addEventListener("keydown", function (event) {
        if (event.key === "Escape") {
            closeMarketReport();
        }
    });
})();
