from __future__ import annotations

from threading import Lock

from flask import Flask
import requests

from blueprints.stock import stock_bp
from blueprints.stock import routes as stock_routes


def create_app():
    app = Flask(__name__)
    app.register_blueprint(stock_bp)
    return app


class FakeQueryResult:
    def __init__(self, *, one=None, many=None):
        self._one = one
        self._many = many or []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class FakeDb:
    def __init__(self, handlers):
        self.handlers = handlers
        self.closed = False

    def execute(self, query, params=()):
        for key, result in self.handlers.items():
            if key in query:
                value = result(query, params) if callable(result) else result
                if isinstance(value, FakeQueryResult):
                    return value
                if isinstance(value, list):
                    return FakeQueryResult(many=value)
                return FakeQueryResult(one=value)
        raise AssertionError(f"Unexpected query: {query}")

    def close(self):
        self.closed = True


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class TestStockHelpers:
    def test_is_valid_ticker_accepts_common_symbols(self):
        assert stock_routes.is_valid_ticker("AAPL") is True
        assert stock_routes.is_valid_ticker("0700.HK") is True
        assert stock_routes.is_valid_ticker("600519.SS") is True

    def test_is_valid_ticker_rejects_bad_input(self):
        assert stock_routes.is_valid_ticker("../secret") is False
        assert stock_routes.is_valid_ticker(".env") is False
        assert stock_routes.is_valid_ticker("TOO-LONG-TICKER") is False

    def test_supported_us_stock_allows_plain_ticker_when_metadata_is_sparse(self):
        assert stock_routes._is_supported_us_stock({"ticker": "AAPL"}, "AAPL") is True
        assert stock_routes._is_supported_us_stock({"ticker": "0700.HK"}, "0700.HK") is False


class TestStockRoutes:
    def test_index_rejects_blacklisted_static_names(self):
        app = create_app()
        client = app.test_client()

        response = client.get("/favicon.ico")

        assert response.status_code == 404

    def test_index_rejects_invalid_ticker(self):
        app = create_app()
        client = app.test_client()

        response = client.get("/bad/ticker")

        assert response.status_code == 404

    def test_index_redirects_to_canonical_ticker(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(stock_routes, "resolve_ticker", lambda raw: "AAPL")
        monkeypatch.setattr(
            stock_routes,
            "get_stock_info",
            lambda ticker: {"name": "Apple", "name_zh_hk": "蘋果", "exchange": "NASDAQ", "market": "US"},
        )
        client = app.test_client()

        response = client.get("/apple")

        assert response.status_code == 301
        assert response.headers["Location"].endswith("/AAPL")

    def test_index_returns_404_template_when_stock_not_found(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(stock_routes, "resolve_ticker", lambda raw: raw.upper())
        monkeypatch.setattr(stock_routes, "get_stock_info", lambda ticker: None)
        monkeypatch.setattr(stock_routes, "get_current_lang", lambda: "zh_hk")
        monkeypatch.setattr(stock_routes, "get_translations", lambda lang: {"lang": lang})
        monkeypatch.setattr(stock_routes, "get_today", lambda: "2026/04/21")
        monkeypatch.setattr(stock_routes, "render_template", lambda template, **context: f"{template}:{context['ticker']}")
        client = app.test_client()

        response = client.get("/AAPL")

        assert response.status_code == 404
        assert response.get_data(as_text=True) == "stock/error.html:AAPL"

    def test_index_rejects_non_us_stock(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(stock_routes, "resolve_ticker", lambda raw: "0700.HK")
        monkeypatch.setattr(
            stock_routes,
            "get_stock_info",
            lambda ticker: {"name": "Tencent", "name_zh_hk": "騰訊控股", "exchange": "HKEX", "market": "HK"},
        )
        monkeypatch.setattr(stock_routes, "get_current_lang", lambda: "zh_hk")
        monkeypatch.setattr(stock_routes, "get_translations", lambda lang: {"lang": lang})
        monkeypatch.setattr(stock_routes, "get_today", lambda: "2026/04/21")
        monkeypatch.setattr(stock_routes, "render_template", lambda template, **context: f"{template}:{context['ticker']}")
        client = app.test_client()

        response = client.get("/700")

        assert response.status_code == 404
        assert response.get_data(as_text=True) == "stock/error.html:700"

    def test_index_syncs_file_cache_metadata_from_db(self, monkeypatch):
        app = create_app()
        saved = {}
        monkeypatch.setattr(stock_routes, "resolve_ticker", lambda raw: raw.upper())
        monkeypatch.setattr(
            stock_routes,
            "get_stock_info",
            lambda ticker: {
                "name": "Apple Updated",
                "name_zh_hk": "蘋果更新",
                "name_zh_cn": "苹果更新",
                "exchange": "NASDAQ",
                "market": "US",
                "sector": "",
                "industry": "",
            },
        )
        monkeypatch.setattr(stock_routes, "save_stock", lambda **kwargs: saved.update(kwargs))
        monkeypatch.setattr(stock_routes, "get_current_lang", lambda: "zh_hk")
        monkeypatch.setattr(stock_routes, "get_translations", lambda lang: {"lang": lang})
        monkeypatch.setattr(stock_routes, "_get_sector_industry_i18n", lambda sector, industry, lang: ("", ""))
        monkeypatch.setattr(stock_routes, "get_section_html", lambda ticker, section, lang: None)
        monkeypatch.setattr(stock_routes, "get_section_date", lambda ticker, section, lang: None)
        monkeypatch.setattr(stock_routes, "render_template", lambda template, **context: context["stock_name"])
        client = app.test_client()

        response = client.get("/AAPL")

        assert response.status_code == 200
        assert saved == {
            "ticker": "AAPL",
            "stock_name": "Apple Updated",
            "chinese_name": "蘋果更新",
            "exchange": "NASDAQ",
        }

    def test_api_stock_display_validates_and_returns_display_name(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(stock_routes, "resolve_ticker", lambda raw: raw.upper())
        monkeypatch.setattr(
            stock_routes,
            "get_stock_info",
            lambda ticker: {
                "name": "Apple",
                "name_zh_hk": "蘋果",
                "name_zh_cn": "苹果",
                "market": "US",
                "exchange": "NASDAQ",
                "sector": "Technology",
                "industry": "Internet",
            },
        )
        monkeypatch.setattr(stock_routes, "_get_sector_industry_i18n", lambda sector, industry, lang: ("科技", "互聯網"))
        client = app.test_client()

        response = client.get("/api/stock_display?ticker=AAPL&lang=zh_hk")

        assert response.status_code == 200
        assert response.get_json()["display_name"] == "蘋果"

    def test_search_stock_returns_us_results_only(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(stock_routes, "get_current_lang", lambda: "zh_hk")
        monkeypatch.setattr(
            stock_routes,
            "search_stocks",
            lambda query, limit=8: [
                {"code": "AAPL", "name": "Apple", "name_zh_hk": "Apple", "name_zh_cn": "Apple", "market": "US", "exchange": "NASDAQ"},
                {"code": "0700.HK", "name": "Tencent", "name_zh_hk": "Tencent", "name_zh_cn": "Tencent", "market": "HK", "exchange": "HKEX"},
            ],
        )
        client = app.test_client()

        response = client.get("/api/search_stock?q=a")

        assert response.status_code == 200
        assert [row["code"] for row in response.get_json()] == ["AAPL"]

    def test_api_stock_display_returns_400_without_ticker(self):
        app = create_app()
        client = app.test_client()

        response = client.get("/api/stock_display")

        assert response.status_code == 400

    def test_api_markdown_section_returns_markdown_attachment(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(stock_routes, "resolve_ticker", lambda raw: raw.upper())
        monkeypatch.setattr(stock_routes, "get_section_md", lambda ticker, section, lang: f"# {lang}")
        client = app.test_client()

        response = client.get("/api/markdown/AAPL/biz?lang=en")

        assert response.status_code == 200
        assert response.get_data(as_text=True) == "# zh_hk"
        assert "attachment;" in response.headers["Content-Disposition"]

    def test_api_markdown_section_rejects_invalid_section(self):
        app = create_app()
        client = app.test_client()

        response = client.get("/api/markdown/AAPL/not-real")

        assert response.status_code == 404

    def test_analyze_section_rejects_invalid_section(self):
        app = create_app()
        client = app.test_client()

        response = client.post("/analyze/not-real", json={"ticker": "AAPL"})

        assert response.status_code == 400
        assert response.get_json()["success"] is False

    def test_analyze_section_requires_json(self):
        app = create_app()
        client = app.test_client()

        response = client.post("/analyze/biz", data="{}", content_type="text/plain")

        assert response.status_code == 415

    def test_analyze_section_rejects_invalid_ticker(self):
        app = create_app()
        client = app.test_client()

        response = client.post("/analyze/biz", json={"ticker": "../bad"})

        assert response.status_code == 400
        assert response.get_json()["error"] == "無效的股票代碼格式"

    def test_analyze_section_returns_404_when_stock_missing(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(stock_routes, "resolve_ticker", lambda raw: raw.upper())
        monkeypatch.setattr(stock_routes, "get_stock_info", lambda ticker: None)
        client = app.test_client()

        response = client.post("/analyze/biz", json={"ticker": "AAPL", "lang": "en", "force_update": True})

        assert response.status_code == 404
        assert "AAPL" in response.get_json()["error"]

    def test_analyze_section_returns_cache_before_db_gate(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(stock_routes, "resolve_ticker", lambda raw: raw.upper())
        monkeypatch.setattr(stock_routes, "get_stock_info", lambda ticker: None)
        monkeypatch.setattr(stock_routes, "get_section_html", lambda ticker, section, lang: "<p>cached</p>")
        monkeypatch.setattr(stock_routes, "is_translation_stale", lambda ticker, section, lang: False)
        monkeypatch.setattr(stock_routes, "get_section_md", lambda ticker, section, lang: "cached summary")
        monkeypatch.setattr(stock_routes, "extract_card_summary", lambda md: "cached summary")
        monkeypatch.setattr(stock_routes, "get_section_date", lambda ticker, section, lang: "2026/04/22")
        client = app.test_client()

        response = client.post("/analyze/biz", json={"ticker": "AAPL", "lang": "en"})

        payload = response.get_json()
        assert response.status_code == 200
        assert payload["from_cache"] is True
        assert payload["report"] == "<p>cached</p>"

    def test_analyze_section_does_not_cache_failed_ai_report(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(stock_routes, "resolve_ticker", lambda raw: raw.upper())
        monkeypatch.setattr(
            stock_routes,
            "get_stock_info",
            lambda ticker: {"name": "Apple", "name_zh_hk": "蘋果", "exchange": "NASDAQ", "market": "US"},
        )
        monkeypatch.setattr(stock_routes, "get_stock", lambda ticker: {"stock_name": "Apple"})
        monkeypatch.setattr(stock_routes.prompt_manager, "build", lambda **kwargs: "prompt")
        monkeypatch.setattr(stock_routes, "call_gemini_api", lambda prompt, use_search=True: "⚠️ API 請求失敗。")
        monkeypatch.setattr(stock_routes, "save_section_md", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not save md")))
        monkeypatch.setattr(stock_routes, "save_section_html", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not save html")))
        client = app.test_client()

        response = client.post("/analyze/biz", json={"ticker": "AAPL", "force_update": True})

        assert response.status_code == 502
        assert response.get_json()["success"] is False

    def test_get_current_lang_ignores_english_browser_preference(self):
        app = create_app()

        with app.test_request_context("/", headers={"Accept-Language": "en-US,en;q=0.9"}):
            assert stock_routes.get_current_lang() == "zh_hk"

    def test_analyze_section_returns_conflict_when_lock_busy(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(stock_routes, "resolve_ticker", lambda raw: raw.upper())
        monkeypatch.setattr(
            stock_routes,
            "get_stock_info",
            lambda ticker: {"name": "Apple", "name_zh_hk": "蘋果", "exchange": "NASDAQ"},
        )
        monkeypatch.setattr(stock_routes, "get_stock", lambda ticker: {"stock_name": "Apple"})
        busy_lock = Lock()
        busy_lock.acquire()
        monkeypatch.setattr(stock_routes, "_get_analysis_lock", lambda key: busy_lock)
        client = app.test_client()

        response = client.post("/analyze/biz", json={"ticker": "AAPL", "force_update": True})

        busy_lock.release()
        assert response.status_code == 409
        assert "正在進行中" in response.get_json()["error"]

    def test_api_price_analysis_validates_input(self):
        app = create_app()
        client = app.test_client()

        invalid_ticker = client.post("/api/price-analysis", json={"symbol": "../bad", "start_date": "2026-01-01"})
        missing_date = client.post("/api/price-analysis", json={"symbol": "AAPL"})
        bad_date = client.post("/api/price-analysis", json={"symbol": "AAPL", "start_date": "2026/01/01"})

        assert invalid_ticker.status_code == 400
        assert missing_date.status_code == 400
        assert bad_date.status_code == 400

    def test_api_key_metrics_returns_400_for_invalid_ticker(self):
        app = create_app()
        client = app.test_client()

        response = client.get("/api/key-metrics?symbol=../bad")

        assert response.status_code == 400

    def test_api_key_metrics_returns_computed_payload(self, monkeypatch):
        app = create_app()
        fake_db = FakeDb(
            {
                "SELECT market_cap, currency FROM stocks_master": {"market_cap": 123456789, "currency": "USD"},
                "SELECT pe, peg, eps, gross_margin, net_margin": {
                    "pe": 20.5,
                    "peg": 1.8,
                    "eps": 6.2,
                    "gross_margin": 44.0,
                    "net_margin": 21.0,
                    "debt_to_equity": 0.5,
                    "dividend_yield": 0.01,
                    "dividend_per_share": 0.5,
                },
                "SELECT revenue, period, fiscal_year, fiscal_quarter": {"revenue": 1200, "period": "2026-03-31", "fiscal_year": 2026, "fiscal_quarter": 1},
                "SELECT revenue FROM financial_statements": {"revenue": 1000},
                "SELECT close, date FROM ohlc_daily": [
                    {"close": 110.0, "date": "2026-04-21"},
                    {"close": 100.0, "date": "2026-04-20"},
                ],
            }
        )
        monkeypatch.setattr(stock_routes, "resolve_ticker", lambda ticker: "AAPL")
        monkeypatch.setattr(stock_routes, "get_stock_info", lambda ticker: {"market": "US", "exchange": "NASDAQ"})
        monkeypatch.setattr(stock_routes, "get_db", lambda: fake_db)
        client = app.test_client()

        response = client.get("/api/key-metrics?symbol=AAPL")

        payload = response.get_json()
        assert response.status_code == 200
        assert payload["price"] == 110.0
        assert payload["change"] == 10.0
        assert round(payload["change_pct"], 1) == 10.0
        assert payload["revenue_yoy"] == 20.0
        assert payload["fiscal"] == "FY2026 Q1"
        assert fake_db.closed is True

    def test_api_market_indices_maps_runtime_error_to_500(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(stock_routes, "_get_cached_market_indices", lambda: (_ for _ in ()).throw(RuntimeError("missing key")))
        client = app.test_client()

        response = client.get("/api/market-indices")

        assert response.status_code == 500
        assert response.get_json()["error"] == "FMP API key not configured"

    def test_api_market_indices_maps_request_exception_to_502(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(
            stock_routes,
            "_get_cached_market_indices",
            lambda: (_ for _ in ()).throw(requests.RequestException("boom")),
        )
        client = app.test_client()

        response = client.get("/api/market-indices")

        assert response.status_code == 502

    def test_api_market_indices_returns_cached_payload(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(stock_routes, "_get_cached_market_indices", lambda: ([{"symbol": "^GSPC"}], "2026-04-21T00:00:00Z"))
        client = app.test_client()

        response = client.get("/api/market-indices")

        payload = response.get_json()
        assert response.status_code == 200
        assert payload["indices"] == [{"symbol": "^GSPC"}]
        assert payload["updated_at"] == "2026-04-21T00:00:00Z"

    def test_api_ohlc_returns_400_for_invalid_ticker(self):
        app = create_app()
        client = app.test_client()

        response = client.get("/api/ohlc?symbol=../bad")

        assert response.status_code == 400

    def test_api_ohlc_returns_rows_in_ascending_order(self, monkeypatch):
        app = create_app()
        fake_db = FakeDb(
            {
                "SELECT date, open, high, low, close, volume": [
                    {"date": "2026-04-21", "open": 11, "high": 12, "low": 10, "close": 11.5, "volume": 200},
                    {"date": "2026-04-20", "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 100},
                ]
            }
        )
        monkeypatch.setattr(stock_routes, "resolve_ticker", lambda ticker: "AAPL")
        monkeypatch.setattr(stock_routes, "get_stock_info", lambda ticker: {"market": "US", "exchange": "NASDAQ"})
        monkeypatch.setattr(stock_routes, "get_db", lambda: fake_db)
        client = app.test_client()

        response = client.get("/api/ohlc?symbol=AAPL&days=2")

        payload = response.get_json()
        assert response.status_code == 200
        assert [row["time"] for row in payload] == ["2026-04-20", "2026-04-21"]
        assert fake_db.closed is True

    def test_api_analyst_price_targets_reads_cached_db(self, monkeypatch):
        app = create_app()
        fake_db = FakeDb(
            {
                "FROM analyst_price_targets": {
                    "target_high": 350.0,
                    "target_low": 239.0,
                    "target_avg": 315.91,
                    "target_median": 325.0,
                    "analyst_count": 50,
                    "analyst_count_label": "last 12 months",
                    "publishers_json": '["TheFly","Benzinga"]',
                    "fetched_at": "2026-04-22T00:00:00Z",
                },
                "SELECT date, close FROM ohlc_daily": {
                    "date": "2026-04-21",
                    "close": 190.0,
                },
            }
        )

        monkeypatch.setattr(stock_routes, "resolve_ticker", lambda ticker: "AAPL")
        monkeypatch.setattr(stock_routes, "get_stock_info", lambda ticker: {"market": "US", "exchange": "NASDAQ"})
        monkeypatch.setattr(stock_routes, "get_db", lambda: fake_db)
        monkeypatch.setattr(stock_routes, "_fmp_get", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("FMP should not be called")))
        client = app.test_client()

        response = client.get("/api/analyst-price-targets?symbol=AAPL")

        payload = response.get_json()
        assert response.status_code == 200
        assert payload["success"] is True
        assert payload["symbol"] == "AAPL"
        assert payload["last_close"] == 190.0
        assert payload["last_close_date"] == "2026-04-21"
        assert payload["target_high"] == 350.0
        assert payload["target_low"] == 239.0
        assert payload["target_avg"] == 315.91
        assert payload["target_median"] == 325.0
        assert payload["analyst_count"] == 50
        assert payload["analyst_count_label"] == "last 12 months"
        assert payload["publishers"] == ["TheFly", "Benzinga"]
        assert fake_db.closed is True

    def test_api_analyst_price_targets_rejects_unsupported_ticker(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(stock_routes, "resolve_ticker", lambda ticker: "0700.HK")
        monkeypatch.setattr(stock_routes, "get_stock_info", lambda ticker: {"market": "HK", "exchange": "HKEX"})
        client = app.test_client()

        response = client.get("/api/analyst-price-targets?symbol=0700.HK")

        assert response.status_code == 404
        assert response.get_json()["error"] == "Unsupported ticker"

    def test_api_analyst_forecast_returns_targets_and_grades(self, monkeypatch):
        app = create_app()
        fake_db = FakeDb(
            {
                "FROM analyst_price_targets": {
                    "target_high": 350.0,
                    "target_low": 239.0,
                    "target_avg": 315.91,
                    "target_median": 325.0,
                    "analyst_count": 50,
                    "analyst_count_label": "last 12 months",
                    "publishers_json": '["TheFly","Benzinga"]',
                    "fetched_at": "2026-04-22T00:00:00Z",
                },
                "FROM analyst_grades_consensus": {
                    "consensus": "Buy",
                    "strong_buy": 1,
                    "buy": 69,
                    "hold": 33,
                    "sell": 7,
                    "strong_sell": 0,
                    "fetched_at": "2026-04-22T00:00:00Z",
                },
                "FROM analyst_grades_historical": [
                    {"date": "2026-03-01", "strong_buy": 6, "buy": 25, "hold": 16, "sell": 1, "strong_sell": 1},
                    {"date": "2026-04-01", "strong_buy": 7, "buy": 25, "hold": 15, "sell": 1, "strong_sell": 1},
                ],
                "FROM analyst_grade_events": [
                    {"date": "2026-04-17", "grading_company": "Roth Capital", "previous_grade": "Neutral", "new_grade": "Outperform", "action": "upgrade"},
                    {"date": "2026-04-14", "grading_company": "B of A Securities", "previous_grade": "Buy", "new_grade": "Buy", "action": "maintain"},
                ],
                "SELECT date, close FROM ohlc_daily": {
                    "date": "2026-04-21",
                    "close": 190.0,
                },
            }
        )

        monkeypatch.setattr(stock_routes, "resolve_ticker", lambda ticker: "AAPL")
        monkeypatch.setattr(stock_routes, "get_stock_info", lambda ticker: {"market": "US", "exchange": "NASDAQ"})
        monkeypatch.setattr(stock_routes, "get_db", lambda: fake_db)
        monkeypatch.setattr(stock_routes, "_fmp_get", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("FMP should not be called")))
        client = app.test_client()

        response = client.get("/api/analyst-forecast?symbol=AAPL&lang=zh_hk")

        payload = response.get_json()
        assert response.status_code == 200
        assert payload["success"] is True
        assert payload["price_targets"]["target_avg"] == 315.91
        assert payload["grades"]["consensus"]["consensus"] == "Buy"
        assert payload["grades"]["historical"][0]["date"] == "2026-03-01"
        assert payload["grades"]["latest"][0]["grading_company"] == "Roth Capital（羅仕資本）"
        assert payload["grades"]["latest"][0]["previous_grade"] == "中性"
        assert payload["grades"]["latest"][0]["new_grade"] == "跑贏大市"
        assert payload["grades"]["latest"][1]["previous_grade"] == "買入"
        assert payload["grades"]["latest"][1]["new_grade"] == "買入"

    def test_rating_verdict_rejects_missing_data(self):
        app = create_app()
        client = app.test_client()

        response = client.post("/api/rating_verdict", json={"ticker": "AAPL"})

        assert response.status_code == 400
        assert response.get_json()["error"] == "Missing data"

    def test_rating_verdict_rejects_invalid_ticker(self):
        app = create_app()
        client = app.test_client()

        response = client.post("/api/rating_verdict", json={"ticker": "../bad", "scores": {"biz": 8}})

        assert response.status_code == 400
        assert response.get_json()["error"] == "Invalid ticker format"

    def test_rating_verdict_returns_cached_json_payload(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(
            stock_routes,
            "get_translations",
            lambda lang: {
                "card_biz": "Business",
                "card_finance": "Finance",
                "card_exec": "Governance",
                "card_call": "Outlook",
                "card_ta_price": "Price",
                "card_ta_analyst": "Analyst",
                "card_ta_social": "Sentiment",
            },
        )
        monkeypatch.setattr(
            stock_routes,
            "get_verdict",
            lambda ticker, lang, cache_key: '{"verdict":"Good company","fair_value":120,"fair_value_basis":"DCF"}',
        )
        client = app.test_client()

        response = client.post(
            "/api/rating_verdict",
            json={"ticker": "AAPL", "scores": {"biz": 8, "finance": 7}, "summaries": {}, "current_price": 100},
        )

        payload = response.get_json()
        assert response.status_code == 200
        assert payload["success"] is True
        assert payload["verdict"] == "Good company"
        assert payload["fair_value"] == 120

    def test_rating_verdict_rejects_unreasonable_cached_fair_value(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(
            stock_routes,
            "get_translations",
            lambda lang: {
                "card_biz": "Business",
                "card_finance": "Finance",
                "card_exec": "Governance",
                "card_call": "Outlook",
                "card_ta_price": "Price",
                "card_ta_analyst": "Analyst",
                "card_ta_social": "Sentiment",
            },
        )
        monkeypatch.setattr(
            stock_routes,
            "get_verdict",
            lambda ticker, lang, cache_key: '{"verdict":"Too expensive","fair_value":10000,"fair_value_basis":"Bad"}',
        )
        client = app.test_client()

        response = client.post(
            "/api/rating_verdict",
            json={"ticker": "AAPL", "scores": {"biz": 8, "finance": 8}, "summaries": {}, "current_price": 100},
        )

        payload = response.get_json()
        assert response.status_code == 200
        assert payload["fair_value"] is None
        assert payload["star_source"] == "quality_only"

    def test_rating_verdict_handles_ai_failure(self, monkeypatch):
        app = create_app()
        monkeypatch.setattr(
            stock_routes,
            "get_translations",
            lambda lang: {
                "card_biz": "Business",
                "card_finance": "Finance",
                "card_exec": "Governance",
                "card_call": "Outlook",
                "card_ta_price": "Price",
                "card_ta_analyst": "Analyst",
                "card_ta_social": "Sentiment",
            },
        )
        monkeypatch.setattr(stock_routes, "get_verdict", lambda ticker, lang, cache_key: None)
        monkeypatch.setattr(stock_routes, "call_gemini_api", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
        client = app.test_client()

        response = client.post(
            "/api/rating_verdict",
            json={"ticker": "AAPL", "scores": {"biz": 8, "finance": 7}, "summaries": {}},
        )

        assert response.status_code == 500
        assert response.get_json()["error"] == "AI verdict failed"

    def test_rating_verdict_passes_cache_key_to_cached_lookup(self, monkeypatch):
        app = create_app()
        seen = {}
        monkeypatch.setattr(
            stock_routes,
            "get_translations",
            lambda lang: {
                "card_biz": "Business",
                "card_finance": "Finance",
                "card_exec": "Governance",
                "card_call": "Outlook",
                "card_ta_price": "Price",
                "card_ta_analyst": "Analyst",
                "card_ta_social": "Sentiment",
            },
        )
        monkeypatch.setattr(
            stock_routes,
            "get_verdict",
            lambda ticker, lang, cache_key: seen.setdefault("cache_key", cache_key) or '{"verdict":"Good company","fair_value":120,"fair_value_basis":"DCF"}',
        )
        client = app.test_client()

        response = client.post(
            "/api/rating_verdict",
            json={"ticker": "AAPL", "scores": {"biz": 8}, "summaries": {"biz": {"summary": "Stable"}}, "current_price": 100},
        )

        assert response.status_code == 200
        assert seen["cache_key"]

    def test_verdict_cache_key_changes_when_input_changes(self):
        first = stock_routes._build_verdict_cache_key(
            {"biz": 8, "finance": 7},
            {"biz": {"summary": "Stable moat"}},
            100.0,
        )
        second = stock_routes._build_verdict_cache_key(
            {"biz": 8, "finance": 6},
            {"biz": {"summary": "Stable moat"}},
            100.0,
        )

        assert first != second

    def test_sanitize_fair_value_accepts_numeric_strings_and_rejects_extremes(self):
        assert stock_routes._sanitize_fair_value("120.5", 100) == 120.5
        assert stock_routes._sanitize_fair_value(10000, 100) is None
        assert stock_routes._sanitize_fair_value(5, 100) is None
        assert stock_routes._sanitize_fair_value("bad", 100) is None
