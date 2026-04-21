from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


def load_data_fetcher_main():
    module_name = "data_fetcher_main_test"
    module_path = Path(__file__).resolve().parents[2] / "Aurum_Data_Fetcher" / "main.py"
    data_fetcher_dir = str(module_path.parent)
    isolated_module_names = [
        "config",
        "logger",
        "db",
        "fmp_client",
        "compute",
        "ticker",
        "fetchers",
        "fetchers.profile",
        "fetchers.ohlc",
        "fetchers.financials",
    ]

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None

    original_sys_path = list(sys.path)
    original_modules = {
        name: sys.modules.get(name)
        for name in isolated_module_names
        if name in sys.modules
    }
    try:
        if data_fetcher_dir not in sys.path:
            sys.path.insert(0, data_fetcher_dir)
        for name in isolated_module_names:
            sys.modules.pop(name, None)
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = original_sys_path
        for name in isolated_module_names:
            sys.modules.pop(name, None)
        sys.modules.update(original_modules)

    return module


class FakeClient:
    def __init__(self, responses: dict[str, list[dict]] | None = None):
        self.responses = responses or {}
        self.calls: list[str] = []
        self.BASE_URL = "https://example.com"
        self.api_key = "demo-key"

    def _get(self, endpoint: str):
        self.calls.append(endpoint)
        return self.responses.get(endpoint)


class FakeConn:
    def __init__(self):
        self.inserted: list[str] = []
        self.closed = False
        self.commit_calls = 0

    def execute(self, query: str, params=()):
        if query.startswith("SELECT ticker FROM stocks_master"):
            ticker = params[0]
            return SimpleNamespace(fetchone=lambda: ticker if ticker in self.inserted else None)

        if query.startswith("INSERT OR IGNORE INTO stocks_master"):
            self.inserted.append(params[0])
            return SimpleNamespace(fetchone=lambda: None)

        raise AssertionError(f"Unexpected query: {query}")

    def commit(self):
        self.commit_calls += 1

    def close(self):
        self.closed = True


class FakeRequestsModule(ModuleType):
    def __init__(self):
        super().__init__("requests")
        self.calls: list[dict] = []

    def get(self, url: str, *, params: dict, timeout: int):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        csv_text = "\n".join(
            [
                "symbol,date,revenue",
                "AAPL,2026-03-31,100",
                "MSFT,2026-03-31,200",
            ]
        )
        return SimpleNamespace(status_code=200, text=csv_text)


class TestLoadIndex:
    def test_returns_sorted_symbols_for_supported_index(self):
        main = load_data_fetcher_main()
        client = FakeClient(
            {
                "sp500-constituent": [
                    {"symbol": "MSFT"},
                    {"symbol": "AAPL"},
                    {"symbol": ""},
                ]
            }
        )

        tickers = main._load_index("sp500", client)

        assert tickers == ["AAPL", "MSFT"]
        assert client.calls == ["sp500-constituent"]

    def test_unsupported_index_exits(self):
        main = load_data_fetcher_main()
        client = FakeClient()

        with pytest.raises(SystemExit):
            main._load_index("nikkei", client)

    def test_empty_index_response_exits(self):
        main = load_data_fetcher_main()
        client = FakeClient({"nasdaq-constituent": []})

        with pytest.raises(SystemExit):
            main._load_index("nasdaq", client)


class TestResolveTickers:
    def test_resolve_tickers_prefers_index_branch(self, monkeypatch):
        main = load_data_fetcher_main()
        client = FakeClient()
        args = SimpleNamespace(index="sp500", all=False, ticker="AAPL")
        monkeypatch.setattr(main, "_load_index", lambda index_name, active_client: ["QQQ", "SPY"])

        tickers = main._resolve_tickers(args, client)

        assert tickers == ["QQQ", "SPY"]

    def test_resolve_tickers_uses_stock_list_for_all(self, monkeypatch):
        main = load_data_fetcher_main()
        args = SimpleNamespace(index=None, all=True, ticker=None)
        monkeypatch.setattr(main, "_load_stock_list", lambda: ["AAPL", "MSFT"])

        tickers = main._resolve_tickers(args)

        assert tickers == ["AAPL", "MSFT"]

    def test_resolve_tickers_normalizes_single_ticker(self, monkeypatch):
        main = load_data_fetcher_main()
        args = SimpleNamespace(index=None, all=False, ticker="700")
        monkeypatch.setattr(main, "resolve_ticker", lambda ticker: "0700.HK")

        tickers = main._resolve_tickers(args)

        assert tickers == ["0700.HK"]

    def test_resolve_tickers_without_selector_exits(self):
        main = load_data_fetcher_main()
        args = SimpleNamespace(index=None, all=False, ticker=None)

        with pytest.raises(SystemExit):
            main._resolve_tickers(args)


class TestCmdBulkFinancials:
    def test_default_bulk_financials_uses_sp500_and_recent_two_years(self, monkeypatch):
        main = load_data_fetcher_main()
        fake_client = FakeClient({"sp500-constituent": [{"symbol": "AAPL"}]})
        fake_conn = FakeConn()
        fake_requests = FakeRequestsModule()
        captured_upserts: list[tuple[str, str, list[dict]]] = []

        monkeypatch.setattr(main, "FMPClient", lambda: fake_client)
        monkeypatch.setattr(main, "get_db", lambda: fake_conn)
        monkeypatch.setattr(
            main,
            "upsert_financial_statement",
            lambda conn, ticker, stmt_type, rows: captured_upserts.append((ticker, stmt_type, rows)),
        )
        monkeypatch.setattr(
            main,
            "datetime",
            SimpleNamespace(now=lambda: SimpleNamespace(year=2026)),
        )
        monkeypatch.setitem(sys.modules, "requests", fake_requests)

        args = SimpleNamespace(year=None, index=None, all=False)
        main.cmd_bulk_financials(args)

        years = [call["params"]["year"] for call in fake_requests.calls]
        assert years == ["2026", "2026", "2026", "2025", "2025", "2025"]
        assert all(call["params"]["period"] == "quarter" for call in fake_requests.calls)
        assert all(item[0] == "AAPL" for item in captured_upserts)
        assert {item[1] for item in captured_upserts} == {"income", "balance", "cashflow"}
        assert fake_conn.closed is True

    def test_bulk_financials_with_all_uses_stock_list_filter(self, monkeypatch):
        main = load_data_fetcher_main()
        fake_client = FakeClient()
        fake_conn = FakeConn()
        fake_requests = FakeRequestsModule()
        captured_upserts: list[str] = []

        monkeypatch.setattr(main, "FMPClient", lambda: fake_client)
        monkeypatch.setattr(main, "get_db", lambda: fake_conn)
        monkeypatch.setattr(main, "_load_stock_list", lambda: ["MSFT"])
        monkeypatch.setattr(
            main,
            "upsert_financial_statement",
            lambda conn, ticker, stmt_type, rows: captured_upserts.append(ticker),
        )
        monkeypatch.setitem(sys.modules, "requests", fake_requests)

        args = SimpleNamespace(year="2024", index=None, all=True)
        main.cmd_bulk_financials(args)

        assert captured_upserts == ["MSFT", "MSFT", "MSFT"]

    def test_bulk_financials_with_missing_index_data_exits(self, monkeypatch):
        main = load_data_fetcher_main()
        fake_client = FakeClient({"nasdaq-constituent": []})
        monkeypatch.setattr(main, "FMPClient", lambda: fake_client)

        with pytest.raises(SystemExit):
            main.cmd_bulk_financials(SimpleNamespace(year="2024", index="nasdaq", all=False))
