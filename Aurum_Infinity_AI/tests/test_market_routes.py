from __future__ import annotations

from blueprints.markets import routes as market_routes


def test_safe_change_pct_handles_missing_or_zero_baseline():
    assert market_routes._safe_change_pct(100, 0) is None
    assert market_routes._safe_change_pct(None, 100) is None
    assert market_routes._safe_change_pct(110, 100) == 10.0


def test_market_flow_conclusion_prefers_active_leading_inflow():
    conclusion = market_routes._market_flow_conclusion(
        10.0,
        9.0,
        [{"sector": "Technology"}],
        [{"sector": "Real Estate"}],
    )

    assert conclusion == "成交額升溫，偏向Technology"


def test_market_flow_conclusion_reports_quiet_market():
    conclusion = market_routes._market_flow_conclusion(-6.0, -9.0, [], [])

    assert conclusion == "整體成交偏淡，資金方向未明"


def test_flow_state_maps_price_and_traded_value_quadrants():
    assert market_routes._flow_state(1.2, 8.0) == {"label": "資金流入跡象", "tone": "inflow"}
    assert market_routes._flow_state(-1.2, 8.0) == {"label": "資金流出跡象", "tone": "outflow"}
    assert market_routes._flow_state(1.2, -8.0) == {"label": "縮量上升", "tone": "thin-up"}
    assert market_routes._flow_state(-1.2, -8.0) == {"label": "縮量下跌", "tone": "thin-down"}
    assert market_routes._flow_state(None, 8.0) == {"label": "資料不足", "tone": "neutral"}
