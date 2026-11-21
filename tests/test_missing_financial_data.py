import central_brain
import chat_copilot


def test_opportunity_scan_does_not_use_seed_cash_when_database_value_missing(monkeypatch):
    monkeypatch.setattr(central_brain, "get_complete_financial_summary", lambda **kwargs: {})

    result = central_brain.scan_shariah_golden_opportunities()

    assert result["status"] == "unavailable"
    assert result["investor_profile"]["available_dry_powder_pkr"] is None
    assert result["golden_opportunities"] == []


def test_chat_context_labels_missing_and_stale_values(monkeypatch):
    monkeypatch.setattr(
        chat_copilot,
        "get_complete_financial_summary",
        lambda **kwargs: {
            "data_status": "stale",
            "data_partial": True,
            "last_quote_received": None,
            "financial_kpis": {
                "total_net_worth": None,
                "fresh_capital_invested": None,
                "ktrade_available_cash": None,
                "portfolio_market_value": 120,
                "unrealized_pnl": None,
                "unrealized_pnl_pct": None,
            },
            "holdings": [{
                "ticker": "SYS",
                "shares": 2,
                "avg_price": 100,
                "current_price": 120,
                "price_status": "stale",
                "price_source": "database_stored",
                "price_timestamp": "2025-01-01T00:00:00+00:00",
            }],
        },
    )
    monkeypatch.setattr(chat_copilot, "get_agent_feed", lambda limit: [])
    monkeypatch.setattr(
        chat_copilot,
        "get_db_connection",
        lambda: (_ for _ in ()).throw(RuntimeError("database unavailable")),
    )

    context = chat_copilot.get_live_financial_context()

    assert "KTrade Available Cash: unavailable" in context
    assert "Total Net Worth: unavailable (stale)" in context
    assert "Rs. 120.00 (stale)" in context
    assert "database_stored" in context
