import datetime

from live_portfolio import build_live_state, mark_live_state_stale


NOW = 1_750_000_000.0
FRESH_QUOTE = {
    "price": 110,
    "source": "dps_company_realtime",
    "timestamp": NOW - 2,
    "timestamp_kind": "exchange",
    "received_timestamp": NOW - 1,
    "freshness_evidence": "source_timestamp",
    "price_limit_status": "passed",
    "price_limit_source": "dps_psx_company_circuit_breaker",
    "lower_limit": 100,
    "upper_limit": 120,
}
HOLDING = {
    "SYS": {
        "shares": 2,
        "invested_cost": 200,
        "avg_display": 100,
    }
}


def test_no_quotes_does_not_use_average_cost_as_price():
    state = build_live_state(HOLDING, {}, 50, True, True, NOW, 1)

    holding = state["holdings"]["SYS"]
    assert holding["price"] is None
    assert holding["status"] == "unavailable"
    assert holding["market_value"] is None
    assert state["totals"]["portfolio_custody_value"] is None
    assert state["totals"]["partial"] is True


def test_stored_seed_price_is_never_live():
    metadata = {
        "SYS": {
            **HOLDING["SYS"],
            "last_price": 119.48,
            "price_timestamp": NOW,
            "price_source": "baseline_seed",
        }
    }
    state = build_live_state(metadata, {}, 50, True, True, NOW, 1)

    holding = state["holdings"]["SYS"]
    assert holding["price"] == 119.48
    assert holding["status"] == "stale"
    assert holding["price_source"] == "baseline_seed"
    assert state["totals"]["partial"] is True


def test_stored_placeholder_source_is_not_shown_as_realtime():
    metadata = {
        "SYS": {
            **HOLDING["SYS"],
            "last_price": 119.48,
            "price_timestamp": NOW,
            "price_source": "realtime_feed",
        }
    }
    state = build_live_state(metadata, {}, 50, True, True, NOW, 1)

    assert state["holdings"]["SYS"]["status"] == "stale"
    assert state["holdings"]["SYS"]["price_source"] == "database_stored"


def test_fresh_verified_quote_is_live():
    state = build_live_state(
        HOLDING,
        {"SYS": {**FRESH_QUOTE}},
        50,
        True,
        True,
        NOW,
        1,
    )

    holding = state["holdings"]["SYS"]
    assert holding["status"] == "live"
    assert holding["price"] == 110
    assert holding["price_source"] == "dps_company_realtime"
    assert holding["price_age_seconds"] == 2
    assert holding["price_timestamp_kind"] == "exchange"
    assert holding["price_received_timestamp"] == datetime.datetime.fromtimestamp(
        NOW - 1,
        datetime.timezone.utc,
    ).isoformat()


def test_stale_exchange_timestamp_is_not_live():
    state = build_live_state(
        HOLDING,
        {"SYS": {**FRESH_QUOTE, "timestamp": NOW - 76}},
        50,
        True,
        True,
        NOW,
        1,
    )

    assert state["holdings"]["SYS"]["status"] == "stale"
    assert state["holdings"]["SYS"]["price"] == 110


def test_received_time_without_activity_evidence_is_not_live():
    state = build_live_state(
        HOLDING,
        {
            "SYS": {
                "price": 110,
                "source": "chromium_psx_live",
                "timestamp": NOW,
                "timestamp_kind": "received",
                "received_timestamp": NOW,
                "freshness_evidence": "none",
                "price_limit_status": "passed",
            }
        },
        50,
        True,
        True,
        NOW,
        1,
    )

    holding = state["holdings"]["SYS"]
    assert holding["status"] == "stale"
    assert holding["price_timestamp_kind"] == "received"


def test_recent_price_change_supports_receipt_only_quote():
    state = build_live_state(
        HOLDING,
        {
            "SYS": {
                "price": 110,
                "source": "chromium_psx_live",
                "timestamp": NOW,
                "timestamp_kind": "received",
                "received_timestamp": NOW,
                "freshness_evidence": "price_change",
                "evidence_timestamp": NOW - 5,
                "price_limit_status": "passed",
            }
        },
        50,
        True,
        True,
        NOW,
        1,
    )

    assert state["holdings"]["SYS"]["status"] == "live"


def test_circuit_limit_breach_is_suspect_and_excluded_from_totals():
    quote = {
        **FRESH_QUOTE,
        "price": 130,
        "price_limit_status": "breach",
    }
    state = build_live_state(HOLDING, {"SYS": quote}, 50, True, True, NOW, 1)

    holding = state["holdings"]["SYS"]
    assert holding["status"] == "suspect"
    assert holding["market_value"] is None
    assert state["totals"]["included_holdings"] == []
    assert state["totals"]["excluded_holdings"] == ["SYS"]
    assert state["totals"]["status"] == "suspect"


def test_market_closed_uses_last_price_as_closed_not_live():
    quote_time = NOW - 3600
    state = build_live_state(
        HOLDING,
        {
            "SYS": {
                **FRESH_QUOTE,
                "timestamp": quote_time,
                "received_timestamp": quote_time,
            }
        },
        50,
        True,
        False,
        NOW,
        1,
    )

    holding = state["holdings"]["SYS"]
    assert holding["status"] == "closed"
    assert holding["price_timestamp"] == datetime.datetime.fromtimestamp(
        quote_time,
        datetime.timezone.utc,
    ).isoformat()


def test_market_closed_receipt_only_quote_without_activity_is_not_called_last_close():
    state = build_live_state(
        HOLDING,
        {
            "SYS": {
                "price": 110,
                "source": "chromium_psx_live",
                "timestamp": NOW - 60,
                "timestamp_kind": "received",
                "received_timestamp": NOW - 60,
                "freshness_evidence": "none",
                "price_limit_status": "passed",
            }
        },
        50,
        True,
        False,
        NOW,
        1,
    )

    assert state["holdings"]["SYS"]["status"] == "stale"


def test_unverified_market_status_cannot_mark_quote_live():
    state = build_live_state(
        HOLDING,
        {"SYS": {**FRESH_QUOTE, "timestamp": NOW, "received_timestamp": NOW}},
        50,
        True,
        None,
        NOW,
        1,
    )

    assert state["market_status"] == "unverified"
    assert state["holdings"]["SYS"]["status"] == "stale"
    assert state["partial"] is True


def test_database_unreachable_means_holdings_and_cash_unavailable():
    state = build_live_state(HOLDING, {}, None, False, True, NOW, 1, "database offline")

    assert state["status"] == "unavailable"
    assert state["holdings_available"] is False
    assert state["holdings"] == {}
    assert state["totals"]["total_net_worth"] is None
    assert state["totals"]["ktrade_cash_balance"] is None
    assert state["error"] == "database offline"


def test_database_connection_failure_clears_seeded_runtime_state(monkeypatch):
    import main
    import portfolio_manager

    def fail_connection():
        raise RuntimeError("database offline")

    monkeypatch.setattr(portfolio_manager, "get_db_connection", fail_connection)
    monkeypatch.setattr(main, "_ACTIVE_HOLDINGS_META", {
        "SYS": {
            **HOLDING["SYS"],
            "last_price": 119.48,
            "price_timestamp": NOW,
            "price_source": "baseline_seed",
        }
    })
    monkeypatch.setattr(main, "_ACTIVE_CASH_BALANCE", 4357.38)
    monkeypatch.setattr(main, "_PORTFOLIO_DB_AVAILABLE", True)
    monkeypatch.setattr(main, "_PORTFOLIO_DB_ERROR", None)
    monkeypatch.setattr(main, "LIVE_PORTFOLIO_STATE", dict(main.LIVE_PORTFOLIO_STATE))

    assert main.refresh_active_holdings_meta() is False
    assert main.LIVE_PORTFOLIO_STATE["holdings"] == {}
    assert main.LIVE_PORTFOLIO_STATE["holdings_available"] is False
    assert main.LIVE_PORTFOLIO_STATE["totals"]["ktrade_cash_balance"] is None
    assert main.LIVE_PORTFOLIO_STATE["totals"]["total_net_worth"] is None
    assert main.LIVE_PORTFOLIO_STATE["error"] == "database offline"


def test_heartbeat_exception_marks_prior_live_price_stale():
    state = build_live_state(
        HOLDING,
        {"SYS": {**FRESH_QUOTE, "timestamp": NOW, "received_timestamp": NOW}},
        50,
        True,
        True,
        NOW,
        1,
    )
    failed = mark_live_state_stale(state, "heartbeat failed", NOW + 1)

    assert failed["holdings"]["SYS"]["status"] == "stale"
    assert failed["status"] == "stale"
    assert failed["error"] == "heartbeat failed"
    assert failed["holdings"]["SYS"]["price_timestamp"] == datetime.datetime.fromtimestamp(
        NOW,
        datetime.timezone.utc,
    ).isoformat()


def test_totals_report_included_and_excluded_holdings():
    metadata = {
        **HOLDING,
        "FFC": {"shares": 1, "invested_cost": 100, "avg_display": 100},
    }
    state = build_live_state(
        metadata,
        {"SYS": {**FRESH_QUOTE, "timestamp": NOW, "received_timestamp": NOW}},
        50,
        True,
        True,
        NOW,
        1,
    )

    assert state["totals"]["included_holdings"] == ["SYS"]
    assert state["totals"]["excluded_holdings"] == ["FFC"]
    assert state["totals"]["partial"] is True
