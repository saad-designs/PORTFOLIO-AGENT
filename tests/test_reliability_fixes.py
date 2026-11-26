import time

import pytest
from fastapi import HTTPException
from starlette.requests import Request

import main
import psx_chromium_monitor
from live_portfolio import build_live_state


def _request(client_host, headers=None):
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/api/ingest-whatsapp-message",
        "raw_path": b"/api/ingest-whatsapp-message",
        "query_string": b"",
        "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
        "server": ("portfolio.example", 80),
        "client": (client_host, 50000),
        "root_path": "",
    }
    return Request(scope)


def test_ingest_without_token_accepts_loopback_only(monkeypatch):
    monkeypatch.setattr(main, "INGEST_TOKEN", "")
    assert main.verify_ingest_auth(_request("127.0.0.1"))
    with pytest.raises(HTTPException) as exc:
        main.verify_ingest_auth(_request("172.17.0.1"))
    assert exc.value.status_code == 401


def test_ingest_with_token_requires_matching_header(monkeypatch):
    monkeypatch.setattr(main, "INGEST_TOKEN", "secret-token")
    assert main.verify_ingest_auth(_request("203.0.113.5", {"x-ingest-token": "secret-token"}))
    with pytest.raises(HTTPException):
        main.verify_ingest_auth(_request("127.0.0.1", {"x-ingest-token": "wrong"}))


def test_live_totals_report_day_pnl_from_priced_holdings():
    now = time.time()
    meta = {
        "SYS": {"shares": 10, "invested_cost": 1000.0, "last_price": 100.0},
        "FFC": {"shares": 5, "invested_cost": 2500.0, "last_price": 500.0},
    }
    quotes = {
        "SYS": {"price": 110.0, "change_abs": 10.0, "change_pct": 10.0, "source": "dps_company_realtime",
                "timestamp": now, "timestamp_kind": "exchange"},
        "FFC": {"price": 500.0, "change_abs": None, "change_pct": None, "source": "dps_company_realtime",
                "timestamp": now, "timestamp_kind": "exchange"},
    }
    state = build_live_state(meta, quotes, 0.0, True, False, now, 1)
    totals = state["totals"]
    assert totals["day_pnl"] == 100.0
    assert totals["day_pnl_holdings"] == ["SYS"]
    # base = 1100 market value - 100 day move = 1000 → +10%
    assert totals["day_pnl_pct"] == 10.0


def test_live_totals_day_pnl_unavailable_without_changes():
    now = time.time()
    meta = {"SYS": {"shares": 10, "invested_cost": 1000.0, "last_price": 100.0}}
    state = build_live_state(meta, {}, 0.0, True, False, now, 1)
    assert state["totals"]["day_pnl"] is None
    assert state["totals"]["day_pnl_pct"] is None


def test_chromium_quote_keeps_bridge_extraction_time(monkeypatch):
    extracted_at = time.time() - 600

    class Response:
        status_code = 200

        def json(self):
            return {"timestamp": extracted_at * 1000, "quotes": {"SYS": {"price": 100.0, "timestamp": extracted_at}}}

    monkeypatch.setattr(psx_chromium_monitor, "_CHROMIUM_QUOTES_CACHE", {})
    monkeypatch.setattr(psx_chromium_monitor, "_CHROMIUM_CACHE_TS", 0.0)
    monkeypatch.setattr(psx_chromium_monitor.requests, "get", lambda *a, **k: Response())

    quote = psx_chromium_monitor.fetch_chromium_psx_quotes(["SYS"])["SYS"]
    assert quote["received_timestamp"] == pytest.approx(extracted_at)


def test_chromium_future_timestamp_falls_back_to_receipt_time(monkeypatch):
    class Response:
        status_code = 200

        def json(self):
            return {"quotes": {"SYS": {"price": 100.0, "timestamp": time.time() + 3600}}}

    monkeypatch.setattr(psx_chromium_monitor, "_CHROMIUM_QUOTES_CACHE", {})
    monkeypatch.setattr(psx_chromium_monitor, "_CHROMIUM_CACHE_TS", 0.0)
    monkeypatch.setattr(psx_chromium_monitor.requests, "get", lambda *a, **k: Response())

    before = time.time()
    quote = psx_chromium_monitor.fetch_chromium_psx_quotes(["SYS"])["SYS"]
    assert before - 1 <= quote["received_timestamp"] <= time.time() + 1
