import datetime
import time

import market_data_scraper
import psx_chromium_monitor


def test_dps_company_quote_without_as_of_is_received_time_only(monkeypatch):
    class Response:
        status_code = 200
        text = """
        <div class="quote__close">Rs. 120.00</div>
        <div class="stats_label">LDCP</div><div class="stats_value">119.00</div>
        <div class="stats_label">Open</div><div class="stats_value">119.50</div>
        <div class="stats_label">High</div><div class="stats_value">121.00</div>
        <div class="stats_label">Low</div><div class="stats_value">118.00</div>
        <div class="stats_label">Volume</div><div class="stats_value">1000</div>
        <div class="quote__change change__text--pos"></div>
        <div class="change__value">1.00</div>
        <div class="change__percent">(0.84%)</div>
        """

    monkeypatch.setattr(market_data_scraper.requests, "get", lambda *args, **kwargs: Response())
    before = time.time()
    quote = market_data_scraper.fetch_dps_company_quote("SYS")
    after = time.time()

    assert quote is not None
    assert quote["price"] == 120.0
    assert quote["source"] == "dps_company_realtime"
    assert quote["timestamp_kind"] == "received"
    assert before <= quote["timestamp"] <= after
    assert before <= quote["received_timestamp"] <= after


def test_dps_company_quote_uses_exchange_as_of_and_official_limits(monkeypatch):
    class Response:
        status_code = 200
        text = """
        <div class="quote__close">Rs. 110.69</div>
        <div class="quote__date">^ As of Fri, Nov 20, 2026 4:49 PM</div>
        <div class="stats_label">LDCP</div><div class="stats_value">112.49</div>
        <div class="stats_label">Open</div><div class="stats_value">113.00</div>
        <div class="stats_label">High</div><div class="stats_value">113.36</div>
        <div class="stats_label">Low</div><div class="stats_value">110.00</div>
        <div class="stats_label">Volume</div><div class="stats_value">1161581</div>
        <div class="stats_label">CIRCUIT BREAKER</div>
        <div class="stats_value">101.24 — 123.74<div class="numRange"
        data-low="101.24" data-high="123.74" data-current="110.69"></div></div>
        <div class="quote__change change__text--neg"></div>
        <div class="change__value">1.80</div>
        <div class="change__percent">(1.60%)</div>
        """

    monkeypatch.setattr(market_data_scraper.requests, "get", lambda *args, **kwargs: Response())
    quote = market_data_scraper.fetch_dps_company_quote("SYS")

    expected_time = datetime.datetime(
        2026,
        11,
        20,
        16,
        49,
        tzinfo=datetime.timezone(datetime.timedelta(hours=5)),
    ).timestamp()
    assert quote is not None
    assert quote["timestamp_kind"] == "exchange"
    assert quote["timestamp"] == expected_time
    assert quote["lower_limit"] == 101.24
    assert quote["upper_limit"] == 123.74


def test_dps_company_quote_does_not_substitute_price_for_missing_fields(monkeypatch):
    class Response:
        status_code = 200
        text = """
        <div class="quote__close">Rs. 100.00</div>
        <div class="quote__date">^ As of Mon, Nov 23, 2026 9:32 AM</div>
        """

    monkeypatch.setattr(market_data_scraper.requests, "get", lambda *args, **kwargs: Response())
    quote = market_data_scraper.fetch_dps_company_quote("SYS")

    assert quote is not None
    assert quote["price"] == 100.0
    assert quote["prev_close"] is None
    assert quote["change"] is None
    assert quote["change_pct"] is None
    assert quote["open"] is None
    assert quote["high"] is None
    assert quote["low"] is None
    assert quote["volume"] is None


def test_receipt_only_source_requires_observed_activity(monkeypatch):
    now = 1_800_000_000.0
    monkeypatch.setattr(market_data_scraper.time, "time", lambda: now)
    market_data_scraper._QUOTE_OBSERVATIONS.pop("TESTFRESH:unit-test", None)
    first = {
        "TESTFRESH": {
            "price": 110,
            "volume": 100,
            "source": "unit-test",
            "timestamp": now,
            "timestamp_kind": "received",
        }
    }
    market_data_scraper._attach_quote_freshness_evidence(first)
    assert first["TESTFRESH"]["freshness_evidence"] == "none"

    second = {
        "TESTFRESH": {
            "price": 110,
            "volume": 101,
            "source": "unit-test",
            "timestamp": now + 1,
            "timestamp_kind": "received",
        }
    }
    market_data_scraper._attach_quote_freshness_evidence(second)
    assert second["TESTFRESH"]["freshness_evidence"] == "volume_change"
    assert second["TESTFRESH"]["evidence_timestamp"] == now + 1


def test_yahoo_quote_keeps_provider_market_timestamp(monkeypatch):
    market_timestamp = 1_800_000_000.0

    class Response:
        status_code = 200

        def json(self):
            return {
                "chart": {
                    "result": [{
                        "meta": {
                            "regularMarketTime": market_timestamp,
                            "regularMarketPrice": 123.45,
                            "chartPreviousClose": 120.0,
                        }
                    }]
                }
            }

    monkeypatch.setattr(market_data_scraper.requests, "get", lambda *args, **kwargs: Response())
    quote = market_data_scraper._fetch_single_yahoo_ka_quote("SYS")

    assert quote is not None
    assert quote["timestamp"] == market_timestamp
    assert quote["timestamp_kind"] == "provider"


def test_official_circuit_limits_are_cached_and_breaches_are_flagged(monkeypatch):
    market_data_scraper._PSX_CIRCUIT_LIMITS.clear()
    market_data_scraper._PSX_CIRCUIT_LIMITS_ATTEMPTED.clear()
    monkeypatch.setattr(market_data_scraper, "_PSX_CIRCUIT_LIMITS_DAY", None)
    monkeypatch.setattr(
        market_data_scraper,
        "fetch_dps_company_quotes_concurrent",
        lambda tickers, timeout: {
            "SYS": {
                "price": 112.49,
                "prev_close": 112.49,
                "lower_limit": 101.24,
                "upper_limit": 123.74,
                "timestamp": 1_800_000_000,
            }
        },
    )
    quotes = {
        "SYS": {
            "price": 123.75,
            "prev_close": 112.49,
            "source": "chromium_psx_live",
        }
    }

    market_data_scraper._attach_official_circuit_limits(quotes, ["SYS"], 1.0)

    quote = quotes["SYS"]
    assert quote["price_limit_status"] == "breach"
    assert quote["lower_limit"] == 101.24
    assert quote["upper_limit"] == 123.74
    assert quote["price_limit_source"] == "dps_psx_company_circuit_breaker"
    assert market_data_scraper._is_price_within_circuit_limits(123.74, 101.24, 123.74) is True
    assert market_data_scraper._is_price_within_circuit_limits(123.75, 101.24, 123.74) is False


def test_missing_official_circuit_limits_are_not_assumed(monkeypatch):
    market_data_scraper._PSX_CIRCUIT_LIMITS.clear()
    market_data_scraper._PSX_CIRCUIT_LIMITS_ATTEMPTED.clear()
    monkeypatch.setattr(market_data_scraper, "_PSX_CIRCUIT_LIMITS_DAY", None)
    monkeypatch.setattr(
        market_data_scraper,
        "fetch_dps_company_quotes_concurrent",
        lambda tickers, timeout: {},
    )
    quotes = {"SYS": {"price": 110, "source": "chromium_psx_live"}}

    market_data_scraper._attach_official_circuit_limits(quotes, ["SYS"], 1.0)

    assert quotes["SYS"]["price_limit_status"] == "unverified"
    assert quotes["SYS"]["lower_limit"] is None


def test_chromium_receipt_only_quote_does_not_invent_previous_close(monkeypatch):
    class Response:
        status_code = 200

        def json(self):
            return {"quotes": {"SYS": {"price": 100.0}}}

    monkeypatch.setattr(psx_chromium_monitor, "_CHROMIUM_QUOTES_CACHE", {})
    monkeypatch.setattr(psx_chromium_monitor, "_CHROMIUM_CACHE_TS", 0.0)
    monkeypatch.setattr(psx_chromium_monitor.requests, "get", lambda *args, **kwargs: Response())

    quote = psx_chromium_monitor.fetch_chromium_psx_quotes(["SYS"])["SYS"]

    assert quote["price"] == 100.0
    assert quote["timestamp_kind"] == "received"
    assert quote["prev_close"] is None
    assert quote["change"] is None
    assert quote["change_pct"] is None
