import time

import market_data_scraper


def test_dps_company_quote_uses_retrieval_timestamp(monkeypatch):
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
    assert before <= quote["timestamp"] <= after
