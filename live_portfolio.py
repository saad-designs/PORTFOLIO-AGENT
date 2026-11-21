"""Pure helpers for classifying quote freshness and building live portfolio state."""

import datetime
import math
from typing import Any, Dict, Optional


LIVE_QUOTE_MAX_AGE_SECONDS = 20
EXCHANGE_QUOTE_MAX_AGE_SECONDS = 75
DATABASE_PRICE_SOURCES = {"database_stored", "db_cached_last_price"}
NON_SOURCE_LABELS = {"realtime_feed", "unknown", "source_unavailable", ""}
STATUS_SEVERITY = {"live": 0, "closed": 1, "stale": 2, "unavailable": 3, "suspect": 4}
SOURCE_TIMESTAMP_KINDS = {
    "dps_company_realtime": "exchange",
    "yahoo_v8_ka": "provider",
    "chromium_psx_live": "received",
    "psx_market_watch": "received",
    "psx_market_summary": "received",
    "tradingview_psx_fallback": "received",
}


def _positive_number(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _finite_number(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _timestamp_epoch(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        pass
    try:
        parsed = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed.timestamp()
    except (TypeError, ValueError, OverflowError):
        return None


def _timestamp_iso(epoch: Optional[float]) -> Optional[str]:
    if epoch is None:
        return None
    return datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc).isoformat()


def _is_real_source(source: Any) -> bool:
    return bool(source) and str(source).strip().lower() not in NON_SOURCE_LABELS | DATABASE_PRICE_SOURCES


def quote_is_fresh(quote: Dict[str, Any], now_ts: float) -> bool:
    if _positive_number(quote.get("price")) is None or quote.get("stored"):
        return False
    if quote.get("price_limit_status") != "passed":
        return False

    kind = quote.get("timestamp_kind") or "received"
    timestamp = _timestamp_epoch(quote.get("timestamp"))
    if kind in {"exchange", "provider"}:
        if timestamp is None or timestamp > now_ts + 1:
            return False
        max_age = EXCHANGE_QUOTE_MAX_AGE_SECONDS if kind == "exchange" else LIVE_QUOTE_MAX_AGE_SECONDS
        return now_ts - timestamp <= max_age

    received_timestamp = _timestamp_epoch(quote.get("received_timestamp")) or timestamp
    evidence_timestamp = _timestamp_epoch(quote.get("evidence_timestamp"))
    evidence = quote.get("freshness_evidence")
    return (
        evidence in {"price_change", "volume_change", "independent_source_agreement"}
        and received_timestamp is not None
        and evidence_timestamp is not None
        and received_timestamp <= now_ts + 1
        and evidence_timestamp <= now_ts + 1
        and now_ts - received_timestamp <= LIVE_QUOTE_MAX_AGE_SECONDS
        and now_ts - evidence_timestamp <= LIVE_QUOTE_MAX_AGE_SECONDS
    )


def _make_holding(
    ticker: str,
    meta: Dict[str, Any],
    quote: Optional[Dict[str, Any]],
    market_open: Optional[bool],
    now_ts: float,
) -> Dict[str, Any]:
    quote = quote or {}
    quote_price = _positive_number(quote.get("price"))
    quote_source = quote.get("source")
    if str(quote_source or "").strip().lower() in NON_SOURCE_LABELS:
        quote_source = None
    quote_price_source = quote.get("price_source") or quote_source
    quote_timestamp = _timestamp_epoch(quote.get("timestamp"))
    quote_timestamp_kind = quote.get("timestamp_kind") or "received"
    quote_received_timestamp = _timestamp_epoch(quote.get("received_timestamp"))
    quote_is_stored = bool(quote.get("stored")) or quote_price_source in DATABASE_PRICE_SOURCES
    if quote_is_stored and quote_timestamp_kind == "stored":
        quote_timestamp_kind = SOURCE_TIMESTAMP_KINDS.get(
            str(quote_price_source or "").strip().lower(),
            "stored",
        )
        if quote_timestamp_kind == "received" and quote_received_timestamp is None:
            quote_received_timestamp = quote_timestamp

    if quote_price is not None:
        price = quote_price
        source = quote_price_source or "source_unavailable"
        if str(source).strip().lower() in NON_SOURCE_LABELS:
            source = "source_unavailable"
        timestamp = quote_timestamp or quote_received_timestamp
        if quote_timestamp is None and quote_received_timestamp is not None:
            quote_timestamp_kind = "received"
        stored = quote_is_stored
    else:
        price = _positive_number(meta.get("last_price"))
        source = meta.get("price_source")
        if price is not None and (
            not source or str(source).strip().lower() in NON_SOURCE_LABELS
        ):
            source = "database_stored"
        timestamp = _timestamp_epoch(meta.get("price_timestamp"))
        quote_timestamp_kind = (
            SOURCE_TIMESTAMP_KINDS.get(str(source or "").strip().lower(), "stored")
            if price is not None
            else "unavailable"
        )
        quote_received_timestamp = timestamp if quote_timestamp_kind == "received" else None
        stored = price is not None

    age = max(0.0, now_ts - timestamp) if timestamp is not None else None
    if price is None:
        status = "unavailable"
    elif quote.get("price_limit_status") == "breach":
        status = "suspect"
    elif market_open is False:
        verified_close_time = timestamp is not None and (
            stored
            or quote_timestamp_kind in {"exchange", "provider"}
            or (
                quote_timestamp_kind == "received"
                and quote.get("freshness_evidence") in {"price_change", "volume_change"}
            )
        )
        status = "closed" if verified_close_time else "stale"
    elif market_open is None:
        status = "stale"
    elif (
        not stored
        and _is_real_source(source)
        and quote_is_fresh(quote, now_ts)
    ):
        status = "live"
    else:
        status = "stale"

    shares = _positive_number(meta.get("shares")) or 0.0
    invested_cost = _positive_number(meta.get("invested_cost")) or 0.0
    avg_display = _positive_number(meta.get("avg_display"))
    valuation_price = price if status != "suspect" else None
    market_value = round(shares * valuation_price, 2) if valuation_price is not None else None
    unrealized_pnl = round(market_value - invested_cost, 2) if market_value is not None else None
    unrealized_pnl_pct = (
        round(unrealized_pnl / invested_cost * 100.0, 2)
        if unrealized_pnl is not None and invested_cost > 0
        else None
    )
    change_abs = _finite_number(quote.get("change_abs", quote.get("change")))
    change_pct = _finite_number(quote.get("change_pct"))

    return {
        "ticker": ticker,
        "shares": shares,
        "price": price,
        "price_source": source,
        "source": source,
        "price_timestamp": _timestamp_iso(timestamp),
        "price_timestamp_kind": quote_timestamp_kind,
        "price_received_timestamp": _timestamp_iso(quote_received_timestamp),
        "price_age_seconds": round(age, 2) if age is not None else None,
        "freshness_evidence": quote.get("freshness_evidence") if quote_price is not None else None,
        "price_limit_status": quote.get("price_limit_status") if quote_price is not None else None,
        "price_limit_source": quote.get("price_limit_source") if quote_price is not None else None,
        "price_limit_retrieved_at": _timestamp_iso(
            _timestamp_epoch(quote.get("limit_retrieved_at"))
        ) if quote_price is not None else None,
        "lower_limit": _positive_number(quote.get("lower_limit")) if quote_price is not None else None,
        "upper_limit": _positive_number(quote.get("upper_limit")) if quote_price is not None else None,
        "status": status,
        "change_pct": change_pct,
        "change_abs": change_abs,
        "prev_close": _positive_number(quote.get("prev_close")),
        "market_value": market_value,
        "invested_cost": round(invested_cost, 2),
        "avg_display": avg_display,
        "unrealized_pnl": unrealized_pnl,
        "unrealized_pnl_pct": unrealized_pnl_pct,
        "day_pnl": round(shares * change_abs, 2) if change_abs is not None and status != "suspect" else None,
        "volume": quote.get("volume"),
        "pe_ttm": quote.get("pe_ttm"),
        "dividend_yield": quote.get("dividend_yield"),
    }


def build_live_state(
    holdings_meta: Dict[str, Dict[str, Any]],
    quotes: Dict[str, Dict[str, Any]],
    cash_balance: Optional[float],
    db_available: bool,
    market_open: Optional[bool],
    now_ts: float,
    tick_seq: int,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    holdings = {}
    if db_available:
        for ticker, meta in holdings_meta.items():
            sym = ticker.upper()
            holdings[sym] = _make_holding(
                sym,
                meta,
                quotes.get(sym),
                market_open,
                now_ts,
            )

    included = [h for h in holdings.values() if h["market_value"] is not None]
    excluded = [h["ticker"] for h in holdings.values() if h["market_value"] is None]
    statuses = [h["status"] for h in holdings.values()]
    status = max(statuses, key=lambda value: STATUS_SEVERITY[value]) if statuses else (
        "unavailable" if not db_available else "live"
    )
    partial = (
        any(value in ("stale", "unavailable", "suspect") for value in statuses)
        or not db_available
        or market_open is None
    )

    custody_value = round(sum(h["market_value"] for h in included), 2) if included else (
        0.0 if db_available and not holdings else None
    )
    invested_cost = round(sum(h["invested_cost"] for h in holdings.values()), 2) if db_available else None
    priced_invested_cost = round(sum(h["invested_cost"] for h in included), 2) if included else (
        0.0 if db_available and not holdings else None
    )
    unrealized_pnl = (
        round(sum(h["unrealized_pnl"] for h in included), 2)
        if included
        else (0.0 if db_available and not holdings else None)
    )
    cash = float(cash_balance) if db_available and cash_balance is not None else None
    net_worth = round(custody_value + cash, 2) if custody_value is not None and cash is not None else None
    total_statuses = statuses + (["unavailable"] if cash is None else [])
    totals_status = max(total_statuses, key=lambda value: STATUS_SEVERITY[value]) if total_statuses else "unavailable"
    last_quotes = [
        _timestamp_epoch(q.get("received_timestamp"))
        or (
            _timestamp_epoch(q.get("timestamp"))
            if (q.get("timestamp_kind") or "received") == "received"
            else None
        )
        for q in quotes.values()
        if not q.get("stored")
        and _is_real_source(q.get("source"))
        and _positive_number(q.get("price")) is not None
    ]

    return {
        "source": "mixed_verified_sources" if included else None,
        "timestamp": _timestamp_iso(now_ts),
        "last_quote_received": _timestamp_iso(max(last_quotes)) if last_quotes else None,
        "market_open": market_open,
        "market_status": "open" if market_open is True else "closed" if market_open is False else "unverified",
        "tick_seq": tick_seq,
        "status": status if db_available else "unavailable",
        "partial": partial,
        "error": error,
        "holdings_available": db_available,
        "totals": {
            "status": totals_status,
            "partial": partial,
            "included_holdings": [h["ticker"] for h in included],
            "excluded_holdings": excluded if db_available else list(holdings_meta),
            "total_net_worth": net_worth,
            "portfolio_custody_value": custody_value,
            "invested_cost": invested_cost,
            "priced_invested_cost": priced_invested_cost,
            "ktrade_cash_balance": cash,
            "unrealized_pnl": unrealized_pnl,
            "unrealized_pnl_pct": (
                round(unrealized_pnl / priced_invested_cost * 100.0, 2)
                if unrealized_pnl is not None and priced_invested_cost
                else None
            ),
            "day_pnl": None,
            "day_pnl_pct": None,
        },
        "holdings": holdings,
    }


def mark_live_state_stale(state: Dict[str, Any], error: str, now_ts: float) -> Dict[str, Any]:
    state["timestamp"] = _timestamp_iso(now_ts)
    state["error"] = error
    state["status"] = "stale"
    for holding in state.get("holdings", {}).values():
        if holding.get("price") is not None:
            holding["status"] = "stale"
            timestamp = _timestamp_epoch(holding.get("price_timestamp"))
            holding["price_age_seconds"] = round(max(0.0, now_ts - timestamp), 2) if timestamp is not None else None
    if state.get("totals"):
        state["totals"]["status"] = "stale"
        state["totals"]["partial"] = True
    state["partial"] = True
    return state
