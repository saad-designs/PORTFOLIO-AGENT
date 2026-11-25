"""
psx_chromium_monitor.py - Persistent 1-Tab Headless Chromium PSX Live Price Client

Communicates with the persistent Headless Chromium PSX monitor service running
on http://127.0.0.1:3000 (Node.js Puppeteer Bridge) to retrieve live PSX quotes
for 580+ stocks in sub-5ms latency without triggering WAF blocks.
"""

import math
import time
import requests
from typing import Dict, Any, Optional, Iterable

NODE_BRIDGE_URL = "http://127.0.0.1:3000"
PSX_QUOTES_ENDPOINT = f"{NODE_BRIDGE_URL}/psx-live-quotes"
PSX_STATUS_ENDPOINT = f"{NODE_BRIDGE_URL}/api/psx/status"
PSX_TIMESERIES_ENDPOINT = f"{NODE_BRIDGE_URL}/api/psx/timeseries"

# In-memory last known good quotes from Chromium
_CHROMIUM_QUOTES_CACHE: Dict[str, Dict[str, Any]] = {}
_CHROMIUM_CACHE_TS: float = 0.0
_CHROMIUM_CACHE_TTL: float = 2.5


def _optional_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _epoch_seconds(value: Any, now: float) -> Optional[float]:
    """Normalizes a bridge timestamp (seconds or milliseconds) to epoch seconds; None if unusable."""
    ts = _optional_float(value)
    if ts is None or ts <= 0:
        return None
    if ts > 1e12:
        ts = ts / 1000.0
    if ts > now + 5:
        return None
    return ts


def is_chromium_monitor_healthy(timeout: float = 0.8) -> bool:
    """Checks whether the Node Puppeteer Chromium PSX monitor on 127.0.0.1:3000 is active and healthy."""
    try:
        resp = requests.get(PSX_STATUS_ENDPOINT, timeout=timeout)
        if resp.status_code == 200:
            data = resp.json() or {}
            return bool(data.get("healthy") or (data.get("count", 0) > 0))
    except Exception:
        pass
    return False


def fetch_chromium_psx_quotes(
    tickers: Optional[Iterable[str]] = None,
    timeout: float = 1.0,
) -> Dict[str, Dict[str, Any]]:
    """
    Fetches live PSX quotes from the 1-Tab Chromium background worker on 127.0.0.1:3000.
    Returns in < 5ms if warm, or within the 1.0s HTTP timeout.
    """
    global _CHROMIUM_QUOTES_CACHE, _CHROMIUM_CACHE_TS

    target_set = {str(t).upper().strip() for t in tickers} if tickers else None

    # Check cache TTL (2.5s)
    now = time.time()
    if _CHROMIUM_QUOTES_CACHE and (now - _CHROMIUM_CACHE_TS < _CHROMIUM_CACHE_TTL):
        if target_set:
            matched = {sym: q for sym, q in _CHROMIUM_QUOTES_CACHE.items() if sym in target_set}
            if len(matched) == len(target_set):
                return matched
        else:
            return dict(_CHROMIUM_QUOTES_CACHE)

    try:
        resp = requests.get(PSX_QUOTES_ENDPOINT, timeout=timeout)
        if resp.status_code == 200:
            data = resp.json() or {}
            raw_quotes = data.get("quotes") or {}
            bridge_ts = _epoch_seconds(data.get("timestamp"), now)
            if raw_quotes:
                normalized: Dict[str, Dict[str, Any]] = {}
                for sym_key, q in raw_quotes.items():
                    # Use the bridge's extraction time, not this HTTP call's time, so a
                    # frozen Chromium page cannot make old quotes look freshly received.
                    received_ts = _epoch_seconds(q.get("timestamp"), now) or bridge_ts or now
                    sym_upper = sym_key.upper().strip()
                    price = _optional_float(q.get("price"))
                    if price is not None and price > 0:
                        ldcp = _optional_float(q.get("ldcp") or q.get("prev_close"))
                        change = _optional_float(q.get("change"))
                        if change is None and ldcp is not None and ldcp > 0:
                            change = price - ldcp
                        change_pct = _optional_float(q.get("change_pct"))
                        if change_pct is None and change is not None and ldcp is not None and ldcp > 0:
                            change_pct = (change / ldcp) * 100.0
                        high = _optional_float(q.get("high"))
                        low = _optional_float(q.get("low"))
                        open_p = _optional_float(q.get("open"))
                        volume = _optional_float(q.get("volume"))

                        item = {
                            "price": round(price, 2),
                            "change": round(change, 2) if change is not None else None,
                            "change_pct": round(change_pct, 2) if change_pct is not None else None,
                            "change_abs": round(change, 2) if change is not None else None,
                            "prev_close": round(ldcp, 2) if ldcp is not None and ldcp > 0 else None,
                            "high": round(high, 2) if high is not None and high > 0 else None,
                            "low": round(low, 2) if low is not None and low > 0 else None,
                            "open": round(open_p, 2) if open_p is not None and open_p > 0 else None,
                            "volume": volume,
                            "sector": q.get("sector") or "",
                            "indices": q.get("indices") or "",
                            "is_shariah": bool(q.get("is_shariah", False)),
                            "source": "chromium_psx_live",
                            "timestamp": received_ts,
                            "timestamp_kind": "received",
                            "received_timestamp": received_ts,
                        }
                        normalized[sym_upper] = item
                        # Also keep raw scrip if different
                        raw_scrip = (q.get("scrip") or "").upper().strip()
                        if raw_scrip and raw_scrip != sym_upper:
                            normalized[raw_scrip] = item

                _CHROMIUM_QUOTES_CACHE = normalized
                _CHROMIUM_CACHE_TS = now

                if target_set:
                    return {sym: q for sym, q in normalized.items() if sym in target_set}
                return dict(normalized)
    except Exception:
        # Fall through to cached quotes if network/timeout occurs
        pass

    if _CHROMIUM_QUOTES_CACHE:
        if target_set:
            return {sym: q for sym, q in _CHROMIUM_QUOTES_CACHE.items() if sym in target_set}
        return dict(_CHROMIUM_QUOTES_CACHE)

    return {}


def fetch_chromium_psx_timeseries(ticker: str, timeout: float = 2.0) -> Optional[Dict[str, Any]]:
    """Requests intraday timeseries for a given ticker from the persistent Chromium browser session."""
    sym = ticker.upper().strip()
    if not sym:
        return None
    try:
        url = f"{PSX_TIMESERIES_ENDPOINT}/{sym}"
        resp = requests.get(url, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None

