"""
psx_chromium_monitor.py - Persistent 1-Tab Headless Chromium PSX Live Price Client

Communicates with the persistent Headless Chromium PSX monitor service running
on http://127.0.0.1:3000 (Node.js Puppeteer Bridge) to retrieve live PSX quotes
for 580+ stocks in sub-5ms latency without triggering WAF blocks.
"""

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
            if raw_quotes:
                normalized: Dict[str, Dict[str, Any]] = {}
                for sym_key, q in raw_quotes.items():
                    sym_upper = sym_key.upper().strip()
                    price = float(q.get("price") or 0.0)
                    if price > 0:
                        ldcp = float(q.get("ldcp") or q.get("prev_close") or price)
                        change = float(q.get("change") or (price - ldcp))
                        change_pct = float(q.get("change_pct") or (((change / ldcp) * 100.0) if ldcp > 0 else 0.0))
                        high = float(q.get("high") or price)
                        low = float(q.get("low") or price)
                        open_p = float(q.get("open") or price)
                        vol = int(q.get("volume") or 0)

                        item = {
                            "price": round(price, 2),
                            "change": round(change, 2),
                            "change_pct": round(change_pct, 2),
                            "change_abs": round(change, 2),
                            "prev_close": round(ldcp, 2),
                            "high": round(high, 2),
                            "low": round(low, 2),
                            "open": round(open_p, 2),
                            "volume": vol,
                            "source": "chromium_psx_live",
                            "timestamp": now,
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
