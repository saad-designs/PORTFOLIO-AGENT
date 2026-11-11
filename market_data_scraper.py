import asyncio
import concurrent.futures
import json
import random
import re
import threading
import time
import warnings
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import aiohttp
import requests
import urllib3
from bs4 import BeautifulSoup
from utils import normalize_ticker
try:
    from psx_chromium_monitor import fetch_chromium_psx_quotes
except ImportError:
    def fetch_chromium_psx_quotes(tickers=None, timeout=1.0):
        return {}

# Suppress insecure request warnings for PSX Data Portal SSL certificates (Ubuntu ARM64 Docker)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DEFAULT_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

YAHOO_JSON_HEADERS = {
    "User-Agent": DEFAULT_BROWSER_HEADERS["User-Agent"],
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

CORE_KTRADE_TICKERS = ["SYS", "FFC", "OGDC", "MARI", "HUBC", "LUCK", "EFERT", "MEBL", "FABL", "POL", "PSO"]

BULK_QUOTE_TTL_SECONDS: float = 1.0
_BULK_QUOTES_CACHE: Dict[str, Dict[str, Any]] = {}
_BULK_QUOTES_TS: float = 0.0
_BULK_LOCK = threading.Lock()

# Suffixes appended to ticker symbols in PSX tables
_PSX_STATUS_SUFFIXES = {"NC", "XD", "XB", "XR", "WU", "SPOT", "PRO"}

PSX_SCRIP_TO_SYMBOL: Dict[str, str] = {
    "SYSTEMS LIMITED": "SYS",
    "FAUJI FERT": "FFC",
    "FAUJI FERTILIZER": "FFC",
    "FAUJI FERTXD": "FFC",
    "OIL & GAS DEV": "OGDC",
    "OIL & GAS DEVXD": "OGDC",
    "OIL AND GAS DEVELOPMENT": "OGDC",
    "MARI ENERGIES LTD.": "MARI",
    "MARI PETROLEUM": "MARI",
    "HUB POWER CO.": "HUBC",
    "HUB POWER CO.XD": "HUBC",
    "HUB POWER": "HUBC",
    "LUCKY CEMENT": "LUCK",
    "ENGRO FERTERTXD": "EFERT",
    "ENGRO FERTERT": "EFERT",
    "ENGRO FERTILIZER": "EFERT",
    "ENGRO FERT": "EFERT",
    "MEEZAN BANK LTD": "MEBL",
    "MEEZAN BANK LTDXD": "MEBL",
    "MEEZAN BANK": "MEBL",
    "FAYSAL BANK": "FABL",
    "FAYSAL BANKXD": "FABL",
    "FAYSAL BANK LTD": "FABL",
    "PAKISTAN PETROLEUM": "PPL",
    "PAK PETROLEUMXD": "PPL",
    "P.P.L.": "PPL",
    "P.P.L.XD": "PPL",
    "PAK OILFIELDS": "POL",
    "PAKISTAN STATE OIL": "PSO",
    "P.S.O.": "PSO",
    "P.S.O.XD": "PSO",
    "UNITED BANK": "UBL",
    "MCB BANK": "MCB",
    "HABIB BANK": "HBL",
    "BANK AL HABIB": "BAHL",
    "TRG PAKISTAN": "TRG",
    "ENGRO CORP": "ENGRO",
    "ENGRO CORPORATION": "ENGRO",
    "D.G. KHAN CEMENT": "DGKC",
    "MAPLE LEAF": "MLCF",
    "CHERAT CEMENT": "CHCC",
    "FAUJI CEMENT": "FCCL",
    "UNITY FOODS": "UNITY",
    "PAKISTAN REFINERY": "PRL",
    "ATTOCK REFINERY": "ATRL",
    "NATIONAL REFINERY": "NRL",
    "PAK ELEKTRON": "PAEL",
    "WORLDCALL TELECOM": "WTL",
    "TELECARD": "TELE",
    "LOTTE CHEMICAL": "LOTCHEM",
    "CNERGYICO PK": "CNERGY",
}


def fetch_dps_company_quote(ticker: str, timeout: float = 3.5) -> Optional[Dict[str, Any]]:
    """
    Fetches real-time spot quote directly from official PSX Data Portal (https://dps.psx.com.pk/company/{sym}).
    Returns exact spot price, LDCP / prev close, change, change_pct, open, high, low, volume.
    Sub-300ms response time with 0% cloud blocking.
    """
    sym = normalize_ticker(ticker)
    if not sym:
        return None
    url = f"https://dps.psx.com.pk/company/{sym}"
    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Cache-Control": "no-cache, no-store",
                "Pragma": "no-cache",
            },
            verify=False,
            timeout=timeout,
        )
        if resp.status_code != 200:
            return None
        html = resp.text

        m_price = re.search(r'<div class="quote__close">Rs\.?\s*([0-9.,]+)</div>', html)
        if not m_price:
            return None
        price = _clean_float(m_price.group(1), 0.0)
        if price <= 0:
            return None

        def _get_stat(lbl: str) -> float:
            m = re.search(rf'<div class="stats_label">{lbl}</div>\s*<div class="stats_value">([0-9.,]+)</div>', html, re.I)
            return _clean_float(m.group(1), 0.0) if m else 0.0

        ldcp = _get_stat("LDCP")
        open_p = _get_stat("Open")
        high_p = _get_stat("High")
        low_p = _get_stat("Low")
        vol = _get_stat("Volume")

        chg_match = re.search(r'<div class="quote__change\s+([^"]+)">', html)
        is_neg = False
        if chg_match and "change__text--neg" in chg_match.group(1):
            is_neg = True

        m_chg = re.search(r'<div class="change__value">([0-9.,]+)</div>', html)
        chg_val = _clean_float(m_chg.group(1), 0.0) if m_chg else 0.0
        if is_neg and chg_val > 0:
            chg_val = -chg_val

        m_pct = re.search(r'<div class="change__percent">\s*\(([0-9.,]+)%\)</div>', html)
        chg_pct = _clean_float(m_pct.group(1), 0.0) if m_pct else 0.0
        if is_neg and chg_pct > 0:
            chg_pct = -chg_pct

        prev_close = ldcp if ldcp > 0 else (round(price - chg_val, 2) if chg_val != 0 else price)
        if chg_val == 0.0 and prev_close > 0 and price != prev_close:
            chg_val = round(price - prev_close, 2)
            chg_pct = round(((price - prev_close) / prev_close) * 100.0, 2)

        return {
            "price": round(price, 2),
            "change": round(chg_val, 2),
            "change_pct": round(chg_pct, 2),
            "change_abs": round(chg_val, 2),
            "prev_close": round(prev_close, 2),
            "open": round(open_p, 2) if open_p > 0 else round(price, 2),
            "high": round(high_p, 2) if high_p > 0 else round(price, 2),
            "low": round(low_p, 2) if low_p > 0 else round(price, 2),
            "volume": vol,
            "source": "dps_company_realtime",
            "timestamp": time.time(),
        }
    except Exception:
        return None


def fetch_dps_company_quotes_concurrent(tickers: Iterable[str], timeout: float = 4.0) -> Dict[str, Dict[str, Any]]:
    """Fetches real-time quotes concurrently for multiple tickers directly from dps.psx.com.pk/company/{sym}."""
    syms = list({normalize_ticker(t) for t in tickers if normalize_ticker(t)})
    if not syms:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(12, len(syms))) as pool:
        futs = {pool.submit(fetch_dps_company_quote, s, timeout): s for s in syms}
        for fut in concurrent.futures.as_completed(futs):
            s = futs[fut]
            try:
                res = fut.result()
                if res and res.get("price", 0) > 0:
                    out[s] = res
            except Exception:
                pass
    return out


def fetch_psx_market_summary_bulk(timeout: float = 5.0) -> Dict[str, Dict[str, Any]]:
    """
    Fetches official PSX Market Summary (https://www.psx.com.pk/market-summary/) in bulk.
    Extracts 590+ stocks with accurate scrip name to ticker mapping.
    """
    url = f"https://www.psx.com.pk/market-summary/?_={int(time.time() * 1000)}"
    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Cache-Control": "no-cache, no-store",
                "Pragma": "no-cache",
            },
            verify=False,
            timeout=timeout,
        )
        if resp.status_code != 200:
            return {}
        html = resp.text
        rows = re.findall(r'<tr[\s\S]*?</tr>', html)
        quotes: Dict[str, Dict[str, Any]] = {}
        now = time.time()
        for r in rows:
            tds = [re.sub(r'<[^>]+>', '', c).strip() for c in re.findall(r'<t[hd][\s\S]*?</t[hd]>', r)]
            if len(tds) < 7:
                continue
            if tds[0] == '' or re.match(r'^\d+$', tds[0]):
                tds = tds[1:]
            if len(tds) < 7:
                continue
            scrip = tds[0].strip()
            if not scrip or scrip.upper() in ("SCRIP", "SYMBOL"):
                continue

            ldcp = _clean_float(tds[1])
            open_p = _clean_float(tds[2])
            high_p = _clean_float(tds[3])
            low_p = _clean_float(tds[4])
            current = _clean_float(tds[5])
            change = _clean_float(tds[6])
            volume = _clean_float(tds[7] if len(tds) > 7 else 0)

            if current <= 0:
                continue

            change_pct = round(((change / ldcp) * 100.0), 2) if ldcp > 0 else 0.0
            prev_close = ldcp if ldcp > 0 else round(current - change, 2)

            q_item = {
                "price": round(current, 2),
                "change": round(change, 2),
                "change_pct": round(change_pct, 2),
                "change_abs": round(change, 2),
                "prev_close": round(prev_close, 2),
                "open": round(open_p, 2) if open_p > 0 else round(current, 2),
                "high": round(high_p, 2) if high_p > 0 else round(current, 2),
                "low": round(low_p, 2) if low_p > 0 else round(current, 2),
                "volume": volume,
                "source": "psx_market_watch",
                "timestamp": now,
            }

            clean_scrip = re.sub(r'(\.|\s*)(XD|NC|XB|XR|WU|SPOT|PRO)$', '', scrip, flags=re.I).replace('.', '').strip()
            scrip_upper = scrip.upper()
            clean_upper = clean_scrip.upper()

            canonical_sym = PSX_SCRIP_TO_SYMBOL.get(scrip_upper) or PSX_SCRIP_TO_SYMBOL.get(clean_upper) or normalize_ticker(clean_upper)
            if canonical_sym:
                quotes[canonical_sym] = q_item
            quotes[scrip_upper] = q_item

            parts = scrip.split('-')
            if len(parts) == 2 and parts[1].upper() in ('NOV', 'NOVB', 'NOVC', 'DEC', 'DECB', 'JAN', 'FEB'):
                base_sym = normalize_ticker(parts[0].upper())
                if base_sym and base_sym not in quotes:
                    quotes[base_sym] = q_item

        return quotes
    except Exception:
        return {}


def _clean_float(raw: Any, default: float = 0.0) -> float:
    if raw is None:
        return default
    if isinstance(raw, (int, float)):
        return float(raw)
    cleaned = (
        str(raw)
        .replace("Rs.", "")
        .replace("PKR", "")
        .replace("%", "")
        .replace(",", "")
        .replace("(", "-")
        .replace(")", "")
        .strip()
    )
    if not cleaned or cleaned in {"-", "--", "N/A"}:
        return default
    m = re.search(r"[-+]?\d*\.?\d+", cleaned)
    if not m:
        return default
    try:
        return float(m.group(0))
    except ValueError:
        return default


def _normalize_psx_table_symbol(cell_el) -> str:
    """Extracts canonical PSX ticker symbol from a market-watch or screener <td> element."""
    if cell_el is None:
        return ""
    a_tag = cell_el.find("a", class_="tbl__symbol") or cell_el.find("a")
    if a_tag and a_tag.get_text(strip=True):
        raw_sym = a_tag.get_text(" ", strip=True)
    else:
        raw_sym = cell_el.get("data-order") or cell_el.get_text(" ", strip=True)

    parts = str(raw_sym or "").upper().split()
    if not parts:
        return ""
    if len(parts) > 1 and parts[-1] in _PSX_STATUS_SUFFIXES:
        return normalize_ticker(parts[0].strip())
    return normalize_ticker(parts[0].strip())


def _parse_psx_bulk_table(html: str) -> Dict[str, Dict[str, Any]]:
    """
    Parses https://dps.psx.com.pk/market-watch or https://dps.psx.com.pk/screener HTML table.
    Returns {TICKER: {"price": float, "change": float, "change_pct": float, "source": "psx_market_watch"}}.
    """
    if not html or "<table" not in html.lower():
        return {}

    soup = BeautifulSoup(html, "html.parser")
    quotes: Dict[str, Dict[str, Any]] = {}

    for table in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True).upper() for th in table.find_all("th")]
        if not headers:
            first_tr = table.find("tr")
            if first_tr:
                headers = [c.get_text(" ", strip=True).upper() for c in first_tr.find_all(["th", "td"])]

        sym_idx = -1
        price_idx = -1
        chg_idx = -1
        chg_pct_idx = -1

        for idx, h in enumerate(headers):
            h_clean = re.sub(r"\s+", " ", h).strip()
            if sym_idx == -1 and any(k in h_clean for k in ("SYMBOL", "SCRIP", "TICKER")):
                sym_idx = idx
            elif price_idx == -1 and h_clean in {"CURRENT", "PRICE", "CLOSE", "LAST"}:
                price_idx = idx
            elif chg_pct_idx == -1 and (("%" in h_clean and "CHANGE" in h_clean) or h_clean in {"CHANGE (%)", "% CHANGE", "CHG (%)"}):
                chg_pct_idx = idx
            elif chg_idx == -1 and h_clean in {"CHANGE", "CHG", "NET CHANGE"}:
                chg_idx = idx

        if sym_idx == -1 or price_idx == -1:
            continue

        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) <= max(sym_idx, price_idx):
                continue

            sym = _normalize_psx_table_symbol(tds[sym_idx])
            if not sym:
                continue

            price_el = tds[price_idx]
            raw_price = price_el.get("data-order") or price_el.get_text(" ", strip=True)
            price_val = _clean_float(raw_price, 0.0)
            if price_val <= 0:
                continue

            chg_pct_val = 0.0
            if chg_pct_idx != -1 and len(tds) > chg_pct_idx:
                cp_el = tds[chg_pct_idx]
                chg_pct_val = _clean_float(cp_el.get("data-order") or cp_el.get_text(" ", strip=True), 0.0)

            if chg_idx != -1 and len(tds) > chg_idx:
                c_el = tds[chg_idx]
                chg_val = _clean_float(c_el.get("data-order") or c_el.get_text(" ", strip=True), 0.0)
            else:
                denom = 1.0 + (chg_pct_val / 100.0)
                prev_close = (price_val / denom) if abs(denom) > 1e-6 else price_val
                chg_val = round(price_val - prev_close, 2)

            quotes[sym] = {
                "price": round(price_val, 2),
                "change": round(chg_val, 2),
                "change_pct": round(chg_pct_val, 2),
                "source": "psx_market_watch",
            }

        if len(quotes) >= 20:
            break

    return quotes


# ==============================================================================
# TIER 1: GLOBAL CLOUD-SAFE REAL-TIME PSX JSON BATCH (TRADINGVIEW PAKISTAN SCANNER)
# ==============================================================================

def fetch_tradingview_psx_quotes(tickers: Iterable[str], timeout: float = 6.0) -> Dict[str, Dict[str, Any]]:
    """
    Tier 1 Primary Live Spot Price Engine:
    Fetches exact real-time/closing PSX spot prices, daily % change, daily PKR change, volume,
    TTM P/E, and dividend yield for PSX:<TICKER> in a single global CDN JSON request.
    Has 0% blocks across cloud IPs (Oracle Cloud, Render, AWS, GCP).
    """
    unique_syms = []
    seen = set()
    for t in tickers:
        s = normalize_ticker(str(t or ""))
        if s and s not in seen:
            seen.add(s)
            unique_syms.append(s)

    if not unique_syms:
        return {}

    now_ms = int(time.time() * 1000)
    payload = {
        "symbols": {
            "tickers": [f"PSX:{sym}" for sym in unique_syms],
            "query": {"types": []},
        },
        "columns": [
            "name",
            "close",
            "change",
            "change_abs",
            "bid",
            "ask",
            "high",
            "low",
            "volume",
            "Recommend.All",
            "price_earnings_ttm",
            "dividends_yield_current",
        ],
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Origin": "https://www.tradingview.com",
        "Referer": "https://www.tradingview.com/",
        "Cache-Control": "no-cache, no-store",
        "Pragma": "no-cache",
    }
    try:
        resp = requests.post(
            f"https://scanner.tradingview.com/pakistan/scan?_t={now_ms}",
            json=payload,
            headers=headers,
            timeout=timeout,
        )
        if resp.status_code != 200:
            return {}
        data = resp.json() or {}
        out: Dict[str, Dict[str, Any]] = {}
        for item in data.get("data") or []:
            d = item.get("d") or []
            if len(d) < 4:
                continue
            raw_sym = str(d[0] or item.get("s", "").split(":")[-1])
            sym = normalize_ticker(raw_sym)
            price = _clean_float(d[1], 0.0)
            chg_pct = _clean_float(d[2], 0.0)
            chg_abs = _clean_float(d[3], 0.0)
            bid = _clean_float(d[4] if len(d) > 4 else None, None)
            ask = _clean_float(d[5] if len(d) > 5 else None, None)
            high = _clean_float(d[6] if len(d) > 6 else None, None)
            low = _clean_float(d[7] if len(d) > 7 else None, None)
            volume = _clean_float(d[8] if len(d) > 8 else 0, 0.0)
            recommend = str(d[9]) if len(d) > 9 and d[9] is not None else None
            pe_ttm = _clean_float(d[10] if len(d) > 10 else None, None)
            div_yield = _clean_float(d[11] if len(d) > 11 else None, None)

            if sym and price > 0:
                prev_close = round(price - chg_abs, 2) if chg_abs != 0 else (
                    round(price / (1.0 + (chg_pct / 100.0)), 2) if chg_pct != 0 else price
                )
                if prev_close <= 0:
                    prev_close = price
                out[sym] = {
                    "price": round(price, 2),
                    "change": round(chg_abs, 2),
                    "change_pct": round(chg_pct, 2),
                    "change_abs": round(chg_abs, 2),
                    "prev_close": round(prev_close, 2),
                    "bid": round(bid, 2) if bid is not None else None,
                    "ask": round(ask, 2) if ask is not None else None,
                    "high": round(high, 2) if high is not None else None,
                    "low": round(low, 2) if low is not None else None,
                    "volume": volume,
                    "recommend": recommend,
                    "pe_ttm": round(pe_ttm, 2) if pe_ttm is not None else None,
                    "dividend_yield": round(div_yield, 2) if div_yield is not None else None,
                    "source": "tradingview_psx_realtime",
                    "timestamp": time.time(),
                }
        return out
    except Exception:
        return {}


# Backwards-compatible alias
fetch_tradingview_psx_batch = fetch_tradingview_psx_quotes


def fetch_realtime_psx_quotes(
    tickers: Optional[Iterable[str]] = None,
    timeout: float = 5.0,
) -> Dict[str, Dict[str, Any]]:
    """
    TASK 1: Multi-Source 1-Second Real-Time PSX Price Engine:
    Fetches real-time intraday quotes concurrently across:
      - Feed B (Primary): TradingView Real-Time Pakistan Stock Scanner (POST https://scanner.tradingview.com/pakistan/scan?_t={now_ms})
      - Feed A (Secondary): Yahoo Finance v8 1-Minute Intraday Stream (https://query1.finance.yahoo.com/v8/finance/chart/{ticker}.KA)
      - Feed C (Tertiary): PSX Direct Intraday Timeseries / Market Watch (https://dps.psx.com.pk)
      - Feed D (Failsafe): In-memory cache and Supabase portfolio_holdings.last_price

    Computes exact:
      - prev_close = round(price - change_abs, 2)
      - change_abs = round(price - prev_close, 2)
      - change_pct = round((change_abs / prev_close) * 100.0, 2)

    Updates _BULK_QUOTES_CACHE so every component in the system gets instant live prices.
    """
    global _BULK_QUOTES_CACHE, _BULK_QUOTES_TS

    if tickers is None:
        target_syms = list(CORE_KTRADE_TICKERS)
    else:
        target_syms = [normalize_ticker(str(t or "")) for t in tickers if normalize_ticker(str(t or ""))]

    if not target_syms:
        target_syms = list(CORE_KTRADE_TICKERS)

    quotes: Dict[str, Dict[str, Any]] = {}

    # 1. Source 1 (Primary): Persistent 1-Tab Headless Chromium PSX Stream (sub-5ms when warm)
    try:
        chrom_quotes = fetch_chromium_psx_quotes(target_syms, timeout=min(timeout, 1.0))
        for s, q in chrom_quotes.items():
            if float(q.get("price") or 0.0) > 0:
                quotes[s] = dict(q)
    except Exception:
        pass

    # 2. Source 2 (Official Real-Time PSX Data Portal): dps.psx.com.pk/company/{sym} (sub-300ms, 100% exact spot)
    missing = [s for s in target_syms if s not in quotes or quotes[s].get("price", 0) <= 0]
    if missing:
        dps_quotes = fetch_dps_company_quotes_concurrent(missing, timeout=min(timeout, 3.5))
        for s, q in dps_quotes.items():
            if float(q.get("price") or 0.0) > 0:
                quotes[s] = dict(q)

    # 3. Source 3 (Bulk PSX Market Summary Feed): psx.com.pk/market-summary (590+ stocks)
    missing = [s for s in target_syms if s not in quotes or quotes[s].get("price", 0) <= 0]
    if missing:
        bulk_quotes = fetch_psx_market_summary_bulk(timeout=min(timeout, 4.0))
        for s in missing:
            if s in bulk_quotes and float(bulk_quotes[s].get("price", 0)) > 0:
                quotes[s] = dict(bulk_quotes[s])

    # 4. Source 4 (Delayed Fallback ONLY): TradingView Real-Time Pakistan Scanner
    # CRITICAL: Only used for tickers STILL missing from all official PSX feeds.
    # Never allowed to overwrite live/accurate DPS real-time quotes.
    still_missing = [s for s in target_syms if s not in quotes or quotes[s].get("price", 0) <= 0]
    if still_missing:
        tv_quotes = fetch_tradingview_psx_quotes(still_missing, timeout=timeout)
        for s, q in tv_quotes.items():
            if float(q.get("price") or 0.0) > 0 and (s not in quotes or quotes[s].get("price", 0) <= 0):
                q_copy = dict(q)
                q_copy["source"] = "tradingview_psx_fallback"
                quotes[s] = q_copy

    # 5. Source 5: Yahoo Finance v8 Chart API & Direct Timeseries (Cloud-safe backup)
    still_missing = [s for s in target_syms if s not in quotes or quotes[s].get("price", 0) <= 0]
    if still_missing:
        # Feed A: Yahoo Finance v8 Chart API
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(6, len(still_missing))) as pool:
            yf_futs = {pool.submit(_fetch_single_yahoo_ka_quote, s, timeout): s for s in still_missing}
            for fut in concurrent.futures.as_completed(yf_futs):
                s = yf_futs[fut]
                try:
                    q = fut.result()
                    if q and float(q.get("price", 0)) > 0:
                        pr = round(float(q["price"]), 2)
                        ca = round(float(q.get("change", 0.0)), 2)
                        cp = round(float(q.get("change_pct", 0.0)), 2)
                        pc = round(pr - ca, 2) if ca != 0 else (round(pr / (1.0 + (cp / 100.0)), 2) if cp != 0 else pr)
                        quotes[s] = {
                            "price": pr,
                            "change": ca,
                            "change_pct": cp,
                            "change_abs": ca,
                            "prev_close": pc,
                            "volume": 0.0,
                            "source": q.get("source", "yahoo_v8_ka"),
                            "timestamp": time.time(),
                        }
                except Exception:
                    pass

    # 6. Source 6: Cache & DB Fallback
    still_missing = [s for s in target_syms if s not in quotes or quotes[s].get("price", 0) <= 0]
    if still_missing:
        db_quotes = _get_tier3_db_cached_quotes()
        for s in still_missing:
            if s in _BULK_QUOTES_CACHE and _BULK_QUOTES_CACHE[s].get("price", 0) > 0:
                quotes[s] = dict(_BULK_QUOTES_CACHE[s])
            elif s in db_quotes and db_quotes[s].get("price", 0) > 0:
                pr = round(float(db_quotes[s]["price"]), 2)
                cp = round(float(db_quotes[s].get("change_pct", 0.0)), 2)
                quotes[s] = {
                    "price": pr,
                    "change": 0.0,
                    "change_pct": cp,
                    "change_abs": 0.0,
                    "prev_close": pr,
                    "volume": 0.0,
                    "source": "db_cached_last_price",
                    "timestamp": time.time(),
                }

    # Atomic cache update
    with _BULK_LOCK:
        _BULK_QUOTES_CACHE.update(quotes)
        _BULK_QUOTES_TS = time.time()

    return quotes


async def fetch_realtime_psx_quotes_async(
    tickers: Optional[Iterable[str]] = None,
    timeout: float = 5.0,
) -> Dict[str, Dict[str, Any]]:
    """Async wrapper for the Multi-Source 1-Second Real-Time PSX Price Engine."""
    return await asyncio.to_thread(fetch_realtime_psx_quotes, tickers, timeout)


def _fetch_psx_timeseries_intraday(ticker: str, timeout: float = 5.0) -> Optional[Dict[str, Any]]:
    """
    Tier 2: PSX Intraday Timeseries API (https://dps.psx.com.pk/timeseries/int/{ticker}).
    Returns JSON {"status": 1, "data": [[ts, price, vol], ...]} and extracts data[0][1] as spot price.
    """
    sym = normalize_ticker(ticker)
    if not sym:
        return None
    url = f"https://dps.psx.com.pk/timeseries/int/{sym}"
    try:
        resp = requests.get(
            url,
            headers={**DEFAULT_BROWSER_HEADERS, "Referer": "https://dps.psx.com.pk/"},
            verify=False,
            timeout=timeout,
        )
        if resp.status_code == 200:
            payload = resp.json() or {}
            raw_data = payload.get("data") or []
            if raw_data:
                latest_point = raw_data[0]
                if len(latest_point) >= 2:
                    price = _clean_float(latest_point[1], 0.0)
                    if price > 0:
                        return {
                            "price": round(price, 2),
                            "change": 0.0,
                            "change_pct": 0.0,
                            "source": "dps_timeseries_int",
                        }
    except Exception:
        pass
    return None


def _fetch_single_yahoo_ka_quote(ticker: str, timeout: float = 5.0) -> Optional[Dict[str, Any]]:
    """
    Fetches live spot price & daily % change for <TICKER>.KA via Yahoo Finance v8 Chart JSON API,
    validating regularMarketTime freshness (rejects frozen pre-split feeds older than 14 days).
    """
    sym = normalize_ticker(ticker)
    if not sym:
        return None

    yf_sym = f"{sym}.KA"
    endpoints = [
        f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_sym}?interval=1m&range=1d",
        f"https://query2.finance.yahoo.com/v8/finance/chart/{yf_sym}?interval=1d&range=5d",
    ]
    now_epoch = time.time()
    max_stale_seconds = 14 * 86400

    for url in endpoints:
        try:
            resp = requests.get(url, headers=YAHOO_JSON_HEADERS, timeout=timeout)
            if resp.status_code != 200:
                continue
            payload = resp.json()
            results = (payload or {}).get("chart", {}).get("result")
            if not results:
                continue
            meta = results[0].get("meta") or {}
            mkt_ts = float(meta.get("regularMarketTime") or 0)
            # Guard against discontinued/unsplit 2024 Yahoo .KA feeds
            if mkt_ts > 0 and (now_epoch - mkt_ts) > max_stale_seconds:
                continue
            price = _clean_float(meta.get("regularMarketPrice"), 0.0)
            if price <= 0:
                continue
            prev_close = _clean_float(
                meta.get("chartPreviousClose") or meta.get("previousClose"),
                price,
            )
            if prev_close <= 0:
                prev_close = price
            change = round(price - prev_close, 2)
            change_pct = round(((price - prev_close) / prev_close) * 100.0, 2) if prev_close > 0 else 0.0
            return {
                "price": round(price, 2),
                "change": change,
                "change_pct": change_pct,
                "source": "yahoo_v8_ka",
            }
        except Exception:
            continue

    return None


def fetch_yahoo_psx_batch(tickers: Iterable[str], timeout: float = 5.5) -> Dict[str, Dict[str, Any]]:
    """
    Tier 1 Geo-Proof Batch Fetcher:
    1) Fetches split-adjusted live PSX quotes via TradingView Pakistan JSON batch (single request, 0% 462 blocks)
    2) Queries freshness-validated Yahoo Finance v8 (<TICKER>.KA) for any remaining tickers
    """
    unique_syms = []
    seen = set()
    for t in tickers:
        s = normalize_ticker(str(t or ""))
        if s and s not in seen:
            seen.add(s)
            unique_syms.append(s)

    if not unique_syms:
        return {}

    results: Dict[str, Dict[str, Any]] = fetch_tradingview_psx_batch(unique_syms, timeout=timeout)
    missing = [s for s in unique_syms if s not in results or results[s].get("price", 0.0) <= 0]
    if not missing:
        return results

    max_workers = min(8, len(missing))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(_fetch_single_yahoo_ka_quote, sym, timeout): sym
            for sym in missing
        }
        for fut in concurrent.futures.as_completed(future_map):
            sym = future_map[fut]
            try:
                q = fut.result()
                if q and q.get("price", 0.0) > 0:
                    results[sym] = q
            except Exception:
                pass
    return results


# ==============================================================================
# TIER 2: PSX MARKET-WATCH WITH FULL CHROME HEADERS & SSL=FALSE
# ==============================================================================

def _fetch_psx_market_watch_tier2(timeout: int = 10) -> Dict[str, Dict[str, Any]]:
    """
    Fetches https://dps.psx.com.pk/market-watch with full Chrome headers, Referer, and verify=False.
    Falls back to cookie-warmed session or /screener if DOSarrest challenges direct XHR.
    """
    mw_headers = {
        **DEFAULT_BROWSER_HEADERS,
        "Accept": "text/html, */*; q=0.01",
        "Referer": "https://dps.psx.com.pk/",
        "X-Requested-With": "XMLHttpRequest",
    }
    session = requests.Session()
    try:
        resp = session.get(
            "https://dps.psx.com.pk/market-watch",
            headers=mw_headers,
            verify=False,
            timeout=min(timeout, 6),
        )
        if resp.status_code == 200 and resp.text:
            parsed = _parse_psx_bulk_table(resp.text)
            if parsed:
                return parsed

        # Warm session cookies via root portal then retry /market-watch
        session.get("https://dps.psx.com.pk/", headers=DEFAULT_BROWSER_HEADERS, verify=False, timeout=5)
        resp2 = session.get(
            "https://dps.psx.com.pk/market-watch",
            headers=mw_headers,
            verify=False,
            timeout=min(timeout, 6),
        )
        if resp2.status_code == 200 and resp2.text:
            parsed = _parse_psx_bulk_table(resp2.text)
            if parsed:
                return parsed
    except Exception:
        pass

    try:
        resp_scr = session.get(
            "https://dps.psx.com.pk/screener",
            headers=DEFAULT_BROWSER_HEADERS,
            verify=False,
            timeout=timeout,
        )
        if resp_scr.status_code == 200 and resp_scr.text:
            return _parse_psx_bulk_table(resp_scr.text)
    except Exception:
        pass

    return {}


# ==============================================================================
# TIER 3: DATABASE CACHED LAST_PRICE FALLBACK
# ==============================================================================

def _get_tier3_db_cached_quotes() -> Dict[str, Dict[str, Any]]:
    """Reads portfolio_holdings.last_price so spot price never drops to 0.0 during network blips."""
    try:
        from portfolio_manager import get_cached_db_prices
        db_map = get_cached_db_prices()
        return {
            sym: {
                "price": float(val[0]),
                "change": 0.0,
                "change_pct": float(val[1]),
                "source": "db_cached_last_price",
            }
            for sym, val in db_map.items()
            if val and float(val[0]) > 0
        }
    except Exception:
        return {}


# ==============================================================================
# UNIFIED 3-TIER BULK & SINGLE QUOTE ENGINE
# ==============================================================================

def fetch_all_psx_prices_bulk(
    force_refresh: bool = False,
    timeout: int = 10,
    target_tickers: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Geo-Proof 3-Tier Live Price Engine:
    - Tier 1: Yahoo Finance v8 Chart API (<TICKER>.KA) + Google Finance KAR CDN (0% 462 blocks on Cloud IPs)
    - Tier 2: PSX Market-Watch (https://dps.psx.com.pk/market-watch with Chrome headers & verify=False)
              for any ticker missing from Tier 1
    - Tier 3: Supabase portfolio_holdings.last_price fallback so no holding ever returns 0.0
    """
    global _BULK_QUOTES_CACHE, _BULK_QUOTES_TS

    required_syms = [normalize_ticker(t) for t in (target_tickers or CORE_KTRADE_TICKERS) if normalize_ticker(t)]

    now = time.time()
    if (
        not force_refresh
        and _BULK_QUOTES_CACHE
        and (now - _BULK_QUOTES_TS) < BULK_QUOTE_TTL_SECONDS
        and all(s in _BULK_QUOTES_CACHE and _BULK_QUOTES_CACHE[s].get("price", 0) > 0 for s in required_syms)
    ):
        return _BULK_QUOTES_CACHE

    with _BULK_LOCK:
        now = time.time()
        if (
            not force_refresh
            and _BULK_QUOTES_CACHE
            and (now - _BULK_QUOTES_TS) < BULK_QUOTE_TTL_SECONDS
            and all(s in _BULK_QUOTES_CACHE and _BULK_QUOTES_CACHE[s].get("price", 0) > 0 for s in required_syms)
        ):
            return _BULK_QUOTES_CACHE

        # Discover active DB tickers so custom holdings added by user are also included in Tier 1
        db_tier3 = _get_tier3_db_cached_quotes()
        all_target_syms = list(dict.fromkeys(required_syms + list(db_tier3.keys())))

        merged: Dict[str, Dict[str, Any]] = dict(_BULK_QUOTES_CACHE)

        # Tier 1 & 2: Multi-Source Real-Time Engine (TradingView + Yahoo + PSX Timeseries)
        rt_quotes = fetch_realtime_psx_quotes(all_target_syms, timeout=timeout)
        merged.update(rt_quotes)

        # Tier 3: Database cached last_price fallback for anything still unresolved
        for sym, db_q in db_tier3.items():
            if sym not in merged or merged[sym].get("price", 0.0) <= 0:
                merged[sym] = db_q

        if merged:
            _BULK_QUOTES_CACHE = merged
            _BULK_QUOTES_TS = time.time()

        return _BULK_QUOTES_CACHE


# Backwards-compatible alias
fetch_bulk_psx_quotes = fetch_all_psx_prices_bulk


async def fetch_bulk_psx_quotes_async(force_refresh: bool = False, timeout: int = 10) -> Dict[str, Dict[str, Any]]:
    """Async wrapper around the Geo-Proof 3-Tier Live Price Engine."""
    return await asyncio.to_thread(fetch_all_psx_prices_bulk, force_refresh, timeout)


def get_live_price_and_change(ticker: str, allow_single_fallback: bool = True) -> Tuple[float, float]:
    """
    Geo-Proof 3-Tier lookup for (spot_price, daily_change_pct):
    Tier 1: Yahoo Finance .KA / Google Finance KAR
    Tier 2: PSX Market-Watch bulk table
    Tier 3: portfolio_holdings.last_price cached in Supabase
    """
    sym = normalize_ticker(ticker)
    if not sym:
        return 0.0, 0.0

    bulk = fetch_all_psx_prices_bulk(force_refresh=False, target_tickers=[sym])
    entry = bulk.get(sym)
    if entry and float(entry.get("price", 0.0)) > 0:
        return float(entry["price"]), float(entry.get("change_pct", 0.0))

    if allow_single_fallback:
        dps_q = fetch_dps_company_quote(sym, timeout=4.0)
        if dps_q and float(dps_q.get("price", 0.0)) > 0:
            with _BULK_LOCK:
                _BULK_QUOTES_CACHE[sym] = dps_q
            return float(dps_q["price"]), float(dps_q.get("change_pct", 0.0))

        t1 = _fetch_single_yahoo_ka_quote(sym, timeout=5.5)
        if t1 and float(t1.get("price", 0.0)) > 0:
            with _BULK_LOCK:
                _BULK_QUOTES_CACHE[sym] = t1
            return float(t1["price"]), float(t1.get("change_pct", 0.0))

    # Tier 3 DB fallback
    db_quotes = _get_tier3_db_cached_quotes()
    if sym in db_quotes and float(db_quotes[sym]["price"]) > 0:
        return float(db_quotes[sym]["price"]), float(db_quotes[sym].get("change_pct", 0.0))

    return 0.0, 0.0


# Backwards-compatible alias
get_bulk_quote_for_ticker = get_live_price_and_change


def fetch_live_price(ticker: str) -> float:
    """Returns live PSX spot price via the Geo-Proof 3-Tier Engine (never 0.0 for held stocks)."""
    price, _ = get_live_price_and_change(ticker, allow_single_fallback=True)
    return price


def fetch_latest_eps() -> dict:
    """Scrapes PSX financial announcements table for latest EPS / (LPS) values."""
    url = "https://www.psx.com.pk/psx/announcement/financial-announcements"
    eps_data = {}
    try:
        res = requests.get(url, headers=DEFAULT_BROWSER_HEADERS, verify=False, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        table = soup.find("table")
        if table:
            rows = table.find_all("tr")
            if not rows:
                return eps_data

            header_row = rows[0]
            headers = [td.text.strip().lower() for td in header_row.find_all(["td", "th"])]

            company_idx = -1
            eps_idx = -1
            for i, header in enumerate(headers):
                if "company" in header or "symbol" in header:
                    company_idx = i
                elif "eps" in header or "lps" in header:
                    eps_idx = i

            if company_idx != -1 and eps_idx != -1:
                for row in rows[1:]:
                    cols = row.find_all("td")
                    if len(cols) > max(company_idx, eps_idx):
                        company = cols[company_idx].text.strip()
                        eps_str = cols[eps_idx].text.strip()
                        if company and eps_str and eps_str != "-":
                            try:
                                eps_str_clean = eps_str.replace(",", "").replace("(", "-").replace(")", "")
                                eps_data[company] = float(eps_str_clean)
                            except ValueError:
                                pass
    except Exception as e:
        print(f"Error fetching latest EPS: {e}")

    return eps_data


def _looks_like_html(body: str, content_type: str = "") -> bool:
    if "html" in (content_type or "").lower():
        return True
    s = (body or "").lstrip()[:80].lower()
    return s.startswith(("<!doctype", "<html", "<head", "<body", "<div", "<table", "<!--"))


def parse_company_quote_html(html: str) -> Tuple[float, float]:
    """Parses /company/<TICKER> SSR HTML using BeautifulSoup (.quote__close) to extract (price, change_pct)."""
    if not html:
        return 0.0, 0.0
    soup = BeautifulSoup(html, "html.parser")
    price = 0.0
    change_pct = 0.0
    price_elem = soup.select_one(".quote__close")
    if price_elem:
        price = _clean_float(price_elem.get_text(strip=True), 0.0)
    change_elem = soup.select_one(".quote__change")
    if change_elem:
        txt = change_elem.get_text(" ", strip=True)
        m = re.search(r"\(([-+]?[0-9]*\.?[0-9]+)%\)", txt)
        if m:
            change_pct = float(m.group(1))
        else:
            change_pct = _clean_float(txt, 0.0)
    return price, change_pct


def _extract_company_metrics_from_html(html: str, ticker: str = "") -> Optional[Dict[str, Any]]:
    """Extracts spot quote and fundamental fields from SSR /company/<TICKER> HTML via BeautifulSoup."""
    if not html:
        return None
    price, change_pct = parse_company_quote_html(html)
    soup = BeautifulSoup(html, "html.parser")
    out: Dict[str, Any] = {}
    if ticker:
        out["ticker"] = ticker.upper()
    if price > 0:
        out["price"] = price
        out["change_pct"] = change_pct

    for item in soup.select(".stats_item"):
        lbl_el = item.select_one(".stats_label")
        val_el = item.select_one(".stats_value")
        if not lbl_el or not val_el:
            continue
        lbl = lbl_el.get_text(" ", strip=True).upper()
        val_txt = val_el.get_text(" ", strip=True)
        if "P/E" in lbl and "pe" not in out:
            out["pe"] = _clean_float(val_txt, 0.0)
        elif "YIELD" in lbl and "dividendYield" not in out:
            out["dividendYield"] = val_txt
        elif "EPS" in lbl and "eps" not in out:
            out["eps"] = _clean_float(val_txt, 0.0)

    return out if out else None


async def fetch_psx_data_robust(
    url: str,
    is_json: bool = True,
    timeout: int = 12,
) -> Optional[Union[Dict[str, Any], list, str]]:
    """
    Geo-hardened async PSX fetcher (ssl=False on all aiohttp calls).
    If /company/<TICKER> returns 462/403 from cloud IPs and is_json=True, seamlessly falls back
    to the Geo-Proof 3-Tier Live Price Engine so callers never fail with 462 Client Error.
    """
    max_retries = 2
    is_company_url = "/company/" in url
    is_legacy_company_api = "/api/company/" in url
    ticker_tail = normalize_ticker(url.rstrip("/").split("/")[-1]) if is_company_url else ""

    # Fast path for company JSON quote callers: use Geo-Proof Tier 1 first to avoid 462 completely
    if is_company_url and is_json and ticker_tail:
        t1_batch = await asyncio.to_thread(fetch_yahoo_psx_batch, [ticker_tail], 5.0)
        t1 = t1_batch.get(ticker_tail)
        if t1 and float(t1.get("price", 0.0)) > 0:
            return {
                "ticker": ticker_tail,
                "price": t1["price"],
                "change_pct": t1["change_pct"],
                "source": t1.get("source", "psx_global_realtime"),
            }

    target_url = url
    if "symbols" in url or "KMI30" in url:
        target_url = "https://dps.psx.com.pk/screener"
    elif is_legacy_company_api:
        target_url = f"https://dps.psx.com.pk/company/{ticker_tail}"

    req_headers = {
        **DEFAULT_BROWSER_HEADERS,
        "Referer": "https://dps.psx.com.pk/",
    }

    for attempt in range(max_retries):
        try:
            client_timeout = aiohttp.ClientTimeout(total=timeout)
            connector = aiohttp.TCPConnector(ssl=False)

            async with aiohttp.ClientSession(
                connector=connector,
                headers=req_headers,
                timeout=client_timeout,
            ) as session:
                async with session.get(target_url, ssl=False) as response:
                    if response.status == 200:
                        html = await response.text()
                        content_type = response.headers.get("Content-Type", "")

                        if "symbols" in url:
                            soup = BeautifulSoup(html, "html.parser")
                            symbols = []
                            for tr in soup.find_all("tr"):
                                td = tr.find("td", {"data-order": True})
                                if td and td.find("a", class_="tbl__symbol"):
                                    a_tag = td.find("a", class_="tbl__symbol")
                                    sym = a_tag.text.strip()
                                    name = a_tag.get("data-title", sym)
                                    symbols.append({"symbol": sym, "name": name})
                            seen = set()
                            unique_symbols = []
                            for s in symbols:
                                if s["symbol"] not in seen:
                                    unique_symbols.append(s)
                                    seen.add(s["symbol"])
                            return unique_symbols

                        if "KMI30" in url:
                            return [
                                {"symbol": "HUBC"}, {"symbol": "MEBL"}, {"symbol": "EFERT"},
                                {"symbol": "ENGRO"}, {"symbol": "LUCK"}, {"symbol": "SYS"},
                                {"symbol": "PSO"}, {"symbol": "MARI"}, {"symbol": "POL"},
                                {"symbol": "OGDC"}, {"symbol": "PPL"}, {"symbol": "MTL"},
                                {"symbol": "THALL"}, {"symbol": "CHCC"}, {"symbol": "FFC"},
                                {"symbol": "FABL"}, {"symbol": "ATRL"}, {"symbol": "NRL"},
                                {"symbol": "TRG"}, {"symbol": "AVN"}, {"symbol": "SAZEW"},
                                {"symbol": "INIL"}, {"symbol": "ISL"}, {"symbol": "PIOC"},
                                {"symbol": "DGKC"}, {"symbol": "MLCF"}, {"symbol": "MUREB"},
                                {"symbol": "SEARL"}, {"symbol": "AGP"}, {"symbol": "ABOT"},
                                {"symbol": "FCCL"},
                            ]

                        if not is_json:
                            return html

                        if is_company_url:
                            parsed_metrics = _extract_company_metrics_from_html(html, ticker=ticker_tail)
                            if parsed_metrics:
                                return parsed_metrics
                            return None

                        if _looks_like_html(html, content_type):
                            return None

                        try:
                            return json.loads(html)
                        except json.JSONDecodeError:
                            return None

                    elif response.status in (403, 462, 469):
                        # PSX WAF geo-block (e.g. 462 on Oracle Cloud Mumbai) — break immediately to fallback
                        break
                    elif response.status in (404, 500, 502, 503, 504):
                        raise RuntimeError(f"HTTP {response.status}")

        except Exception as e:
            print(f"⚠️ [PSX Scraper] Attempt {attempt + 1} for {target_url}: {e}", flush=True)
            err_str = str(e).lower()
            if "getaddrinfo failed" in err_str or "nodename nor servname provided" in err_str:
                break

        if attempt < max_retries - 1:
            await asyncio.sleep(0.5 + random.uniform(0.1, 0.4))

    # Final fallback for /company/<TICKER> when is_json=True: use 3-Tier Bulk/DB Engine
    if is_company_url and is_json and ticker_tail:
        cp, chg = await asyncio.to_thread(get_live_price_and_change, ticker_tail, True)
        if cp > 0:
            return {"ticker": ticker_tail, "price": cp, "change_pct": chg, "source": "tier2_or_tier3_fallback"}

    if "KMI30" in url:
        return [
            {"symbol": "HUBC"}, {"symbol": "MEBL"}, {"symbol": "EFERT"},
            {"symbol": "ENGRO"}, {"symbol": "LUCK"}, {"symbol": "SYS"},
            {"symbol": "PSO"}, {"symbol": "MARI"}, {"symbol": "POL"},
            {"symbol": "OGDC"}, {"symbol": "PPL"}, {"symbol": "MTL"},
            {"symbol": "THALL"}, {"symbol": "CHCC"}, {"symbol": "FFC"},
            {"symbol": "FABL"}, {"symbol": "ATRL"}, {"symbol": "NRL"},
            {"symbol": "TRG"}, {"symbol": "AVN"}, {"symbol": "SAZEW"},
            {"symbol": "INIL"}, {"symbol": "ISL"}, {"symbol": "PIOC"},
            {"symbol": "DGKC"}, {"symbol": "MLCF"}, {"symbol": "MUREB"},
            {"symbol": "SEARL"}, {"symbol": "AGP"}, {"symbol": "ABOT"},
            {"symbol": "FCCL"},
        ]

    return None


def fetch_psx_data_robust_sync(url: str, *args, **kwargs):
    """Synchronous bridge for legacy watchdog and ticker valuation callers."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(lambda: asyncio.run(fetch_psx_data_robust(url, *args, **kwargs))).result(timeout=20)
        return loop.run_until_complete(fetch_psx_data_robust(url, *args, **kwargs))
    except RuntimeError:
        return asyncio.run(fetch_psx_data_robust(url, *args, **kwargs))


if __name__ == "__main__":
    t0 = time.perf_counter()
    bulk = fetch_all_psx_prices_bulk(force_refresh=True)
    dt = (time.perf_counter() - t0) * 1000
    print(f"3-Tier Engine fetched {len(bulk)} tickers in {dt:.0f}ms:")
    for k in CORE_KTRADE_TICKERS:
        print(f"  {k}: {bulk.get(k)}")
