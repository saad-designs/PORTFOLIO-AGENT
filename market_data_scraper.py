import asyncio
import json
import random
import re
import threading
import time
import warnings
from typing import Any, Dict, Optional, Tuple, Union

import aiohttp
import requests
import urllib3
from bs4 import BeautifulSoup

# Suppress insecure request warnings for PSX Data Portal SSL certificates (especially on Ubuntu ARM64 Docker)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DEFAULT_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

BULK_QUOTE_TTL_SECONDS: float = 120.0
_BULK_QUOTES_CACHE: Dict[str, Dict[str, float]] = {}
_BULK_QUOTES_TS: float = 0.0
_BULK_LOCK = threading.Lock()

# Suffixes appended to ticker symbols in PSX tables (Non-Compliant, Ex-Dividend, Ex-Bonus, Ex-Right, Winding-Up)
_PSX_STATUS_SUFFIXES = {"NC", "XD", "XB", "XR", "WU", "SPOT", "PRO"}


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
    # Extract leading signed float if extra text exists
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
    # Strip trailing status flags like "AASM NC" -> "AASM" or "EFERT XD" -> "EFERT"
    if len(parts) > 1 and parts[-1] in _PSX_STATUS_SUFFIXES:
        return parts[0].strip()
    return parts[0].strip()


def _parse_psx_bulk_table(html: str) -> Dict[str, Dict[str, float]]:
    """
    Parses either:
    1) https://dps.psx.com.pk/market-watch (SYMBOL, LDCP, OPEN, HIGH, LOW, CURRENT, CHANGE, CHANGE (%), VOLUME)
    2) https://dps.psx.com.pk/screener (SYMBOL, SECTOR, LISTED IN, MARKET CAP., PRICE, CHANGE (%), ...)
    Returns {TICKER: {"price": float, "change": float, "change_pct": float}}.
    """
    if not html or "<table" not in html.lower():
        return {}

    soup = BeautifulSoup(html, "html.parser")
    quotes: Dict[str, Dict[str, float]] = {}

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
            elif price_idx == -1 and h_clean in {"CURRENT", "PRICE", "CLOSE", "LAST", "LDCP"}:
                if h_clean != "LDCP":
                    price_idx = idx
            elif chg_pct_idx == -1 and ("%" in h_clean and "CHANGE" in h_clean or h_clean in {"CHANGE (%)", "% CHANGE", "CHG (%)"}):
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
                # Reconstruct PKR change from price and change_pct when using /screener
                denom = 1.0 + (chg_pct_val / 100.0)
                prev_close = (price_val / denom) if abs(denom) > 1e-6 else price_val
                chg_val = round(price_val - prev_close, 2)

            quotes[sym] = {
                "price": round(price_val, 2),
                "change": round(chg_val, 2),
                "change_pct": round(chg_pct_val, 2),
            }

        if len(quotes) >= 20:
            break

    return quotes


def fetch_all_psx_prices_bulk(force_refresh: bool = False, timeout: int = 12) -> Dict[str, Dict[str, float]]:
    """
    Fetches all PSX ticker prices in a single HTTP request to https://dps.psx.com.pk/market-watch
    (verify=False) and caches {ticker: {"price": float, "change": float, "change_pct": float}}
    in memory for 120 seconds.
    Automatically falls back to https://dps.psx.com.pk/screener if DOSarrest blocks XHR /market-watch.
    """
    global _BULK_QUOTES_CACHE, _BULK_QUOTES_TS

    now = time.time()
    if not force_refresh and _BULK_QUOTES_CACHE and (now - _BULK_QUOTES_TS) < BULK_QUOTE_TTL_SECONDS:
        return _BULK_QUOTES_CACHE

    with _BULK_LOCK:
        now = time.time()
        if not force_refresh and _BULK_QUOTES_CACHE and (now - _BULK_QUOTES_TS) < BULK_QUOTE_TTL_SECONDS:
            return _BULK_QUOTES_CACHE

        parsed: Dict[str, Dict[str, float]] = {}

        # 1. Primary attempt: https://dps.psx.com.pk/market-watch (verify=False)
        try:
            mw_headers = {
                **DEFAULT_BROWSER_HEADERS,
                "Accept": "text/html, */*; q=0.01",
                "Referer": "https://dps.psx.com.pk/",
                "X-Requested-With": "XMLHttpRequest",
            }
            resp = requests.get(
                "https://dps.psx.com.pk/market-watch",
                headers=mw_headers,
                verify=False,
                timeout=min(timeout, 6),
            )
            if resp.status_code == 200 and resp.text:
                parsed = _parse_psx_bulk_table(resp.text)
        except Exception:
            parsed = {}

        # 2. Automatic SSR single-page fallback: https://dps.psx.com.pk/screener (verify=False)
        if not parsed:
            try:
                resp_scr = requests.get(
                    "https://dps.psx.com.pk/screener",
                    headers=DEFAULT_BROWSER_HEADERS,
                    verify=False,
                    timeout=timeout,
                )
                if resp_scr.status_code == 200 and resp_scr.text:
                    parsed = _parse_psx_bulk_table(resp_scr.text)
            except Exception as e:
                print(f"⚠️ [PSX Bulk Scraper] Bulk quote fetch failed: {e}", flush=True)

        if parsed:
            _BULK_QUOTES_CACHE = parsed
            _BULK_QUOTES_TS = time.time()
            return _BULK_QUOTES_CACHE

        # Return stale cache if available rather than empty dict during transient network blips
        return _BULK_QUOTES_CACHE


# Backwards-compatible alias
fetch_bulk_psx_quotes = fetch_all_psx_prices_bulk


async def fetch_bulk_psx_quotes_async(force_refresh: bool = False, timeout: int = 12) -> Dict[str, Dict[str, float]]:
    """
    Async bulk PSX quote fetcher using aiohttp with ssl=False (Ubuntu ARM64 safe) and 120s memory cache.
    """
    global _BULK_QUOTES_CACHE, _BULK_QUOTES_TS

    now = time.time()
    if not force_refresh and _BULK_QUOTES_CACHE and (now - _BULK_QUOTES_TS) < BULK_QUOTE_TTL_SECONDS:
        return _BULK_QUOTES_CACHE

    client_timeout = aiohttp.ClientTimeout(total=timeout)
    connector = aiohttp.TCPConnector(ssl=False)
    parsed: Dict[str, Dict[str, float]] = {}

    async with aiohttp.ClientSession(connector=connector, headers=DEFAULT_BROWSER_HEADERS, timeout=client_timeout) as session:
        # 1. Try /market-watch with ssl=False
        try:
            mw_headers = {
                **DEFAULT_BROWSER_HEADERS,
                "Accept": "text/html, */*; q=0.01",
                "Referer": "https://dps.psx.com.pk/",
                "X-Requested-With": "XMLHttpRequest",
            }
            async with session.get("https://dps.psx.com.pk/market-watch", headers=mw_headers, ssl=False) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    parsed = _parse_psx_bulk_table(html)
        except Exception:
            parsed = {}

        # 2. Fallback to SSR /screener with ssl=False
        if not parsed:
            try:
                async with session.get("https://dps.psx.com.pk/screener", ssl=False) as resp_scr:
                    if resp_scr.status == 200:
                        html = await resp_scr.text()
                        parsed = _parse_psx_bulk_table(html)
            except Exception as e:
                print(f"⚠️ [PSX Async Bulk Scraper] Bulk quote fetch failed: {e}", flush=True)

    if parsed:
        with _BULK_LOCK:
            _BULK_QUOTES_CACHE = parsed
            _BULK_QUOTES_TS = time.time()
        return _BULK_QUOTES_CACHE

    return _BULK_QUOTES_CACHE


def get_bulk_quote_for_ticker(ticker: str, allow_single_fallback: bool = True) -> Tuple[float, float]:
    """
    Returns (price, change_pct) for a PSX ticker using the 120s in-memory bulk market cache first.
    Falls back to single-company HTML scrape (verify=False, .quote__close) only if missing from bulk table.
    """
    sym = str(ticker or "").strip().upper()
    if not sym:
        return 0.0, 0.0

    bulk = fetch_all_psx_prices_bulk(force_refresh=False)
    entry = bulk.get(sym)
    if entry and entry.get("price", 0.0) > 0:
        return float(entry["price"]), float(entry.get("change_pct", 0.0))

    if not allow_single_fallback:
        return 0.0, 0.0

    url = f"https://dps.psx.com.pk/company/{sym}"
    try:
        res = requests.get(url, headers=DEFAULT_BROWSER_HEADERS, verify=False, timeout=10)
        res.raise_for_status()
        price, change_pct = parse_company_quote_html(res.text)
        if price > 0:
            with _BULK_LOCK:
                _BULK_QUOTES_CACHE[sym] = {"price": price, "change": 0.0, "change_pct": change_pct}
        return price, change_pct
    except Exception as e:
        print(f"Error fetching live price for {sym}: {e}", flush=True)
        return 0.0, 0.0


def fetch_live_price(ticker: str) -> float:
    """
    Returns live PSX spot price for `ticker`.
    Uses 120s cached bulk market table first, falling back to /company/<TICKER> (verify=False).
    """
    price, _ = get_bulk_quote_for_ticker(ticker, allow_single_fallback=True)
    return price


def fetch_latest_eps() -> dict:
    """
    Scrapes the PSX financial announcements page.
    Parses the HTML table to extract companies and their 'EPS /(LPS)' values.
    Returns a dictionary of {company_name: updated_eps}.
    """
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
                                eps = float(eps_str_clean)
                                eps_data[company] = eps
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
    """
    Parses /company/<TICKER> SSR HTML using BeautifulSoup (soup.select_one('.quote__close'))
    to extract (spot_price, change_pct).
    """
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
    timeout: int = 15,
) -> Optional[Union[Dict[str, Any], list, str]]:
    """
    Hardened async PSX scraper with ssl=False on all aiohttp calls (fixes SSL certificate
    verification failures on Ubuntu ARM64 Docker).
    When url contains '/company/', parses HTML via BeautifulSoup (soup.select_one('.quote__close'))
    when is_json=True (or returns raw HTML when is_json=False), never calling json.loads(html).
    """
    max_retries = 3
    is_company_url = "/company/" in url
    is_legacy_company_api = "/api/company/" in url
    ticker_tail = url.rstrip("/").split("/")[-1].upper() if is_company_url else ""

    target_url = url
    if "symbols" in url or "KMI30" in url:
        target_url = "https://dps.psx.com.pk/screener"
    elif is_legacy_company_api:
        target_url = f"https://dps.psx.com.pk/company/{ticker_tail}"

    for attempt in range(max_retries):
        try:
            client_timeout = aiohttp.ClientTimeout(total=timeout)
            connector = aiohttp.TCPConnector(ssl=False)

            async with aiohttp.ClientSession(
                connector=connector,
                headers=DEFAULT_BROWSER_HEADERS,
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

                        # Raw HTML requested explicitly (e.g., _scrape_psx_pe or announcements)
                        if not is_json:
                            return html

                        # /company/<TICKER> or /api/company/<TICKER> with is_json=True:
                        # Parse HTML via BeautifulSoup (.quote__close) instead of json.loads(html)
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

                    elif response.status in [403, 404, 500, 502, 503, 504]:
                        raise RuntimeError(f"HTTP {response.status}")

        except Exception as e:
            print(f"⚠️ [PSX Scraper] Error on attempt {attempt + 1} for {target_url}: {e}", flush=True)
            err_str = str(e).lower()
            if "getaddrinfo failed" in err_str or "nodename nor servname provided" in err_str:
                print("🛑 [PSX Scraper] DNS resolution failed. Aborting retries immediately.", flush=True)
                break

        if attempt < max_retries - 1:
            await asyncio.sleep((2 ** attempt) + random.uniform(0.1, 0.8))

    print(f"🛑 [PSX Scraper] All {max_retries} fetch attempts failed for {url}. Triggering fallback cache.", flush=True)
    return None


def fetch_psx_data_robust_sync(url: str, *args, **kwargs):
    """Synchronous bridge for legacy watchdog and ticker valuation callers."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Avoid blocking or requiring nest_asyncio inside FastAPI/uvloop threads: run in worker thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(lambda: asyncio.run(fetch_psx_data_robust(url, *args, **kwargs))).result(timeout=25)
        return loop.run_until_complete(fetch_psx_data_robust(url, *args, **kwargs))
    except RuntimeError:
        return asyncio.run(fetch_psx_data_robust(url, *args, **kwargs))


if __name__ == "__main__":
    t0 = time.perf_counter()
    bulk = fetch_bulk_psx_quotes(force_refresh=True)
    dt = (time.perf_counter() - t0) * 1000
    print(f"Bulk fetched {len(bulk)} PSX tickers in {dt:.0f}ms")
    for k in ["SYS", "FFC", "OGDC", "MARI", "HUBC", "LUCK", "EFERT", "MEBL", "FABL"]:
        print(f"  {k}: {bulk.get(k)}")
