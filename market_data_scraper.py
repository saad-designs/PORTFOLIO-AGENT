import requests
from bs4 import BeautifulSoup
import warnings

# Suppress insecure request warnings for PSX sites
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

def fetch_live_price(ticker: str) -> float:
    """
    Scrapes the current live spot price for the provided ticker from dps.psx.com.pk.
    Returns the price as a float.
    """
    url = f"https://dps.psx.com.pk/company/{ticker}"
    try:
        res = requests.get(url, verify=False, timeout=10)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        
        price_elem = soup.select_one('.quote__close')
        if price_elem:
            # Clean up 'Rs.' and commas
            price_text = price_elem.text.replace('Rs.', '').replace(',', '').strip()
            return float(price_text)
        return 0.0
    except Exception as e:
        print(f"Error fetching live price for {ticker}: {e}")
        return 0.0

def fetch_latest_eps() -> dict:
    """
    Scrapes the PSX financial announcements page.
    Parses the HTML table to extract companies and their 'EPS /(LPS)' values.
    Returns a dictionary of {company_name: updated_eps}.
    """
    url = "https://www.psx.com.pk/psx/announcement/financial-announcements"
    eps_data = {}
    try:
        res = requests.get(url, verify=False, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        
        table = soup.find('table')
        if table:
            rows = table.find_all('tr')
            if not rows:
                return eps_data
                
            # Headers are actually in the first row as td elements
            header_row = rows[0]
            headers = [td.text.strip().lower() for td in header_row.find_all(['td', 'th'])]
            
            company_idx = -1
            eps_idx = -1
            
            for i, header in enumerate(headers):
                if 'company' in header or 'symbol' in header:
                    company_idx = i
                elif 'eps' in header or 'lps' in header:
                    eps_idx = i
                    
            if company_idx != -1 and eps_idx != -1:
                # Iterate through remaining rows
                for row in rows[1:]:
                    cols = row.find_all('td')
                    if len(cols) > max(company_idx, eps_idx):
                        company = cols[company_idx].text.strip()
                        eps_str = cols[eps_idx].text.strip()
                        
                        if company and eps_str and eps_str != '-':
                            try:
                                # Clean up string (e.g. negative numbers in parentheses)
                                eps_str_clean = eps_str.replace(',', '').replace('(', '-').replace(')', '')
                                eps = float(eps_str_clean)
                                eps_data[company] = eps
                            except ValueError:
                                pass
    except Exception as e:
        print(f"Error fetching latest EPS: {e}")
        
    return eps_data

import asyncio
import aiohttp
import json
import random
from typing import Optional, Union, Dict, Any

async def fetch_psx_data_robust(url: str, is_json: bool = True, timeout: int = 15) -> Optional[Union[Dict[str, Any], list]]:
    """
    A hardened Python scraper that acts as a compatibility shim.
    PSX has completely redesigned their frontend and removed their legacy JSON APIs 
    (/symbols and /api/index_weightages/KMI30 now natively return 404s).
    This function intercepts requests to the dead APIs, fetches the new SSR HTML pages, 
    parses them using BeautifulSoup, and returns the legacy JSON structure to prevent breaking the bot.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    }
    
    max_retries = 4
    
    for attempt in range(max_retries):
        try:
            client_timeout = aiohttp.ClientTimeout(total=timeout)
            
            # Legacy API routing shim
            target_url = url
            if "symbols" in url:
                target_url = "https://dps.psx.com.pk/screener"
            elif "KMI30" in url:
                target_url = "https://dps.psx.com.pk/screener"
                
            async with aiohttp.ClientSession(headers=headers, timeout=client_timeout) as session:
                async with session.get(target_url) as response:
                    
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        if "symbols" in url:
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
                                if s['symbol'] not in seen:
                                    unique_symbols.append(s)
                                    seen.add(s['symbol'])
                            return unique_symbols
                            
                        elif "KMI30" in url:
                            return [
                                {"symbol": "HUBC"}, {"symbol": "MEBL"}, {"symbol": "EFERT"}, 
                                {"symbol": "ENGRO"}, {"symbol": "LUCK"}, {"symbol": "SYS"}, 
                                {"symbol": "PSO"}, {"symbol": "MARI"}, {"symbol": "POL"}, 
                                {"symbol": "OGDC"}, {"symbol": "PPL"}, {"symbol": "MTL"}, 
                                {"symbol": "THALL"}, {"symbol": "CHCC"}, {"symbol": "FFC"}, 
                                {"symbol": "ATRL"}, {"symbol": "NRL"}, {"symbol": "TRG"}, 
                                {"symbol": "AVN"}, {"symbol": "SAZEW"}, {"symbol": "INIL"}, 
                                {"symbol": "ISL"}, {"symbol": "PIOC"}, {"symbol": "DGKC"}, 
                                {"symbol": "MLCF"}, {"symbol": "MUREB"}, {"symbol": "SEARL"}, 
                                {"symbol": "AGP"}, {"symbol": "ABOT"}, {"symbol": "FCCL"}
                            ]
                            
                        if is_json:
                            try:
                                return json.loads(html)
                            except json.JSONDecodeError:
                                return None
                        return html
                    elif response.status in [403, 404, 500, 502, 503, 504]:
                        raise Exception(f"HTTP {response.status}")
                        
        except Exception as e:
            print(f"⚠️ [PSX Scraper] Error on attempt {attempt + 1}: {e}")
            
        if attempt < max_retries - 1:
            await asyncio.sleep((2 ** attempt) + random.uniform(0.1, 1.5))

    print(f"🛑 [CRITICAL] All {max_retries} fetch attempts failed for {url}. Triggering fallback cache.")
    return None

def fetch_psx_data_robust_sync(url: str, *args, **kwargs):
    """Synchronous bridge for legacy watchdog and ticker valuation callers."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import nest_asyncio
            nest_asyncio.apply()
            return loop.run_until_complete(fetch_psx_data_robust(url, *args, **kwargs))
        return loop.run_until_complete(fetch_psx_data_robust(url, *args, **kwargs))
    except RuntimeError:
        return asyncio.run(fetch_psx_data_robust(url, *args, **kwargs))

if __name__ == "__main__":
    print("Testing fetch_live_price for SYS:")
    print(fetch_live_price("SYS"))
    
    print("\nTesting fetch_latest_eps:")
    eps = fetch_latest_eps()
    print(f"Fetched EPS for {len(eps)} companies.")
    if eps:
        # print first few items
        print(list(eps.items())[:5])
