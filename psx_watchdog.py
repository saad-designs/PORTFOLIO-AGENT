import requests
from bs4 import BeautifulSoup
import asyncio

def fetch_latest_psx_announcements():
    """
    Scrapes the official PSX Data Portal for the latest corporate disclosures.
    Returns a list of dictionaries containing the ticker, title, timestamp, and link.
    """
    from portfolio_manager import get_checkpoint
    from datetime import datetime
    import dateutil.parser
    
    # The PSX DPS announcements page
    url = "https://dps.psx.com.pk/announcements" 
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
    }
    
    announcements = []
    try:
        from market_data_scraper import fetch_psx_data_robust_sync
        html_data = fetch_psx_data_robust_sync(url, is_json=False)
        if not html_data:
            return []
            
        soup = BeautifulSoup(html_data, 'html.parser')
        
        last_timestamp = get_checkpoint('psx_last_timestamp')
        
        rows = soup.find_all('tr') 
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 4:
                date = cols[0].text.strip()
                time = cols[1].text.strip()
                symbol = cols[2].text.strip()
                title = cols[3].text.strip()
                
                try:
                    # PSX time format typically looks like "04 Sep 2024" and "09:30:00"
                    # But can be parsed safely using dateutil
                    dt_str = f"{date} {time}"
                    dt_obj = dateutil.parser.parse(dt_str)
                    unix_ts = int(dt_obj.timestamp())
                except Exception:
                    continue
                
                # Only return announcements strictly greater than the watermark
                if unix_ts > last_timestamp and symbol:
                    announcements.append({
                        "symbol": symbol,
                        "date": date,
                        "time": time,
                        "timestamp": unix_ts,
                        "title": title
                    })
                        
        return announcements
    except Exception as e:
        print(f"🚨 PSX Watchdog Extraction Failed: {e}")
        return []


def run_psx_announcements_sweep(force_sample: bool = True) -> dict:
    """
    Executes live PSX announcements sweep and returns structured telemetry.
    If force_sample is True, returns latest announcements even if watermark hasn't moved.
    """
    import time
    from portfolio_manager import get_checkpoint, update_checkpoint, publish_agent_event
    t0 = time.perf_counter()

    url = "https://dps.psx.com.pk/announcements"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }
    announcements = []
    try:
        from market_data_scraper import fetch_psx_data_robust_sync
        html_data = fetch_psx_data_robust_sync(url, is_json=False)
        if html_data:
            soup = BeautifulSoup(html_data, 'html.parser')
            last_ts = get_checkpoint('psx_last_timestamp') or 0
            max_seen_ts = last_ts

            for row in soup.find_all('tr'):
                cols = row.find_all('td')
                if len(cols) >= 4:
                    date_txt = cols[0].text.strip()
                    time_txt = cols[1].text.strip()
                    symbol = cols[2].text.strip().upper()
                    title = cols[3].text.strip()
                    if not symbol:
                        continue
                    try:
                        import dateutil.parser
                        dt_obj = dateutil.parser.parse(f"{date_txt} {time_txt}")
                        unix_ts = int(dt_obj.timestamp())
                    except Exception:
                        unix_ts = int(time.time())

                    if unix_ts > max_seen_ts:
                        max_seen_ts = unix_ts

                    if force_sample or unix_ts > last_ts:
                        announcements.append({
                            "symbol": symbol,
                            "date": date_txt,
                            "time": time_txt,
                            "timestamp": unix_ts,
                            "title": title
                        })
                    if len(announcements) >= 10:
                        break

            if max_seen_ts > last_ts:
                update_checkpoint('psx_last_timestamp', max_seen_ts)

    except Exception as e:
        print(f"🚨 [PSX Announcements] Sweep error: {e}")

    lat_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    return {
        "status": "PASS" if announcements else "WARN",
        "latency_ms": lat_ms,
        "count": len(announcements),
        "announcements": announcements[:5],
    }
