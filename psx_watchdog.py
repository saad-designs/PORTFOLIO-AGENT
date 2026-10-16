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
