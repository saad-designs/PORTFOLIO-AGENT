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

if __name__ == "__main__":
    # Test execution block
    print("Testing fetch_live_price for SYS:")
    print(fetch_live_price("SYS"))
    
    print("\nTesting fetch_latest_eps:")
    eps = fetch_latest_eps()
    print(f"Fetched EPS for {len(eps)} companies.")
    if eps:
        # print first few items
        print(list(eps.items())[:5])
