import asyncio
from playwright.async_api import async_playwright

_PSX_SCREENER_CACHE = {}

def extract_yields_from_nuxt(data, cache):
    """
    Recursively hunts through the nested Nuxt state dictionary for the market metrics.
    """
    if isinstance(data, dict):
        # If we hit a dictionary containing the financial metrics
        if "SYMBOL" in data and "DIVIDEND_YIELD" in data:
            symbol = data.get("SYMBOL", "")
            raw_yield = data.get("DIVIDEND_YIELD", 0.0)
            
            if symbol and isinstance(symbol, str):
                # Normalize the percentage string/float to a raw decimal
                cache[symbol] = round(float(raw_yield) / 100.0, 4) if raw_yield else 0.0
        
        # Keep digging down the JSON tree
        for value in data.values():
            extract_yields_from_nuxt(value, cache)
            
    elif isinstance(data, list):
        for item in data:
            extract_yields_from_nuxt(item, cache)

async def fetch_psx_master_data():
    """
    Bypasses all WAFs and XHR intercepts by ripping the hydration state 
    directly out of the DOM's memory.
    """
    global _PSX_SCREENER_CACHE
    
    if _PSX_SCREENER_CACHE:
        return _PSX_SCREENER_CACHE

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        print("Navigating to PSX Screener...")
        # 'domcontentloaded' is much faster than 'networkidle' because we 
        # don't need to wait for tracking scripts or images to finish downloading.
        await page.goto("https://dps.psx.com.pk/screener", wait_until="domcontentloaded")
        
        print("Extracting Nuxt state...")
        # Evaluate the JS object and serialize it perfectly into a Python dict
        nuxt_state = await page.evaluate("window.__NUXT__")
        
        # Pass the raw dictionary to the recursive hunter
        extract_yields_from_nuxt(nuxt_state, _PSX_SCREENER_CACHE)
        
        await browser.close()
        
    return _PSX_SCREENER_CACHE

# Execution block for testing
if __name__ == "__main__":
    cache = asyncio.run(fetch_psx_master_data())
    print(f"Intercepted {len(cache)} tickers.")
    for ticker in ["HUBC", "EFERT", "MEBL"]:
        print(f"{ticker}: {cache.get(ticker, 0.0) * 100:.2f}%")
