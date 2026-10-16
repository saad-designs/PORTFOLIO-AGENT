import os
import json
import requests
from cachetools import cached, TTLCache
import asyncio
import re

# Cache the rate for 12 hours (43200 seconds)
def _load_macro_config() -> dict:
    config_path = os.path.join(os.path.dirname(__file__), "macro_config.json")
    try:
        with open(config_path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"🚨 Failed to load macro_config.json: {e}. Using hardcoded failsafes.")
        return {"sbp_policy_rate": 0.175, "cpi_inflation_rate": 0.118}

@cached(cache=TTLCache(maxsize=1, ttl=43200))
def get_sbp_policy_rate() -> float:
    """
    Reads the SBP policy rate from the static macro config file.
    """
    config = _load_macro_config()
    return float(config.get("sbp_policy_rate", 0.175))

def get_current_inflation_rate() -> float:
    """
    Reads the CPI inflation rate from the static macro config file.
    """
    config = _load_macro_config()
    return float(config.get("cpi_inflation_rate", 0.118))

async def get_usd_pkr_rate() -> float:
    """Fetches live USD/PKR rate from ExchangeRate-API."""
    api_key = os.getenv("EXCHANGE_RATE_API_KEY")
    if not api_key:
        print("⚠️ No ExchangeRate API key found. Using fallback.")
        return 278.50 # Fallback 

    try:
        url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = await response.json()
        return data['conversion_rates']['PKR']
    except Exception as e:
        print(f"🚨 FX API Error: {e}")
        return 278.50 # Fallback

def get_currency_depreciation() -> float:
    """
    Calculates annualized depreciation. 
    In a full build, this compares today's rate against a historical database entry.
    For now, we model a baseline 6% structural depreciation.
    """
    # Placeholder for database delta calculation
    # e.g., (current_rate - rate_1_year_ago) / rate_1_year_ago
    return 0.06 

def get_dynamic_discount_rate() -> float:
    """
    Calculates the mathematically correct Cost of Equity.
    Does NOT double-count depreciation. Adjusts ERP based on real rates.
    """
    base_erp = 0.07 # Standard 7% base risk premium
    
    current_sbp_rate = get_sbp_policy_rate()
    current_inflation_rate = get_current_inflation_rate()
    
    # Calculate Real Interest Rate (SBP Rate - Inflation)
    real_rate = current_sbp_rate - current_inflation_rate
    
    # If real rates are negative, central bank is behind the curve. Increase risk premium.
    macro_penalty = abs(real_rate) if real_rate < 0 else 0.0
    
    dynamic_erp = base_erp + macro_penalty
    discount_rate = current_sbp_rate + dynamic_erp
    
    return round(discount_rate, 4)

def passes_real_yield_hurdle(projected_upside_pct: float) -> bool:
    """
    Strict filter: Expected return must outpace currency devaluation.
    """
    depreciation = get_currency_depreciation()
    return projected_upside_pct > depreciation


