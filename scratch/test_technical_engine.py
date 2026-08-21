import os
import sys

# Load env file to get SUPABASE_DB_URL
from dotenv import load_dotenv
load_dotenv()

from technical_engine import fetch_technical_signals, run_technical_scanner

def main():
    ticker = "SYS"
    print(f"Testing fetch_technical_signals for {ticker}...")
    signals = fetch_technical_signals(ticker)
    if signals:
        print(f"Signals for {ticker}: {signals}")
    else:
        print(f"No signals returned for {ticker}")
        
    print("\nTesting run_technical_scanner...")
    alerts = run_technical_scanner()
    if alerts:
        print("Alerts triggered:")
        for a in alerts:
            print(a)
    else:
        print("No alerts triggered based on current data (which is perfectly normal if conditions aren't met).")

if __name__ == "__main__":
    main()
