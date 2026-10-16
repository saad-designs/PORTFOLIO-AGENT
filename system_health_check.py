import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

async def run_full_diagnostic():
    print("==================================================")
    print("   autonomous psx engine - end-to-end diagnostic  ")
    print("==================================================")

    # 1. Test Market Scraper (Price & EPS)
    print("\n[1/5] Testing Market Data Scrapers...")
    try:
        from market_data_scraper import fetch_live_price, fetch_latest_eps
        spot_price = fetch_live_price("SYS")
        print(f"  ✅ Live Price Scraper: SYS = Rs. {spot_price}")
        
        eps_dict = fetch_latest_eps()
        print(f"  ✅ EPS Scraper: Successfully parsed {len(eps_dict)} announcements from PSX.")
    except Exception as e:
        print(f"  ❌ Market Scraper Error: {e}")

    # 2. Test Valuation & Supabase Sync
    print("\n[2/5] Testing Valuation Math & Supabase Sync...")
    try:
        from valuation_engine import sync_eps_and_targets, run_price_sentinel
        sync_eps_and_targets()
        print("  ✅ EPS & Targets Sync: Successfully executed deterministic math.")
        
        sentinel_report = run_price_sentinel()
        print(f"  ✅ Price Sentinel Output:\n     {sentinel_report}")
    except Exception as e:
        print(f"  ❌ Valuation Engine Error: {e}")

    # 3. Test Technical Analysis Engine
    print("\n[3/5] Testing Technical Engine (RSI & Volume Profile)...")
    try:
        from technical_engine import fetch_technical_signals, run_technical_scanner
        signals = fetch_technical_signals("SYS")
        if signals:
            print(f"  ✅ Time-Series Math (SYS.KA): RSI-14 = {signals['rsi']:.2f} | Vol Surge = {signals['volume_surge']:.2f}x | Spot = Rs. {signals['current_price']}")
        else:
            print("  ⚠️ Technical Signals: No data returned (market holiday / insufficient candles).")
            
        alerts = run_technical_scanner()
        print(f"  ✅ Watchlist Technical Scanner: {len(alerts)} alerts triggered.")
    except Exception as e:
        print(f"  ❌ Technical Engine Error: {e}")

    # 4. Test RSS Macro News Engine
    print("\n[4/5] Testing Asynchronous Macro Feed...")
    try:
        from news_scraper import fetch_business_news
        news = await fetch_business_news()
        print(f"  ✅ Async Feedparser: Pulled {len(news)} characters of macro headlines.")
        print(f"     Preview: {news[:180]}...")
    except Exception as e:
        print(f"  ❌ News Scraper Error: {e}")

    # 5. Test Trade Parser NLP Gatekeeper
    print("\n[5/5] Testing Conversational NLP Trade Parser...")
    try:
        from trade_parser import extract_trade_intent

        # Case A: Chat message (must return None)
        chat_msg = "What is the economic outlook for cement stocks this quarter?"
        res_chat = extract_trade_intent(chat_msg)
        assert res_chat is None, "Bypass check failed: casual chat routed as trade."
        print(f"  ✅ Casual Chat Bypass Check: Cleanly returned None.")

        # Case B: Valid trade execution (must return validated TradeExecution object)
        trade_msg = "I bought 25 shares of SYS at 128.50"
        res_trade = extract_trade_intent(trade_msg)
        assert res_trade is not None and res_trade.ticker.upper() == "SYS", "Trade extraction failed."
        print(f"  ✅ Trade Execution Extractor: Parsed {res_trade.action} {res_trade.shares} {res_trade.ticker} @ Rs. {res_trade.price}")
    except Exception as e:
        print(f"  ❌ Trade Parser Error: {e}")

    print("\n==================================================")
    print("         DIAGNOSTIC SUITE COMPLETE                ")
    print("==================================================")

if __name__ == "__main__":
    asyncio.run(run_full_diagnostic())
