import asyncio
import os
from datetime import datetime
import portfolio_manager as pm
import schedulers

def run_diagnostic():
    print("=" * 60)
    print("🚀 PORTFOLIO AGENT MASTER SYSTEM AUDIT")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 1. Supabase Database & Cash Ledger Check
    print("\n[1/5] Testing PostgreSQL Connection & Cash Ledger...")
    try:
        conn = pm.get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM cash_ledger")
        cash_res = cursor.fetchone()
        cash = float(cash_res[0]) if cash_res else 0.0
        cursor.close()
        conn.close()
        print(f"  ✅ Connection: SUCCESS | Liquid Cash: Rs. {cash:,.2f}")
    except Exception as e:
        print(f"  ❌ Database Connection Failed: {e}")

    # 2. Holdings & Market Data Fetch
    print("\n[2/5] Testing Live PSX Price Feeds & Holdings...")
    try:
        conn = pm.get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT ticker, total_quantity, average_price FROM portfolio WHERE total_quantity > 0")
        holdings = cursor.fetchall()
        cursor.close()
        conn.close()
        print(f"  ✅ Active Positions Tracked: {len(holdings)}")
        for h in holdings:
            ticker = h[0]
            shares = h[1]
            avg_price = h[2]
            current_price = pm.get_live_price(ticker)
            print(f"     • {ticker}: {shares} shs @ Rs. {current_price:,.2f} (Avg: Rs. {avg_price:,.2f})")
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"  ❌ Holdings Fetch Failed: {e}")

    # 3. Quantitative Trigger Engine
    print("\n[3/5] Testing Profit-Taking & Concentration Engine...")
    try:
        alerts = pm.check_profit_and_rebalance_triggers()
        print(f"  ✅ Alert Scan Complete: {len(alerts)} active trigger(s) found.")
        for a in alerts:
            print(f"     ⚠️ Trigger: {a}")
    except Exception as e:
        print(f"  ❌ Trigger Calculation Failed: {e}")

    # 4. Snapshot Table Seed & Verification
    print("\n[4/5] Seeding Baseline Snapshot into 'portfolio_daily_snapshots'...")
    try:
        pm.record_daily_snapshot()
        conn = pm.get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT total_portfolio_value FROM portfolio_daily_snapshots WHERE snapshot_date = CURRENT_DATE")
        res = cursor.fetchone()
        total_val = float(res[0]) if res else 0.0
        cursor.close()
        conn.close()
        print(f"  ✅ Snapshot Committed: Total Val: Rs. {total_val:,.2f}")
    except Exception as e:
        print(f"  ❌ Snapshot Recording Failed: {e}")

    # 5. Email Ingestion HTML Parser Verification
    print("\n[5/5] Checking Email MIME/HTML Parser Import...")
    try:
        import ingestion
        print("  ✅ Ingestion Engine: BeautifulSoup & Parser loaded successfully.")
    except Exception as e:
        print(f"  ❌ Ingestion Module Error: {e}")

    print("\n" + "=" * 60)
    print("🎯 SYSTEM AUDIT COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    run_diagnostic()
