import psycopg2
import psycopg2.extras
import os
import requests
# --- SAFE PSYCOPG2 MODULE-LEVEL IMPORT ---
try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    psycopg2 = None
    PSYCOPG2_AVAILABLE = False
# -----------------------------------------
import functools
import datetime
import pandas as pd
import numpy as np
# yfinance removed: replaced by direct PSX portal scraper (update_ticker_valuations)
import concurrent.futures
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from langchain.tools import tool
from typing import List, Dict
from cachetools import cached, TTLCache

load_dotenv()
DATABASE_URL = os.getenv("SUPABASE_DB_URL")

def get_db_connection(db_url=None):
    if not PSYCOPG2_AVAILABLE or psycopg2 is None:
        raise RuntimeError(
            "CRITICAL: 'psycopg2-binary' is missing from Python 3.12. "
            "Run 'py -3.12 -m pip install psycopg2-binary' in your terminal."
        )
    
    conn_str = db_url if db_url else DATABASE_URL
    return psycopg2.connect(conn_str)

@cached(cache=TTLCache(maxsize=100, ttl=900))
def get_live_price(ticker: str) -> float:
    """Scrapes the live current price from the PSX Data Portal."""
    try:
        url = f"https://dps.psx.com.pk/company/{ticker.upper()}"
        # We use a user-agent to bypass basic security blocks
        headers = {'User-Agent': 'Mozilla/5.0'} 
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # The PSX Data Portal stores the live price inside the 'quote__close' class
        price_div = soup.find('div', class_='quote__close')
        if price_div:
            # Clean the string (e.g., "Rs. 135.50" -> 135.50)
            price_text = price_div.text.replace('Rs.', '').replace(',', '').strip()
            return float(price_text)
        return 0.0
    except Exception as e:
        print(f"Error fetching live price for {ticker}: {e}")
        return 0.0

@cached(cache=TTLCache(maxsize=100, ttl=900))
def get_live_price_and_change(ticker: str) -> tuple[float, float]:
    """Scrapes the live current price and daily percentage change from the PSX Data Portal."""
    try:
        url = f"https://dps.psx.com.pk/company/{ticker.upper()}"
        headers = {'User-Agent': 'Mozilla/5.0'} 
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        price = 0.0
        change_pct = 0.0
        
        price_div = soup.find('div', class_='quote__close')
        if price_div:
            price_text = price_div.text.replace('Rs.', '').replace(',', '').strip()
            price = float(price_text)
            
        change_div = soup.find('div', class_='quote__change')
        if change_div:
            import re
            match = re.search(r'\(([-+]?[0-9]*\.?[0-9]+)%\)', change_div.text)
            if match:
                change_pct = float(match.group(1))
                
        return price, change_pct
    except Exception as e:
        print(f"Error fetching live price and change for {ticker}: {e}")
        return 0.0, 0.0

def log_trade(ticker: str, action: str, quantity: float, price: float, fees: float = 0.0) -> str:
    ticker = ticker.upper()
    action = action.upper()
    
    if action not in ['BUY', 'SELL']:
        return "🚨 Error: Action must be 'BUY' or 'SELL'."
        
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO transactions (ticker, action, quantity, price, fees)
            VALUES (%s, %s, %s, %s, %s)
        """, (ticker, action, quantity, price, fees))
        
        if action == 'BUY':
            # On buy, the cost includes the fees.
            # average_price = (current_invested + new_invested) / total_quantity
            # new_invested = (quantity * price) + fees
            cursor.execute("""
                INSERT INTO portfolio (ticker, total_quantity, average_price)
                VALUES (%s, %s, (%s * %s + %s) / %s)
                ON CONFLICT (ticker) DO UPDATE SET
                average_price = ((portfolio.average_price * portfolio.total_quantity) + (%s * %s + %s)) / (portfolio.total_quantity + %s),
                total_quantity = portfolio.total_quantity + %s,
                last_updated = CURRENT_TIMESTAMP;
            """, (ticker, quantity, quantity, price, fees, quantity, quantity, price, fees, quantity, quantity))
            
        elif action == 'SELL':
            cursor.execute("""
                UPDATE portfolio 
                SET total_quantity = total_quantity - %s,
                    last_updated = CURRENT_TIMESTAMP
                WHERE ticker = %s;
            """, (quantity, ticker))
            
            cursor.execute("DELETE FROM portfolio WHERE total_quantity <= 0 AND ticker = %s", (ticker,))
            
        conn.commit()
        cursor.close()
        conn.close()
        
        return f"✅ **{action} LOGGED:** {quantity} shares of {ticker} at Rs. {price} (Fees: Rs. {fees})."
        
    except Exception as e:
        import traceback
        return f"🚨 Database Error: {e}\n{traceback.format_exc()}"

def set_target(ticker: str, target_price: float, stop_loss: float = 0.0) -> str:
    ticker = ticker.upper()
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Upsert target logic
        cursor.execute("""
            INSERT INTO watchlist_targets (ticker, target_price, stop_loss)
            VALUES (%s, %s, %s)
            ON CONFLICT (ticker) DO UPDATE SET
            target_price = EXCLUDED.target_price,
            stop_loss = EXCLUDED.stop_loss,
            notes = 'Target updated via Discord',
            created_at = CURRENT_TIMESTAMP;
        """, (ticker, target_price, stop_loss))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return f"🎯 **TARGET SET:** {ticker} | Target: Rs. {target_price} | Stop Loss: Rs. {stop_loss}"
    except Exception as e:
        import traceback
        return f"🚨 Database Error: {e}\n{traceback.format_exc()}"

def get_portfolio_summary() -> str:
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # We join the portfolio table with targets to show everything at once
        cursor.execute("""
            SELECT p.ticker, p.total_quantity, p.average_price, w.target_price
            FROM portfolio p
            LEFT JOIN watchlist_targets w ON p.ticker = w.ticker
            WHERE p.total_quantity > 0 
            ORDER BY p.ticker ASC
        """)
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()

        if not rows:
            return "📊 **Your Portfolio is currently empty.**"

        summary = "📊 **Live PSX Portfolio & PnL:**\n```text\n"
        summary += f"{'TICKER':<8} | {'QTY':<6} | {'AVG COST':<8} | {'LIVE':<8} | {'VALUE (Rs)':<10} | {'PnL (Rs)':<10} | {'PnL %'}\n"
        summary += "-" * 75 + "\n"
        
        total_invested = 0.0
        total_current_value = 0.0
        
        for row in rows:
            ticker = row[0]
            qty = float(row[1])
            avg_price = float(row[2])
            target_price = float(row[3]) if row[3] else None
            
            live_price = get_live_price(ticker)
            if live_price == 0.0:
                live_price = avg_price # Fallback to prevent math errors if PSX site is down
                
            invested = qty * avg_price
            current_value = qty * live_price
            pnl_rs = current_value - invested
            pnl_pct = (pnl_rs / invested) * 100 if invested > 0 else 0
            
            total_invested += invested
            total_current_value += current_value
            
            pnl_sign = "+" if pnl_rs >= 0 else ""
            
            summary += f"{ticker:<8} | {qty:<6.0f} | {avg_price:<8.2f} | {live_price:<8.2f} | {current_value:<10,.2f} | {pnl_sign}{pnl_rs:<9,.2f} | {pnl_sign}{pnl_pct:.2f}%\n"
            
            if target_price:
                summary += f"  ↳ Target: {target_price:.2f} | Away: {((target_price - live_price) / live_price) * 100:.1f}%\n"
        
        total_pnl_rs = total_current_value - total_invested
        total_pnl_pct = (total_pnl_rs / total_invested) * 100 if total_invested > 0 else 0
        total_pnl_sign = "+" if total_pnl_rs >= 0 else ""

        summary += "-" * 75 + "\n"
        summary += f"TOTAL DEPLOYED: Rs. {total_invested:,.2f}\n"
        summary += f"CURRENT VALUE:  Rs. {total_current_value:,.2f}\n"
        summary += f"TOTAL PnL:      {total_pnl_sign}Rs. {total_pnl_rs:,.2f} ({total_pnl_sign}{total_pnl_pct:.2f}%)\n```"
        
        return summary

    except Exception as e:
        import traceback
        return f"🚨 Database Error: {e}\n{traceback.format_exc()}"

def get_watchlist_summary() -> str:
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT ticker, target_price FROM watchlist_targets")
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()

        if not rows:
            return "📊 **Your Watchlist is currently empty.**"

        summary = ""
        for row in rows:
            ticker = row[0]
            target_price = float(row[1]) if row[1] else 0.0
            
            live_price = get_live_price(ticker)
            
            if live_price > 0 and target_price > 0:
                # distance can be positive or negative, but user just asked for percentage distance away
                distance = abs(((target_price - live_price) / live_price) * 100)
                summary += f"{ticker} | Live: {live_price:.2f} | Target: {target_price:.2f} | Distance: {distance:.1f}% away\n"
            else:
                summary += f"{ticker} | Live: {live_price:.2f} | Target: {target_price:.2f}\n"
                
        return summary.strip()

    except Exception as e:
        import traceback
        return f"🚨 Database Error: {e}\n{traceback.format_exc()}"

def check_profit_and_rebalance_triggers() -> list[dict]:
    """
    Evaluates the portfolio against quantitative rules and returns actionable alerts.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get total cash (single-tenant assumption for global cash pool)
        cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM cash_ledger")
        cash_res = cursor.fetchone()
        cash_balance = float(cash_res[0]) if cash_res else 0.0
        
        # Get portfolio holdings
        cursor.execute("SELECT ticker, total_quantity, average_price FROM portfolio WHERE total_quantity > 0")
        holdings = cursor.fetchall()
        
        # Get watchlist targets and sector data
        # Ticker-by-ticker valuation data — reads the new columns added to watchlist_targets
        cursor.execute("SELECT ticker, target_price, last_recalculated_date, historical_fair_pe, current_trailing_pe FROM watchlist_targets")
        watchlist_data = {
            row[0]: {
                "target_price": float(row[1]) if row[1] else None,
                "last_recalculated_date": row[2],
                "historical_fair_pe": float(row[3]) if row[3] else None,
                "current_trailing_pe": float(row[4]) if row[4] else None,
            }
            for row in cursor.fetchall()
        }
        
        cursor.close()
        conn.close()
        
        if not holdings:
            return []
            
        # Calculate total portfolio value
        positions = []
        equity_value = 0.0
        
        for row in holdings:
            ticker = row[0]
            shares = float(row[1])
            avg_price = float(row[2])
            current_price = get_live_price(ticker)
            
            if current_price == 0.0:
                current_price = avg_price # fallback
                
            pos_value = shares * current_price
            equity_value += pos_value
            
            positions.append({
                "ticker": ticker,
                "shares": shares,
                "avg_price": avg_price,
                "current_price": current_price,
                "position_value": pos_value
            })
            
        total_value = equity_value + cash_balance
        if total_value <= 0:
            return []
            
        alerts = []
        
        # Identify best candidate for cash buffer trim (e.g., largest gain)
        best_trim_candidate = None
        max_gain_pct = -100.0
        
        for pos in positions:
            ticker = pos["ticker"]
            shares = pos["shares"]
            avg_price = pos["avg_price"]
            current_price = pos["current_price"]
            pos_value = pos["position_value"]
            
            gain_pct = ((current_price - avg_price) / avg_price) * 100 if avg_price > 0 else 0.0
            
            if gain_pct > max_gain_pct:
                max_gain_pct = gain_pct
                best_trim_candidate = pos
                
            # Fundamental Target Valuation Trigger (Replaces static 15% gain)
            w_data = watchlist_data.get(ticker, {})
            target_price = w_data.get("target_price")
            last_recalculated = w_data.get("last_recalculated_date")
            sector_name = w_data.get("sector_name")
            
            if target_price and current_price >= target_price:
                import datetime
                # 90-day Data Rot Check
                if last_recalculated and (datetime.datetime.now() - last_recalculated).days > 90:
                    alerts.append({
                        "type": "stale_target_warning",
                        "ticker": ticker,
                        "current_price": current_price,
                        "target_price": target_price,
                        "message": f"Target price of {target_price} is over 90 days old. Fundamental re-evaluation required."
                    })
                else:
                    shares_to_sell = max(1, round(shares * 0.30))
                    cash_gen = shares_to_sell * current_price
                    alerts.append({
                        "type": "target_valuation_reached",
                        "ticker": ticker,
                        "current_price": current_price,
                        "target_price": target_price,
                        "gain_pct": gain_pct,
                        "shares_to_sell": shares_to_sell,
                        "cash_generated": cash_gen,
                        "new_cash_balance": cash_balance + cash_gen
                    })
                    
            # Macro Overvaluation Trigger (ticker-by-ticker, no sector proxy)
            hist_pe = w_data.get("historical_fair_pe")
            curr_pe = w_data.get("current_trailing_pe")

            if hist_pe and curr_pe and curr_pe > (hist_pe * 1.2):
                shares_to_sell = max(1, round(shares * 0.30))
                cash_gen = shares_to_sell * current_price
                alerts.append({
                    "type": "macro_overvaluation",
                    "ticker": ticker,
                    "current_price": current_price,
                    "current_trailing_pe": curr_pe,
                    "historical_fair_pe": hist_pe,
                    "shares_to_sell": shares_to_sell,
                    "cash_generated": cash_gen,
                    "new_cash_balance": cash_balance + cash_gen
                })
            # Concentration Trigger
            if pos_value / total_value >= 0.40:
                # Target max 35% exposure
                target_value = total_value * 0.35
                excess_value = pos_value - target_value
                shares_to_sell = max(1, round(excess_value / current_price))
                cash_gen = shares_to_sell * current_price
                alerts.append({
                    "type": "concentration_risk",
                    "ticker": ticker,
                    "current_price": current_price,
                    "gain_pct": gain_pct,
                    "shares_to_sell": shares_to_sell,
                    "cash_generated": cash_gen,
                    "new_cash_balance": cash_balance + cash_gen
                })
                
        # Cash Buffer Trigger
        if cash_balance < total_value * 0.05 and best_trim_candidate:
            target_cash = total_value * 0.10
            cash_needed = target_cash - cash_balance
            trim_ticker = best_trim_candidate["ticker"]
            trim_price = best_trim_candidate["current_price"]
            trim_shares = max(1, round(cash_needed / trim_price))
            
            # Ensure we don't sell more than we own
            if trim_shares > best_trim_candidate["shares"]:
                trim_shares = int(best_trim_candidate["shares"])
                
            if trim_shares > 0:
                cash_gen = trim_shares * trim_price
                alerts.append({
                    "type": "low_cash_buffer",
                    "ticker": trim_ticker,
                    "current_price": trim_price,
                    "gain_pct": max_gain_pct,
                    "shares_to_sell": trim_shares,
                    "cash_generated": cash_gen,
                    "new_cash_balance": cash_balance + cash_gen
                })
            
        return alerts
        
    except Exception as e:
        import traceback
        print(f"Error checking profit/rebalance triggers: {e}")
        traceback.print_exc()
        return []

def generate_daily_closing_summary() -> dict:
    """Generates the daily closing summary and liquidity report."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM cash_ledger")
        cash_res = cursor.fetchone()
        liquid_cash = float(cash_res[0]) if cash_res else 0.0
        
        cursor.execute("SELECT ticker, total_quantity, average_price FROM portfolio WHERE total_quantity > 0")
        holdings = cursor.fetchall()
        
        cursor.execute("SELECT daily_pnl, daily_pnl_pct FROM portfolio_daily_snapshots WHERE snapshot_date = CURRENT_DATE")
        pnl_res = cursor.fetchone()
        daily_pnl = float(pnl_res[0]) if pnl_res else 0.0
        daily_pnl_pct = float(pnl_res[1]) if pnl_res else 0.0
        
        cursor.close()
        conn.close()
        
        equity_value = 0.0
        top_gainer = {"ticker": None, "change": -999.9}
        top_loser = {"ticker": None, "change": 999.9}
        
        if not holdings:
            return {
                "total_portfolio_value": liquid_cash,
                "liquid_cash": liquid_cash,
                "equity_allocation_pct": 0.0,
                "top_gainer": ("N/A", 0.0),
                "top_loser": ("N/A", 0.0),
                "daily_pnl": 0.0,
                "daily_pnl_pct": 0.0
            }
            
        for row in holdings:
            ticker = row[0]
            shares = float(row[1])
            
            current_price, change_pct = get_live_price_and_change(ticker)
            if current_price == 0.0:
                current_price = float(row[2])
                change_pct = 0.0
                
            equity_value += shares * current_price
            
            if change_pct > top_gainer["change"]:
                top_gainer = {"ticker": ticker, "change": change_pct}
                
            if change_pct < top_loser["change"]:
                top_loser = {"ticker": ticker, "change": change_pct}
                
        total_value = equity_value + liquid_cash
        allocation_pct = (equity_value / total_value) * 100 if total_value > 0 else 0.0
        
        return {
            "total_portfolio_value": total_value,
            "liquid_cash": liquid_cash,
            "equity_allocation_pct": allocation_pct,
            "top_gainer": (top_gainer["ticker"], top_gainer["change"]),
            "top_loser": (top_loser["ticker"], top_loser["change"]),
            "daily_pnl": daily_pnl,
            "daily_pnl_pct": daily_pnl_pct
        }
    except Exception as e:
        import traceback
        print(f"Error generating daily summary: {e}")
        traceback.print_exc()
        return {}

def record_daily_snapshot():
    """
    Records the daily portfolio snapshot into the database, calculating daily PnL.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Fetch current cash
        cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM cash_ledger")
        cash_res = cursor.fetchone()
        cash_balance = float(cash_res[0]) if cash_res else 0.0
        
        # 2. Fetch current holdings and calculate equity value
        cursor.execute("SELECT ticker, total_quantity, average_price FROM portfolio WHERE total_quantity > 0")
        holdings = cursor.fetchall()
        
        equity_value = 0.0
        for row in holdings:
            ticker = row[0]
            shares = float(row[1])
            current_price = get_live_price(ticker)
            if current_price == 0.0:
                current_price = float(row[2]) # fallback
            equity_value += shares * current_price
            
        total_portfolio_value = equity_value + cash_balance
        
        # 3. Query the previous trading day's total_portfolio_value
        cursor.execute("""
            SELECT total_portfolio_value 
            FROM portfolio_daily_snapshots 
            WHERE snapshot_date < CURRENT_DATE 
            ORDER BY snapshot_date DESC LIMIT 1
        """)
        prev_res = cursor.fetchone()
        prev_value = float(prev_res[0]) if prev_res else total_portfolio_value
        
        # 4. Calculate PnL
        daily_pnl = total_portfolio_value - prev_value
        daily_pnl_pct = (daily_pnl / prev_value) * 100 if prev_value > 0 else 0.0
        
        # 5. Insert or update the snapshot
        cursor.execute("""
            INSERT INTO portfolio_daily_snapshots 
                (snapshot_date, equity_value, cash_balance, total_portfolio_value, daily_pnl, daily_pnl_pct)
            VALUES (CURRENT_DATE, %s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_date) DO UPDATE SET
                equity_value = EXCLUDED.equity_value,
                cash_balance = EXCLUDED.cash_balance,
                total_portfolio_value = EXCLUDED.total_portfolio_value,
                daily_pnl = EXCLUDED.daily_pnl,
                daily_pnl_pct = EXCLUDED.daily_pnl_pct,
                created_at = NOW();
        """, (equity_value, cash_balance, total_portfolio_value, daily_pnl, daily_pnl_pct))
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        import traceback
        print(f"Error recording daily snapshot: {e}")
        traceback.print_exc()

def scan_shariah_market() -> str:
    """Scans predefined Shariah-compliant heavyweights for buying opportunities based on historical fair-value zones."""
    # Define an exclusive subset of the official KMI-30 / PSX-KMI compliant universe
    # This acts as our strict Shariah filter layer.
    kmi30_universe = {
        "SYS": 300.0,    # Systems Limited (Technology)
        "MEBL": 200.0,   # Meezan Bank (Islamic Banking)
        "LUCK": 800.0,   # Lucky Cement (Cement)
        "HUBC": 115.0,   # Hub Power (Power Generation)
        "ENGRO": 300.0,  # Engro Corporation (Fertilizer)
        "POL": 400.0,    # Pakistan Oilfields (Oil & Gas)
        "MTL": 500.0,    # Millat Tractors (Auto)
        "MARI": 2500.0,  # Mari Petroleum (Oil & Gas)
        "HUBC": 115.0    # Hub Power
    }
    
    summary = "🕌 **Strict Shariah-Compliant Market Scan (KMI-30 Universe)**\n```text\n"
    summary += f"{'TICKER':<8} | {'LIVE PRICE':<12} | {'FAIR VALUE':<12} | {'STATUS'}\n"
    summary += "-" * 55 + "\n"
    
    opportunities = 0
    
    for ticker, fair_value in kmi30_universe.items():
        live_price = get_live_price(ticker)
        
        if live_price == 0.0:
            status = "DATA UNAVAILABLE"
        elif live_price <= fair_value:
            distance = ((fair_value - live_price) / fair_value) * 100
            status = f"🟢 BUY ZONE ({distance:.1f}% discount)"
            opportunities += 1
        else:
            distance = ((live_price - fair_value) / fair_value) * 100
            status = f"🔴 OVERVALUED ({distance:.1f}% premium)"
            
        summary += f"{ticker:<8} | {live_price:<12.2f} | {fair_value:<12.2f} | {status}\n"
        
    summary += "```\n"
    
    if opportunities > 0:
        summary += f"✅ Found **{opportunities}** KMI-30 compliant equities trading in their fair-value entry zone."
    else:
        summary += "⚠️ No KMI-30 compliant heavyweights are currently trading below their historical fair-value."
        
    return summary

import json
import os
import psycopg2

def sync_portfolio_tool(holdings_json: str) -> str:
    """Purges the current portfolio table and injects clean, type-safe records."""
    try:
        holdings = json.loads(holdings_json)
        db_url = os.getenv("DATABASE_URL")
        conn = get_db_connection(db_url)
        cursor = conn.cursor()
        
        # Purge old records completely
        cursor.execute("DELETE FROM portfolio;")
        
        for item in holdings:
            ticker = str(item.get("ticker")).strip().upper()
            shares = int(item.get("shares"))          # Enforce integer
            avg_cost = float(item.get("avg_cost"))    # Enforce float
            
            cursor.execute(
                """
                INSERT INTO portfolio (ticker, total_quantity, average_price)
                VALUES (%s, %s, %s)
                """,
                (ticker, shares, avg_cost)
            )
            
        conn.commit()
        cursor.close()
        conn.close()
        return "Database synchronization successful: Old records purged, clean portfolio rows inserted."
    except Exception as e:
        import traceback
        return f"Database sync failed: {str(e)}\n{traceback.format_exc()}"

@cached(cache=TTLCache(maxsize=100, ttl=900))
def get_agent_trade_recommendation(ticker: str) -> dict:
    """This function is obsolete and stubbed. Agent logic lives in agent.py graph."""
    return {}


def _scrape_psx_pe(ticker: str) -> float | None:
    """
    Scrapes the live TTM P/E ratio for a single ticker directly from the PSX Data Portal.
    The page is server-rendered (Nuxt SSR), so BeautifulSoup reads the static HTML payload.
    Target element: <div class="stats_label">P/E Ratio (TTM) **</div>
                    <div class="stats_value">11.21</div>

    FRAGILITY NOTE: This scraper targets CSS class names (stats_label / stats_value).
    A PSX frontend update that renames these classes will cause silent NoneType failures.
    Monitor ingestion logs for the [PSX Scraper STRUCTURAL FAILURE] warning below.
    """
    try:
        url = f"https://dps.psx.com.pk/company/{ticker.upper()}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # Sanity check: verify the DOM still has the expected structure at all.
        # If stats_label divs are absent entirely, the PSX UI has changed.
        all_labels = soup.find_all("div", class_="stats_label")
        if not all_labels:
            print(
                f"[PSX Scraper STRUCTURAL FAILURE] {ticker}: No 'stats_label' divs found. "
                "The PSX portal DOM structure has likely changed. "
                "Re-inspect dps.psx.com.pk/company/{ticker} and update _scrape_psx_pe()."
            )
            return None

        # Locate the stats_label containing "P/E Ratio (TTM)" and grab its sibling value
        for label_div in all_labels:
            if "P/E Ratio (TTM)" in label_div.get_text():
                value_div = label_div.find_next_sibling("div", class_="stats_value")
                if value_div:
                    text = value_div.get_text(strip=True).replace(",", "")
                    try:
                        pe = float(text)
                        if pe > 0:
                            return pe
                    except ValueError:
                        print(f"[PSX Scraper] {ticker}: Could not parse P/E value '{text}' as float.")
                        return None
                else:
                    # Label found but sibling value div missing — partial DOM change
                    print(
                        f"[PSX Scraper STRUCTURAL FAILURE] {ticker}: 'P/E Ratio (TTM)' label found "
                        "but sibling 'stats_value' div is missing. Check for DOM structure change."
                    )
                    return None

        # Label not found at all — P/E may genuinely not be listed (e.g., negative earnings)
        print(f"[PSX Scraper] {ticker}: 'P/E Ratio (TTM)' label not present (earnings may be negative or N/A).")
        return None

    except requests.exceptions.HTTPError as e:
        print(f"[PSX Scraper] HTTP error fetching {ticker}: {e}")
        return None
    except Exception as e:
        print(f"[PSX Scraper] Failed to fetch P/E for {ticker}: {e}")
        return None



def update_ticker_valuations():
    """
    Iterates through all active watchlist tickers and scrapes their live TTM P/E
    directly from the PSX Data Portal, storing results in watchlist_targets.current_trailing_pe.
    Replaces the flawed yfinance sector-proxy approach (update_sector_valuations).
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT ticker FROM watchlist_targets")
        tickers = [row[0] for row in cursor.fetchall()]

        updated = 0
        for ticker in tickers:
            pe = _scrape_psx_pe(ticker)
            if pe is not None:
                cursor.execute("""
                    UPDATE watchlist_targets
                    SET current_trailing_pe = %s
                    WHERE ticker = %s
                """, (pe, ticker))
                updated += 1
                print(f"[update_ticker_valuations] {ticker}: TTM P/E = {pe}")
            else:
                print(f"[update_ticker_valuations] {ticker}: P/E not available on PSX portal")

        conn.commit()
        cursor.close()
        conn.close()
        print(f"[update_ticker_valuations] Done. Updated {updated}/{len(tickers)} tickers.")
    except Exception as e:
        print(f"Error in update_ticker_valuations: {e}")

@cached(cache=TTLCache(maxsize=100, ttl=900))
def get_fundamentals(ticker: str) -> dict:
    """
    Fetches available fundamentals from the PSX Data Portal for a given ticker.
    Returns a dict with at least 'trailingPE' if available.
    """
    try:
        pe = _scrape_psx_pe(ticker)
        result = {}
        if pe is not None:
            result["trailingPE"] = pe
        return result
    except Exception as e:
        print(f"Error fetching fundamentals for {ticker}: {e}")
        return {}

@functools.lru_cache(maxsize=32)
def get_cached_historical_prices(ticker: str, start_date: str):
    """
    Fetches and caches historical PSX price data.
    NOTE: yfinance is no longer used. This function returns an empty Series
    as a stub. Historical price fetching should be implemented via PSX /timeseries
    endpoints or manual data seeding if needed.
    """
    print(f"[get_cached_historical_prices] yfinance removed. No historical data for {ticker}.")
    return pd.Series(dtype=float)

def fetch_supabase_equity_curve() -> pd.DataFrame:
    """
    Reconstructs the daily market-to-market equity curve.
    Guarantees a pd.DataFrame return with a 'portfolio_value' column.
    """
    try:
        # 1. Fetch transactions from Supabase
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT ticker, action, quantity, transaction_date FROM transactions ORDER BY transaction_date ASC")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not rows or len(rows) == 0:
            return pd.DataFrame()

        records = []
        for row in rows:
            records.append({'ticker': row[0], 'action': row[1], 'quantity': float(row[2]), 'transaction_date': row[3]})
        
        tx_df = pd.DataFrame(records)
        tx_df['transaction_date'] = pd.to_datetime(tx_df['transaction_date'])
        
        # Determine date range
        start_date = tx_df['transaction_date'].min().strftime('%Y-%m-%d')
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        
        # 2. Extract unique tickers and build position ledger
        tickers = tx_df['ticker'].unique().tolist()
        psx_tickers = [f"{t}.KA" if not t.endswith(".KA") else t for t in tickers]
        
        # Create a full daily business day range
        date_index = pd.date_range(start=start_date, end=end_date, freq='B')
        if len(date_index) == 0:
            return pd.DataFrame()

        # Build daily shares balance per ticker
        shares_df = pd.DataFrame(0.0, index=date_index, columns=tickers)
        for _, row in tx_df.iterrows():
            date = row['transaction_date'].normalize()
            qty = row['quantity'] if row['action'].lower() == 'buy' else -row['quantity']
            if date in shares_df.index:
                shares_df.loc[date:, row['ticker']] += qty

        # 3. Download Historical Prices safely
        # Note: If single ticker, yfinance formatting differs
        price_data = yf.download(psx_tickers, start=start_date, end=end_date, progress=False)
        
        if isinstance(price_data, (float, int, np.float64)):
            return pd.DataFrame() 

        if price_data.empty:
            return pd.DataFrame()

        # Extract 'Close' prices
        if 'Close' in price_data:
            prices_df = price_data['Close']
        else:
            prices_df = price_data

        # Normalize column names in prices_df to match tickers (strip '.KA')
        if isinstance(prices_df, pd.DataFrame):
            prices_df.columns = [col.replace('.KA', '') for col in prices_df.columns]
        elif isinstance(prices_df, pd.Series):
            # Handle single ticker case where yfinance returns a Series
            prices_df = prices_df.to_frame(name=tickers[0])

        # Align prices to shares index and forward-fill non-trading days
        prices_df = prices_df.reindex(date_index).ffill().bfill()
        
        # Align columns
        common_cols = [col for col in tickers if col in prices_df.columns]
        if not common_cols:
            return pd.DataFrame()

        # 4. Explicit Vectorized Multiplication across columns (axis=1)
        # Multiplying aligned DataFrames and summing row-by-row (horizontal sum)
        daily_stock_values = (shares_df[common_cols] * prices_df[common_cols]).sum(axis=1)

        # 5. Guarantee DataFrame construction
        equity_curve_df = pd.DataFrame({
            'portfolio_value': daily_stock_values
        }, index=date_index)

        # Drop any initial zero-value days before first purchase
        equity_curve_df = equity_curve_df[equity_curve_df['portfolio_value'] > 0]

        return equity_curve_df

    except Exception as e:
        import traceback
        print(f"Error building equity curve: {e}")
        traceback.print_exc()
        return pd.DataFrame()
