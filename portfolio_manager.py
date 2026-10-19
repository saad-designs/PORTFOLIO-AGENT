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
import yfinance as yf
import concurrent.futures
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from langchain.tools import tool
from typing import List, Dict
from cachetools import cached, TTLCache

load_dotenv()
DATABASE_URL = os.getenv("SUPABASE_DB_URL")

try:
    from supabase import create_client, Client
    supabase_url: str = os.getenv("SUPABASE_URL")
    supabase_key: str = os.getenv("SUPABASE_KEY")
    supabase: Client = create_client(supabase_url, supabase_key)
except ImportError:
    supabase = None
    print("Warning: supabase-py not installed.")

import aiohttp
import json

DISCORD_BRIEFS_WEBHOOK = os.getenv("DISCORD_BRIEFS_WEBHOOK") # Add to your .env

async def send_learning_brief(source: str, title: str, summary: str):
    """Fires an educational summary to Discord without triggering trading logic."""
    if not DISCORD_BRIEFS_WEBHOOK:
        return
        
    payload = {
        "embeds": [{
            "title": f"📚 {source} Intelligence: {title}",
            "description": summary,
            "color": 3447003, # Blue for informational
            "footer": {"text": "Agent Ingestion Pipeline"}
        }]
    }
    
    async with aiohttp.ClientSession() as session:
        await session.post(DISCORD_BRIEFS_WEBHOOK, json=payload)

def get_db_connection(db_url=None):
    if not PSYCOPG2_AVAILABLE or psycopg2 is None:
        raise RuntimeError(
            "CRITICAL: 'psycopg2-binary' is missing from Python 3.12. "
            "Run 'py -3.12 -m pip install psycopg2-binary' in your terminal."
        )
    
    conn_str = db_url if db_url else DATABASE_URL
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return psycopg2.connect(conn_str)
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                import time
                time.sleep(1)
            else:
                raise e

def get_whatsapp_checkpoint() -> int:
    """Returns the unix timestamp of the last analyzed WhatsApp message."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT value FROM agent_checkpoints WHERE key = 'whatsapp_last_timestamp'")
        res = cur.fetchone()
        if res:
            return int(res[0])
        return 0
    except Exception as e:
        print(f"[ERROR] fetching whatsapp checkpoint: {e}")
        return 0
    finally:
        cur.close()
        conn.close()

def update_whatsapp_checkpoint(timestamp: int):
    """Advances the WhatsApp watermark forward."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO agent_checkpoints (key, value, updated_at)
            VALUES ('whatsapp_last_timestamp', %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();
        """, (str(timestamp),))
        conn.commit()
    except Exception as e:
        print(f"[ERROR] updating whatsapp checkpoint: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def get_checkpoint(task_key: str) -> int:
    """Fetches the Unix timestamp watermark for a specific ingestion pipeline."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT value FROM agent_checkpoints WHERE key = %s", (task_key,))
        res = cur.fetchone()
        if res:
            raw_val = res[0]
            if isinstance(raw_val, dict):
                if 'value' in raw_val:
                    return int(raw_val['value'])
                return raw_val
            try:
                return int(raw_val)
            except (ValueError, TypeError):
                return raw_val
        return 0
    except Exception as e:
        print(f"[ERROR] fetching checkpoint {task_key}: {e}")
        return 0
    finally:
        cur.close()
        conn.close()

def update_checkpoint(task_key: str, timestamp: int):
    """Advances the watermark forward after successful ingestion."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO agent_checkpoints (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();
        """, (task_key, str(timestamp)))
        conn.commit()
    except Exception as e:
        print(f"[ERROR] updating checkpoint {task_key}: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def get_whatsapp_monitored_chats() -> list[str]:
    """Dynamically fetches active mentor WhatsApp IDs from the database."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT chat_id FROM whatsapp_monitored_chats WHERE is_active = TRUE")
        res = cur.fetchall()
        return [r[0] for r in res]
    except Exception as e:
        print(f"[ERROR] fetching monitored chats: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def sanitize_price(raw_price) -> float:
    """Strips commas and handles raw API strings to prevent PnL hallucinations."""
    if raw_price is None:
        return 0.0
    if isinstance(raw_price, (int, float)):
        return float(raw_price)
    
    clean_string = str(raw_price).replace(',', '').replace('Rs.', '').replace(' ', '').strip()
    try:
        return float(clean_string)
    except ValueError:
        print(f"[ERROR] Could not parse price: {raw_price}")
        return 0.0

@cached(cache=TTLCache(maxsize=100, ttl=900))
def get_live_price(ticker: str) -> float:
    """Scrapes the live current price from the PSX Data Portal."""
    try:
        url = f"https://dps.psx.com.pk/company/{ticker.upper()}"
        from market_data_scraper import fetch_psx_data_robust_sync
        html_data = fetch_psx_data_robust_sync(url, is_json=False)
        if not html_data:
            return 0.0
            
        soup = BeautifulSoup(html_data, 'html.parser')
        
        # The PSX Data Portal stores the live price inside the 'quote__close' class
        price_div = soup.find('div', class_='quote__close')
        if price_div:
            return sanitize_price(price_div.text)
        return 0.0
    except Exception as e:
        print(f"Error fetching live price for {ticker}: {e}")
        return 0.0

@cached(cache=TTLCache(maxsize=100, ttl=900))
def get_live_price_and_change(ticker: str) -> tuple[float, float]:
    """Scrapes the live current price and daily percentage change from the PSX Data Portal."""
    try:
        url = f"https://dps.psx.com.pk/company/{ticker.upper()}"
        from market_data_scraper import fetch_psx_data_robust_sync
        html_data = fetch_psx_data_robust_sync(url, is_json=False)
        if not html_data:
            return 0.0, 0.0
            
        soup = BeautifulSoup(html_data, 'html.parser')
        
        price = 0.0
        change_pct = 0.0
        
        price_div = soup.find('div', class_='quote__close')
        if price_div:
            price = sanitize_price(price_div.text)
            
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
                INSERT INTO portfolio_holdings (ticker, shares, avg_buy_price)
                VALUES (%s, %s, (%s * %s + %s) / %s)
                ON CONFLICT (ticker) DO UPDATE SET
                avg_buy_price = ((portfolio_holdings.avg_buy_price * portfolio_holdings.shares) + (%s * %s + %s)) / (portfolio_holdings.shares + %s),
                shares = portfolio_holdings.shares + %s,
                updated_at = CURRENT_TIMESTAMP;
            """, (ticker, quantity, quantity, price, fees, quantity, quantity, price, fees, quantity, quantity))
            
        elif action == 'SELL':
            cursor.execute("""
                UPDATE portfolio_holdings 
                SET shares = shares - %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE ticker = %s;
            """, (quantity, ticker))
            
            cursor.execute("DELETE FROM portfolio_holdings WHERE shares <= 0 AND ticker = %s", (ticker,))
            
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
            SELECT p.ticker, p.shares as total_quantity, p.avg_buy_price as average_price, w.target_price
            FROM portfolio_holdings p
            LEFT JOIN watchlist_targets w ON p.ticker = w.ticker
            WHERE p.shares > 0 
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

def add_pending_signal(source: str, payload: dict) -> bool:
    """Inserts a pending signal into the Supabase pending_signals table."""
    try:
        import json
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO pending_signals (source, signal_payload)
            VALUES (%s, %s)
            """,
            (source, json.dumps(payload))
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        import traceback
        print(f"Error adding pending signal: {e}\n{traceback.format_exc()}")
        return False


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

import math

def calculate_concentration_trim(ticker: str, current_shares: int, current_price: float, total_portfolio_value: float, max_asset_cap: float = 0.30, dcf_fair_value: float = 0.0) -> dict:
    """
    Calculates the exact whole number of shares to liquidate to bring an asset
    strictly back under the max allowable single-asset portfolio cap, guarded by fundamental value logic.
    """
    if current_price <= 0 or total_portfolio_value <= 0:
        return {"action": "HOLD", "shares_to_sell": 0, "excess_pkr": 0.0, "expected_cash_gen": 0.0}

    current_holding_value = current_shares * current_price
    weight = current_holding_value / total_portfolio_value

    # RULE 1: Only consider concentration risk if the position exceeds an extreme structural limit (e.g., 30%)
    if weight <= max_asset_cap:
        return {
            "action": "HOLD",
            "current_weight": weight,
            "shares_to_sell": 0,
            "excess_pkr": 0.0,
            "expected_cash_gen": 0.0
        }

    # RULE 2: If it does exceed cap, check valuation before selling. 
    # If it's deeply undervalued, we accept the concentration because selling a compounder for tax/friction reasons is illogical.
    margin_of_safety = 0.0
    if dcf_fair_value > 0:
        margin_of_safety = (dcf_fair_value - current_price) / dcf_fair_value
        
    if margin_of_safety > 0.15:
        return {
            "action": "HOLD",
            "current_weight": weight,
            "shares_to_sell": 0,
            "excess_pkr": 0.0,
            "expected_cash_gen": 0.0,
            "override_msg": f"Position exceeds weight limit ({weight*100:.1f}%), but maintains a strong DCF margin of safety ({margin_of_safety*100:.1f}%). Overriding concentration trim to protect compounding."
        }

    # Calculate target shares and exact shares to liquidate
    max_allowed_value = total_portfolio_value * max_asset_cap
    target_shares_to_hold = math.floor(max_allowed_value / current_price)
    shares_to_sell = current_shares - target_shares_to_hold
    expected_cash_gen = shares_to_sell * current_price
    new_weight = (target_shares_to_hold * current_price) / total_portfolio_value

    return {
        "action": "TRIM",
        "current_shares": current_shares,
        "target_shares": target_shares_to_hold,
        "shares_to_sell": shares_to_sell,
        "expected_cash_gen": expected_cash_gen,
        "current_weight": weight,
        "projected_weight": new_weight
    }

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
        cursor.execute("SELECT ticker, shares as total_quantity, avg_buy_price as average_price FROM portfolio_holdings WHERE shares > 0")
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
                        "current_shares": shares,
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
                        "current_shares": shares,
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
                    "current_shares": shares,
                    "current_trailing_pe": curr_pe,
                    "historical_fair_pe": hist_pe,
                    "shares_to_sell": shares_to_sell,
                    "cash_generated": cash_gen,
                    "new_cash_balance": cash_balance + cash_gen
                })
            # Concentration Trigger
            dcf_fair = w_data.get("target_price") or 0.0
            trim_data = calculate_concentration_trim(ticker, shares, current_price, total_value, max_asset_cap=0.30, dcf_fair_value=dcf_fair)
            
            if trim_data["action"] == "TRIM" and trim_data["shares_to_sell"] > 0:
                shares_to_sell = trim_data["shares_to_sell"]
                cash_gen = trim_data["expected_cash_gen"]
                alerts.append({
                    "type": "concentration_risk",
                    "ticker": ticker,
                    "current_price": current_price,
                    "current_shares": shares,
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
        
        # 1. Fetch Liquid Cash (Assuming withdrawals/buys are logged as negative amounts)
        cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM cash_ledger")
        cash_res = cursor.fetchone()
        liquid_cash = float(cash_res[0]) if cash_res else 0.0
        
        # 2. FIXED: Aligned schema columns to match your actual database structure
        cursor.execute("SELECT ticker, shares, avg_buy_price FROM portfolio_holdings WHERE shares > 0")
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
                "daily_pnl_pct": 0.0,
                "current_holdings": []
            }
            
        current_holdings_list = []
        for row in holdings:
            ticker = row[0]
            shares = float(row[1])
            avg_price = float(row[2])
            
            current_price, change_pct = get_live_price_and_change(ticker)
            if current_price == 0.0:
                current_price = avg_price  # Fallback to avg buy price if PSX API fails
                change_pct = 0.0
                
            market_value = shares * current_price
            equity_value += market_value
            current_holdings_list.append({"ticker": ticker, "market_value": market_value})
            
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
            "daily_pnl_pct": daily_pnl_pct,
            "current_holdings": current_holdings_list
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
        cursor.execute("SELECT ticker, shares as total_quantity, avg_buy_price as average_price FROM portfolio_holdings WHERE shares > 0")
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
        cursor.execute("DELETE FROM portfolio_holdings;")
        
        for item in holdings:
            ticker = str(item.get("ticker")).strip().upper()
            shares = int(item.get("shares"))          # Enforce integer
            avg_cost = float(item.get("avg_cost"))    # Enforce float
            
            cursor.execute(
                """
                INSERT INTO portfolio_holdings (ticker, shares, avg_buy_price)
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
        from market_data_scraper import fetch_psx_data_robust_sync
        html_data = fetch_psx_data_robust_sync(url, is_json=False)
        if not html_data:
            return None
            
        soup = BeautifulSoup(html_data, "html.parser")

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

import requests
import re

def fetch_dividend_yield(ticker: str) -> float:
    """
    Fetches the dividend yield securely via Yahoo Finance API, bypassing 
    regional WAFs completely. 
    """
    try:
        # Append .KA for Karachi Stock Exchange indexing
        yahoo_ticker = f"{ticker}.KA"
        stock = yf.Ticker(yahoo_ticker)
        
        # yfinance sometimes uses trailingAnnualDividendYield instead of dividendYield
        div_yield = stock.info.get("dividendYield")
        if div_yield is None:
            div_yield = stock.info.get("trailingAnnualDividendYield", 0.0)
        
        # Guard against Yahoo returning None for stocks with no yield
        if div_yield is None:
            return 0.0
            
        return round(float(div_yield), 4)

    except Exception as e:
        print(f"[ERROR] yfinance routing failed for {ticker}: {e}")
        return 0.0

@cached(cache=TTLCache(maxsize=100, ttl=900))
def get_fundamentals(ticker: str) -> dict:
    """
    Fetches available fundamentals from the PSX Data Portal JSON API (Option A)
    with a fallback to scraping Sarmaaya.pk (Option B).
    Returns a dict with 'eps', 'roic', and 'trailingPE'.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Query only the columns present in the Supabase schema
        cur.execute("SELECT dividend_yield, eps, roic FROM fundamentals WHERE ticker = %s", (ticker,))
        row = cur.fetchone()
        conn.close()
        
        if row and row[0] is not None:
            return {
                "dividend_yield": float(row[0]) if row[0] is not None else 0.0,
                "trailingPE": 0.0,  # Not stored in table schema
                "eps": float(row[1]) if row[1] is not None else 0.0,
                "roic": float(row[2]) if row[2] is not None else 0.0
            }
    except Exception as e:
        print(f"[Fundamentals] DB check failed for {ticker}: {e}")
        
    # 2. Only fall back to APIs if the database has nothing
    print(f"[Fundamentals] Falling back to APIs/Yahoo Finance for {ticker}...")
    import re
    result = {'eps': 0.0, 'roic': 0.0, 'trailingPE': None, 'dividend_yield': 0.0}
    
    # Also fetch trailing PE if possible
    pe = _scrape_psx_pe(ticker)
    if pe is not None:
        result["trailingPE"] = pe

    # Option A: Fetch EPS, ROIC, and Dividend Yield from the PSX DPS internal JSON API endpoint
    try:
        url = f"https://dps.psx.com.pk/api/company/{ticker.upper()}"
        from market_data_scraper import fetch_psx_data_robust_sync
        data = fetch_psx_data_robust_sync(url, is_json=True)
        if data:
            if 'eps' in data:
                result['eps'] = float(data['eps'])
            if 'roic' in data:
                result['roic'] = float(data['roic'])
            if 'dividendYield' in data:
                val = data['dividendYield']
                if val:
                    val_str = str(val).strip()
                    is_percentage_str = '%' in val_str
                    val_clean = val_str.replace('%', '').strip()
                    try:
                        parsed_val = float(val_clean)
                        # If it has a '%' or is > 1.0 (e.g. 16.5), we divide by 100 to get a true decimal.
                        # Some APIs return 0.165 directly. We assume values > 1.0 are definitely not already decimals.
                        if is_percentage_str or parsed_val > 1.0:
                            result['dividend_yield'] = parsed_val / 100.0
                        else:
                            # If it's something like 0.165 (already a decimal), we leave it.
                            # If it's a very low yield like 0.5 meaning 0.5%, and has no %, it'll be treated as 50% (0.5),
                            # but Pakistan yields are rarely that low, and if they are, the % sign is usually present.
                            result['dividend_yield'] = parsed_val
                    except ValueError:
                        pass
            return result
    except Exception as e:
        print(f"[Fundamentals] PSX API failed for {ticker}: {e}")

    # Option B: Fallback to PSX Master Screener for Dividend Yield
    try:
        print(f"[Fundamentals] Falling back to Yahoo Finance for {ticker}...")
        result['dividend_yield'] = fetch_dividend_yield(ticker)
        # EPS and ROIC scrapers are deprecated due to Next.js SSR architecture
        return result
    except Exception as e:
        print(f"[Fundamentals] PSX Screener API fallback failed for {ticker}: {e}")
        return result

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


# ====================================================================
# PSX INVESTMENT, CAPITAL & 3-STAGE DIVIDEND ACCOUNTING ENGINE
# ====================================================================

def save_desk_manager_thought(thought: str):
    """
    Persists the Desk Manager's synthesis / macro thought to Supabase (agent_checkpoints)
    and mirrors to local disk (memory_cache/proactive_thought.txt).
    Prevents ephemeral cloud disk wipes from losing the latest memorandum.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO agent_checkpoints (key, value, updated_at)
            VALUES ('proactive_thought', %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();
        """, (thought,))
        conn.commit()
        cur.close()
        conn.close()
        print("💾 [Desk Manager] Saved memorandum to Supabase agent_checkpoints.")
    except Exception as e:
        print(f"⚠️ [Desk Manager] Error saving thought to Supabase: {e}")

    try:
        os.makedirs("memory_cache", exist_ok=True)
        with open("memory_cache/proactive_thought.txt", "w", encoding="utf-8") as f:
            f.write(f"Proactive thought generated at {datetime.datetime.now()}:\n{thought}")
    except Exception as f_err:
        print(f"⚠️ [Desk Manager] Disk cache mirror write failed: {f_err}")

def get_desk_manager_thought() -> str:
    """Retrieves the latest Desk Manager memorandum from Supabase or disk cache."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT value FROM agent_checkpoints WHERE key = 'proactive_thought'")
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row and row[0]:
            return str(row[0])
    except Exception as e:
        print(f"⚠️ [Desk Manager] Error reading thought from Supabase: {e}")

    cache_path = "memory_cache/proactive_thought.txt"
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            pass
    return "Desk Manager synthesis standing by. Run manual sweep to generate initial thesis."

def record_capital_transaction(tx_type: str, amount: float, reference_note: str = None) -> dict:
    """
    Records cash moving in/out of KTrade & Bank in capital_ledger:
    - DEPOSIT_TO_BROKER (Fresh capital deposited to KTrade)
    - WITHDRAWAL_FROM_BROKER (Cash withdrawn from KTrade to personal bank)
    - DIVIDEND_BANK_TO_BROKER (Reinvested dividend moved from bank account to KTrade)
    - FEE_ADJUSTMENT (Broker fee / withholding / adjustment)
    Also keeps legacy cash_ledger synchronized for uninterrupted automated execution.
    """
    import uuid
    valid_types = {'DEPOSIT_TO_BROKER', 'WITHDRAWAL_FROM_BROKER', 'DIVIDEND_BANK_TO_BROKER', 'FEE_ADJUSTMENT'}
    clean_type = tx_type.strip().upper()
    if clean_type not in valid_types:
        raise ValueError(f"Invalid tx_type: {tx_type}. Must be one of {valid_types}")

    amt = float(amount)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO capital_ledger (tx_type, amount, reference_note, tx_date)
            VALUES (%s, %s, %s, NOW())
            RETURNING id, tx_type, amount, reference_note, tx_date;
        """, (clean_type, amt, reference_note))
        row = cur.fetchone()
        
        # Mirror to cash_ledger
        cash_delta = amt if clean_type in {'DEPOSIT_TO_BROKER', 'DIVIDEND_BANK_TO_BROKER'} else (-amt if clean_type == 'WITHDRAWAL_FROM_BROKER' else amt)
        user_id = os.getenv("DISCORD_USER_ID", "1536522727287164979")
        cur.execute("""
            INSERT INTO cash_ledger (transaction_id, discord_user_id, transaction_type, amount, created_at)
            VALUES (%s, %s, %s, %s, NOW())
        """, (str(uuid.uuid4()), user_id, clean_type, cash_delta))
        
        conn.commit()
        return {
            "id": row[0],
            "tx_type": row[1],
            "amount": float(row[2]),
            "reference_note": row[3],
            "tx_date": row[4].isoformat() if row[4] else None
        }
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def record_dividend(
    ticker: str,
    shares_held: float,
    dps_gross: float,
    tax_rate_pct: float = 15.00,
    ex_date: str = None,
    payment_date: str = None,
    status: str = 'ENTITLED',
    notes: str = None
) -> dict:
    """
    Tracks the exact PSX Dividend Lifecycle:
    ANNOUNCED -> ENTITLED (Pending Payout) -> CREDITED_TO_BANK (15% WHT deducted) -> SENT_TO_KTRADE (Reinvested)
    """
    from utils import normalize_ticker
    clean_ticker = normalize_ticker(ticker)
    shares = float(shares_held)
    dps = float(dps_gross)
    tax_rate = float(tax_rate_pct) if tax_rate_pct is not None else 15.00
    
    gross_amount = round(shares * dps, 2)
    tax_withheld = round(gross_amount * (tax_rate / 100.0), 2)
    net_amount = round(gross_amount - tax_withheld, 2)
    clean_status = status.strip().upper() if status else 'ENTITLED'

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO dividend_ledger (
                ticker, shares_held, dps_gross, gross_amount, tax_rate_pct,
                tax_withheld, net_amount, ex_date, payment_date, status, notes, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            RETURNING id, ticker, shares_held, dps_gross, gross_amount, tax_rate_pct,
                      tax_withheld, net_amount, ex_date, payment_date, status, notes, created_at;
        """, (
            clean_ticker, shares, dps, gross_amount, tax_rate,
            tax_withheld, net_amount, ex_date, payment_date, clean_status, notes
        ))
        row = cur.fetchone()
        conn.commit()
        
        # If created directly with status 'SENT_TO_KTRADE', credit capital ledger immediately
        if clean_status == 'SENT_TO_KTRADE':
            record_capital_transaction(
                tx_type='DIVIDEND_BANK_TO_BROKER',
                amount=net_amount,
                reference_note=f"Reinvested Dividend #{row[0]}: {clean_ticker} ({shares} shs @ Rs. {dps})"
            )

        return {
            "id": row[0],
            "ticker": row[1],
            "shares_held": float(row[2]),
            "dps_gross": float(row[3]),
            "gross_amount": float(row[4]),
            "tax_rate_pct": float(row[5]),
            "tax_withheld": float(row[6]),
            "net_amount": float(row[7]),
            "ex_date": str(row[8]) if row[8] else None,
            "payment_date": str(row[9]) if row[9] else None,
            "status": row[10],
            "notes": row[11],
            "created_at": row[12].isoformat() if row[12] else None
        }
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def update_dividend_status(dividend_id: int, new_status: str) -> dict:
    """
    Transitions dividend across lifecycle states.
    CRITICAL REALITY LOGIC:
    When new_status is 'SENT_TO_KTRADE', automatically inserts a corresponding
    'DIVIDEND_BANK_TO_BROKER' entry into capital_ledger and increments available KTrade cash!
    """
    valid_statuses = {'ANNOUNCED', 'ENTITLED', 'CREDITED_TO_BANK', 'SENT_TO_KTRADE'}
    clean_status = new_status.strip().upper()
    if clean_status not in valid_statuses:
        raise ValueError(f"Invalid dividend status: {new_status}. Must be one of {valid_statuses}")

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, ticker, shares_held, dps_gross, net_amount, status
            FROM dividend_ledger
            WHERE id = %s
        """, (dividend_id,))
        existing = cur.fetchone()
        if not existing:
            raise ValueError(f"Dividend #{dividend_id} not found.")

        old_status = existing[5]
        ticker = existing[1]
        shares = float(existing[2])
        dps = float(existing[3])
        net_amount = float(existing[4])

        cur.execute("""
            UPDATE dividend_ledger
            SET status = %s
            WHERE id = %s
            RETURNING id, ticker, shares_held, dps_gross, gross_amount, tax_rate_pct,
                      tax_withheld, net_amount, ex_date, payment_date, status, notes, created_at;
        """, (clean_status, dividend_id))
        row = cur.fetchone()
        conn.commit()

        # If transitioning to SENT_TO_KTRADE and not already transferred, inject into capital_ledger!
        if clean_status == 'SENT_TO_KTRADE' and old_status != 'SENT_TO_KTRADE':
            cur.execute("""
                SELECT id FROM capital_ledger 
                WHERE tx_type = 'DIVIDEND_BANK_TO_BROKER' AND reference_note LIKE %s
            """, (f"%Dividend #{dividend_id}:%",))
            cap_exists = cur.fetchone()
            if not cap_exists:
                record_capital_transaction(
                    tx_type='DIVIDEND_BANK_TO_BROKER',
                    amount=net_amount,
                    reference_note=f"Reinvested Dividend #{dividend_id}: {ticker} ({shares} shs @ Rs. {dps})"
                )
                print(f"💰 [Dividend Ledger] Automatically transferred Net Dividend Rs. {net_amount} to KTrade Available Cash.")

        return {
            "id": row[0],
            "ticker": row[1],
            "shares_held": float(row[2]),
            "dps_gross": float(row[3]),
            "gross_amount": float(row[4]),
            "tax_rate_pct": float(row[5]),
            "tax_withheld": float(row[6]),
            "net_amount": float(row[7]),
            "ex_date": str(row[8]) if row[8] else None,
            "payment_date": str(row[9]) if row[9] else None,
            "status": row[10],
            "notes": row[11],
            "created_at": row[12].isoformat() if row[12] else None
        }
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def record_trade(
    ticker: str,
    side: str,
    quantity: float,
    price: float,
    fees_and_tax: float = 0.00,
    notes: str = None
) -> dict:
    """
    Logs BUY / SELL trade in trade_history, updates portfolio_holdings weighted average cost,
    and reflects trade cash flow in cash_ledger.
    """
    from utils import normalize_ticker
    clean_ticker = normalize_ticker(ticker)
    clean_side = side.strip().upper()
    if clean_side not in {'BUY', 'SELL'}:
        raise ValueError("Trade side must be 'BUY' or 'SELL'.")

    qty = float(quantity)
    prc = float(price)
    fees = float(fees_and_tax) if fees_and_tax is not None else 0.00
    gross = round(qty * prc, 2)
    net_total = round(gross + fees if clean_side == 'BUY' else gross - fees, 2)

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # 1. Insert into trade_history
        cur.execute("""
            INSERT INTO trade_history (ticker, side, quantity, price, fees_and_tax, net_total, executed_at, notes)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s)
            RETURNING id, ticker, side, quantity, price, fees_and_tax, net_total, executed_at, notes;
        """, (clean_ticker, clean_side, qty, prc, fees, net_total, notes))
        t_row = cur.fetchone()

        # 2. Update portfolio_holdings
        cur.execute("""
            SELECT id, shares, avg_buy_price FROM portfolio_holdings WHERE ticker = %s
        """, (clean_ticker,))
        existing_h = cur.fetchone()

        if clean_side == 'BUY':
            if existing_h:
                old_shares = float(existing_h[1])
                old_avg = float(existing_h[2])
                new_shares = old_shares + qty
                new_avg = round(((old_shares * old_avg) + (qty * prc)) / new_shares, 4) if new_shares > 0 else prc
                cur.execute("""
                    UPDATE portfolio_holdings
                    SET shares = %s, avg_buy_price = %s, updated_at = NOW()
                    WHERE id = %s
                """, (new_shares, new_avg, existing_h[0]))
            else:
                cur.execute("""
                    INSERT INTO portfolio_holdings (ticker, shares, avg_buy_price, updated_at)
                    VALUES (%s, %s, %s, NOW())
                """, (clean_ticker, qty, prc))
            
            # Cash flow deduction
            record_capital_transaction('FEE_ADJUSTMENT', -net_total, f"Trade Execution: BUY {qty} {clean_ticker} @ {prc}")

        elif clean_side == 'SELL':
            if existing_h:
                old_shares = float(existing_h[1])
                new_shares = max(0.0, old_shares - qty)
                cur.execute("""
                    UPDATE portfolio_holdings
                    SET shares = %s, updated_at = NOW()
                    WHERE id = %s
                """, (new_shares, existing_h[0]))
            
            # Cash flow credit
            record_capital_transaction('FEE_ADJUSTMENT', net_total, f"Trade Execution: SELL {qty} {clean_ticker} @ {prc}")

        conn.commit()
        return {
            "id": t_row[0],
            "ticker": t_row[1],
            "side": t_row[2],
            "quantity": float(t_row[3]),
            "price": float(t_row[4]),
            "fees_and_tax": float(t_row[5]),
            "net_total": float(t_row[6]),
            "executed_at": t_row[7].isoformat() if t_row[7] else None,
            "notes": t_row[8]
        }
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_complete_financial_summary() -> dict:
    """
    Computes complete PSX investment summary, capital accounting, and live mark-to-market valuations.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # 1. Capital Ledger Summary
        cur.execute("""
            SELECT 
                COALESCE(SUM(CASE WHEN tx_type = 'DEPOSIT_TO_BROKER' THEN amount WHEN tx_type = 'WITHDRAWAL_FROM_BROKER' THEN -amount ELSE 0 END), 0) as fresh_capital,
                COALESCE(SUM(CASE WHEN tx_type = 'DIVIDEND_BANK_TO_BROKER' THEN amount ELSE 0 END), 0) as reinvested_divs
            FROM capital_ledger;
        """)
        cap_row = cur.fetchone()
        fresh_capital = float(cap_row[0]) if cap_row else 0.0
        reinvested_divs = float(cap_row[1]) if cap_row else 0.0

        # 2. Available Cash Balance
        cur.execute("SELECT COALESCE(SUM(amount), 0) FROM cash_ledger;")
        cash_row = cur.fetchone()
        available_cash = float(cash_row[0]) if cash_row else 0.0

        # 3. Dividend Ledger Summary
        cur.execute("""
            SELECT 
                COALESCE(SUM(gross_amount), 0) as total_gross,
                COALESCE(SUM(tax_withheld), 0) as total_tax,
                COALESCE(SUM(CASE WHEN status = 'CREDITED_TO_BANK' THEN net_amount ELSE 0 END), 0) as in_bank,
                COALESCE(SUM(CASE WHEN status = 'SENT_TO_KTRADE' THEN net_amount ELSE 0 END), 0) as reinvested,
                COALESCE(SUM(CASE WHEN status IN ('ANNOUNCED', 'ENTITLED') THEN net_amount ELSE 0 END), 0) as pending
            FROM dividend_ledger;
        """)
        div_row = cur.fetchone()
        total_gross_div = float(div_row[0]) if div_row else 0.0
        total_tax_wht = float(div_row[1]) if div_row else 0.0
        net_div_bank = float(div_row[2]) if div_row else 0.0
        net_div_reinvested = float(div_row[3]) if div_row else 0.0
        pending_div = float(div_row[4]) if div_row else 0.0

        # 4. Live Portfolio Holdings
        cur.execute("""
            SELECT ticker, shares, avg_buy_price, dividend_yield, research_sentiment
            FROM portfolio_holdings
            WHERE shares > 0
            ORDER BY (shares * avg_buy_price) DESC;
        """)
        holdings_rows = cur.fetchall()

        # Load KMI-30 Shariah status
        kmi_set = set()
        kmi_cache = os.path.join(os.path.dirname(__file__), "kmi30_cache.json")
        if os.path.exists(kmi_cache):
            try:
                with open(kmi_cache, "r", encoding="utf-8") as f:
                    kmi_data = json.load(f)
                    if isinstance(kmi_data, list):
                        kmi_set = set(kmi_data)
                    elif isinstance(kmi_data, dict):
                        kmi_set = set(kmi_data.get("tickers", []))
            except Exception:
                pass
        if not kmi_set:
            kmi_set = {"SYS", "HUBC", "LUCK", "FFC", "EFERT", "MEBL", "MARI", "OGDC", "PPL", "POL"}

        portfolio_market_val = 0.0
        portfolio_cost = 0.0
        holdings_detail = []

        for row in holdings_rows:
            tick = row[0]
            shares = float(row[1])
            avg_price = float(row[2])
            div_yield = float(row[3]) if row[3] else None
            sentiment = row[4] or "NEUTRAL"
            
            curr_price, change_pct = get_live_price_and_change(tick)
            if curr_price <= 0:
                curr_price = avg_price

            mv = round(shares * curr_price, 2)
            cb = round(shares * avg_price, 2)
            pnl = round(mv - cb, 2)
            pnl_pct = round((pnl / cb * 100), 2) if cb > 0 else 0.0

            portfolio_market_val += mv
            portfolio_cost += cb

            holdings_detail.append({
                "ticker": tick,
                "shares": shares,
                "avg_price": avg_price,
                "current_price": curr_price,
                "change_pct": change_pct,
                "market_value": mv,
                "cost_basis": cb,
                "unrealized_pnl": pnl,
                "unrealized_pnl_pct": pnl_pct,
                "dividend_yield": div_yield,
                "sentiment": sentiment,
                "is_kmi30": tick.upper() in kmi_set
            })

        for h in holdings_detail:
            h["weight_pct"] = round((h["market_value"] / portfolio_market_val * 100), 2) if portfolio_market_val > 0 else 0.0

        unrealized_pnl = round(portfolio_market_val - portfolio_cost, 2)
        unrealized_pnl_pct = round((unrealized_pnl / portfolio_cost * 100), 2) if portfolio_cost > 0 else 0.0
        net_worth = round(portfolio_market_val + available_cash, 2)
        true_total_return = round((net_worth + net_div_bank) - fresh_capital, 2)
        true_roi_pct = round((true_total_return / fresh_capital * 100), 2) if fresh_capital > 0 else 0.0

        return {
            "financial_kpis": {
                "total_net_worth": net_worth,
                "fresh_capital_invested": fresh_capital,
                "ktrade_available_cash": round(available_cash, 2),
                "portfolio_market_value": round(portfolio_market_val, 2),
                "portfolio_cost_basis": round(portfolio_cost, 2),
                "unrealized_pnl": unrealized_pnl,
                "unrealized_pnl_pct": unrealized_pnl_pct,
                "total_gross_dividends": total_gross_div,
                "total_wht_tax_paid": total_tax_wht,
                "net_dividends_in_bank": net_div_bank,
                "net_dividends_reinvested": net_div_reinvested,
                "pending_dividends": pending_div,
                "true_total_return": true_total_return,
                "true_roi_pct": true_roi_pct
            },
            "holdings": holdings_detail,
            "holdings_count": len(holdings_detail)
        }
    finally:
        cur.close()
        conn.close()

def get_all_dividends(limit: int = 50) -> list[dict]:
    """Retrieves all rows from dividend_ledger ordered by created_at DESC."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, ticker, shares_held, dps_gross, gross_amount, tax_rate_pct,
                   tax_withheld, net_amount, ex_date, payment_date, status, notes, created_at
            FROM dividend_ledger
            ORDER BY created_at DESC
            LIMIT %s;
        """, (limit,))
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "ticker": r[1],
                "shares_held": float(r[2]),
                "dps_gross": float(r[3]),
                "gross_amount": float(r[4]),
                "tax_rate_pct": float(r[5]),
                "tax_withheld": float(r[6]),
                "net_amount": float(r[7]),
                "ex_date": str(r[8]) if r[8] else None,
                "payment_date": str(r[9]) if r[9] else None,
                "status": r[10],
                "notes": r[11],
                "created_at": r[12].isoformat() if r[12] else None
            }
            for r in rows
        ]
    finally:
        cur.close()
        conn.close()

def get_capital_ledger_entries(limit: int = 50) -> list[dict]:
    """Retrieves recent cash transactions from capital_ledger."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, tx_type, amount, reference_note, tx_date
            FROM capital_ledger
            ORDER BY tx_date DESC
            LIMIT %s;
        """, (limit,))
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "tx_type": r[1],
                "amount": float(r[2]),
                "reference_note": r[3],
                "tx_date": r[4].isoformat() if r[4] else None
            }
            for r in rows
        ]
    finally:
        cur.close()
        conn.close()

def get_trade_history_entries(limit: int = 50) -> list[dict]:
    """Retrieves executed trade audit trail from trade_history."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, ticker, side, quantity, price, fees_and_tax, net_total, executed_at, notes
            FROM trade_history
            ORDER BY executed_at DESC
            LIMIT %s;
        """, (limit,))
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "ticker": r[1],
                "side": r[2],
                "quantity": float(r[3]),
                "price": float(r[4]),
                "fees_and_tax": float(r[5]),
                "net_total": float(r[6]),
                "executed_at": r[7].isoformat() if r[7] else None,
                "notes": r[8]
            }
            for r in rows
        ]
    finally:
        cur.close()
        conn.close()

def save_desk_manager_thought(thought_text: str):
    """
    Saves Desk Manager synthesis memorandum into Supabase agent_checkpoints
    and mirrors to local memory_cache/proactive_thought.txt.
    Immune to cloud container ephemeral disk wipes.
    """
    if not thought_text:
        return
    # 1. Mirror to local disk
    try:
        os.makedirs("memory_cache", exist_ok=True)
        with open("memory_cache/proactive_thought.txt", "w", encoding="utf-8") as f:
            f.write(thought_text)
    except Exception as e:
        print(f"⚠️ [Memory Cache] Failed writing local proactive_thought.txt: {e}")

    # 2. Persist to Supabase agent_checkpoints (jsonb format)
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO agent_checkpoints (key, value, updated_at)
            VALUES ('desk_manager_proactive_thought', %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();
        """, (json.dumps({"thought": thought_text, "saved_at": datetime.datetime.now().isoformat()}),))
        conn.commit()
        cur.close()
        conn.close()
        print("💾 [Supabase] Desk Manager memorandum successfully persisted to agent_checkpoints.")
    except Exception as e:
        print(f"❌ [Supabase] Failed persisting Desk Manager thought: {e}")

def get_desk_manager_thought() -> str:
    """
    Retrieves the latest Desk Manager synthesis memorandum.
    Checks Supabase agent_checkpoints first (cloud safe), then falls back to local disk.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT value FROM agent_checkpoints WHERE key = 'desk_manager_proactive_thought'")
        res = cur.fetchone()
        cur.close()
        conn.close()
        if res and res[0]:
            val = res[0]
            if isinstance(val, dict):
                return val.get("thought", "")
            elif isinstance(val, str):
                try:
                    parsed = json.loads(val)
                    if isinstance(parsed, dict) and "thought" in parsed:
                        return parsed["thought"]
                    return parsed
                except Exception:
                    return val
            return str(val)
    except Exception as e:
        print(f"⚠️ [Supabase] Could not fetch desk manager thought: {e}")

    # Fallback to local file if available
    try:
        if os.path.exists("memory_cache/proactive_thought.txt"):
            with open("memory_cache/proactive_thought.txt", "r", encoding="utf-8") as f:
                return f.read()
    except Exception:
        pass

    return "No Desk Manager synthesis memorandum generated yet."


