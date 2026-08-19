import os
import requests
import psycopg2
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

load_dotenv()
DATABASE_URL = os.getenv("SUPABASE_DB_URL")

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

def log_trade(ticker: str, action: str, quantity: float, price: float, fees: float = 0.0) -> str:
    ticker = ticker.upper()
    action = action.upper()
    
    if action not in ['BUY', 'SELL']:
        return "🚨 Error: Action must be 'BUY' or 'SELL'."
        
    try:
        conn = psycopg2.connect(DATABASE_URL)
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
        return f"🚨 Database Error: {e}"

def set_target(ticker: str, target_price: float, stop_loss: float = 0.0) -> str:
    ticker = ticker.upper()
    try:
        conn = psycopg2.connect(DATABASE_URL)
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
        return f"🚨 Database Error: {e}"

def get_portfolio_summary() -> str:
    try:
        conn = psycopg2.connect(DATABASE_URL)
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
        return f"🚨 Database Error: {e}"

def get_watchlist_summary() -> str:
    try:
        conn = psycopg2.connect(DATABASE_URL)
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
        return f"🚨 Database Error: {e}"

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
        
        conn = psycopg2.connect(db_url)
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
        return f"Database sync failed: {str(e)}"

def get_fundamentals(ticker: str) -> dict:
    """
    Fetches fundamentals from yfinance and cleans missing values safely.
    Replaces all NaN or empty float values with None to preserve math operations,
    allowing downstream string formatters to handle 'N/A' presentation.
    """
    try:
        yf_ticker = f"{ticker}.KA" if not ticker.endswith(".KA") else ticker
        stock = yf.Ticker(yf_ticker)
        info = stock.info
        if not info:
            return {}
            
        cleaned_info = {}
        for k, v in info.items():
            if pd.isna(v):
                cleaned_info[k] = np.nan
            else:
                cleaned_info[k] = v
        return cleaned_info
    except Exception as e:
        print(f"Error fetching fundamentals for {ticker}: {e}")
        return {}

@functools.lru_cache(maxsize=32)
def get_cached_historical_prices(ticker: str, start_date: str):
    """Fetches and caches historical PSX data from Yahoo Finance to avoid API bans."""
    try:
        yf_ticker = f"{ticker}.KA"
        
        def _download():
            return yf.download(yf_ticker, start=start_date, progress=False)
            
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = executor.submit(_download)
        try:
            data = future.result(timeout=5)
        except concurrent.futures.TimeoutError:
            print(f"yfinance download timed out for {ticker}")
            return pd.Series(dtype=float)
        finally:
            executor.shutdown(wait=False)

        if isinstance(data, (float, int, np.float64)):
            return pd.Series(dtype=float)

        if data.empty:
            return pd.Series(dtype=float)
        
        # Handle multi-level columns in yfinance >= 0.2.40
        if isinstance(data.columns, pd.MultiIndex):
            close_col = data['Close'].iloc[:, 0]
        else:
            close_col = data['Close']
            
        return close_col.squeeze()
    except Exception as e:
        print(f"Error fetching historical for {ticker}: {e}")
        return pd.Series(dtype=float)

def fetch_supabase_equity_curve() -> pd.DataFrame:
    """
    Reconstructs the daily market-to-market equity curve.
    Guarantees a pd.DataFrame return with a 'portfolio_value' column.
    """
    try:
        # 1. Fetch transactions from Supabase
        conn = psycopg2.connect(DATABASE_URL)
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
