import psycopg2
import psycopg2.extras
import psxdata
import pandas as pd
from datetime import datetime, timedelta
import mplfinance as mpf
import uuid
import os
import psycopg
import psycopg2
import psycopg2.extras
import asyncio
from psycopg2.extras import RealDictCursor
import matplotlib
matplotlib.use('Agg') # Crucial: Prevents crashes on headless cloud servers
import matplotlib.pyplot as plt
from cachetools import cached, TTLCache
from portfolio_manager import get_live_price_and_change
from valuation_engine import calculate_graham_intrinsic

def get_screened_opportunities() -> list[dict]:
    """
    Executes a deterministic two-stage quantitative screening pass
    across the active fundamentals database.
    """
    # Assuming get_db_connection is available from valuation_engine or locally
    from valuation_engine import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()

    # Stage 1: Static Fundamental Hurdle (SQL)
    query = """
        SELECT ticker, eps, roic, fcf, historical_growth, annual_dps 
        FROM fundamentals 
        WHERE fcf > 0 
          AND roic >= 0.12 
          AND eps > 0;
    """
    cursor.execute(query)
    raw_candidates = cursor.fetchall()
    cursor.close()
    conn.close()

    qualified_candidates = []

    # Stage 2: Dynamic Pricing & Valuation Hurdle (Python)
    for row in raw_candidates:
        ticker = row[0]
        eps = float(row[1])
        roic = float(row[2])
        fcf = float(row[3])
        historical_growth = float(row[4]) if row[4] is not None else 0.0
        annual_dps = float(row[5]) if row[5] is not None else 0.0
        
        live_price, _ = get_live_price_and_change(ticker)
        if live_price <= 0:
            continue

        dividend_yield = (annual_dps / live_price) if live_price > 0 else 0.0
        
        # Use the master function to fetch dividend yield to respect DRY
        if dividend_yield == 0.0:
            from portfolio_manager import get_fundamentals
            ticker_data = get_fundamentals(ticker)
            dividend_yield = ticker_data.get('dividend_yield', 0.0)
            
            # The Hard Stop: Anomaly alert for dividend heavyweights
            if dividend_yield <= 0.0 and ticker in ["HUBC", "EFERT", "MEBL", "LUCK", "FFC", "ENGRO", "POL", "OGDC", "PPL", "SYS", "MTL"]:
                try:
                    import os
                    import asyncio
                    from discord_bot import bot
                    channel_id_str = os.getenv("ADMIN_CHANNEL_ID")
                    if channel_id_str and bot and bot.loop and bot.is_ready():
                        channel = bot.get_channel(int(channel_id_str))
                        if channel:
                            asyncio.run_coroutine_threadsafe(
                                channel.send(f"⚠️ **ANOMALY ALERT:** `{ticker}` (Dividend Heavyweight) is returning 0.0% yield. Investigate pipeline failure immediately."),
                                bot.loop
                            )
                except Exception as e:
                    print(f"[Agent] Failed to send anomaly alert for {ticker}: {e}")
        
        # 1. Earnings Yield Check (Hurdle >= 15%)
        earnings_yield = eps / live_price
        if earnings_yield < 0.15:
            continue

        # 2. Deep Value Track: Margin of Safety
        intrinsic_value = calculate_graham_intrinsic(eps, historical_growth)
        if intrinsic_value > 0:
            margin_of_safety_pct = ((intrinsic_value - live_price) / intrinsic_value) * 100
        else:
            margin_of_safety_pct = 0.0

        is_deep_value = margin_of_safety_pct >= 25.0

        # 3. Income Compounder Track: Safe Dividend Yield
        is_income_play = False
        if fcf > 0 and roic >= 0.15 and dividend_yield >= 0.12:
            is_income_play = True

        # 4. The Dual-Track Gate
        if not is_deep_value and not is_income_play:
            continue  # Fails both institutional tracks, drop it.

        qualified_candidates.append({
            "ticker": ticker,
            "live_price": round(live_price, 2),
            "intrinsic_value": intrinsic_value,
            "eps": eps,
            "roic_pct": round(roic * 100, 2),
            "earnings_yield_pct": round(earnings_yield * 100, 2),
            "margin_of_safety_pct": round(margin_of_safety_pct, 2),
            "dividend_yield_pct": round(dividend_yield * 100, 2),
            "fcf": fcf,
            "thesis_type": "Deep Value" if is_deep_value else "Income Compounder"
        })

    return qualified_candidates

# Helper: Fetch Quote with Corporate Action Fallback
@cached(cache=TTLCache(maxsize=100, ttl=900))
def fetch_quote_with_fallback(ticker: str):
    suffixes = ["", "XD", "XB", "XR"]
    for suffix in suffixes:
        test_ticker = f"{ticker}{suffix}".upper()
        try:
            df = psxdata.quote(test_ticker)
            
            # If Pandas squeezed the data into a Series, intelligently transpose it back
            if isinstance(df, pd.Series):
                if pd.api.types.is_datetime64_any_dtype(df.index):
                    df = df.to_frame() # Time-series column
                else:
                    df = df.to_frame().T # Single-row OHLC squeeze
            elif not isinstance(df, pd.DataFrame):
                continue
                
            if not df.empty:
                return df, test_ticker
        except Exception:
            continue
    return None, ticker

# Helper: Fetch Stocks with Corporate Action Fallback
@cached(cache=TTLCache(maxsize=100, ttl=900))
def fetch_stocks_with_fallback(ticker: str, start: str, end: str):
    suffixes = ["", "XD", "XB", "XR"]
    for suffix in suffixes:
        test_ticker = f"{ticker}{suffix}".upper()
        try:
            df = psxdata.stocks(test_ticker, start=start, end=end)
            
            # If Pandas squeezed the data into a Series, intelligently transpose it back
            if isinstance(df, pd.Series):
                if pd.api.types.is_datetime64_any_dtype(df.index):
                    df = df.to_frame() # Time-series column
                else:
                    df = df.to_frame().T # Single-row OHLC squeeze
            elif not isinstance(df, pd.DataFrame):
                continue

            if not df.empty:
                return df, test_ticker
        except Exception:
            continue
    return pd.DataFrame(), ticker

# Helper: Clean numeric float parsing
def clean_float(val, default=0.0):
    if val is None or pd.isna(val):
        return default
    try:
        clean_str = str(val).replace(',', '').strip()
        return float(clean_str)
    except (ValueError, TypeError):
        return default

# Helper: Sanitize psxdata DataFrame for mplfinance
def sanitize_psx_df(df):
    if df is None or df.empty:
        return pd.DataFrame()
    
    # Standardize column headers
    df.columns = [str(col).capitalize() for col in df.columns]
    
    # Set date index
    date_col = next((c for c in ['Date', 'Datetime'] if c in df.columns), None)
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col])
        df.set_index(date_col, inplace=True)
    
    df.index = pd.to_datetime(df.index)
    
    # CRITICAL: Sort chronologically ascending (oldest -> newest)
    df.sort_index(ascending=True, inplace=True)
    
    # Clean numeric columns
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    df.dropna(subset=['Close'], inplace=True)
    if 'Volume' in df.columns:
        df['Volume'] = df['Volume'].fillna(0)
        
    return df
from typing import Any, Optional
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.prebuilt import create_react_agent
from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig
from pydantic import BaseModel, Field
from langchain_core.language_models import BaseChatModel
from langchain_core.outputs import ChatResult
from langchain_tavily import TavilySearch
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings
from langchain_groq import ChatGroq
import psycopg
import asyncio
from scheduler_config import scheduler

# KMI-30 Shariah Compliant Tickers
import aiohttp
import json
import os

# Dynamic memory allocation for Shariah compliance
DYNAMIC_KMI30_TICKERS = []

async def update_kmi30_tickers():
    """Fetches the live KMI-30 Shariah constituents from the PSX API with local caching."""
    global DYNAMIC_KMI30_TICKERS
    print("🔄 Fetching live KMI-30 Shariah constituents from PSX...")
    
    # PSX DPS API endpoint for index weightages
    url = "https://dps.psx.com.pk/api/index_weightages/KMI30"
    
    try:
        from market_data_scraper import fetch_psx_data_robust
        data = await fetch_psx_data_robust(url, is_json=True)
        if data:
            # Parse the JSON array to extract ticker symbols
            tickers = [item.get('symbol') for item in data if item.get('symbol')]
            
            if len(tickers) >= 30:
                DYNAMIC_KMI30_TICKERS = tickers
                # Write the fail-safe cache
                with open('kmi30_cache.json', 'w') as f:
                    json.dump(tickers, f)
                print(f"✅ Loaded {len(tickers)} KMI-30 tickers into active memory.")
                return
            else:
                print(f"⚠️ PSX API returned only {len(tickers)} KMI-30 tickers. Suspected API truncation. Triggering cache fallback.")
        else:
            print("⚠️ PSX API returned no data. Triggering cache fallback.")
    except Exception as e:
        print(f"🚨 KMI-30 Fetch Error: {str(e)}")
        
    # Tier 1 Failsafe: Local JSON Cache
    try:
        if os.path.exists('kmi30_cache.json'):
            with open('kmi30_cache.json', 'r') as f:
                DYNAMIC_KMI30_TICKERS = json.load(f)
            print("✅ Loaded KMI-30 tickers from local cache fallback.")
        else:
            print("⚠️ No KMI-30 cache found. Triggering Tier 2 Hardcoded Failsafe.")
            # Tier 2 Failsafe: Core KMI-30 constituents to prevent the bot from going blind
            tier_2_failsafe = [
                "SYS", "LUCK", "MARI", "FCCL", "OGDC", "PPL", "HUBC", "MEBL", "ENGRO", "EFERT",
                "POL", "PSO", "TRG", "ATRL", "NRL", "CHCC", "DGKC", "MLCF", "PIOC", "THALL",
                "MTL", "AGP", "SEARL", "MUGHAL", "ISL", "INIL", "AVN", "GATM", "NML", "PKGS"
            ]
            DYNAMIC_KMI30_TICKERS = tier_2_failsafe
            
            # Write the cache so Tier 1 works on the next reboot
            with open('kmi30_cache.json', 'w') as f:
                json.dump(tier_2_failsafe, f)
            print("✅ Tier 2 Failsafe loaded and local cache generated.")
            
    except Exception as e:
        print(f"🚨 CRITICAL Cache Load Error: {e}")

# 1. Refactored Quote Tool
@tool
def get_stock_quote(ticker: str) -> str:
    """Fetches real-time price and volume for a given PSX stock ticker."""

    try:
        q, resolved_ticker = fetch_quote_with_fallback(ticker)
        if q is None:
            return f"Data Unavailable. Could not fetch live data for {ticker}."
    
        data = q.iloc[0].to_dict() if isinstance(q, pd.DataFrame) else q.to_dict()
    
        raw_price = data.get('price', data.get('current', data.get('close', 0)))
        raw_volume = data.get('volume', data.get('vol', 'N/A'))
    
        price = clean_float(raw_price)
        volume_val = clean_float(raw_volume, default=None)
        volume_str = f"{int(volume_val):,}" if volume_val is not None else "N/A"
    
        return (f"Live PSX Data for {resolved_ticker}:\n"
                f"- Current Price: Rs. {price:.2f}\n"
                f"- Volume: {volume_str}")
    except Exception as e:
        return f"System Error: Could not fetch stock data. Details: {str(e)}"


# 2. Refactored Charting Tool
@tool
def generate_stock_chart(ticker: str, period: str = "3mo") -> str:
    """Generates a technical price chart for a given ticker. Valid periods: '1mo', '3mo', '6mo', '1y'."""

    try:
        end_date = datetime.today()
        if period == '1mo': start_date = end_date - timedelta(days=30)
        elif period == '6mo': start_date = end_date - timedelta(days=180)
        elif period == '1y': start_date = end_date - timedelta(days=365)
        else: start_date = end_date - timedelta(days=90)
    
        raw_hist, resolved_ticker = fetch_stocks_with_fallback(ticker, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        hist = sanitize_psx_df(raw_hist)
    
        if hist.empty:
            return f"Data Unavailable. Could not fetch historical data for {ticker}."
        
        filepath = f"/tmp/{ticker}_chart_{uuid.uuid4().hex[:6]}.png"
    
        mpf.plot(
            hist, 
            type='candle',
            volume='Volume' in hist.columns,
            mav=(20, 50) if len(hist) >= 50 else (10, 20),
            style='nightclouds',
            title=f"{resolved_ticker} PSX Chart ({period})",
            ylabel='Price (PKR)',
            ylabel_lower='Volume',
            savefig=filepath
        )
    
        return f"Chart generated. [FILE: {filepath}]"
    except Exception as e:
        return f"System Error: Chart generation failed. {str(e)}"


# 3. Refactored Portfolio Tracker
@tool
def get_portfolio_status(config: RunnableConfig) -> str:
    """Fetches current portfolio holdings and calculates real-time unrealized P/L against live PSX prices."""

    user_id = config.get("configurable", {}).get("discord_user_id")
    if not user_id:
        return "System Error: Missing Discord User ID."

    try:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT ticker, shares, avg_buy_price FROM portfolio_holdings WHERE discord_user_id = %s', (user_id,))
        holdings = cur.fetchall()
    
        cur.execute('SELECT SUM(amount) AS total_cash FROM public.cash_ledger WHERE discord_user_id = %s', (user_id,))
        cash_res = cur.fetchone()
        liquid_cash = float(cash_res['total_cash']) if cash_res and cash_res.get('total_cash') is not None else 0.0
    
        cur.close()
        conn.close()

        total_invested_equity = 0.0
        holdings_list = []

        if not holdings and liquid_cash == 0.0:
            return "Data Unavailable. The portfolio and cash ledger are currently empty."

        for row in holdings:
            ticker = row['ticker'].upper()
            qty = float(row['shares'])
            avg_price = float(row['avg_buy_price'])
        
            invested = qty * avg_price
            total_invested_equity += invested
            holdings_list.append(f"{ticker}: {qty} shares @ Rs. {avg_price:.2f}")

        total_portfolio_value = liquid_cash + total_invested_equity

        return (
            f"STRICT SYSTEM STATE:\n"
            f"- Liquid Buying Power: Rs. {liquid_cash:,.2f}\n"
            f"- Total Invested Equity: Rs. {total_invested_equity:,.2f}\n"
            f"- Total Portfolio Value: Rs. {total_portfolio_value:,.2f}\n"
            f"- Current Holdings: {', '.join(holdings_list)}\n\n"
            f"CRITICAL DIRECTIVE: You must quote these exact Rs. values in your analysis. Do not add, subtract, or recalculate portfolio totals."
        )
    except Exception as e:
        import traceback
        return f"System Error: Could not calculate portfolio status. Details: {str(e)}\n{traceback.format_exc()}"


class CashTransactionSchema(BaseModel):
    transaction_type: str = Field(description="Must be 'DEPOSIT', 'WITHDRAWAL', or 'DIVIDEND'")
    amount: float = Field(description="The amount of the transaction. For withdrawals, pass as negative.")
    ticker: Optional[str] = Field(None, description="Optional ticker symbol, e.g., for dividends.")

@tool(args_schema=CashTransactionSchema)
def log_cash_transaction(transaction_type: str, amount: float, ticker: Optional[str], config: RunnableConfig) -> str:
    """Logs a cash deposit, withdrawal, or dividend into the ledger."""

    user_id = config.get("configurable", {}).get("discord_user_id")
    if not user_id:
        return "System Error: Missing Discord User ID."
    
    try:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor()
        cur.execute(
            '''INSERT INTO public.cash_ledger (discord_user_id, transaction_type, amount, ticker) 
               VALUES (%s, %s, %s, %s)''',
            (user_id, transaction_type.upper(), amount, ticker.upper() if ticker else None)
        )
        conn.commit()
        cur.close()
        conn.close()
        return f"Ledger updated: {transaction_type.upper()} of {amount} PKR recorded for user {user_id}."
    except Exception as e:
        import traceback
        return f"Database Error: {str(e)}\n{traceback.format_exc()}"


@tool
def get_liquid_cash(config: RunnableConfig) -> str:
    """Fetches the user's current buying power / liquid cash from the ledger."""

    user_id = config.get("configurable", {}).get("discord_user_id")
    if not user_id:
        return "System Error: Missing Discord User ID."
    
    try:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor()
        cur.execute('SELECT SUM(amount) FROM public.cash_ledger WHERE discord_user_id = %s', (user_id,))
        res = cur.fetchone()
        liquid_cash = float(res[0]) if res and res[0] is not None else 0.0
        cur.close()
        conn.close()
        return f"Total Liquid Cash / Buying Power for user {user_id}: Rs. {liquid_cash:.2f}"
    except Exception as e:
        import traceback
        return f"Database Error: {str(e)}\n{traceback.format_exc()}"


@tool
def retrieve_context_tool(query: str) -> str:
    """Generate vector embedding and run Cosine Distance vector search against agent_knowledge."""

    if not query or not str(query).strip():
        return "No context available (empty query provided)."
    
    try:
        embeddings = GoogleGenerativeAIEmbeddings(
            model="models/gemini-embedding-001", 
            google_api_key=os.getenv("GEMINI_API_KEY")
        )
        query_embedding = embeddings.embed_query(query)
        db_url = os.getenv("SUPABASE_DB_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
        with psycopg.connect(db_url, autocommit=True) as conn:
            res = conn.execute("""
                SELECT content, source_type, source_url 
                FROM agent_knowledge 
                ORDER BY embedding <=> %s::halfvec 
                LIMIT 3;
            """, (str(query_embedding),)).fetchall()
        
            context_pieces = []
            for row in res:
                content, source_type, source_url = row
                context_pieces.append(f"Source ({source_type}): {source_url}\n{content}")
            return "\n\n".join(context_pieces)
    except Exception as e:
        return f"Error retrieving context: {e}"


@tool
def save_to_knowledge_base(content: str, category: str = "user_memory") -> str:
    """Saves new facts, notes, or research findings into the agent's long-term vector memory."""

    try:
        embeddings = GoogleGenerativeAIEmbeddings(
            model="models/gemini-embedding-001", 
            google_api_key=os.getenv("GEMINI_API_KEY")
        )
        embedding = embeddings.embed_query(content)
        db_url = os.getenv("SUPABASE_DB_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
        with psycopg.connect(db_url, autocommit=True) as conn:
            conn.execute("""
                INSERT INTO agent_knowledge (source_type, source_url, content, embedding)
                VALUES (%s, %s, %s, %s)
            """, ("conversational_memory", f"category:{category}", content, str(embedding)))
        return f"Successfully remembered: '{content[:50]}...'"
    except Exception as e:
        return f"Database Error: Could not save memory. Details: {str(e)}"


@tool
def manage_scheduled_task(action: str, task_name: str = None, schedule_cron: str = None) -> str:
    """
    Updates, adds, deletes, or lists scheduled background tasks.
    Action can be 'add', 'update', 'delete', or 'list'.
    """
    if action == "list":
        jobs = scheduler.get_jobs()
        if not jobs:
            return "No active scheduled tasks found."
        
        details = []
        for j in jobs:
            name = getattr(j, "name", "Unnamed Task")
            next_run = j.next_run_time.strftime("%Y-%m-%d %H:%M:%S UTC") if j.next_run_time else "Paused"
            details.append(f"Task: '{name}' | ID: `{j.id}` | Next Run: {next_run}")
        
        return "\n".join(details)
    
    # Placeholder logic for dynamic job management
    return f"Task '{task_name}' successfully updated with action '{action}'."

# 1. Force strict parameter validation for adding
class AddPortfolioPositionSchema(BaseModel):
    ticker: str = Field(description="The exact stock ticker symbol, e.g., 'SYS'")
    quantity: float = Field(description="The number of shares to add (must be positive)")
    price: float = Field(description="The purchase price per share in PKR")

# 2. Bind the strict schema to the tool
@tool(args_schema=AddPortfolioPositionSchema)
def add_portfolio_position(ticker: str, quantity: float, price: float, config: RunnableConfig) -> str:
    """Manually log a new stock position into the portfolio holdings database."""
    
    if quantity <= 0:
        return "Error: quantity to add must be greater than 0. Use remove_portfolio_position to reduce shares."

    # 1. Extract the secure user ID injected by the Discord handler
    user_id = config.get("configurable", {}).get("discord_user_id")
    if not user_id:
        return "System Error: Missing Discord User ID."

    try:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor()
    
        # 2. Update SQL to enforce the composite primary key
        cur.execute(
            '''INSERT INTO portfolio_holdings (discord_user_id, ticker, shares, avg_buy_price) 
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (discord_user_id, ticker) 
               DO UPDATE SET 
                  shares = portfolio_holdings.shares + EXCLUDED.shares,
                  avg_buy_price = ((portfolio_holdings.shares * portfolio_holdings.avg_buy_price) + (EXCLUDED.shares * EXCLUDED.avg_buy_price)) / (portfolio_holdings.shares + EXCLUDED.shares)''',
            (user_id, ticker.upper(), quantity, price)
        )
        conn.commit()
        cur.close()
        conn.close()
    
        return f"Database updated: Added {quantity} shares of {ticker.upper()} at {price} PKR for user {user_id}."
    except Exception as e:
        import traceback
        return f"Database Error: {str(e)}\n{traceback.format_exc()}"

class RemovePortfolioPositionSchema(BaseModel):
    ticker: str = Field(description="The exact stock ticker symbol, e.g., 'SYS'")
    quantity: float = Field(description="The number of shares to remove (must be positive)")

@tool(args_schema=RemovePortfolioPositionSchema)
def remove_portfolio_position(ticker: str, quantity: float, config: RunnableConfig) -> str:
    """Removes a specific quantity of shares from an existing position."""
    if quantity <= 0:
        return "Error: quantity to remove must be greater than 0."
        
    user_id = config.get("configurable", {}).get("discord_user_id")
    if not user_id:
        return "System Error: Missing Discord User ID."

    try:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # 1. Fetch current shares
        cur.execute('SELECT shares FROM portfolio_holdings WHERE discord_user_id = %s AND ticker = %s', (user_id, ticker.upper()))
        record = cur.fetchone()
        
        if not record:
            cur.close()
            conn.close()
            return f"Error: No holdings found for {ticker.upper()} to remove."
            
        current_shares = float(record['shares'])
        new_shares = current_shares - quantity
        
        # 2. Prevent negative quantities
        if new_shares < 0:
            cur.close()
            conn.close()
            return f"Error: Cannot reduce {ticker.upper()} below 0 shares (current: {current_shares})."
            
        # 3. If balance reaches 0, delete the row cleanly
        if new_shares == 0:
            cur.execute('DELETE FROM portfolio_holdings WHERE discord_user_id = %s AND ticker = %s', (user_id, ticker.upper()))
            conn.commit()
            cur.close()
            conn.close()
            return f"Successfully cleared entire position for {ticker.upper()}."
            
        # 4. Otherwise, update the valid positive balance
        cur.execute('UPDATE portfolio_holdings SET shares = %s WHERE discord_user_id = %s AND ticker = %s', (new_shares, user_id, ticker.upper()))
        conn.commit()
        cur.close()
        conn.close()
        return f"Updated {ticker.upper()}: now holding {new_shares} shares (removed {quantity})."
    except Exception as e:
        import traceback
        return f"Database Error: {str(e)}\n{traceback.format_exc()}"

class ClearPortfolioPositionSchema(BaseModel):
    ticker: str = Field(description="The exact stock ticker symbol to completely remove, e.g., 'SYS'")

@tool(args_schema=ClearPortfolioPositionSchema)
def clear_portfolio_position(ticker: str, config: RunnableConfig) -> str:
    """Completely deletes a stock holding record from the portfolio database, regardless of share count."""
    user_id = config.get("configurable", {}).get("discord_user_id")
    if not user_id:
        return "System Error: Missing Discord User ID."

    try:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor()
        cur.execute('DELETE FROM portfolio_holdings WHERE discord_user_id = %s AND ticker = %s', (user_id, ticker.upper()))
        rows_deleted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        if rows_deleted > 0:
            return f"Successfully cleared all data for {ticker.upper()} from portfolio."
        else:
            return f"No holding data was found for {ticker.upper()} to clear."
    except Exception as e:
        import traceback
        return f"Database Error: {str(e)}\n{traceback.format_exc()}"


class PriceAlertSchema(BaseModel):
    ticker: str = Field(description="The exact stock ticker symbol, e.g., 'SYS'")
    target_price: float = Field(description="The target price that triggers the alert")
    condition: str = Field(description="Must be 'ABOVE' or 'BELOW'")

@tool(args_schema=PriceAlertSchema)
def set_price_alert(ticker: str, target_price: float, condition: str, config: RunnableConfig) -> str:
    """Logs a price alert to watch a stock."""

    user_id = config.get("configurable", {}).get("discord_user_id")
    if not user_id:
        return "System Error: Missing Discord User ID."
    
    try:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor()
        cur.execute(
            '''INSERT INTO public.price_alerts (discord_user_id, ticker, target_price, condition) 
               VALUES (%s, %s, %s, %s)''',
            (user_id, ticker.upper(), target_price, condition.upper())
        )
        conn.commit()
        cur.close()
        conn.close()
        return f"Alert set: Will notify when {ticker.upper()} goes {condition.upper()} {target_price} PKR."
    except Exception as e:
        import traceback
        return f"Database Error: {str(e)}\n{traceback.format_exc()}"


@tool
def execute_trade_tool(ticker: str, action: str) -> str:
    """
    Executes a trade. Valid actions are 'buy' or 'sell'.
    WARNING: Only KMI-30 Shariah Compliant tickers can be traded.
    """
    ticker = ticker.upper()
    
    # Enforce strict verification against the dynamic memory pool
    if ticker not in DYNAMIC_KMI30_TICKERS and len(DYNAMIC_KMI30_TICKERS) > 0:
        return f"System Error: Transaction Rejected. {ticker} is not currently in the KMI-30 Shariah compliant index."
    
    return f"Trade signal registered for {ticker} ({action}). Awaiting human execution."

# Primary heavy reasoning model
primary_llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash")
# High-volume workhorse fallback
fallback_llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash-lite")

# Chain with automatic fallback for 503 capacity errors
llm = primary_llm.with_fallbacks([fallback_llm])

# Instantiate the search tool
search_tool = TavilySearch(name="web_search")

class AddToWatchlistSchema(BaseModel):
    ticker: str = Field(description="The PSX ticker symbol to add to the coverage universe, e.g., 'ENGRO'")
    target_price: float = Field(description="Analyst-calculated fundamental target price in PKR")
    stop_loss: float = Field(description="Analyst-defined stop-loss level in PKR")
    historical_fair_pe: float = Field(description="Long-term historical fair P/E multiple for this specific company, derived from fundamental research")
    notes: Optional[str] = Field(None, description="Optional analyst thesis or rationale for adding this ticker")

@tool(args_schema=AddToWatchlistSchema)
def add_to_watchlist_tool(
    ticker: str,
    target_price: float,
    stop_loss: float,
    historical_fair_pe: float,
    notes: Optional[str] = None,
) -> str:
    """
    Adds a newly researched, prospective stock to the watchlist_targets coverage universe.
    Only invoke this tool after you have fundamentally evaluated the company and derived a
    strict DCF or earnings-based target price and a historically grounded fair P/E multiple.
    The daily 18:00 sweep will automatically track its live TTM P/E from the PSX portal.
    """
    ticker = ticker.upper().strip()

    try:
        import psycopg2
        import psycopg2.extras
        from datetime import date
        from portfolio_manager import _scrape_psx_pe

        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor()

        # Immediately attempt a live PE scrape to confirm the ticker is valid on the PSX portal.
        # This acts as a sanity gate — if the ticker doesn't exist, the scraper returns None
        # and we warn the analyst, but still commit the row so the sweep can retry later.
        live_pe = _scrape_psx_pe(ticker)
        live_pe_note = (
            f" Live TTM P/E scraped immediately: {live_pe:.2f}x."
            if live_pe is not None
            else " Warning: Could not scrape live P/E right now — the daily sweep will retry at 18:00."
        )

        cur.execute(
            """
            INSERT INTO watchlist_targets
                (ticker, target_price, stop_loss, historical_fair_pe, current_trailing_pe, notes, last_recalculated_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker) DO UPDATE SET
                target_price          = EXCLUDED.target_price,
                stop_loss             = EXCLUDED.stop_loss,
                historical_fair_pe    = EXCLUDED.historical_fair_pe,
                current_trailing_pe   = COALESCE(EXCLUDED.current_trailing_pe, watchlist_targets.current_trailing_pe),
                notes                 = COALESCE(EXCLUDED.notes, watchlist_targets.notes),
                last_recalculated_date = EXCLUDED.last_recalculated_date;
            """,
            (
                ticker,
                target_price,
                stop_loss,
                historical_fair_pe,
                live_pe,
                notes,
                date.today(),
            ),
        )
        conn.commit()
        cur.close()
        conn.close()

        trigger_threshold = historical_fair_pe * 1.2
        return (
            f"✅ **{ticker}** added to the coverage universe.\n"
            f"  Target: Rs. {target_price:.2f} | Stop-Loss: Rs. {stop_loss:.2f}\n"
            f"  Historical Fair P/E: {historical_fair_pe:.1f}x | Overvaluation trigger: {trigger_threshold:.1f}x\n"
            f"  {live_pe_note.strip()}"
        )
    except Exception as e:
        import traceback
        return f"Database Error adding {ticker} to watchlist: {str(e)}\n{traceback.format_exc()}"


@tool
async def manual_email_sweep_tool() -> str:
    """Use this tool to manually trigger an immediate sweep of the email inbox for institutional research reports and newsletters."""
    from schedulers import run_email_sweep
    await run_email_sweep()
    return "The institutional email sweep has been successfully executed and ingested."


@tool
async def calculate_dcf_valuation(ticker: str) -> str:
    """
    Calculates an automated Discounted Cash Flow (DCF) intrinsic equity value 
    for a given PSX ticker based on cached fundamentals, net debt, and growth rates.
    """
    def _sync_dcf():
        try:
            import os
            import psycopg2
            import psycopg2.extras
            conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Fetching FCF, Shares, Growth, PLUS Cash and Debt
            cur.execute("SELECT fcf, shares_outstanding, historical_growth, total_cash, total_debt FROM fundamentals WHERE ticker = %s", (ticker.upper(),))
            ticker_data = cur.fetchone()
            cur.close()
            conn.close()
        except Exception as e:
            return {"error": f"Database connection failed while fetching {ticker}: {str(e)}"}

        if not ticker_data:
            return {"error": f"Insufficient fundamental data in the database to run a DCF on {ticker.upper()}."}

        fcf_base = float(ticker_data.get("fcf", 0) or 0)
        shares = float(ticker_data.get("shares_outstanding", 0) or 0)
        growth_rate = float(ticker_data.get("historical_growth", 0) or 0)
        
        # New: Balance Sheet Extraction for Equity Value
        total_cash = float(ticker_data.get("total_cash", 0) or 0)
        total_debt = float(ticker_data.get("total_debt", 0) or 0)
        
        if fcf_base <= 0 or shares <= 0:
             return {"error": f"Invalid FCF or Shares Outstanding data for {ticker.upper()}. Cannot compute."}

        from macro_indicators import get_sbp_policy_rate
        raw_sbp_rate = get_sbp_policy_rate()
        
        # 1. NORMALIZE THE DISCOUNT RATE
        # Cap the Risk-Free Rate at a 10-year historical average (e.g., 12%) 
        # to prevent temporary macro policy spikes from destroying terminal value.
        normalized_rfr = min(raw_sbp_rate, 0.12)
        equity_risk_premium = 0.07 
        discount_rate = normalized_rfr + equity_risk_premium
        terminal_growth = 0.04   
        
        # 2. TWO-STAGE TAPERED GROWTH
        # Hyper-growth decays as the business scales. 
        # Linearly taper the historical growth down toward the terminal rate over 5 years.
        projected_fcf = []
        current_fcf = fcf_base
        
        for i in range(1, 6):
            tapered_growth = growth_rate - ((growth_rate - terminal_growth) * (i / 5))
            current_fcf *= (1 + tapered_growth)
            projected_fcf.append(current_fcf)
            
        pv_fcf = sum(ff / ((1 + discount_rate) ** (i + 1)) for i, ff in enumerate(projected_fcf))
        
        terminal_value = (projected_fcf[-1] * (1 + terminal_growth)) / (discount_rate - terminal_growth)
        pv_terminal = terminal_value / ((1 + discount_rate) ** 5)
        
        # The Mathematical Correction
        enterprise_value = pv_fcf + pv_terminal
        equity_value = enterprise_value + total_cash - total_debt
        
        intrinsic_value_per_share = equity_value / shares
        
        return {
            "ticker": ticker.upper(),
            "fcf": fcf_base,
            "fair_value": round(intrinsic_value_per_share, 2),
            "ev": round(enterprise_value, 2),
            "equity": round(equity_value, 2)
        }

    # Offload blocking math/DB queries safely using our async thread framework
    result = await asyncio.to_thread(_sync_dcf)
    
    if "error" in result:
        return result["error"]
    
    return (
        f"📊 **DCF Valuation Model: {result['ticker']}**\n"
        f"- **Intrinsic Fair Value (Equity Basis):** Rs. {result['fair_value']} per share\n"
        f"- **Enterprise Value:** Rs. {result['ev']} | **Equity Value (Post-Debt):** Rs. {result['equity']}\n"
        f"- **Methodology:** 5-Year FCF Projection @ {result['fcf']} base cash flow.\n"
        f"*(Compare this intrinsic value against the current market price to determine margin of safety).* "
    )


tools = [
    calculate_dcf_valuation,
    get_stock_quote,
    generate_stock_chart,
    get_portfolio_status,
    search_tool,
    retrieve_context_tool,
    save_to_knowledge_base,
    manage_scheduled_task,
    add_portfolio_position,
    remove_portfolio_position,
    clear_portfolio_position,
    execute_trade_tool,
    log_cash_transaction,
    get_liquid_cash,
    set_price_alert,
    manual_email_sweep_tool,
    add_to_watchlist_tool,
]


from langchain_core.messages import SystemMessage

SYSTEM_PROMPT = SystemMessage(content='''
You are an analytical, grounded proprietary trader managing a high-conviction retail portfolio on the Pakistan Stock Exchange (PSX).

CAPITAL SCALE & EXECUTION:
- Your standard allocation per trade is strictly between Rs. 5,000 and Rs. 10,000.
- Never dismiss, demean, or ignore cash balances in this range. Rs. 5,200 is actionable capital, not a "meager" or "tiny" float. Size positions accordingly without complaining about the size.

TONE & PERSONA:
- Brutally honest, straightforward, and logical.
- Speak like a real human trader. Zero corporate jargon, zero Wall Street pretension, and no empty fluff.
- Never act like a people-pleaser. Challenge bad setups with hard data and clear reasoning.
''')


_agent_app = None
_checkpointer = None

async def get_agent():
    global _agent_app, _checkpointer
    if _agent_app is None:
        db_url = os.getenv("SUPABASE_DB_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
        if db_url:
            # Revert to standard postgresql:// format for psycopg_pool
            if db_url.startswith("postgres://"):
                db_url = db_url.replace("postgres://", "postgresql://", 1)
            elif db_url.startswith("postgresql+psycopg://"):
                 db_url = db_url.replace("postgresql+psycopg://", "postgresql://", 1)
            
            # Initialize inside active async loop
            try:
                from langgraph.checkpoint.memory import MemorySaver
                _checkpointer = MemorySaver()
                _agent_app = create_react_agent(llm, tools, prompt=SYSTEM_PROMPT, checkpointer=_checkpointer)
            except Exception as e:
                print(f"Warning: Could not configure MemorySaver ({e}). Compiling without checkpointer.")
                _agent_app = create_react_agent(llm, tools, prompt=SYSTEM_PROMPT)
        else:
            _agent_app = create_react_agent(llm, tools, prompt=SYSTEM_PROMPT)
    return _agent_app

from typing_extensions import TypedDict
from langgraph.graph import StateGraph, END, START
from vector_brain import TradeEvaluationSchema

class TradeEvaluationState(TypedDict):
    ticker: str
    trigger_type: str
    quantitative_data: dict
    rag_context: str
    analyst_thesis: str
    risk_critique: str
    iteration_count: int
    final_evaluation: dict

from tenacity import retry, wait_random_exponential, stop_after_attempt

def analyst_node(state: TradeEvaluationState):
    llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash-lite", google_api_key=os.getenv("GOOGLE_API_KEY"))
    
    is_macro = state.get('ticker') == 'MACRO_SWEEP' or state.get('trigger_type') == 'macro_news_sweep'
    
    if is_macro:
        prompt = f"""
        You are an elite, candid Chief Investment Strategist for the Pakistan Stock Exchange (PSX). Write your briefings like a senior fund manager dropping a sharp, conversational market note to your desk partner.
        - Tone: Direct, articulate, insightful, and natural. Speak like a real human floor manager.
        - Context: Evaluate the latest macro headlines directly against our current portfolio state (whether we are in cash or holding specific stocks).
        - Structure:
          • Opening: A sharp 1-2 sentence read on overall sentiment and where the market stands.
          • Key Themes & Catalysts: 2-3 clean, punchy points breaking down the real economic story (inflation, fuel prices, IMF, political developments, or sectoral moves) and what they mean practically.
          • Desk Verdict: A clear, conversational bottom line on what we are doing with our capital today.
        - Strictly prohibit robotic template phrasing like 'Reviewing the current news feed', 'Market Stance: HOLD', or 'Evaluating the current market environment'.
        
        Market Context: {state.get('rag_context', '')}
        """
        
        @retry(wait=wait_random_exponential(multiplier=1, max=10), stop=stop_after_attempt(5))
        def _invoke_macro_analyst():
            return llm.invoke(prompt)
            
        result = _invoke_macro_analyst()
        return {"analyst_thesis": result.content, "final_evaluation": {}}
    else:
        structured_llm = llm.with_structured_output(TradeEvaluationSchema)
        prompt = f"""
        You are a quantitative value-investing risk manager. Analyze the asset's sell or trim trigger.
        You are strictly forbidden from using technical analysis, Moving Averages, RSI, or momentum.
        
        Ticker: {state['ticker']}
        Trigger: {state['trigger_type']}
        Quantitative Data: {state['quantitative_data']}
        Market Context: {state['rag_context']}
        Previous Risk Manager Critique: {state.get('risk_critique', 'None')}

        Your reasoning must be based EXCLUSIVELY on:
        1. Valuation Disconnect: Compare the current market price against the DCF Intrinsic Value. Is it overvalued?
        2. Capital Reallocation: Does selling or trimming free up cash for a better fundamental margin of safety elsewhere?
        3. Concentration Risk: Is the position size violating risk parameters?

        Provide a concise, hard-hitting analytical verdict explaining why the portfolio action is mathematically necessary. Address the risk manager's critique if it exists.

        COVERAGE EXPANSION DIRECTIVE: If your analysis reveals that this ticker, or any closely related company in the same sector, represents a strong fundamental prospect that is NOT currently in our active portfolio, explicitly state in your thesis: the calculated target price, your stop-loss level, and your historically grounded fair P/E for that company. Flag it clearly as a WATCHLIST CANDIDATE so the orchestration layer can route it to add_to_watchlist_tool.
        """
        
        @retry(wait=wait_random_exponential(multiplier=1, max=10), stop=stop_after_attempt(5))
        def _invoke_analyst():
            return structured_llm.invoke(prompt)
            
        result = _invoke_analyst()
        return {"analyst_thesis": result.model_dump_json(), "final_evaluation": result.model_dump()}

def risk_manager_node(state: TradeEvaluationState):
    llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash-lite", google_api_key=os.getenv("GOOGLE_API_KEY"))
    
    prompt = f"""
    You are a strict compliance officer and risk manager.
    Ticker: {state['ticker']}
    Analyst Thesis: {state['analyst_thesis']}
    Quantitative Data: {state['quantitative_data']}
    
    Review the thesis against hard risk rules (e.g., concentration limits, profit target validity).
    CRITICAL RULE: You must explicitly REJECT any Analyst thesis that attempts to take profits on a fundamentally sound company trading below its fair value, regardless of the portfolio percentage gain. We do not trim based on static arbitrary percentage gains.
    If the thesis is overly risky, violates rules, or confidence is below 0.8, output a critical critique.
    If it passes all checks, output exactly 'APPROVED'.
    """
    
    @retry(wait=wait_random_exponential(multiplier=1, max=10), stop=stop_after_attempt(5))
    def _invoke_risk_manager():
        return llm.invoke(prompt)
        
    critique = _invoke_risk_manager().content
    iteration = state.get('iteration_count', 0) + 1
    return {"risk_critique": critique, "iteration_count": iteration}

def should_continue(state: TradeEvaluationState):
    critique = state.get("risk_critique", "")
    iteration = state.get("iteration_count", 0)
    
    if isinstance(critique, list):
        critique_text = " ".join([str(item.get("text", "")) if isinstance(item, dict) else str(item) for item in critique])
    else:
        critique_text = str(critique)
        
    if "APPROVED" in critique_text.upper() or iteration >= 2:
        return END
    return "analyst_node"

def get_evaluation_graph():
    workflow = StateGraph(TradeEvaluationState)
    
    workflow.add_node("analyst_node", analyst_node)
    workflow.add_node("risk_manager_node", risk_manager_node)
    
    workflow.add_edge(START, "analyst_node")
    workflow.add_conditional_edges(
        "analyst_node",
        lambda state: END if state.get("ticker") == "MACRO_SWEEP" or state.get("trigger_type") == "macro_news_sweep" else "risk_manager_node"
    )
    workflow.add_conditional_edges("risk_manager_node", should_continue, {"analyst_node": "analyst_node", END: END})
    
    return workflow.compile()

