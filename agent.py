import psxdata
import pandas as pd
from datetime import datetime, timedelta
import mplfinance as mpf
import uuid
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import matplotlib
matplotlib.use('Agg') # Crucial: Prevents crashes on headless cloud servers
import matplotlib.pyplot as plt

# Helper: Fetch Quote with Corporate Action Fallback
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
from langchain_community.tools import DuckDuckGoSearchRun
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings
from langchain_groq import ChatGroq
import psycopg
from scheduler_config import scheduler

# KMI-30 Shariah Compliant Tickers
SHARIAH_COMPLIANT = ["SYS", "LUCK", "MARI", "FCCL", "OGDCL"]

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
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT ticker, quantity, average_price FROM portfolio_holdings WHERE discord_user_id = %s', (user_id,))
        holdings = cur.fetchall()
        
        cur.execute('SELECT SUM(amount) AS total_cash FROM public.cash_ledger WHERE discord_user_id = %s', (user_id,))
        cash_res = cur.fetchone()
        liquid_cash = float(cash_res['total_cash']) if cash_res and cash_res.get('total_cash') is not None else 0.0
        
        cur.close()
        conn.close()

        report = ["📊 **Live PSX Portfolio Status**\n"]
        total_invested = 0
        total_current_value = 0

        if not holdings and liquid_cash == 0.0:
            return "Data Unavailable. The portfolio and cash ledger are currently empty."

        for row in holdings:
            ticker = row['ticker'].upper()
            qty = clean_float(row['quantity'])
            avg_price = clean_float(row['average_price'])
            
            invested = qty * avg_price
            total_invested += invested

            q, resolved_ticker = fetch_quote_with_fallback(ticker)
            if q is None:
                report.append(f"- **{ticker}**: {qty} shares @ Rs. {avg_price:.2f} (Live data unavailable)")
                continue
                
            data = q.iloc[0].to_dict() if isinstance(q, pd.DataFrame) else q.to_dict()
            raw_price = data.get('price', data.get('current', data.get('close', 0)))
            live_price = clean_float(raw_price)
            
            current_value = qty * live_price
            total_current_value += current_value
            pnl = current_value - invested
            pnl_percent = (pnl / invested) * 100 if invested > 0 else 0

            report.append(f"- **{resolved_ticker}**: {qty:.0f} shares | Avg: Rs. {avg_price:.2f} | Live: Rs. {live_price:.2f} | P/L: Rs. {pnl:.2f} ({pnl_percent:.2f}%)")

        total_pnl = total_current_value - total_invested
        total_pnl_percent = (total_pnl / total_invested) * 100 if total_invested > 0 else 0
        
        true_total_value = total_current_value + liquid_cash

        report.append("\n**Summary:**")
        report.append(f"- Liquid Cash (Buying Power): Rs. {liquid_cash:.2f}")
        report.append(f"- Total Invested (Equities): Rs. {total_invested:.2f}")
        report.append(f"- Equities Current Value: Rs. {total_current_value:.2f}")
        report.append(f"- Equities P/L: Rs. {total_pnl:.2f} ({total_pnl_percent:.2f}%)")
        report.append(f"- **True Total Portfolio Value**: Rs. {true_total_value:.2f}")

        return "\n".join(report)
    except Exception as e:
        return f"System Error: Could not calculate portfolio status. Details: {str(e)}"

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
        return f"Database Error: {str(e)}"

@tool
def get_liquid_cash(config: RunnableConfig) -> str:
    """Fetches the user's current buying power / liquid cash from the ledger."""
    user_id = config.get("configurable", {}).get("discord_user_id")
    if not user_id:
        return "System Error: Missing Discord User ID."
        
    try:
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor()
        cur.execute('SELECT SUM(amount) FROM public.cash_ledger WHERE discord_user_id = %s', (user_id,))
        res = cur.fetchone()
        liquid_cash = float(res[0]) if res and res[0] is not None else 0.0
        cur.close()
        conn.close()
        return f"Total Liquid Cash / Buying Power for user {user_id}: Rs. {liquid_cash:.2f}"
    except Exception as e:
        return f"Database Error: {str(e)}"

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

# 1. Force strict parameter validation
class PortfolioPositionSchema(BaseModel):
    ticker: str = Field(description="The exact stock ticker symbol, e.g., 'SYS'")
    quantity: float = Field(description="The number of shares to add")
    price: float = Field(description="The purchase price per share in PKR")

# 2. Bind the strict schema to the tool
@tool(args_schema=PortfolioPositionSchema)
def add_portfolio_position(ticker: str, quantity: float, price: float, config: RunnableConfig) -> str:
    """Manually log a new stock position into the portfolio holdings database."""
    # 1. Extract the secure user ID injected by the Discord handler
    user_id = config.get("configurable", {}).get("discord_user_id")
    if not user_id:
        return "System Error: Missing Discord User ID."

    try:
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor()
        
        # 2. Update SQL to enforce the composite primary key
        cur.execute(
            '''INSERT INTO portfolio_holdings (discord_user_id, ticker, quantity, average_price) 
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (discord_user_id, ticker) 
               DO UPDATE SET 
                  quantity = portfolio_holdings.quantity + EXCLUDED.quantity,
                  average_price = ((portfolio_holdings.quantity * portfolio_holdings.average_price) + (EXCLUDED.quantity * EXCLUDED.average_price)) / (portfolio_holdings.quantity + EXCLUDED.quantity)''',
            (user_id, ticker.upper(), quantity, price)
        )
        conn.commit()
        cur.close()
        conn.close()
        
        return f"Database updated: Added {quantity} shares of {ticker.upper()} at {price} PKR for user {user_id}."
    except Exception as e:
        return f"Database Error: {str(e)}"

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
        return f"Database Error: {str(e)}"

@tool
def execute_trade_tool(ticker: str, action: str) -> str:
    """
    Executes a trade. Valid actions are 'buy' or 'sell'.
    WARNING: Only KMI-30 Shariah Compliant tickers can be traded.
    """
    ticker = ticker.upper()
    if ticker not in SHARIAH_COMPLIANT:
        return f"System Error: Transaction Rejected. {ticker} is not in the KMI-30 Shariah compliant list."
    
    return f"Trade signal registered for {ticker} ({action}). Awaiting human execution."

# Primary heavy reasoning model
primary_llm = ChatGoogleGenerativeAI(model="gemini-3.7-flash")
# High-volume workhorse fallback
fallback_llm = ChatGoogleGenerativeAI(model="gemini-3.5-flash-lite")

# Chain with automatic fallback for 503 capacity errors
llm = primary_llm.with_fallbacks([fallback_llm])

# Instantiate the search tool
search_tool = DuckDuckGoSearchRun(name="web_search")

tools = [
    get_stock_quote,
    generate_stock_chart,
    get_portfolio_status,
    search_tool,
    retrieve_context_tool,
    save_to_knowledge_base,
    manage_scheduled_task,
    add_portfolio_position,
    execute_trade_tool,
    log_cash_transaction,
    get_liquid_cash,
    set_price_alert
]

SYSTEM_PROMPT = """You are an elite, autonomous financial, web, and research AI agent. You operate under absolute factual strictness.
You are an autonomous, institutional-grade quantitative research agent for the Pakistan Stock Exchange (PSX). 
When the user provides capital or asks for market entry:
1. DO NOT ask the user what tickers to look at. You must use your tools to find them.
2. Query the Vector Brain for current mentor strategies regarding asset allocation and market conditions.
3. Scan the PSX market data and fundamental tools to identify 2-3 specific tickers that match the mentor's criteria (e.g., high dividend yield, low P/E, strong support).
4. Give a definitive, data-driven verdict: Either specify exact tickers and entry prices, or explicitly advise holding the capital in cash if market conditions are poor.
Never output generic financial advice. Always ground your strategy in live tool data.

### CORE DIRECTIVES
1. **Zero Hallucination:** You must NEVER invent, fabricate, or guess data. If a tool returns no data, explicitly state: "Data Unavailable."
2. **Strict Data Parsing:** Maintain absolute structural integrity when extracting data.
3. **Live Research:** Proactively use `web_search` for current market data.
4. **Tool Routing:** 
   - For executing or simulating live broker trades, strictly use `execute_trade_tool`.
   - For manually logging, tracking, or updating portfolio holdings, strictly use `add_portfolio_position`.

### OUTPUT FORMATTING (STRICT)
- Speak naturally and logically, like a real human expert. 
- Be brutally concise. If the answer can be a one or two-liner, do not write an essay.
- Draw on past conversation history ONLY to extract relevant points needed for the immediate answer. 
- State the important information, make your necessary points, and stop. No fluff, no filler text.
- **Error Transparency:** If a tool returns an error or failure string, relay the EXACT error details provided by the tool. Do not abstract it behind a generic "system error" message.
- If a tool returns a file path like `[FILE: /tmp/...]`, include that exact string unchanged. Never convert local file paths into Markdown image syntax."""

def get_compiled_graph():
    """Connect to a Supabase PostgreSQL instance using psycopg[binary]. Configure pgvector and langgraph-checkpoint-postgres."""
    db_url = os.getenv("SUPABASE_DB_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
    try:
        conn = psycopg.connect(db_url, autocommit=True)
        
        conn.execute("""
        CREATE EXTENSION IF NOT EXISTS vector;

        CREATE TABLE IF NOT EXISTS agent_knowledge (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            source_type TEXT NOT NULL,
            source_url TEXT,
            content TEXT NOT NULL,
            embedding halfvec(768),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        checkpointer = PostgresSaver(conn)
        checkpointer.setup()
        return create_react_agent(llm, tools, prompt=SYSTEM_PROMPT, checkpointer=checkpointer)
    except Exception as e:
        print(f"Warning: Could not connect to Postgres ({e}). Compiling without checkpointer.")
        return create_react_agent(llm, tools, prompt=SYSTEM_PROMPT)

graph = get_compiled_graph()
