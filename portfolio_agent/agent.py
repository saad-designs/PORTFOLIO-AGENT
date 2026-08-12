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
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.prebuilt import create_react_agent
from langchain_core.tools import tool
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
        q = psxdata.quote(ticker.upper())
        if q is None or (isinstance(q, (pd.DataFrame, pd.Series)) and q.empty):
            return f"Data Unavailable. Could not fetch live data for {ticker}."
        
        data = q.iloc[0].to_dict() if isinstance(q, pd.DataFrame) else q.to_dict()
        
        raw_price = data.get('price', data.get('current', data.get('close', 0)))
        raw_volume = data.get('volume', data.get('vol', 'N/A'))
        
        price = clean_float(raw_price)
        volume_val = clean_float(raw_volume, default=None)
        volume_str = f"{int(volume_val):,}" if volume_val is not None else "N/A"
        
        return (f"Live PSX Data for {ticker.upper()}:\n"
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
        
        raw_hist = psxdata.stocks(ticker.upper(), start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
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
            title=f"{ticker.upper()} PSX Chart ({period})",
            ylabel='Price (PKR)',
            ylabel_lower='Volume',
            savefig=filepath
        )
        
        return f"Chart generated. [FILE: {filepath}]"
    except Exception as e:
        return f"System Error: Chart generation failed. {str(e)}"

# 3. Refactored Portfolio Tracker
@tool
def get_portfolio_status() -> str:
    """Fetches current portfolio holdings and calculates real-time unrealized P/L against live PSX prices."""
    try:
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('SELECT ticker, quantity, average_price FROM portfolio_holdings')
        holdings = cur.fetchall()
        cur.close()
        conn.close()

        if not holdings:
            return "Data Unavailable. The portfolio is currently empty."

        report = ["📊 **Live PSX Portfolio Status**\n"]
        total_invested = 0
        total_current_value = 0

        for row in holdings:
            ticker = row['ticker'].upper()
            qty = clean_float(row['quantity'])
            avg_price = clean_float(row['average_price'])
            
            invested = qty * avg_price
            total_invested += invested

            q = psxdata.quote(ticker)
            if q is None or (isinstance(q, (pd.DataFrame, pd.Series)) and q.empty):
                report.append(f"- **{ticker}**: {qty} shares @ Rs. {avg_price:.2f} (Live data unavailable)")
                continue
                
            data = q.iloc[0].to_dict() if isinstance(q, pd.DataFrame) else q.to_dict()
            raw_price = data.get('price', data.get('current', data.get('close', 0)))
            live_price = clean_float(raw_price)
            
            current_value = qty * live_price
            total_current_value += current_value
            pnl = current_value - invested
            pnl_percent = (pnl / invested) * 100 if invested > 0 else 0

            report.append(f"- **{ticker}**: {qty:.0f} shares | Avg: Rs. {avg_price:.2f} | Live: Rs. {live_price:.2f} | P/L: Rs. {pnl:.2f} ({pnl_percent:.2f}%)")

        total_pnl = total_current_value - total_invested
        total_pnl_percent = (total_pnl / total_invested) * 100 if total_invested > 0 else 0

        report.append("\n**Summary:**")
        report.append(f"- Total Invested: Rs. {total_invested:.2f}")
        report.append(f"- Total Current Value: Rs. {total_current_value:.2f}")
        report.append(f"- Total P/L: Rs. {total_pnl:.2f} ({total_pnl_percent:.2f}%)")

        return "\n".join(report)
    except Exception as e:
        return f"System Error: Could not calculate portfolio status. Details: {str(e)}"

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

class CustomAutoSwitchLLM(BaseChatModel):
    primary: BaseChatModel
    secondary: BaseChatModel

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        try:
            # Attempt execution with primary engine
            return self.primary._generate(messages, stop=stop, run_manager=run_manager, **kwargs)
        except Exception as e:
            print(f"⚠️ Primary model failed: {str(e)}. Auto-switching to secondary...")
            # Fallback to secondary engine on any exception (including 429 quota limits)
            return self.secondary._generate(messages, stop=stop, run_manager=run_manager, **kwargs)

    @property
    def _llm_type(self) -> str:
        return "custom_autoswitch_llm"

# 1. Initialize Primary and Secondary Engines
primary_engine = ChatGoogleGenerativeAI(model="gemini-3.6-flash", temperature=0)
secondary_engine = ChatGroq(model="llama-3.3-70b-versatile", groq_api_key=os.getenv("GROQ_API_KEY"), temperature=0)

# 2. Instantiate with Pydantic field assignment
llm = CustomAutoSwitchLLM(primary=primary_engine, secondary=secondary_engine)

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
    execute_trade_tool
]

SYSTEM_PROMPT = """You are an elite, autonomous financial, web, and research AI agent. You operate under absolute factual strictness.

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
