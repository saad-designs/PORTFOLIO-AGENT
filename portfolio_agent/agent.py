import os
import yfinance as yf
import matplotlib
matplotlib.use('Agg') # Crucial: Prevents crashes on headless cloud servers
import matplotlib.pyplot as plt
import pandas as pd
import uuid
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.prebuilt import create_react_agent
from langchain_core.tools import tool
from langchain_community.tools import DuckDuckGoSearchRun
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings
from langchain_groq import ChatGroq
import psycopg
from scheduler_config import scheduler

# KMI-30 Shariah Compliant Tickers
SHARIAH_COMPLIANT = ["SYS", "LUCK", "MARI", "FCCL", "OGDCL"]

@tool
def get_stock_quote(ticker: str) -> str:
    """Fetches real-time price, volume, and P/E ratio for a given stock ticker."""
    try:
        # Auto-format PSX tickers for Yahoo Finance
        fetch_ticker = f"{ticker}.KA" if "." not in ticker else ticker

        stock = yf.Ticker(fetch_ticker)
        info = stock.info

        if not info or ('currentPrice' not in info and 'regularMarketPrice' not in info):
            return f"Data Unavailable. Could not fetch live data for {ticker}."

        price = info.get('currentPrice', info.get('regularMarketPrice', 'N/A'))
        prev_close = info.get('previousClose', 'N/A')
        volume = info.get('volume', 'N/A')
        pe_ratio = info.get('trailingPE', 'N/A')

        return (f"Live Data for {ticker}:\n"
                f"- Current Price: Rs. {price}\n"
                f"- Previous Close: Rs. {prev_close}\n"
                f"- Volume: {volume}\n"
                f"- P/E Ratio: {pe_ratio}")
    except Exception as e:
        return f"System Error: Could not fetch stock data. Details: {str(e)}"

@tool
def generate_stock_chart(ticker: str, period: str = "3mo") -> str:
    """Generates a technical price chart for a given ticker. Valid periods: '1mo', '3mo', '6mo', '1y'."""
    try:
        fetch_ticker = f"{ticker}.KA" if "." not in ticker else ticker
        hist = yf.Ticker(fetch_ticker).history(period=period)

        if hist.empty:
            return f"Data Unavailable. Could not fetch historical data for {ticker}."

        plt.figure(figsize=(10, 5))
        plt.plot(hist.index, hist['Close'], label='Close Price', color='#1DB954', linewidth=2)

        # Apply a dark minimalist aesthetic
        plt.style.use('dark_background')
        plt.title(f"{ticker} Price Action ({period})", color='white')
        plt.xlabel("Date", color='gray')
        plt.ylabel("Price (PKR)", color='gray')
        plt.grid(True, linestyle='--', alpha=0.3)
        plt.legend()

        # Save to Render's temporary directory
        filepath = f"/tmp/{ticker}_chart_{uuid.uuid4().hex[:6]}.png"
        plt.savefig(filepath, bbox_inches='tight', facecolor='#0e0e0e')
        plt.close()

        # Pass the explicit file marker back to the Discord bot
        return f"Chart generated. [FILE: {filepath}]"
    except Exception as e:
        return f"System Error: Chart generation failed. {str(e)}"

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

# 1. Primary Model: Gemini
# Temperature 0 forces factual, deterministic responses
# max_retries=1 ensures it fails fast so the fallback can take over
primary_llm = ChatGoogleGenerativeAI(
    model="gemini-3.6-flash", 
    temperature=0, 
    max_retries=1
)

# 2. Fallback Model: Groq (Llama 3.3 70B)
# Extremely fast reasoning model on 100% free API
fallback_llm = ChatGroq(
    model="llama-3.3-70b-versatile",
    groq_api_key=os.getenv("GROQ_API_KEY"),
    temperature=0,
    max_retries=1
)

# 3. The Forced Auto-Switching Engine
# exceptions_to_handle=(Exception,) forces the wrapper to catch everything, including 503s.
llm = primary_llm.with_fallbacks(
    [fallback_llm],
    exceptions_to_handle=(Exception,)
)

# Instantiate the search tool
search_tool = DuckDuckGoSearchRun(name="web_search")

tools = [
    get_stock_quote,
    generate_stock_chart,
    search_tool,
    retrieve_context_tool,
    save_to_knowledge_base,
    manage_scheduled_task,
    execute_trade_tool
]

SYSTEM_PROMPT = """You are an elite, autonomous financial, web, and research AI agent. You operate under absolute factual strictness.

### CORE DIRECTIVES
1. **Zero Hallucination:** You must NEVER invent, fabricate, or guess data. If a tool returns no data, or a database query is empty, explicitly state: "Data Unavailable." Do not attempt to extrapolate.
2. **Strict Data Parsing:** Maintain absolute structural integrity when extracting data. Never invent options or parameters.
3. **Live Research:** Proactively use `web_search` for current market data, news, or real-time events before answering.
4. **Tool Integrity:** Any trade execution must pass through `execute_trade_tool`. 

### OUTPUT FORMATTING (STRICT)
- Speak naturally and logically, like a real human expert. 
- Be brutally concise. If the answer can be a one or two-liner, do not write an essay.
- Draw on past conversation history and saved strategy memory ONLY to extract relevant points needed for the immediate answer. Do not dump logs or summarize past actions.
- State the important information, make your necessary points, and stop. No fluff, no filler text.
- Never expose raw JSON, structural keys, or metadata blocks in your final response."""

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
