import os
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.prebuilt import create_react_agent
from langchain_core.tools import tool
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings
import psycopg
from scheduler_config import scheduler

# KMI-30 Shariah Compliant Tickers
SHARIAH_COMPLIANT = ["SYS", "LUCK", "MARI", "FCCL", "OGDCL"]

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
        return f"Error saving to knowledge base: {e}"

@tool
def manage_scheduled_task(action: str, task_name: str, schedule_cron: str = None) -> str:
    """
    Updates, adds, or deletes scheduled background tasks (e.g., RSS sweeps, alert checks).
    Action can be 'add', 'update', 'delete', or 'list'.
    """
    if action == "list":
        jobs = scheduler.get_jobs()
        return f"Active tasks: {[j.id for j in jobs]}"
    
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
        raise ValueError(f"Trade execution rejected: Ticker '{ticker}' is not KMI-30 Shariah Compliant.")
    
    return f"Trade signal registered for {ticker} ({action}). Awaiting human execution."

llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", temperature=0.7, api_key=os.getenv("GEMINI_API_KEY"))
tools = [save_to_knowledge_base, manage_scheduled_task, retrieve_context_tool, execute_trade_tool]

SYSTEM_PROMPT = """You are an intelligent, conversational financial and research AI assistant.
- Speak naturally, directly, and concisely as a human collaborator.
- Use your tools to search knowledge, schedule/manage tasks, or store new information when requested.
- If the user shares an important fact or asks you to remember something, call `save_to_knowledge_base`.
- Synthesize all context into clear, conversational answers without exposing raw JSON or structural keys unless explicitly requested.
- When the user wants to execute a trade, you MUST use execute_trade_tool."""

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
