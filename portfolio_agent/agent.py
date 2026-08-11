import os
from typing import TypedDict
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.types import interrupt
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings
import psycopg

class AgentState(TypedDict):
    tickers: list[str]
    analysis: str
    decision: str
    user_feedback: str

# KMI-30 Shariah Compliant Tickers
SHARIAH_COMPLIANT = ["SYS", "LUCK", "MARI", "FCCL", "OGDCL"]
# Non-compliant / Conventional banking / Debt
NON_COMPLIANT = ["UBL", "MCB", "HBL"]

def filter_tickers(state: AgentState):
    """Hardcode a filter that drops any ticker associated with conventional banking or non-compliant debt."""
    filtered = [t for t in state.get("tickers", []) if t in SHARIAH_COMPLIANT and t not in NON_COMPLIANT]
    return {"tickers": filtered}

def retrieve_context(query: str) -> str:
    """Generate vector embedding and run Cosine Distance vector search against agent_knowledge."""
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
                ORDER BY embedding <=> %s::vector 
                LIMIT 3;
            """, (str(query_embedding),)).fetchall()
            
            context_pieces = []
            for row in res:
                content, source_type, source_url = row
                context_pieces.append(f"Source ({source_type}): {source_url}\n{content}")
            return "\n\n".join(context_pieces)
    except Exception as e:
        print(f"Error retrieving context: {e}")
        return ""

def analyze_sentiment(state: AgentState):
    """Extract sentiment and actionable tickers using gemini-2.5-flash."""
    query = " ".join(state.get("tickers", []))
    context = retrieve_context(query)
    
    prompt = f"Context:\n{context}\n\nAnalyzed {state.get('tickers', [])}"
    # In a full implementation, this uses langchain-google-genai
    # llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", api_key=os.getenv("GEMINI_API_KEY"))
    return {"analysis": prompt, "decision": "buy signal detected"}

def human_approval(state: AgentState):
    """The LangGraph agent MUST NEVER execute a trade. When a structural floor or buy/sell signal is detected, the agent must trigger LangGraph's interrupt() function."""
    if "buy" in state.get("decision", "").lower() or "sell" in state.get("decision", "").lower():
        # Trigger LangGraph's interrupt function and save state
        feedback = interrupt(f"Action required for decision: {state['decision']}")
        return {"user_feedback": feedback}
    return {"user_feedback": "None"}

def execute_trade(state: AgentState):
    """Executes trade based on human in the loop feedback."""
    if state.get("user_feedback") == "Approve Trade":
        return {"decision": "Trade Executed"}
    elif state.get("user_feedback") == "Deny & Recalculate":
        return {"decision": "Trade Denied and Recalculated"}
    return {"decision": "No action taken"}

workflow = StateGraph(AgentState)
workflow.add_node("filter", filter_tickers)
workflow.add_node("analyze", analyze_sentiment)
workflow.add_node("human_in_the_loop", human_approval)
workflow.add_node("execute", execute_trade)

workflow.add_edge(START, "filter")
workflow.add_edge("filter", "analyze")
workflow.add_edge("analyze", "human_in_the_loop")
workflow.add_edge("human_in_the_loop", "execute")
workflow.add_edge("execute", END)

def get_compiled_graph():
    """Connect to a Supabase PostgreSQL instance using psycopg[binary]. Configure pgvector and langgraph-checkpoint-postgres."""
    db_url = os.getenv("SUPABASE_DB_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
    try:
        # Note: psycopg3 is used by langgraph-checkpoint-postgres
        conn = psycopg.connect(db_url, autocommit=True)
        
        # Initialize vector extension and knowledge table
        conn.execute("""
        CREATE EXTENSION IF NOT EXISTS vector;

        CREATE TABLE IF NOT EXISTS agent_knowledge (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            source_type TEXT NOT NULL,
            source_url TEXT,
            content TEXT NOT NULL,
            embedding VECTOR(768),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        checkpointer = PostgresSaver(conn)
        checkpointer.setup()
        return workflow.compile(checkpointer=checkpointer)
    except Exception as e:
        print(f"Warning: Could not connect to Postgres ({e}). Compiling without checkpointer.")
        return workflow.compile()

graph = get_compiled_graph()
