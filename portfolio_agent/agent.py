import os
from typing import TypedDict
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.types import interrupt
from langchain_google_genai import ChatGoogleGenerativeAI
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

def analyze_sentiment(state: AgentState):
    """Extract sentiment and actionable tickers using gemini-2.5-flash."""
    # In a full implementation, this uses langchain-google-genai
    # llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", api_key=os.getenv("GEMINI_API_KEY"))
    return {"analysis": f"Analyzed {state.get('tickers', [])}", "decision": "buy signal detected"}

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
        checkpointer = PostgresSaver(conn)
        checkpointer.setup()
        return workflow.compile(checkpointer=checkpointer)
    except Exception as e:
        print(f"Warning: Could not connect to Postgres ({e}). Compiling without checkpointer.")
        return workflow.compile()
