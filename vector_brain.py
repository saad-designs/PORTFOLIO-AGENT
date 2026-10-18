import os
import re
from langchain.tools import tool
from langchain_google_genai import GoogleGenerativeAIEmbeddings, ChatGoogleGenerativeAI
from langchain_text_splitters import RecursiveCharacterTextSplitter
from youtube_scraper import get_transcript
from dotenv import load_dotenv
from tenacity import retry, wait_exponential, stop_after_attempt
from pydantic import BaseModel, Field
import sqlite3

load_dotenv()

# Initialize embeddings exactly as requested
embeddings = GoogleGenerativeAIEmbeddings(
    model="models/gemini-embedding-001",
    google_api_key=os.getenv("GOOGLE_API_KEY")
)

from langchain_postgres import PGVector
from utils import normalize_ticker
import asyncio
from duckduckgo_search import DDGS

async def execute_live_web_search(query: str):
    print(f"🧠 [Hybrid Gate] RAG Vector Search: Scant context found for {query}. Triggering live web search.")
    
    def _sync_search():
        results = []
        try:
            tool = get_web_search_tool()
            if tool:
                res = tool.invoke(query)
                results.append(res)
            else:
                print("Web search failed: Tavily tool not available.")
        except Exception as e:
            print(f"Web search failed: {e}")
        return results

    # Execute the synchronous scraper in a thread so it doesn't hang the Discord bot
    return await asyncio.to_thread(_sync_search)

def get_web_search_tool():
    """
    Initializes Tavily search tool safely without throwing Pydantic validation errors
    if the API key is missing.
    """
    tavily_key = os.getenv("TAVILY_API_KEY")
    
    if not tavily_key:
        print("[WARNING] TAVILY_API_KEY missing from environment. Web search fallback disabled.")
        return None

    try:
        from langchain_tavily import TavilySearch
        return TavilySearch(max_results=3)
    except Exception as e:
        print(f"[ERROR] Failed to initialize Tavily search: {e}")
        return None

# Persistent Supabase pgvector Storage
db_url = os.getenv("SUPABASE_DB_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
vector_db = PGVector(
    embeddings=embeddings,
    collection_name="financial_knowledge",
    connection=db_url,
    use_jsonb=True,
)

@tool
async def ingest_knowledge_tool(input_text_or_url: str) -> str:
    """Ingests raw financial text, articles, or YouTube URLs into the Vector Brain. Use this autonomously when the user shares knowledge."""
    try:
        content_to_embed = input_text_or_url
        source_id = "text_input"

        # Check if input is a YouTube URL
        youtube_match = re.search(r'(?:v=|youtu\.be/)([^&?]+)', input_text_or_url)
        if youtube_match:
            video_id = youtube_match.group(1)
            source_id = f"youtube_{video_id}"
            try:
                # Direct async audio extraction
                transcript = await get_transcript(video_id)
                if not transcript:
                    return "Error: Could not retrieve transcript for this video."
                content_to_embed = transcript
            except Exception as e:
                # Tool-level exception trapping
                return f"Error: Could not retrieve captions for this video. Captions may be disabled. ({str(e)})"
        
        # Chunking the text
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
        chunks = text_splitter.split_text(content_to_embed)
        
        if not chunks:
            return "Error: No text could be extracted or chunked."
            
        metadatas = [{"source": source_id} for _ in chunks]
        
        # Push to Vector DB
        @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
        def _add_texts():
            vector_db.add_texts(texts=chunks, metadatas=metadatas)
            
        _add_texts()
        
        return f"✅ Successfully ingested and memorized {len(chunks)} chunks of knowledge into the Vector Brain."
        
    except Exception as e:
        return f"Tool Execution Error: ingest_knowledge_tool failed. Details: {str(e)}"


@tool
def query_mentor_brain_tool(query: str) -> str:
    """Queries the AI Vector Brain for past financial mentor advice. Use this when the user asks a question about strategies, market conditions, or mentor insights to ground your reasoning."""
    try:
        @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
        def _similarity_search():
            return vector_db.similarity_search(query, k=6)
            
        results = _similarity_search()
        
        if not results:
            return "🧠 **Memory Blank:** I haven't ingested any advice relevant to that question yet."
            
        context = "\n\n".join([doc.page_content for doc in results])
        
        from model_router import get_langchain_llm, TASK_FAST_INGESTION
        llm = get_langchain_llm(TASK_FAST_INGESTION)
        
        prompt = f"""
        You are a highly analytical, conversational trading assistant. You speak like a real human mentor.
        You are a long-term value investor. You hold compounding businesses indefinitely. You NEVER trim or sell a stock simply because it is in profit. You only recommend a TRIM or SELL if the asset has exceeded its fundamental target price, if the core business thesis is broken, or to clear extreme concentration risk.
        Analyze the Mentor Advice Context provided below (note that transcripts may be in Roman Hindi, Urdu, or English). 
        Synthesize the available insights to answer the user's question. Formulate your answer conversationally—reason with the user and break down complex concepts naturally instead of outputting massive blocks of text.
        Do not hallucinate facts outside the context, but interpret the economic/market commentary provided.

        Mentor Advice Context:
        {context}

        User Question: {query}
        """
        
        @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
        def _invoke_llm():
            return llm.invoke(prompt)
            
        response = _invoke_llm()
        content = response.content
        if isinstance(content, list):
            text_answer = "".join([item.get("text", "") if isinstance(item, dict) else str(item) for item in content])
        else:
            text_answer = str(content)
            
        return text_answer
        
    except Exception as e:
        return f"Tool Execution Error: query_mentor_brain_tool failed. Details: {str(e)}"

import asyncio

class TradeEvaluationSchema(BaseModel):
    step_by_step_analysis: str = Field(description="Step-by-step internal monologue breaking down the logic")
    quantitative_assessment: str = Field(description="Evaluation of DCF Intrinsic Value and fundamental metrics")
    risk_critique_response: str = Field(description="Defense of the trade thesis against core risk parameters")
    ticker: str = Field(description="The normalized ticker symbol (e.g., 'MEBL')")
    action: str = Field(description="Must be 'HOLD', 'SELL', or 'TRIM'")
    trim_percentage: float = Field(description="The percentage of the position to trim (0-100)")
    risk_score_1_to_10: int = Field(description="A subjective risk score from 1 (lowest) to 10 (highest)")
    confidence: float = Field(description="Model's confidence in this decision (0.0 to 1.0)")
    rationale: str = Field(description="Brutally objective 1-2 sentence verdict on the trade action")

async def get_market_context(ticker: str) -> str:
    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
    def _similarity_search():
        import time
        from langchain_core.documents import Document
        
        # Temporal Filter: Get documents within last 72 hours
        seventy_two_hours_ago = int(time.time()) - (72 * 3600)
        filter_dict = {"timestamp": {"$gte": seventy_two_hours_ago}}
        
        try:
            return vector_db.similarity_search(ticker, k=6, filter=filter_dict)
        except Exception as e:
            print(f"Filter failed, falling back to basic search: {e}")
            return vector_db.similarity_search(ticker, k=6)
            
    results = await asyncio.to_thread(_similarity_search)
    
    if not results or len(results) < 2:
        try:
            query = f"{ticker} PSX Pakistan stock market news"
            live_snippets = await execute_live_web_search(query)
            context = f"Live Web Search Snippets:\n{live_snippets}\n"
            if results:
                 context += "\nArchived Vector Context:\n" + "\n\n".join([doc.page_content for doc in results])
        except Exception as e:
            print(f"[WARNING] Live web search query failed for {ticker}: {e}")
            if results:
                 context = "\nArchived Vector Context:\n" + "\n\n".join([doc.page_content for doc in results])
            else:
                 context = "No archived context or live web search data available. Rely only on quantitative metrics provided."
    else:
        context = "\n\n".join([doc.page_content for doc in results])
        print(f"🧠 [Hybrid Gate] RAG Vector Search successful for {ticker}. Retrieved {len(results)} chunks.")
    return context

async def evaluate_trade_trigger(trigger_dict: dict) -> str:
    """Evaluates a quantitative trade trigger against current market context."""
    try:
        raw_ticker = trigger_dict.get('ticker')
        ticker = normalize_ticker(raw_ticker)
        
        context = await get_market_context(ticker)
        
        from model_router import get_langchain_llm, TASK_FAST_INGESTION
        llm = get_langchain_llm(TASK_FAST_INGESTION)
        structured_llm = llm.with_structured_output(TradeEvaluationSchema)
        
        prompt = f"""You are a long-term value investor. You hold compounding businesses indefinitely. You NEVER trim or sell a stock simply because it is in profit. You only recommend a TRIM or SELL if the asset has exceeded its fundamental target price, if the core business thesis is broken, or to clear extreme concentration risk.

### ASSET CLASSIFICATION MANDATE (CRITICAL)
Before evaluating any contextual news or macroeconomic data for a trim/sell decision, you MUST classify the target asset into its correct structural category:

1. YIELD ANCHORS (e.g., HUBC, FFC, EFERT, KAPCO): 
   - Purpose: Held strictly for double-digit dividend cash flow and portfolio stabilization.
   - Trimming Rules: DO NOT recommend trimming or selling based on generic "macro-overvaluation," high P/E multiples, or broad market pullbacks. 
   - Sell Triggers: ONLY recommend trimming if there is a verified, structural threat to their cash flow generation (e.g., sovereign default on circular debt, forced PPA renegotiations, or slashed capacity payments).

2. GROWTH / ALPHA (e.g., SYS):
   - Purpose: Capital appreciation and compounding.
   - Trimming Rules: Highly sensitive to multiple compression. Trimming is acceptable if growth decelerates or extreme overvaluation occurs relative to forward earnings.

3. CYCLICALS (e.g., LUCK):
   - Purpose: Macro-economic leverage.
   - Trimming Rules: Sensitive to interest rates, taxation, and domestic dispatch data. Trimming is acceptable at cycle peaks.

DIRECTIVE: If the ticker being evaluated is a YIELD ANCHOR and the negative context is merely "overvaluation" or "market noise," you must IGNORE the noise and explicitly rule: "NO ACTION - Asset is a Yield Anchor insulated by dividend cash flow."

A quantitative portfolio trigger has fired: {trigger_dict}. Here is the latest market context for {ticker}: {context}. 
Analyze this context, evaluate the quantitative metrics, defend against risks, and provide a brutally objective verdict on whether to execute the algorithmic trim, or override it and hold due to market conditions."""
        
        @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
        def _invoke_llm():
            return structured_llm.invoke(prompt)
            
        response = await asyncio.to_thread(_invoke_llm)
        
        print(f"🧠 [Hybrid Gate] LLM Verdict for {ticker}: {response.model_dump_json()}")
        return response.model_dump_json()
        
    except Exception as e:
        return f'{{"error": "Error evaluating trade trigger: {str(e)}"}}'
