import os
import re
from langchain.tools import tool
from langchain_chroma import Chroma
from langchain_google_genai import GoogleGenerativeAIEmbeddings, ChatGoogleGenerativeAI
from langchain_text_splitters import RecursiveCharacterTextSplitter
from youtube_transcript_api import YouTubeTranscriptApi
from dotenv import load_dotenv
from tenacity import retry, wait_exponential, stop_after_attempt
import sqlite3

load_dotenv()

# Initialize embeddings exactly as requested
embeddings = GoogleGenerativeAIEmbeddings(
    model="models/gemini-embedding-001",
    google_api_key=os.getenv("GOOGLE_API_KEY")
)

# Persistent ChromaDB Storage
vector_db = Chroma(
    persist_directory="./vector_db",
    embedding_function=embeddings
)

# Enable SQLite WAL mode to prevent database locking across background sweeps
db_path = os.path.join("./vector_db", "chroma.sqlite3")
if os.path.exists(db_path):
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.close()
    except Exception as e:
        print(f"Warning: Failed to enable SQLite WAL mode on ChromaDB - {str(e)}")

@tool
def ingest_knowledge_tool(input_text_or_url: str) -> str:
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
                # Multi-language extraction logic
                transcript_list = YouTubeTranscriptApi.get_transcript(video_id, languages=['ur', 'en', 'en-US', 'hi'])
                content_to_embed = " ".join([item['text'] for item in transcript_list])
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
            return vector_db.similarity_search(query, k=5)
            
        results = _similarity_search()
        
        if not results:
            return "🧠 **Memory Blank:** I haven't ingested any advice relevant to that question yet."
            
        context = "\n\n".join([doc.page_content for doc in results])
        
        # Update the LLM used for vector retrieval summarization
        llm = ChatGoogleGenerativeAI(
            model="gemini-3.5-flash-lite", # Or gemini-3.7-flash
            google_api_key=os.getenv("GOOGLE_API_KEY")
        )
        
        prompt = f"""
        You are a highly analytical, conversational trading assistant. You speak like a real human mentor.
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

async def evaluate_trade_trigger(trigger_dict: dict) -> str:
    """Evaluates a quantitative trade trigger against current market context."""
    try:
        ticker = trigger_dict.get('ticker')
        
        @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
        def _similarity_search():
            return vector_db.similarity_search(ticker, k=5)
            
        results = await asyncio.to_thread(_similarity_search)
        
        if not results:
            context = f"No recent news or context found for {ticker}."
            print(f"🧠 [Hybrid Gate] RAG Vector Search: No context found for {ticker}.")
        else:
            context = "\n\n".join([doc.page_content for doc in results])
            print(f"🧠 [Hybrid Gate] RAG Vector Search successful for {ticker}. Retrieved {len(results)} chunks.")
            
        llm = ChatGoogleGenerativeAI(
            model="gemini-3.5-flash-lite",
            google_api_key=os.getenv("GOOGLE_API_KEY")
        )
        
        prompt = f"A quantitative portfolio trigger has fired: {trigger_dict}. Here is the latest market context for {ticker}: {context}. Analyze this context and provide a brutally objective 2-sentence verdict on whether to execute the algorithmic trim, or override it and hold due to market conditions."
        
        @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
        def _invoke_llm():
            return llm.invoke(prompt)
            
        response = await asyncio.to_thread(_invoke_llm)
        content = response.content
        if isinstance(content, list):
            text_answer = "".join([item.get("text", "") if isinstance(item, dict) else str(item) for item in content])
        else:
            text_answer = str(content)
            
        print(f"🧠 [Hybrid Gate] LLM Verdict for {ticker}: {text_answer}")
        return text_answer
        
    except Exception as e:
        return f"Error evaluating trade trigger: {str(e)}"
