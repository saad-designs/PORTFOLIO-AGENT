import time
import logging
from langchain_google_genai import ChatGoogleGenerativeAI
from vector_brain import ingest_knowledge_tool
# Assuming you have a scraper function in ingestion.py, e.g., get_youtube_transcript
from ingestion import get_youtube_transcript 

logger = logging.getLogger("HistoricalBackfill")

# Use Flash-Lite for fast, cheap extraction of strategies from massive texts
extraction_llm = ChatGoogleGenerativeAI(model="gemini-3.5-flash-lite")

def extract_techniques_from_raw_text(raw_text: str, mentor_name: str) -> str:
    """Forces the LLM to discard outdated news and only extract timeless strategies."""
    prompt = f"""
    You are an expert financial strategist analyzing historical content from {mentor_name}.
    Analyze the following raw transcript/post. 
    
    CRITICAL INSTRUCTIONS:
    1. IGNORE all specific stock tips, price targets, or market news from the past.
    2. EXTRACT ONLY timeless investing techniques, psychological rules, risk management models, and market prediction frameworks.
    3. Format the output as a dense, highly factual summary of the core strategies.
    
    Raw Text: {raw_text[:8000]} # Truncated to fit context window safely
    """
    try:
        response = extraction_llm.invoke(prompt)
        return response.content
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        return ""

def batch_process_historical_content(video_urls: list, mentor_name: str):
    """Safely iterates through historical links without triggering IP bans."""
    print(f"Initiating Deep Learning on historical archives for {mentor_name}...")
    
    for index, url in enumerate(video_urls):
        print(f"[{index+1}/{len(video_urls)}] Downloading transcript for {url}")
        
        try:
            # 1. Scrape the raw text
            raw_transcript = get_youtube_transcript(url)
            
            if not raw_transcript:
                continue
                
            # 2. Distill it into pure technique
            strategic_model = extract_techniques_from_raw_text(raw_transcript, mentor_name)
            
            if strategic_model:
                # 3. Inject the clean, timeless technique into the Vector Brain
                final_payload = f"MENTOR STRATEGY | Source: {mentor_name} | Concept: {strategic_model}"
                ingest_knowledge_tool.invoke({"text": final_payload})
                print(f"Strategy learned and embedded.")
            
            # 4. Mandatory API Throttling to prevent IP bans and API limits
            time.sleep(15) 
            
        except Exception as e:
            print(f"Failed processing {url}: {str(e)}")
            time.sleep(30) # Longer cooldown on failure

if __name__ == "__main__":
    # Example Batch: Do not run 500 links at once. Run them in batches of 10-20.
    sarmaaya_masterclass_links = [
        "https://youtu.be/UbydEmCjwYM?si=8e-hHNtLSiGdPNR7",
        "https://youtu.be/cTzijhWLeNY?si=3ioqXNKSe7igJNp6",
        "https://youtu.be/WgBCU0piLfU?si=jXLieZcBcUWgFeX0",
        "https://youtu.be/6t-630XKm1I?si=0wwxudbP1wSE0Uoo",
        "https://youtu.be/ly7w9G7k1CY?si=8tbZgTm2C2or9Tzk",
        "https://youtu.be/QMEPGEvmBEU?si=YWoW_H3AHmF68gEd",
        "https://youtu.be/nulFMLdINgE?si=2LhQD9b0pfd0dlDz",
        "https://youtu.be/OZhPaGkJJpM?si=igfODJ2L0oHDzUE2",
        "https://youtu.be/bs5Ioe7BJLM?si=z_Bj5aTAXsv9a1pJ",
        "https://youtu.be/zrAbxXEDWSs?si=kqCyeSkEOpAhjv_x",
        "https://youtu.be/cybhGEywndI?si=UidHmXU-Cz5ggUuj",
        "https://youtu.be/haKQrovZMXA?si=qcru5HB5OBlnUidb",
        "https://youtu.be/0EBArgKvNHU?si=bWySplnC6UN9lY45"
    ]
    
    batch_process_historical_content(sarmaaya_masterclass_links, "Sarmaaya Financials")
