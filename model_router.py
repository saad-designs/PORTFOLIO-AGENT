import os
import sys
import time
import re
import asyncio
from typing import List, Dict, Any, Optional

# --- API KEY ENVIRONMENT SANITIZATION ---
# Prevent Google GenAI and LangChain from spamming:
# "Both GOOGLE_API_KEY and GEMINI_API_KEY are set. Using GOOGLE_API_KEY."
if os.getenv("GEMINI_API_KEY") and not os.getenv("GOOGLE_API_KEY"):
    os.environ["GOOGLE_API_KEY"] = os.getenv("GEMINI_API_KEY")
os.environ.pop("GEMINI_API_KEY", None)

import json

# --- ACTIVE PSX TICKERS & PRE-LLM NOISE FILTER ---
ACTIVE_PSX_TICKERS: set[str] = set()

def load_psx_tickers():
    global ACTIVE_PSX_TICKERS
    cache_path = os.path.join(os.path.dirname(__file__), "psx_tickers_cache.json")
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    ACTIVE_PSX_TICKERS = set(str(v).upper() for v in data.values())
        except Exception as e:
            print(f"⚠️ [Model Router] Error loading psx_tickers_cache.json: {e}")
            
    if not ACTIVE_PSX_TICKERS:
        # High-liquidity PSX fallbacks
        ACTIVE_PSX_TICKERS = {
            "SYS", "LUCK", "HBL", "MCB", "UBL", "MEBL", "ENGRO", "EFERT", "FFC",
            "OGDC", "PPL", "POL", "HUBC", "PSO", "PRL", "ATRL", "DGKC", "MLCF",
            "CHCC", "ILP", "TRG", "AVN", "SAZEW", "MARI", "BAFL", "BAHL", "KAPCO"
        }

load_psx_tickers()

FINANCIAL_KEYWORDS: set[str] = {
    "buy", "sell", "tp", "sl", "target", "accum", "div", "eps", "ktrade",
    "profit", "loss", "dividend", "yield", "dcf", "kmi", "kmi30", "kse",
    "kse100", "entry", "exit", "hold", "call", "long", "short", "stop"
}

def is_financial_or_signal_text(text: str, has_media: bool = False) -> bool:
    """
    Zero-Cost Pre-LLM Noise Filter:
    If not has_media and text is short (< 30 chars), checks if it contains
    any active PSX ticker symbol or financial keyword.
    Returns False if pure chitchat / noise ('ok', 'thanks', etc.) with 0 LLM calls!
    """
    if has_media:
        return True
    clean = text.strip()
    if not clean:
        return False
    if len(clean) >= 30:
        return True

    # Check financial keywords
    words_lower = set(re.findall(r'\b[a-zA-Z0-9]+\b', clean.lower()))
    if any(k in words_lower for k in FINANCIAL_KEYWORDS):
        return True

    # Check active PSX tickers
    words_upper = set(re.findall(r'\b[a-zA-Z0-9]+\b', clean.upper()))
    if any(w in ACTIVE_PSX_TICKERS for w in words_upper):
        return True

    return False

# --- TASK ROUTING CONSTANTS ---
TASK_FAST_INGESTION = "TASK_FAST_INGESTION"
TASK_DEEP_REASONING = "TASK_DEEP_REASONING"

# Clean models list: gemini-2.5-flash and gemini-2.5-flash-lite are strictly eliminated
TASK_ROUTING: Dict[str, List[str]] = {
    # High-volume tasks: starts with 3.5-flash-lite so 20 RPD quota of 3.8-flash is never burned on ingestion
    TASK_FAST_INGESTION: [
        "gemini-3.5-flash-lite",
        "gemini-3.5-flash",
        "gemini-3.8-flash"
    ],
    # High-reasoning tasks: desk manager synthesis, analyst_node, risk_manager_node
    TASK_DEEP_REASONING: [
        "gemini-3.8-flash",
        "gemini-3.5-flash",
        "gemini-3.5-flash-lite"
    ]
}

# --- IN-MEMORY CIRCUIT BREAKER ---
# Maps model_name -> timestamp until which model is on cooldown
MODEL_COOLDOWN_UNTIL: Dict[str, float] = {}

def is_model_available(model: str) -> bool:
    """Returns True if the model is currently active and not in cooldown."""
    cooldown_until = MODEL_COOLDOWN_UNTIL.get(model, 0.0)
    return time.time() >= cooldown_until

def get_candidate_models(task: str = TASK_FAST_INGESTION) -> List[str]:
    """
    Returns candidate models for a given task, dynamically filtering out
    any model that is currently in MODEL_COOLDOWN_UNTIL.
    """
    models = TASK_ROUTING.get(task, TASK_ROUTING[TASK_FAST_INGESTION])
    now = time.time()
    available = [m for m in models if MODEL_COOLDOWN_UNTIL.get(m, 0.0) <= now]
    
    if available:
        return available

    # If all models are in cooldown, find the one with the soonest expiring cooldown as a desperate fallback
    soonest = min(models, key=lambda m: MODEL_COOLDOWN_UNTIL.get(m, 0.0))
    remaining = max(0, int(MODEL_COOLDOWN_UNTIL.get(soonest, 0.0) - now))
    print(f"⚠️ [Circuit Breaker] All models in cooldown for {task}. Soonest available will be '{soonest}' in {remaining}s.")
    return [soonest]

def record_model_error(model: str, error: Any):
    """
    Inspects an error thrown by Gemini and sets circuit breaker cooldowns:
    - 404 NOT_FOUND: 24-hour disable (model dead/unavailable on key)
    - 429 Daily Limit (GenerateRequestsPerDay or retryDelay > 120s): 1-hour (3600s) cooldown
    - 429 RPM Limit (GenerateRequestsPerMinute): 45s cooldown
    """
    err_str = str(error)
    err_lower = err_str.lower()
    now = time.time()

    # 404 NOT_FOUND
    if "404" in err_lower or "not_found" in err_lower:
        MODEL_COOLDOWN_UNTIL[model] = now + 86400.0
        print(f"🚫 [Circuit Breaker] Model '{model}' returned 404 NOT FOUND. Cooldown set for 24 hours.")
        return

    # 429 RESOURCE_EXHAUSTED / Quota limits
    if any(k in err_lower for k in ["429", "resource_exhausted", "quota", "quota exceeded"]):
        # Parse retryDelay if present
        delay_match = re.search(r'retrydelay[^\d]*(\d+)', err_lower)
        retry_delay = int(delay_match.group(1)) if delay_match else 0

        is_daily = any(k in err_lower for k in [
            "generaterequestsperday", "perday", "daily", "freetier", "limit: 20", "free_tier_requests"
        ]) or (retry_delay > 120)

        if is_daily:
            MODEL_COOLDOWN_UNTIL[model] = now + 3600.0
            print(f"🛑 [Circuit Breaker] Model '{model}' DAILY quota exhausted (retryDelay={retry_delay}s). Cooldown set for 3600s (1h). Subsequent calls will skip in 0ms without making HTTP requests.")
        else:
            MODEL_COOLDOWN_UNTIL[model] = now + 45.0
            print(f"⏳ [Circuit Breaker] Model '{model}' RPM limit hit. Cooldown set for 45s.")

async def generate_with_fallback(client, contents: list, task: str = TASK_FAST_INGESTION) -> str:
    """
    Executes Gemini generation with circuit-breaker aware model cascading.
    Skips dead or cooling down models in 0ms.
    """
    candidates = get_candidate_models(task)
    last_error = None
    now = time.time()

    for model_name in candidates:
        if not is_model_available(model_name):
            remaining = int(MODEL_COOLDOWN_UNTIL.get(model_name, 0.0) - now)
            print(f"⚡ [Circuit Breaker] Skipping '{model_name}' (cooling down for {remaining}s more).")
            continue

        max_attempts = 2
        for attempt in range(max_attempts):
            try:
                chat = client.chats.create(model=model_name)
                response = await asyncio.to_thread(chat.send_message, contents)
                return response.text.strip()
            except Exception as e:
                last_error = e
                record_model_error(model_name, e)

                # If error put model into cooldown, stop retrying this model immediately
                if not is_model_available(model_name):
                    print(f"⚠️ [Model Router] '{model_name}' entered cooldown after error. Moving immediately to next candidate...")
                    break

                if attempt < max_attempts - 1:
                    await asyncio.sleep(2 * (attempt + 1))

    raise RuntimeError(f"❌ All Gemini models failed in generate_with_fallback for {task}. Last error: {repr(last_error)}")

def get_langchain_llm(task: str = TASK_DEEP_REASONING, **kwargs):
    """
    Builds a LangChain ChatGoogleGenerativeAI instance or fallback chain using
    currently active (non-cooled-down) models for the given task.
    """
    from langchain_google_genai import ChatGoogleGenerativeAI
    api_key = os.getenv("GOOGLE_API_KEY")
    candidates = get_candidate_models(task)

    llms = [
        ChatGoogleGenerativeAI(model=m, google_api_key=api_key, **kwargs)
        for m in candidates
    ]

    if not llms:
        fallback_model = TASK_ROUTING[task][0]
        return ChatGoogleGenerativeAI(model=fallback_model, google_api_key=api_key, **kwargs)

    if len(llms) == 1:
        return llms[0]

    primary = llms[0]
    return primary.with_fallbacks(llms[1:])
