import os
import sys
import time
import re
import json
import asyncio
from typing import List, Dict, Any, Optional, Union

# --- API KEY ENVIRONMENT SANITIZATION ---
# Prevent Google GenAI and LangChain from spamming:
# "Both GOOGLE_API_KEY and GEMINI_API_KEY are set. Using GOOGLE_API_KEY."
if os.getenv("GEMINI_API_KEY") and not os.getenv("GOOGLE_API_KEY"):
    os.environ["GOOGLE_API_KEY"] = os.getenv("GEMINI_API_KEY")
os.environ.pop("GEMINI_API_KEY", None)

# --- GROQ CLIENT INITIALIZATION ---
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
groq_client = None

def get_groq_client():
    global groq_client
    if groq_client is not None:
        return groq_client
    key = os.getenv("GROQ_API_KEY")
    if key and key.strip():
        try:
            from groq import Groq
            groq_client = Groq(api_key=key.strip())
            return groq_client
        except Exception as e:
            print(f"⚠️ [Model Router] Groq client initialization failed: {e}")
    return None

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
TASK_MULTIMODAL = "TASK_MULTIMODAL"

# Groq text models: High-speed free text models.
GROQ_TEXT_MODELS: List[str] = [
    "llama-3.3-70b-versatile",     # Llama 3.3 70B
    "llama-3.1-8b-instant",        # Llama 3.1 8B instant
    "openai/gpt-oss-120b",         # High-capacity open weights on Groq
    "openai/gpt-oss-20b",          # High-speed open weights on Groq
    "qwen/qwen3.8-27b"             # Fast reasoning open weights on Groq
]

# Gemini fallback cascade:
GEMINI_TASK_ROUTING: Dict[str, List[str]] = {
    TASK_FAST_INGESTION: [
        "gemini-3.5-flash-lite",
        "gemini-3.5-flash"
    ],
    TASK_DEEP_REASONING: [
        "gemini-3.8-flash",
        "gemini-3.5-flash",
        "gemini-3.5-flash-lite"
    ],
    TASK_MULTIMODAL: [
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
    Returns candidate Gemini models for a given task, dynamically filtering out
    any model currently in MODEL_COOLDOWN_UNTIL.
    """
    models = GEMINI_TASK_ROUTING.get(task, GEMINI_TASK_ROUTING[TASK_FAST_INGESTION])
    now = time.time()
    available = [m for m in models if MODEL_COOLDOWN_UNTIL.get(m, 0.0) <= now]
    
    if available:
        return available

    soonest = min(models, key=lambda m: MODEL_COOLDOWN_UNTIL.get(m, 0.0))
    remaining = max(0, int(MODEL_COOLDOWN_UNTIL.get(soonest, 0.0) - now))
    print(f"⚠️ [Circuit Breaker] All Gemini models in cooldown for {task}. Soonest will be '{soonest}' in {remaining}s.")
    return [soonest]

def record_model_error(model: str, error: Any):
    """
    Inspects an error thrown by Groq or Gemini and sets circuit breaker cooldowns:
    - 404 NOT_FOUND: 24-hour disable
    - 429 Daily Limit (GenerateRequestsPerDay or retryDelay > 120s): 1-hour (3600s) cooldown
    - 429 RPM/TPM Limit: 45s cooldown
    """
    err_str = str(error)
    err_lower = err_str.lower()
    now = time.time()

    # 401 Unauthorized / Invalid API Key
    if "401" in err_lower or "invalid_api_key" in err_lower or "invalid api key" in err_lower or "unauthorized" in err_lower:
        MODEL_COOLDOWN_UNTIL[model] = now + 86400.0
        if any(m in model for m in ["llama", "groq", "gpt-oss", "qwen", "whisper"]):
            for gm in GROQ_TEXT_MODELS + ["whisper-large-v3-turbo", "whisper-large-v3"]:
                MODEL_COOLDOWN_UNTIL[gm] = now + 86400.0
            print(f"🚫 [Circuit Breaker] Groq API key is invalid or unauthorized (401). Disabling Groq models; cascading cleanly to Gemini.")
        else:
            print(f"🚫 [Circuit Breaker] Model '{model}' authorization failed (401). Cooldown set for 24 hours.")
        return

    # 404 NOT_FOUND / decommissioned
    if "404" in err_lower or "not_found" in err_lower or "model_not_found" in err_lower or "decommissioned" in err_lower:
        MODEL_COOLDOWN_UNTIL[model] = now + 86400.0
        print(f"🚫 [Circuit Breaker] Model '{model}' not found / decommissioned. Cooldown set for 24 hours.")
        return

    # 429 RESOURCE_EXHAUSTED / Quota limits / Rate limits
    if any(k in err_lower for k in ["429", "resource_exhausted", "quota", "quota exceeded", "rate_limit_exceeded"]):
        delay_match = re.search(r'retrydelay[^\d]*(\d+)', err_lower)
        retry_delay = int(delay_match.group(1)) if delay_match else 0

        is_daily = any(k in err_lower for k in [
            "generaterequestsperday", "perday", "daily", "freetier", "limit: 20", "free_tier_requests"
        ]) or (retry_delay > 120)

        if is_daily:
            MODEL_COOLDOWN_UNTIL[model] = now + 3600.0
            print(f"🛑 [Circuit Breaker] Model '{model}' DAILY quota exhausted (retryDelay={retry_delay}s). Cooldown set for 3600s (1h). Subsequent calls will skip in 0ms.")
        else:
            MODEL_COOLDOWN_UNTIL[model] = now + 45.0
            print(f"⏳ [Circuit Breaker] Model '{model}' rate limit hit. Cooldown set for 45s.")

# --- DEDICATED GROQ AUDIO TRANSCRIPTION ---
def transcribe_audio_groq(
    audio_source: Any,
    filename: str = "audio.m4a",
    mime_type: str = "audio/mp4"
) -> Optional[str]:
    """
    Dedicated speech-to-text via Groq whisper-large-v3-turbo.
    Accepts:
    - file path (str)
    - raw bytes / bytearray
    - open file-like object
    Returns transcript string on success, or None on failure/missing key.
    """
    client = get_groq_client()
    if not client:
        return None

    whisper_models = ["whisper-large-v3-turbo", "whisper-large-v3"]
    for model_name in whisper_models:
        if not is_model_available(model_name):
            continue
        try:
            print(f"🎙️ [Groq Whisper] Transcribing audio with '{model_name}'...")
            if isinstance(audio_source, str) and os.path.exists(audio_source):
                with open(audio_source, "rb") as f:
                    transcript = client.audio.transcriptions.create(
                        model=model_name,
                        file=f,
                        response_format="text"
                    )
            elif isinstance(audio_source, (bytes, bytearray)):
                safe_fname = filename or "audio.m4a"
                safe_mime = (mime_type or "audio/mp4").split(";")[0].strip()
                transcript = client.audio.transcriptions.create(
                    model=model_name,
                    file=(safe_fname, bytes(audio_source), safe_mime),
                    response_format="text"
                )
            elif hasattr(audio_source, "read"):
                transcript = client.audio.transcriptions.create(
                    model=model_name,
                    file=audio_source,
                    response_format="text"
                )
            else:
                return None

            text = str(transcript).strip() if transcript else ""
            if text:
                print(f"✅ [Groq Whisper] Audio successfully transcribed ({len(text)} chars).")
                return text
        except Exception as e:
            record_model_error(model_name, e)
            print(f"⚠️ [Groq Whisper] '{model_name}' failed: {e}")

    return None

# --- GROQ TEXT COMPLETION ---
def generate_text_groq(prompt: str, system_prompt: Optional[str] = None) -> Optional[str]:
    """
    High-speed free text completion via Groq (llama-3.3-70b-versatile -> llama-3.1-8b-instant).
    Returns generated string or None on failure.
    """
    client = get_groq_client()
    if not client:
        return None

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    for model_name in GROQ_TEXT_MODELS:
        if not is_model_available(model_name):
            continue
        try:
            resp = client.chat.completions.create(
                model=model_name,
                messages=messages,
                temperature=0.2
            )
            if resp.choices and resp.choices[0].message and resp.choices[0].message.content:
                text = resp.choices[0].message.content.strip()
                if text:
                    return text
        except Exception as e:
            record_model_error(model_name, e)
            print(f"⚠️ [Groq Text] '{model_name}' failed: {e}")

    return None

def is_pure_text_contents(contents: Any) -> bool:
    """Checks whether contents contains only plain text strings (no binary Part or File objects)."""
    if isinstance(contents, str):
        return True
    if isinstance(contents, list):
        return all(isinstance(c, str) for c in contents)
    return False

def extract_prompt_string(contents: Any) -> str:
    """Combines list of strings or single string into unified prompt text."""
    if isinstance(contents, str):
        return contents
    if isinstance(contents, list):
        return "\n\n".join(str(c) for c in contents if isinstance(c, str))
    return str(contents)

# --- UNIFIED MULTI-PROVIDER GENERATION CASCADE ---
async def generate_with_fallback(client, contents: list, task: str = TASK_FAST_INGESTION) -> str:
    """
    Multi-provider generation with circuit breaker and quota preservation:
    1. For text-only TASK_FAST_INGESTION: Routes first to Groq (llama-3.3-70b-versatile -> llama-3.1-8b-instant).
    2. Falls back to Gemini cascade (gemini-3.5-flash-lite -> gemini-3.5-flash).
    3. Multimodal contents (images, PDFs, video URLs) and TASK_DEEP_REASONING route strictly
       to Gemini (gemini-3.8-flash -> gemini-3.5-flash -> gemini-3.5-flash-lite).
    """
    is_text = is_pure_text_contents(contents)

    # 1. Text-only FAST_INGESTION offloaded to Groq to protect Google 20 RPD free tier quota
    if task == TASK_FAST_INGESTION and is_text:
        prompt_str = extract_prompt_string(contents)
        groq_result = await asyncio.to_thread(generate_text_groq, prompt_str)
        if groq_result:
            return groq_result
        print("ℹ️ [Model Router] Groq text models unavailable or exhausted. Falling back to Gemini cascade...")

    # 2. Gemini Cascade
    effective_task = task
    if not is_text and task == TASK_FAST_INGESTION:
        effective_task = TASK_MULTIMODAL

    candidates = get_candidate_models(effective_task)
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
                    print(f"⚠️ [Model Router] '{model_name}' entered cooldown after error. Moving to next candidate...")
                    break

                if attempt < max_attempts - 1:
                    await asyncio.sleep(2 * (attempt + 1))

    raise RuntimeError(f"❌ All models (Groq & Gemini) failed in generate_with_fallback for {task}. Last error: {repr(last_error)}")

# --- LANGCHAIN LLM ROUTER ---
def get_langchain_llm(task: str = TASK_DEEP_REASONING, **kwargs):
    """
    Builds a LangChain chat model with multi-provider fallbacks:
    - TASK_FAST_INGESTION: ChatGroq (gpt-oss-120b / llama-3.3) -> ChatGoogleGenerativeAI (gemini-3.5-flash-lite).
    - TASK_DEEP_REASONING: ChatGoogleGenerativeAI (gemini-3.8-flash -> gemini-3.5-flash -> gemini-3.5-flash-lite).
    """
    from langchain_google_genai import ChatGoogleGenerativeAI
    google_key = os.getenv("GOOGLE_API_KEY")
    groq_key = os.getenv("GROQ_API_KEY")

    if task == TASK_FAST_INGESTION and groq_key:
        try:
            from langchain_groq import ChatGroq
            groq_llms = []
            for g_model in GROQ_TEXT_MODELS:
                if is_model_available(g_model):
                    groq_llms.append(ChatGroq(model=g_model, groq_api_key=groq_key.strip(), **kwargs))

            gemini_candidates = get_candidate_models(TASK_FAST_INGESTION)
            gemini_llms = [
                ChatGoogleGenerativeAI(model=m, google_api_key=google_key, **kwargs)
                for m in gemini_candidates
            ]

            all_llms = groq_llms + gemini_llms
            if all_llms:
                if len(all_llms) == 1:
                    return all_llms[0]
                return all_llms[0].with_fallbacks(all_llms[1:])
        except Exception as e:
            print(f"⚠️ [Model Router] Failed building Groq LangChain LLM: {e}")

    # Fallback / Deep reasoning on Gemini
    candidates = get_candidate_models(task)
    gemini_llms = [
        ChatGoogleGenerativeAI(model=m, google_api_key=google_key, **kwargs)
        for m in candidates
    ]

    if not gemini_llms:
        fallback_model = GEMINI_TASK_ROUTING[task][0]
        return ChatGoogleGenerativeAI(model=fallback_model, google_api_key=google_key, **kwargs)

    if len(gemini_llms) == 1:
        return gemini_llms[0]

    return gemini_llms[0].with_fallbacks(gemini_llms[1:])
