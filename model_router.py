import os
import sys
# Force UTF-8 stdout on Windows cp1252 consoles
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')
import time
import re
import json
import asyncio
from typing import List, Dict, Any, Optional, Union

# --- API KEY ENVIRONMENT SANITIZATION & MULTI-KEY ROTATION ---
# Prevent Google GenAI and LangChain from spamming duplicate key warning
if os.getenv("GEMINI_API_KEY") and not os.getenv("GOOGLE_API_KEY"):
    os.environ["GOOGLE_API_KEY"] = os.getenv("GEMINI_API_KEY")
os.environ.pop("GEMINI_API_KEY", None)

CURRENT_GOOGLE_KEY_INDEX: int = 0
GOOGLE_KEY_COOLDOWN_UNTIL: Dict[str, float] = {}

def get_google_api_keys() -> List[str]:
    """Returns list of configured Google API keys in priority order (GOOGLE_API_KEY, GOOGLE_API_KEY_2, GOOGLE_API_KEY_3)."""
    keys = []
    primary = os.getenv("GOOGLE_API_KEY")
    if primary and primary.strip():
        cleaned = primary.strip().strip('"').strip("'").strip()
        if cleaned:
            keys.append(cleaned)
    for env_var in ["GOOGLE_API_KEY_2", "GOOGLE_API_KEY_3", "GOOGLE_API_KEY_BACKUP"]:
        val = os.getenv(env_var)
        if val and val.strip():
            cleaned = val.strip().strip('"').strip("'").strip()
            if cleaned and cleaned not in keys:
                keys.append(cleaned)
    return keys

def get_active_google_api_key() -> Optional[str]:
    """Returns the currently active Google API key, rotating if in cooldown."""
    global CURRENT_GOOGLE_KEY_INDEX
    keys = get_google_api_keys()
    if not keys:
        return None
    now = time.time()
    current_key = keys[CURRENT_GOOGLE_KEY_INDEX % len(keys)]
    if GOOGLE_KEY_COOLDOWN_UNTIL.get(current_key, 0.0) <= now:
        return current_key

    # Search for an available non-cooling key
    for idx, k in enumerate(keys):
        if GOOGLE_KEY_COOLDOWN_UNTIL.get(k, 0.0) <= now:
            CURRENT_GOOGLE_KEY_INDEX = idx
            print(f"🔄 [Key Rotation] Active key in cooldown. Rotated to Google API Key #{idx + 1}")
            return k

    # If all keys are in cooldown, return the one that recovers soonest
    soonest_key = min(keys, key=lambda k: GOOGLE_KEY_COOLDOWN_UNTIL.get(k, 0.0))
    return soonest_key

def rotate_to_next_google_key(reason: str = "Quota Exhausted") -> Optional[str]:
    """Forces rotation to the next available backup Google API key and resets model cooldowns for the new key."""
    global CURRENT_GOOGLE_KEY_INDEX
    keys = get_google_api_keys()
    if len(keys) <= 1:
        return None

    now = time.time()
    old_key = keys[CURRENT_GOOGLE_KEY_INDEX % len(keys)]
    GOOGLE_KEY_COOLDOWN_UNTIL[old_key] = now + 3600.0  # 1 hour cooldown for exhausted key

    old_idx = CURRENT_GOOGLE_KEY_INDEX
    CURRENT_GOOGLE_KEY_INDEX = (CURRENT_GOOGLE_KEY_INDEX + 1) % len(keys)
    new_key = keys[CURRENT_GOOGLE_KEY_INDEX]

    # Reset Gemini model cooldowns for the fresh key (excluding permanently blacklisted 404 strings)
    for model_list in GEMINI_TASK_ROUTING.values():
        for m in model_list:
            if m not in BLACKLISTED_MODELS:
                MODEL_COOLDOWN_UNTIL.pop(m, None)

    print(f"🔄 [Key Rotation] Google Key #{old_idx + 1} marked on cooldown ({reason}). Rotated to Key #{CURRENT_GOOGLE_KEY_INDEX + 1} (quota refreshed).")
    return new_key

# --- GROQ CLIENT INITIALIZATION ---
groq_client = None
_groq_models_probed = False
PROTECTED_GROQ_MODELS = {"llama-3.3-70b-versatile", "llama-3.1-8b-instant"}

def get_groq_client():
    global groq_client, _groq_models_probed
    if not is_model_available("groq"):
        return None
    if groq_client is not None:
        return groq_client
    key = os.getenv("GROQ_API_KEY")
    if key and key.strip():
        try:
            from groq import Groq
            groq_client = Groq(api_key=key.strip())
            if not _groq_models_probed:
                _groq_models_probed = True
                try:
                    live_models = {m.id for m in groq_client.models.list().data if getattr(m, "id", None)}
                    if live_models:
                        for candidate in list(GROQ_TEXT_MODELS):
                            if candidate not in live_models and candidate not in PROTECTED_GROQ_MODELS:
                                BLACKLISTED_MODELS.add(candidate)
                                print(f"ℹ️ [Groq Init] Model '{candidate}' not in active Groq catalog; pre-blacklisted cleanly.", flush=True)
                except Exception as probe_err:
                    # Do NOT pre-blacklist llama-3.3-70b-versatile or llama-3.1-8b-instant on boot if models.list() fails
                    status_code = getattr(probe_err, "status_code", None)
                    if status_code == 401 or "invalid_api_key" in str(probe_err).lower():
                        record_model_error("groq", probe_err)
                        groq_client = None
                        return None
                    print(f"ℹ️ [Groq Init] Skipping boot catalog probe ({probe_err}); keeping Groq models active.", flush=True)
            return groq_client
        except Exception:
            return None
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
            "CHCC", "ILP", "TRG", "AVN", "SAZEW", "MARI", "BAFL", "BAHL", "KAPCO", "FABL"
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

# Groq text models: Current supported Groq production models with resilient fallback cascade.
GROQ_TEXT_MODELS: List[str] = [
    "llama-3.3-70b-versatile",                     # Llama 3.3 70B versatile (primary)
    "meta-llama/llama-4-scout-17b-16e-instruct",   # Active Meta Llama 4 Scout on Groq
    "openai/gpt-oss-120b",                         # Active Groq 120B MoE
    "qwen/qwen3-32b",                              # Active Qwen 3 32B production model on Groq
    "openai/gpt-oss-20b",                          # Active Groq ultra-fast 20B
    "llama-3.1-8b-instant"                         # Llama 3.1 8B instant fallback
]

# Gemini fallback cascade (active Google GenAI 2.5 models):
GEMINI_TASK_ROUTING: Dict[str, List[str]] = {
    TASK_FAST_INGESTION: [
        "gemini-2.5-flash-lite",
        "gemini-2.5-flash"
    ],
    TASK_DEEP_REASONING: [
        "gemini-2.5-flash",
        "gemini-2.5-pro",
        "gemini-2.5-flash-lite"
    ],
    TASK_MULTIMODAL: [
        "gemini-2.5-flash",
        "gemini-2.5-flash-lite"
    ]
}

# --- IN-MEMORY CIRCUIT BREAKER & DEAD-MODEL BLACKLIST ---
# Maps model_name -> timestamp until which model is on cooldown
MODEL_COOLDOWN_UNTIL: Dict[str, float] = {}
# Permanently blacklists individual 404 / decommissioned model IDs without locking out other models
BLACKLISTED_MODELS: set[str] = set()

def is_model_available(model: str) -> bool:
    """Returns True if the model is not blacklisted and not currently in cooldown."""
    if model in BLACKLISTED_MODELS:
        return False
    cooldown_until = MODEL_COOLDOWN_UNTIL.get(model, 0.0)
    return time.time() >= cooldown_until

def is_any_groq_text_model_available() -> bool:
    """Returns True if GROQ_API_KEY is configured, Groq auth is valid, and at least one Groq text model is available."""
    if not is_model_available("groq"):
        return False
    return any(is_model_available(m) for m in GROQ_TEXT_MODELS)

def get_candidate_models(task: str = TASK_FAST_INGESTION) -> List[str]:
    """
    Returns candidate Gemini models for a given task, dynamically filtering out
    any blacklisted 404 model or model currently in MODEL_COOLDOWN_UNTIL.
    """
    raw_models = GEMINI_TASK_ROUTING.get(task, GEMINI_TASK_ROUTING[TASK_FAST_INGESTION])
    models = [m for m in raw_models if m not in BLACKLISTED_MODELS] or raw_models
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
    - 404 NOT_FOUND / decommissioned: Blacklists ONLY the dead model string (never locks out other models)
    - 401 Invalid API Key: 24-hour disable for that provider's invalid key
    - 429 Daily Limit (GenerateRequestsPerDay or retryDelay > 120s): 1-hour (3600s) cooldown
    - 429 RPM/TPM Limit: 45s cooldown
    """
    err_str = str(error)
    err_lower = err_str.lower()
    now = time.time()
    status_code = getattr(error, "status_code", None) or getattr(error, "code", None)

    # 1. Check 404 NOT_FOUND / decommissioned FIRST so request IDs/timestamps containing "401" never trigger global lockout!
    is_not_found = (
        status_code == 404
        or bool(re.search(r"\b(?:error[_\s]*code|status[_\s]*code|http)\s*[:=]?\s*404\b", err_lower))
        or any(k in err_lower for k in [
            "not_found", "model_not_found", "does not exist",
            "decommissioned", "model_decommissioned", "no longer supported"
        ])
    )
    if is_not_found:
        if model != "groq":
            BLACKLISTED_MODELS.add(model)
            print(f"🚫 [Circuit Breaker] Model '{model}' returned 404 / decommissioned. Blacklisted '{model}' ONLY (other models unaffected).", flush=True)
        return

    # 2. Check strict 401 Unauthorized / Invalid API Key (no loose substring match on "401" inside IDs/numbers)
    is_unauthorized = (
        status_code == 401
        or bool(re.search(r"\b(?:error[_\s]*code|status[_\s]*code|http)\s*[:=]?\s*401\b", err_lower))
        or any(k in err_lower for k in ["invalid_api_key", "invalid api key", "authenticationerror"])
    )
    if is_unauthorized:
        MODEL_COOLDOWN_UNTIL[model] = now + 86400.0
        if model == "groq" or any(m in model for m in ["llama", "groq", "gpt-oss", "qwen", "whisper"]):
            MODEL_COOLDOWN_UNTIL["groq"] = now + 86400.0
            for gm in GROQ_TEXT_MODELS + ["whisper-large-v3-turbo", "whisper-large-v3"]:
                MODEL_COOLDOWN_UNTIL[gm] = now + 86400.0
            print(f"🚫 [Circuit Breaker] Groq API key is invalid or unauthorized (401). Disabling Groq models; cascading cleanly to Gemini.")
        else:
            print(f"🚫 [Circuit Breaker] Model '{model}' authorization failed (401). Cooldown set for 24 hours.")
        return

    # 429 RESOURCE_EXHAUSTUSTED / Quota limits / Rate limits
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
async def generate_with_fallback(
    client,
    contents: list,
    task: str = TASK_FAST_INGESTION,
    priority: bool = False
) -> str:
    """
    Multi-provider generation with circuit breaker, priority queueing, and quota preservation:
    1. If GROQ_API_KEY is configured: For text-only TASK_FAST_INGESTION, routes first to Groq
       (llama-3.3-70b-versatile -> llama-3.1-8b-instant -> open-weights).
    2. Falls back to Gemini cascade (gemini-2.5-flash-lite -> gemini-2.5-flash).
    3. Multimodal contents (images, PDFs, video URLs) and TASK_DEEP_REASONING route strictly
       to Gemini (gemini-2.5-flash -> gemini-2.5-pro -> gemini-2.5-flash-lite).
    4. When `priority=True` (interactive user chat), enters `user_priority_context()` and uses
       native async `client.aio.models.generate_content` so `asyncio.wait_for` cancels cleanly
       and releases concurrency locks immediately without waiting behind background sweeps.
    """
    from contextlib import nullcontext
    from rate_limiter import user_priority_context, is_user_priority

    effective_priority = priority or is_user_priority()
    ctx_mgr = user_priority_context() if effective_priority else nullcontext()

    with ctx_mgr:
        is_text = is_pure_text_contents(contents)

        # 1. Text-only FAST_INGESTION offloaded to Groq if configured
        groq_key = os.getenv("GROQ_API_KEY")
        if task == TASK_FAST_INGESTION and is_text and groq_key and groq_key.strip():
            prompt_str = extract_prompt_string(contents)
            groq_result = await asyncio.to_thread(generate_text_groq, prompt_str)
            if groq_result:
                return groq_result
            if is_model_available("groq"):
                print("ℹ️ [Model Router] Groq text models unavailable or exhausted. Cascading to Gemini...")

        # 2. Gemini Cascade with Multi-Key Rotation
        effective_task = task
        if not is_text and task == TASK_FAST_INGESTION:
            effective_task = TASK_MULTIMODAL

        google_keys = get_google_api_keys()
        max_key_attempts = max(1, len(google_keys))
        last_error = None
        client_timeout_ms = 28000 if effective_priority else 300000

        for key_idx in range(max_key_attempts):
            active_key = get_active_google_api_key()
            active_client = client
            if active_client is None or getattr(active_client, "_active_google_key", None) != active_key:
                from google import genai
                active_client = genai.Client(api_key=active_key, http_options={"timeout": client_timeout_ms})
                active_client._active_google_key = active_key

            candidates = get_candidate_models(effective_task)
            now = time.time()

            for model_name in candidates:
                if not is_model_available(model_name):
                    remaining = int(MODEL_COOLDOWN_UNTIL.get(model_name, 0.0) - now)
                    print(f"⚡ [Circuit Breaker] Skipping '{model_name}' (cooling down for {remaining}s more).")
                    continue

                max_attempts = 1 if effective_priority else 2
                for attempt in range(max_attempts):
                    try:
                        if effective_priority:
                            response = await active_client.aio.models.generate_content(
                                model=model_name,
                                contents=contents
                            )
                        else:
                            chat = active_client.chats.create(model=model_name)
                            response = await asyncio.to_thread(chat.send_message, contents)
                        return (response.text or "").strip()
                    except (asyncio.CancelledError, asyncio.TimeoutError, TimeoutError):
                        raise
                    except Exception as e:
                        last_error = e
                        record_model_error(model_name, e)

                        # If error put model into cooldown, stop retrying this model immediately
                        if not is_model_available(model_name):
                            print(f"⚠️ [Model Router] '{model_name}' entered cooldown after error. Moving to next candidate...")
                            break

                        if attempt < max_attempts - 1:
                            await asyncio.sleep(2 * (attempt + 1))

            # Check if 429 / daily quota limits caused all candidates on this key to fail
            err_lower = str(last_error).lower() if last_error else ""
            if ("429" in err_lower or "resource_exhausted" in err_lower or "quota" in err_lower) and len(google_keys) > 1:
                next_k = rotate_to_next_google_key(reason="All candidate models on active key hit 429 quota exhaustion")
                if next_k and key_idx < max_key_attempts - 1:
                    client = None
                    continue

        raise RuntimeError(f"❌ All models (Groq & Gemini across {len(google_keys)} keys) failed in generate_with_fallback for {task}. Last error: {repr(last_error)}")

# --- LANGCHAIN LLM ROUTER ---
def get_langchain_llm(task: str = TASK_DEEP_REASONING, **kwargs):
    """
    Builds a LangChain chat model with multi-provider fallbacks:
    - TASK_FAST_INGESTION: ChatGroq (llama-3.3 / gpt-oss) -> ChatGoogleGenerativeAI (gemini-2.5-flash-lite).
    - TASK_DEEP_REASONING: ChatGoogleGenerativeAI (gemini-2.5-flash -> gemini-2.5-pro -> gemini-2.5-flash-lite).
    Supports multi-key Gemini rotation across GOOGLE_API_KEY, GOOGLE_API_KEY_2, GOOGLE_API_KEY_3.
    """
    from langchain_google_genai import ChatGoogleGenerativeAI
    groq_key = os.getenv("GROQ_API_KEY")

    groq_llms = []
    if task == TASK_FAST_INGESTION and groq_key and groq_key.strip():
        try:
            from langchain_groq import ChatGroq
            for g_model in GROQ_TEXT_MODELS:
                if is_model_available(g_model):
                    groq_llms.append(ChatGroq(model=g_model, groq_api_key=groq_key.strip(), **kwargs))
        except Exception as e:
            pass

    # Build Gemini LLMs with multi-key fallbacks
    google_keys = get_google_api_keys()
    gemini_llms = []
    candidates = get_candidate_models(task)

    for g_key in google_keys:
        for m in candidates:
            gemini_llms.append(ChatGoogleGenerativeAI(model=m, google_api_key=g_key, **kwargs))

    all_llms = groq_llms + gemini_llms
    if not all_llms:
        fallback_model = GEMINI_TASK_ROUTING[task][0]
        fallback_key = get_active_google_api_key() or os.getenv("GOOGLE_API_KEY")
        if fallback_key:
            fallback_key = fallback_key.strip().strip('"').strip("'").strip()
        return ChatGoogleGenerativeAI(model=fallback_model, google_api_key=fallback_key or "DUMMY_KEY", **kwargs)

    if len(all_llms) == 1:
        return all_llms[0]

    return all_llms[0].with_fallbacks(all_llms[1:])
