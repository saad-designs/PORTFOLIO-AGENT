import sys
# Force UTF-8 stdout before ANY import that may print emojis (Windows cp1252 fix)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
import asyncio

# Force SelectorEventLoop on Windows to support psycopg async pools
if sys.platform == "win32":
    import sys
    if sys.version_info < (3, 14):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# --- GLOBAL PSYCOPG2 FORCE INJECTION ---
import sys
try:
    import psycopg2
    import psycopg2.extras
    sys.modules['psycopg2'] = psycopg2
    sys.modules['psycopg2.extras'] = psycopg2.extras
except ImportError as e:
    print(f"⚠️ [Startup Warning] psycopg2-binary import issue: {e}", flush=True)
# ---------------------------------------
from dotenv import load_dotenv
load_dotenv()

# --- API KEY & CRITICAL ENVIRONMENT SANITIZATION ---
import os
CRITICAL_ENV_VARS = [
    "SUPABASE_DB_URL", "SUPABASE_URL", "SUPABASE_KEY",
    "GOOGLE_API_KEY", "GROQ_API_KEY", "YOUTUBE_API_KEY",
    "DATABASE_URL", "PORT", "GOOGLE_API_KEY_2", "GOOGLE_API_KEY_3", "GOOGLE_API_KEY_BACKUP",
    "ENABLE_WHATSAPP_BRIDGE", "DISABLE_WHATSAPP_BRIDGE"
]
for _k in CRITICAL_ENV_VARS:
    _v = os.getenv(_k)
    if _v is not None:
        os.environ[_k] = _v.strip().strip('"').strip("'").strip()

if os.getenv("GEMINI_API_KEY") and not os.getenv("GOOGLE_API_KEY"):
    os.environ["GOOGLE_API_KEY"] = os.getenv("GEMINI_API_KEY")
os.environ.pop("GEMINI_API_KEY", None)

import subprocess
import atexit
import time
import base64
import uuid
import json
import re

if os.name != "nt":
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")
    os.makedirs(os.environ["MPLCONFIGDIR"], exist_ok=True)

node_process = None
bridge_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "whatsapp_bridge")

def is_whatsapp_bridge_enabled() -> bool:
    """Checks ENABLE_WHATSAPP_BRIDGE (default 'true') and legacy DISABLE_WHATSAPP_BRIDGE."""
    if os.getenv("DISABLE_WHATSAPP_BRIDGE", "false").lower() in ("true", "1"):
        return False
    return os.getenv("ENABLE_WHATSAPP_BRIDGE", "true").lower() == "true"

def start_whatsapp_bridge() -> bool:
    """Launches the Node.js WhatsApp Bridge subprocess inside FastAPI startup lifecycle."""
    global node_process
    if not is_whatsapp_bridge_enabled():
        print("ℹ️ WhatsApp bridge disabled by configuration", flush=True)
        return False
    if node_process is not None and node_process.poll() is None:
        return True
    if os.path.exists(bridge_dir):
        print("🚀 Booting Node.js WhatsApp Bridge concurrently in background (127.0.0.1:3000)...", flush=True)
        try:
            node_process = subprocess.Popen(
                ["node", "whatsapp_bridge.js"],
                cwd=bridge_dir,
                shell=(os.name == 'nt')
            )
            return True
        except Exception as bridge_err:
            print(f"⚠️ [WhatsApp Bridge] Failed to launch background bridge subprocess: {bridge_err}", flush=True)
    return False

try:
    import psutil
except Exception as psutil_err:
    psutil = None
    print(f"⚠️ [Startup Warning] psutil unavailable ({psutil_err}); falling back to standard subprocess cleanup.", flush=True)

def cleanup_node_process():
    """Ruthlessly hunts down and terminates the WhatsApp Node bridge on exit."""
    global node_process
    if node_process:
        proc = node_process
        node_process = None
        print("🛑 Terminating background Node.js WhatsApp Bridge...", flush=True)
        if psutil is not None and hasattr(psutil, "Process"):
            try:
                parent = psutil.Process(proc.pid)
                for child in parent.children(recursive=True):
                    try:
                        child.kill()
                    except Exception:
                        pass
                parent.kill()
                return
            except Exception:
                pass
        try:
            proc.kill()
        except Exception:
            pass

atexit.register(cleanup_node_process)

import rate_limiter
rate_limiter.apply_global_rate_limits()

import urllib.parse

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, UploadFile, File, Form
from fastapi.responses import PlainTextResponse, HTMLResponse, FileResponse
from pydantic import BaseModel
from typing import Optional, List

client = None
try:
    from google import genai
    raw_key = os.getenv("GOOGLE_API_KEY")
    if raw_key:
        clean_key = raw_key.strip().strip('"').strip("'")
        client = genai.Client(api_key=clean_key, http_options={"timeout": 300000})
except Exception as genai_err:
    print(f"⚠️ [GenAI Warning] Could not initialize genai.Client: {genai_err}", flush=True)

import agent
from langgraph.types import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from ingestion import sweep_rss_feeds, sweep_youtube_transcripts, process_whatsapp_media_payload, process_whatsapp_text_payload
from scheduler_config import scheduler
import aiohttp
from portfolio_manager import get_whatsapp_checkpoint, update_whatsapp_checkpoint, get_checkpoint, update_checkpoint
from model_router import is_financial_or_signal_text

# Whitelisted KTrade group IDs
ALLOWED_KTRADE_CHATS = [
    "120363405626670375@g.us",
    "120363404462896702@g.us"
]

# --- ASYNC NON-BLOCKING INGESTION QUEUE ---
whatsapp_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

async def whatsapp_queue_worker():
    """
    Sequential background consumer for WhatsApp incoming payloads.
    Guarantees fast HTTP returns (<5ms) to Express, applies pre-LLM noise filtering,
    and micro-batches consecutive KTrade text messages within a 15-second window (up to 8 messages)
    into a single consolidated LLM call.
    """
    print("🚀 [WhatsApp Queue Worker] Ingestion queue consumer online.")
    buffered_payloads = []

    def _archive_payload(p):
        from portfolio_manager import supabase
        if supabase:
            try:
                c_id = p.chat_id or "KTrade"
                sdr = p.sender or "KTrade"
                bdy = p.body or ""
                ts = p.timestamp or int(time.time())
                supabase.table("messages").insert({
                    "chat_id": c_id,
                    "sender": sdr,
                    "body": bdy[:500] if bdy else f"[{p.filename or 'Media Attachment'}]",
                    "timestamp": ts,
                    "is_group": p.is_group
                }).execute()
            except Exception:
                pass

    while True:
        is_pulled_from_queue = False
        try:
            if buffered_payloads:
                payload = buffered_payloads.pop(0)
                is_pulled_from_queue = True
            else:
                payload = await whatsapp_queue.get()
                is_pulled_from_queue = True

            try:
                chat_id = payload.chat_id or "KTrade"
                sender = payload.sender or "KTrade"
                body = payload.body or ""
                source = payload.source or "KTrade"
                timestamp = payload.timestamp or int(time.time())

                # Supabase message archival
                _archive_payload(payload)

                # 1. Multimodal media payload
                if payload.has_media and payload.media_base64:
                    try:
                        file_bytes = base64.b64decode(payload.media_base64)
                        mimetype = payload.mime_type or "image/png"
                        filename = payload.filename or "media_attachment"
                        print(f"📥 [WhatsApp Worker] Processing multimodal: {filename} ({mimetype}, {len(file_bytes)} bytes) from {source}")
                        await process_whatsapp_media_payload(
                            file_bytes=file_bytes,
                            filename=filename,
                            mimetype=mimetype,
                            caption=body,
                            source=source,
                            timestamp=timestamp
                        )
                    except Exception as media_err:
                        print(f"❌ [WhatsApp Worker] Multimodal processing failed: {media_err}")

                # 2. Text intelligence payload with 15-second micro-batching
                elif body and len(body.strip()) > 0:
                    batched_bodies = [body.strip()]
                    batched_timestamps = [timestamp]

                    # Micro-batching window: wait and drain up to 8 text messages from the same chat_id within 15s
                    max_batch = 8
                    window_deadline = time.time() + 15.0

                    while len(batched_bodies) < max_batch:
                        remaining_window = window_deadline - time.time()
                        if remaining_window <= 0:
                            break
                        try:
                            # Wait up to 3s of typing silence or until window deadline
                            next_payload = await asyncio.wait_for(
                                whatsapp_queue.get(),
                                timeout=min(remaining_window, 3.0)
                            )
                        except asyncio.TimeoutError:
                            # Typing silence reached
                            break

                        _archive_payload(next_payload)

                        # Check if next_payload can be batched (same chat_id, text-only, non-empty body)
                        if (
                            not next_payload.has_media
                            and (next_payload.chat_id or "KTrade") == chat_id
                            and (next_payload.body or "").strip()
                        ):
                            batched_bodies.append(next_payload.body.strip())
                            if next_payload.timestamp:
                                batched_timestamps.append(next_payload.timestamp)
                            whatsapp_queue.task_done()
                        else:
                            # Different chat or has media! Buffer it for next worker iteration
                            buffered_payloads.append(next_payload)
                            break

                    consolidated_text = "\n---\n".join(batched_bodies)
                    consolidated_ts = max(batched_timestamps)

                    if len(batched_bodies) > 1:
                        print(f"📦 [Micro-Batching] Consolidated {len(batched_bodies)} consecutive KTrade text messages into 1 payload ({len(consolidated_text)} chars).")

                    # Zero-Cost Pre-LLM Noise Filter (Filters chitchat < 30 chars with no tickers or financial keywords)
                    if not is_financial_or_signal_text(consolidated_text, has_media=False):
                        print(f"ℹ️ [Noise Filter] 0ms bypass on short non-financial commentary (< 30 chars, no tickers/keywords): '{consolidated_text}'")
                        if consolidated_ts:
                            update_whatsapp_checkpoint(consolidated_ts)
                    else:
                        print(f"📥 [WhatsApp Worker] Processing text intel ({len(consolidated_text)} chars) from {source}...")
                        await process_whatsapp_text_payload(
                            text=consolidated_text,
                            source=source,
                            timestamp=consolidated_ts
                        )

            except Exception as item_err:
                print(f"❌ [WhatsApp Worker] Error handling message item: {item_err}")
            finally:
                if is_pulled_from_queue:
                    whatsapp_queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as q_err:
            print(f"❌ [WhatsApp Worker] Queue loop error: {q_err}")
            await asyncio.sleep(1)

async def poll_and_catchup_whatsapp():
    """
    Polls WhatsApp Bridge /ping every 5s up to 90s in non-blocking background.
    Triggers catch-up sweep as soon as bridge reports READY.
    """
    last_ts = get_whatsapp_checkpoint()
    bridge_ready = False
    print("📱 [Startup Orchestrator] Polling WhatsApp Bridge (http://127.0.0.1:3000/ping) in background (up to 90s)...")

    async with aiohttp.ClientSession() as session:
        for attempt in range(18):  # 18 * 5s = 90s
            try:
                async with session.get("http://127.0.0.1:3000/ping", timeout=aiohttp.ClientTimeout(total=4)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("status") == "pong" or "ready" in data.get("message", "").lower():
                            bridge_ready = True
                            print("✅ [Startup Orchestrator] WhatsApp Bridge is READY.")
                            break
            except Exception:
                pass
            await asyncio.sleep(5)

        if bridge_ready:
            try:
                print(f"🔄 [Startup Orchestrator] Executing WhatsApp catch-up for {len(ALLOWED_KTRADE_CHATS)} KTrade chats...")
                async with session.post(
                    "http://127.0.0.1:3000/api/catchup",
                    json={"since_timestamp": last_ts, "target_chat_ids": ALLOWED_KTRADE_CHATS},
                    timeout=aiohttp.ClientTimeout(total=120)
                ) as resp:
                    data = await resp.json()
                    print(f"✅ [Startup Orchestrator] WhatsApp catch-up complete: {data.get('replayed', 0)} messages replayed.")
            except Exception as wa_err:
                print(f"⚠️ [Startup Orchestrator] WhatsApp catch-up request failed: {wa_err}")
        else:
            print("ℹ️ [Startup Orchestrator] WhatsApp Bridge still initializing after 90s. Bridge will process live messages on connect.")

async def boot_and_poll_whatsapp_bridge():
    """
    Waits briefly after FastAPI lifespan yields so Uvicorn binds 0.0.0.0:8000 first,
    then launches whatsapp_bridge.js on 127.0.0.1:3000 and starts catch-up polling.
    """
    if not is_whatsapp_bridge_enabled():
        print("ℹ️ WhatsApp bridge disabled by configuration", flush=True)
        return
    await asyncio.sleep(2)
    if start_whatsapp_bridge():
        await poll_and_catchup_whatsapp()

async def run_startup_orchestration():
    """
    Sequential Startup Orchestrator with Lazily Staggered Checks:
    Prevents boot stampede and quota exhaustion.
    1. WhatsApp Bridge launch + catch-up polling runs in background after Uvicorn binds 0.0.0.0:8000.
    2. At t+30s: Business Sweep runs ONLY IF last sweep was > 4 hours ago.
    3. At t+90s: YouTube Sweep runs ONLY IF last sweep was > 6 hours ago.
    """
    print("🚀 [Startup Orchestrator] Initializing non-blocking startup sequences...")

    # 1. Non-blocking WhatsApp Bridge delayed launch & catch-up polling task
    asyncio.create_task(boot_and_poll_whatsapp_bridge())

    # 2. Lazily staggered Business & Portfolio Sweep at t+30s
    await asyncio.sleep(30)
    now = int(time.time())
    last_business_sweep = get_checkpoint("business_sweep_last_timestamp")
    # 4 hours = 14400s
    if (now - last_business_sweep) > (4 * 3600):
        print(f"⚖️ [Startup Orchestrator] Last Business Sweep was {now - last_business_sweep}s ago (>4h). Running Business & Portfolio Sweep...")
        try:
            from schedulers import run_business_portfolio_sweep
            await run_business_portfolio_sweep()
            update_checkpoint("business_sweep_last_timestamp", now)
            print("✅ [Startup Orchestrator] Business Sweep completed.")
        except Exception as bus_err:
            print(f"⚠️ [Startup Orchestrator] Business Sweep failed: {bus_err}")
    else:
        mins_ago = int((now - last_business_sweep) / 60)
        print(f"ℹ️ [Startup Orchestrator] Skipping Business Sweep on boot: last run was {mins_ago}m ago (<4h).")

    # 3. Lazily staggered YouTube Sweep at t+90s (60s after business sweep check)
    await asyncio.sleep(60)
    now = int(time.time())
    last_youtube_sweep = get_checkpoint("youtube_last_timestamp")
    # 6 hours = 21600s
    if (now - last_youtube_sweep) > (6 * 3600):
        print(f"🕒 [Startup Orchestrator] Last YouTube Sweep was {now - last_youtube_sweep}s ago (>6h). Running YouTube catch-up sweep (capped at 2 videos)...")
        try:
            from schedulers import run_youtube_sweep
            await run_youtube_sweep()
            update_checkpoint("youtube_last_timestamp", now)
            print("✅ [Startup Orchestrator] YouTube catch-up sweep completed.")
        except Exception as yt_err:
            print(f"⚠️ [Startup Orchestrator] YouTube sweep failed: {yt_err}")
    else:
        mins_ago = int((now - last_youtube_sweep) / 60)
        print(f"ℹ️ [Startup Orchestrator] Skipping YouTube Sweep on boot: last run was {mins_ago}m ago (<6h).")

    print("🎯 [Startup Orchestrator] Staggered startup checks fully initialized.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    try:
        from portfolio_manager import create_dashboard_and_ledger_tables
        create_dashboard_and_ledger_tables()
    except Exception as db_init_err:
        print(f"⚠️ [Startup] DB Table init error: {db_init_err}", flush=True)

    app.state.graph = await agent.get_agent()
    print("ℹ️ [Decommissioned] Discord bot is offline; persistent Agent Feed and Matte Dark Web Dashboard active.", flush=True)

    # Start WhatsApp ingestion queue worker
    asyncio.create_task(whatsapp_queue_worker())

    scheduler.add_job(
        sweep_rss_feeds, 
        'interval', 
        hours=4,
        id="rss_ingestion_periodic",
        name="Periodic RSS Feed Sweep"
    )
    scheduler.add_job(
        sweep_youtube_transcripts, 
        'interval', 
        hours=24,
        id="youtube_ingestion_daily",
        name="Daily YouTube Transcript Sweep"
    )
    scheduler.start()
    
    loop = asyncio.get_running_loop()
    from schedulers import initialize_background_schedulers
    initialize_background_schedulers(loop)
    print("🚀 Portfolio Agent core running with autonomous background learning.")
    
    # Trigger sequential startup orchestration (including WhatsApp Bridge launch after port 8000 bind)
    asyncio.create_task(run_startup_orchestration())
    yield
    # Shutdown
    cleanup_node_process()
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

from fastapi.staticfiles import StaticFiles
static_dir = os.path.join(os.path.dirname(__file__), "static")
memory_cache_dir = os.path.join(os.path.dirname(__file__), "memory_cache")
os.makedirs(static_dir, exist_ok=True)
os.makedirs(memory_cache_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Microscopic health endpoint
@app.get("/ping", response_class=PlainTextResponse)
@app.head("/ping", response_class=PlainTextResponse)
async def ping():
    return "OK"

DASHBOARD_KEY = os.getenv("DASHBOARD_KEY", "").strip()

def verify_dashboard_auth(request: Request):
    if not DASHBOARD_KEY:
        return True
    key = request.headers.get("x-dashboard-key") or request.query_params.get("key")
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        key = auth_header[7:].strip()
    if key != DASHBOARD_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid or missing DASHBOARD_KEY")
    return True

class DividendCreatePayload(BaseModel):
    ticker: str
    shares_held: float
    dps_gross: float
    tax_rate_pct: Optional[float] = 15.00
    ex_date: Optional[str] = None
    payment_date: Optional[str] = None
    status: Optional[str] = "ENTITLED"
    notes: Optional[str] = None

class DividendStatusUpdatePayload(BaseModel):
    status: str

class CapitalTransactionPayload(BaseModel):
    tx_type: str
    amount: float
    reference_note: Optional[str] = None

class TradeRecordPayload(BaseModel):
    ticker: str
    side: str
    quantity: float
    price: float
    fees_and_tax: Optional[float] = 0.00
    notes: Optional[str] = None

@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard():
    dashboard_path = os.path.join(os.path.dirname(__file__), "static", "dashboard.html")
    if os.path.exists(dashboard_path):
        return FileResponse(dashboard_path, media_type="text/html")
    return HTMLResponse("<h1>PSX Co-Investor Dashboard</h1>", status_code=200)

@app.get("/api/dashboard/overview")
async def get_dashboard_overview(request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import get_complete_financial_summary, get_desk_manager_thought, get_db_connection
    from model_router import MODEL_COOLDOWN_UNTIL, is_model_available, is_any_groq_text_model_available, CURRENT_GOOGLE_KEY_INDEX
    
    summary = await asyncio.to_thread(get_complete_financial_summary)
    
    # WhatsApp bridge status
    bridge_status = "DISABLED" if not is_whatsapp_bridge_enabled() else "OFFLINE"
    monitored_chats = []
    if is_whatsapp_bridge_enabled():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("http://127.0.0.1:3000/qr", timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    if resp.status == 200:
                        b_data = await resp.json()
                        bridge_status = b_data.get("status", "OFFLINE")
                        monitored_chats = b_data.get("monitored_chats", [])
        except Exception:
            pass
        
    now = time.time()
    model_cooldowns = {
        m: round(t - now, 1) for m, t in MODEL_COOLDOWN_UNTIL.items() if t > now
    }
    
    checkpoints = {}
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT key, updated_at FROM agent_checkpoints;")
        for k, u in cur.fetchall():
            checkpoints[k] = u.isoformat() if u else None
        cur.close()
        conn.close()
    except Exception:
        pass

    desk_thought = await asyncio.to_thread(get_desk_manager_thought)

    return {
        "status": "success",
        "financial_summary": summary,
        "system_health": {
            "whatsapp_bridge": {
                "status": bridge_status,
                "monitored_chats": monitored_chats
            },
            "model_router": {
                "active_google_key_index": CURRENT_GOOGLE_KEY_INDEX,
                "cooldowns": model_cooldowns,
                "models": {
                    "llama-3.3-70b-versatile": is_any_groq_text_model_available(),
                    "gemini-3.5-flash-lite": is_model_available("gemini-3.5-flash-lite"),
                    "gemini-3.5-flash": is_model_available("gemini-3.5-flash"),
                    "gemini-3.8-flash": is_model_available("gemini-3.8-flash")
                }
            },
            "checkpoints": checkpoints
        },
        "desk_manager_thought": desk_thought
    }

@app.get("/api/dashboard/dividends")
async def get_dashboard_dividends(request: Request, limit: int = 100):
    verify_dashboard_auth(request)
    from portfolio_manager import get_all_dividends, get_complete_financial_summary
    divs = await asyncio.to_thread(get_all_dividends, limit)
    summary = await asyncio.to_thread(get_complete_financial_summary)
    kpis = summary.get("financial_kpis", {})
    return {
        "dividends": divs,
        "metrics": {
            "total_gross_dividends": kpis.get("total_gross_dividends", 0.0),
            "total_wht_tax_paid": kpis.get("total_wht_tax_paid", 0.0),
            "net_dividends_in_bank": kpis.get("net_dividends_in_bank", 0.0),
            "net_dividends_reinvested": kpis.get("net_dividends_reinvested", 0.0),
            "pending_dividends": kpis.get("pending_dividends", 0.0)
        }
    }

@app.post("/api/dashboard/dividends")
async def create_dashboard_dividend(payload: DividendCreatePayload, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import record_dividend
    entry = await asyncio.to_thread(
        record_dividend,
        ticker=payload.ticker,
        shares_held=payload.shares_held,
        dps_gross=payload.dps_gross,
        tax_rate_pct=payload.tax_rate_pct if payload.tax_rate_pct is not None else 15.00,
        ex_date=payload.ex_date,
        payment_date=payload.payment_date,
        status=payload.status or "ENTITLED",
        notes=payload.notes
    )
    return {"status": "success", "dividend": entry}

@app.patch("/api/dashboard/dividends/{id}")
@app.patch("/api/dashboard/dividends/{id}/status")
async def update_dashboard_dividend(id: int, payload: DividendStatusUpdatePayload, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import update_dividend_status
    updated = await asyncio.to_thread(update_dividend_status, id, payload.status)
    return {"status": "success", "dividend": updated}

@app.get("/api/dashboard/capital")
async def get_dashboard_capital(request: Request, limit: int = 100):
    verify_dashboard_auth(request)
    from portfolio_manager import get_capital_ledger_entries
    entries = await asyncio.to_thread(get_capital_ledger_entries, limit)
    return {"status": "success", "entries": entries}

@app.post("/api/dashboard/capital")
async def create_dashboard_capital(payload: CapitalTransactionPayload, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import record_capital_transaction
    entry = await asyncio.to_thread(
        record_capital_transaction,
        tx_type=payload.tx_type,
        amount=payload.amount,
        reference_note=payload.reference_note
    )
    return {"status": "success", "entry": entry}

@app.get("/api/dashboard/trades")
async def get_dashboard_trades(request: Request, limit: int = 100):
    verify_dashboard_auth(request)
    from portfolio_manager import get_trade_history_entries
    entries = await asyncio.to_thread(get_trade_history_entries, limit)
    return {"status": "success", "trades": entries}

@app.post("/api/dashboard/trades")
async def create_dashboard_trade(payload: TradeRecordPayload, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import record_trade
    trade = await asyncio.to_thread(
        record_trade,
        ticker=payload.ticker,
        side=payload.side,
        quantity=payload.quantity,
        price=payload.price,
        fees_and_tax=payload.fees_and_tax or 0.00,
        notes=payload.notes
    )
    return {"status": "success", "trade": trade}

@app.get("/api/dashboard/intelligence")
async def get_dashboard_intelligence(request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import get_db_connection, get_desk_manager_thought
    
    def _fetch_intelligence():
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT id, chat_id, sender, body, timestamp, created_at, extension
                FROM messages
                ORDER BY created_at DESC
                LIMIT 25;
            """)
            messages = [
                {
                    "id": r[0], "chat_id": r[1], "sender": r[2], "body": r[3],
                    "timestamp": r[4], "created_at": r[5].isoformat() if r[5] else None,
                    "extension": r[6]
                }
                for r in cur.fetchall()
            ]

            cur.execute("""
                SELECT id, source, signal_payload, status, created_at
                FROM pending_signals
                ORDER BY created_at DESC
                LIMIT 20;
            """)
            pending_signals = [
                {
                    "id": r[0], "source": r[1], "payload": r[2], "status": r[3],
                    "created_at": r[4].isoformat() if r[4] else None
                }
                for r in cur.fetchall()
            ]

            cur.execute("""
                SELECT id, source, ticker, subject, sentiment, summary, created_at
                FROM institutional_reports
                ORDER BY created_at DESC
                LIMIT 20;
            """)
            institutional_reports = [
                {
                    "id": r[0], "source": r[1], "ticker": r[2], "subject": r[3],
                    "sentiment": r[4], "summary": r[5], "created_at": r[6].isoformat() if r[6] else None
                }
                for r in cur.fetchall()
            ]

            cur.execute("""
                SELECT id, source, rule, created_at
                FROM active_playbook
                ORDER BY created_at DESC
                LIMIT 20;
            """)
            active_playbook = [
                {
                    "id": r[0], "source": r[1], "rule": r[2],
                    "created_at": r[3].isoformat() if r[3] else None
                }
                for r in cur.fetchall()
            ]

            cur.execute("""
                SELECT id, source_type, source_url, content, created_at
                FROM agent_knowledge
                ORDER BY created_at DESC
                LIMIT 20;
            """)
            mentor_knowledge = [
                {
                    "id": r[0], "source_type": r[1], "source_url": r[2],
                    "content": r[3], "created_at": r[4].isoformat() if r[4] else None
                }
                for r in cur.fetchall()
            ]

            return {
                "messages": messages,
                "pending_signals": pending_signals,
                "institutional_reports": institutional_reports,
                "active_playbook": active_playbook,
                "mentor_knowledge": mentor_knowledge
            }
        finally:
            cur.close()
            conn.close()

    intel_data = await asyncio.to_thread(_fetch_intelligence)
    intel_data["desk_manager_thought"] = await asyncio.to_thread(get_desk_manager_thought)
    return {"status": "success", **intel_data}

@app.get("/api/dashboard/feed")
async def get_dashboard_feed(request: Request, limit: int = 50):
    verify_dashboard_auth(request)
    from portfolio_manager import get_agent_feed, get_db_connection
    feed = await asyncio.to_thread(get_agent_feed, limit)
    
    def _fetch_ancillary():
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT id, source, signal_payload, status, created_at FROM pending_signals ORDER BY created_at DESC LIMIT 20;")
            pending = [
                {"id": r[0], "source": r[1], "payload": r[2], "status": r[3], "created_at": r[4].isoformat() if r[4] else None}
                for r in cur.fetchall()
            ]
            cur.execute("SELECT id, source, rule, created_at FROM active_playbook ORDER BY created_at DESC LIMIT 20;")
            playbook = [
                {"id": r[0], "source": r[1], "rule": r[2], "created_at": r[3].isoformat() if r[3] else None}
                for r in cur.fetchall()
            ]
            cur.execute("SELECT id, source_type, source_url, content, created_at FROM agent_knowledge ORDER BY created_at DESC LIMIT 20;")
            knowledge = [
                {"id": r[0], "source_type": r[1], "source_url": r[2], "content": r[3], "created_at": r[4].isoformat() if r[4] else None}
                for r in cur.fetchall()
            ]
            return pending, playbook, knowledge
        finally:
            cur.close()
            conn.close()

    pending_signals, active_playbook, mentor_knowledge = await asyncio.to_thread(_fetch_ancillary)

    return {
        "status": "success",
        "agent_feed": feed,
        "pending_signals": pending_signals,
        "active_playbook": active_playbook,
        "mentor_knowledge": mentor_knowledge
    }

class DashboardChatPayload(BaseModel):
    message: str

@app.post("/api/dashboard/chat")
async def dashboard_chat_endpoint(
    request: Request,
    message: str = Form(""),
    files: Optional[List[UploadFile]] = File(None),
    file: Optional[UploadFile] = File(None)
):
    verify_dashboard_auth(request)
    import mimetypes
    from chat_copilot import process_dashboard_chat_with_multimodal

    incoming_uploads: List[UploadFile] = []
    if files:
        incoming_uploads.extend(files)
    if file:
        incoming_uploads.append(file)

    processed_files = []
    for f in incoming_uploads:
        if not f or not f.filename:
            continue
        content = await f.read()
        if not content:
            continue
        guessed_mime, _ = mimetypes.guess_type(f.filename)
        raw_ct = (f.content_type or "").strip()
        if raw_ct and raw_ct != "application/octet-stream":
            resolved_mime = raw_ct
        else:
            resolved_mime = guessed_mime or (
                "application/pdf" if f.filename.lower().endswith(".pdf") else "image/png"
            )
        processed_files.append({
            "filename": f.filename,
            "mime_type": resolved_mime,
            "bytes": content
        })

    with rate_limiter.user_priority_context():
        response = await process_dashboard_chat_with_multimodal(message or "", processed_files)
    return response

@app.get("/api/dashboard/chat-history")
async def get_dashboard_chat_history_endpoint(request: Request, limit: int = 50):
    verify_dashboard_auth(request)
    from portfolio_manager import get_chat_history
    history = await asyncio.to_thread(get_chat_history, limit)
    return {"status": "success", "history": history}

@app.get("/api/dashboard/whatsapp-qr")
async def get_dashboard_whatsapp_qr():
    if not is_whatsapp_bridge_enabled():
        return {
            "status": "DISABLED",
            "qr": None,
            "monitored_chats": ALLOWED_KTRADE_CHATS
        }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("http://127.0.0.1:3000/qr", timeout=aiohttp.ClientTimeout(total=3)) as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception:
        pass
    return {
        "status": "OFFLINE",
        "qr": None,
        "monitored_chats": ALLOWED_KTRADE_CHATS
    }

@app.post("/api/dashboard/trigger-sweep/{sweep_type}")
async def trigger_dashboard_sweep(sweep_type: str, background_tasks: BackgroundTasks, request: Request):
    verify_dashboard_auth(request)
    st = sweep_type.strip().lower()
    if st in {"business", "desk_manager"}:
        from schedulers import run_business_portfolio_sweep
        background_tasks.add_task(run_business_portfolio_sweep)
    elif st == "youtube":
        from ingestion import sweep_youtube_transcripts
        background_tasks.add_task(sweep_youtube_transcripts)
    elif st == "news":
        from news_scraper import run_news_sweep
        background_tasks.add_task(run_news_sweep)
    else:
        raise HTTPException(status_code=400, detail=f"Unknown sweep type: {sweep_type}. Use 'business', 'youtube', or 'news'.")
    return {
        "status": "triggered",
        "sweep_type": st,
        "timestamp": time.time()
    }

@app.get("/whatsapp-qr", response_class=HTMLResponse)
async def whatsapp_qr_page():
    qr_data = None
    status = "DISABLED" if not is_whatsapp_bridge_enabled() else "INITIALIZING"
    
    if is_whatsapp_bridge_enabled():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("http://127.0.0.1:3000/qr", timeout=aiohttp.ClientTimeout(total=3)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        status = data.get("status", "INITIALIZING")
                        qr_data = data.get("qr")
        except Exception:
            status = "CONNECTING"

    if status == "DISABLED":
        content_html = """
        <div class="card">
            <div class="icon">ℹ️</div>
            <h2>WhatsApp Bridge Disabled</h2>
            <p class="subtitle">Set <b>ENABLE_WHATSAPP_BRIDGE=true</b> in your environment configuration to run headless Chromium.</p>
            <div class="badge badge-init">STATUS: DISABLED BY CONFIG</div>
        </div>
        """
        refresh_tag = ""
    elif status == "READY":
        content_html = """
        <div class="card">
            <div class="icon">✅</div>
            <h2>WhatsApp Bridge Connected</h2>
            <p class="subtitle">Live listener is actively authenticated and monitoring KTrade groups.</p>
            <div class="badge badge-ready">STATUS: READY & MONITORING</div>
        </div>
        """
        refresh_tag = '<meta http-equiv="refresh" content="30">'
    elif status in ("WAITING_FOR_QR", "QR_READY") and qr_data and qr_data != "AUTH_TIMEOUT_RETRYING":
        encoded_qr = urllib.parse.quote(qr_data)
        qr_img_url = f"https://api.qrserver.com/v1/create-qr-code/?size=320x320&data={encoded_qr}&margin=10"
        content_html = f"""
        <div class="card">
            <div class="icon">📱</div>
            <h2>Scan WhatsApp QR Code</h2>
            <p class="subtitle">Open WhatsApp on your phone &rarr; <b>Linked Devices</b> &rarr; <b>Link a Device</b>.</p>
            <div class="qr-container">
                <img src="{qr_img_url}" alt="WhatsApp QR Code" class="qr-image" />
            </div>
            <p class="timer">This QR code auto-refreshes every 6 seconds until connected.</p>
            <div class="badge badge-waiting">STATUS: WAITING FOR SCAN</div>
        </div>
        """
        refresh_tag = '<meta http-equiv="refresh" content="6">'
    elif status == "AUTH_TIMEOUT_RETRYING" or qr_data == "AUTH_TIMEOUT_RETRYING":
        content_html = """
        <div class="card">
            <div class="icon">🔄</div>
            <h2>Auth Timeout &mdash; Retrying...</h2>
            <p class="subtitle">WhatsApp QR/session timed out. Restarting headless Chromium cleanly for a fresh QR code &mdash; reload or wait a few seconds.</p>
            <div class="spinner"></div>
            <div class="badge badge-waiting">STATUS: AUTH_TIMEOUT_RETRYING</div>
        </div>
        """
        refresh_tag = '<meta http-equiv="refresh" content="5">'
    else:
        content_html = f"""
        <div class="card">
            <div class="icon">⏳</div>
            <h2>Chromium Initializing...</h2>
            <p class="subtitle">Headless browser is booting up. WhatsApp Web will generate a QR code momentarily.</p>
            <div class="spinner"></div>
            <div class="badge badge-init">STATUS: {status}</div>
        </div>
        """
        refresh_tag = '<meta http-equiv="refresh" content="5">'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    {refresh_tag}
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>WhatsApp Bridge — Cloud Authentication</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            background: linear-gradient(135deg, #0d1117 0%, #161b22 100%);
            color: #c9d1d9;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 20px;
        }}
        .card {{
            background: #1c2128;
            border: 1px solid #30363d;
            border-radius: 16px;
            padding: 36px 32px;
            max-width: 440px;
            width: 100%;
            text-align: center;
            box-shadow: 0 12px 36px rgba(0,0,0,0.45);
        }}
        .icon {{ font-size: 48px; margin-bottom: 12px; }}
        h2 {{ color: #f0f6fc; font-size: 24px; font-weight: 600; margin-bottom: 8px; }}
        .subtitle {{ color: #8b949e; font-size: 14px; line-height: 1.5; margin-bottom: 24px; }}
        .qr-container {{
            background: #ffffff;
            padding: 16px;
            border-radius: 12px;
            display: inline-block;
            margin-bottom: 18px;
            box-shadow: 0 4px 16px rgba(0,0,0,0.25);
        }}
        .qr-image {{
            display: block;
            width: 280px;
            height: 280px;
            border-radius: 6px;
        }}
        .timer {{ color: #8b949e; font-size: 12px; margin-bottom: 20px; }}
        .badge {{
            display: inline-block;
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
            letter-spacing: 0.5px;
        }}
        .badge-ready {{ background: #238636; color: #ffffff; }}
        .badge-waiting {{ background: #9e6a03; color: #f0f6fc; }}
        .badge-init {{ background: #388bfd; color: #ffffff; }}
        .spinner {{
            width: 40px;
            height: 40px;
            margin: 24px auto;
            border: 4px solid #30363d;
            border-top: 4px solid #58a6ff;
            border-radius: 50%;
            animation: spin 1s linear infinite;
        }}
        @keyframes spin {{ 0% {{ transform: rotate(0deg); }} 100% {{ transform: rotate(360deg); }} }}
    </style>
</head>
<body>
    {content_html}
</body>
</html>
"""
    return HTMLResponse(content=html)



async def process_webhook_alert(ticker: str, signal: str, price: float):
    import os
    import traceback
    
    print(f"⚡ [Webhook Task] Background worker started for {ticker}...")
    try:
        from portfolio_manager import publish_agent_event
        from agent import get_agent
        
        prompt = f"Market Alert: {ticker} triggered a {signal} signal at Rs. {price}. Run a calculate_dcf_valuation for {ticker} and give me a strict tactical verdict comparing fair value to the current price."
        initial_state = {"messages": [("user", prompt)]}
        
        agent_app = await get_agent()
        config = {"configurable": {"thread_id": f"webhook_alert_{ticker}"}}
        ai_verdict = await agent_app.ainvoke(initial_state, config)
        
        publish_agent_event(
            category='SYSTEM_ALERT',
            title=f"Market Alert: {ticker} ({signal})",
            content=str(ai_verdict),
            metadata={"ticker": ticker, "signal": signal, "price": price}
        )
        print(f"🚀 [Webhook Task] Webhook alert pushed to Agent Feed for {ticker}.")

    except Exception as e:
        print(f"\n❌ [CRITICAL WEBHOOK ERROR] The background task crashed:")
        traceback.print_exc()
        print("\n")

@app.post("/webhook/market-alert")
async def handle_market_alert(request: Request, background_tasks: BackgroundTasks):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    ticker = data.get("ticker")
    signal_type = data.get("signal")
    price = data.get("price")

    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker symbol missing")

    background_tasks.add_task(process_webhook_alert, ticker, signal_type, price)
    return {"status": "success", "processed_ticker": ticker}

class WhatsAppMessagePayload(BaseModel):
    chat_id: Optional[str] = None
    sender: Optional[str] = "KTrade"
    body: Optional[str] = ""
    timestamp: Optional[int] = None
    is_group: Optional[bool] = True
    source: Optional[str] = "KTrade"
    has_media: Optional[bool] = False
    media_base64: Optional[str] = None
    mime_type: Optional[str] = None
    filename: Optional[str] = None

@app.post("/api/ingest-whatsapp-message")
async def ingest_whatsapp_message(payload: WhatsAppMessagePayload):
    """
    Non-blocking endpoint: puts incoming payload into in-memory queue
    and returns {"status": "queued"} in < 5ms.
    """
    try:
        whatsapp_queue.put_nowait(payload)
        return {"status": "queued"}
    except asyncio.QueueFull:
        print("⚠️ [WhatsApp Endpoint] Queue full. Dropping message to protect service.")
        return {"status": "dropped", "reason": "queue_full"}

@app.post("/api/ingest-whatsapp-media")
async def ingest_whatsapp_media_endpoint(
    file: UploadFile = File(...),
    caption: str = Form(""),
    source: str = Form("WhatsApp Feed"),
    timestamp: Optional[int] = Form(None)
):
    try:
        file_bytes = await file.read()
        mimetype = file.content_type or "application/octet-stream"
        
        result = await process_whatsapp_media_payload(
            file_bytes=file_bytes,
            filename=file.filename,
            mimetype=mimetype,
            caption=caption,
            source=source,
            timestamp=timestamp
        )
        return result
    except Exception as e:
        print(f"❌ [WhatsApp Media Endpoint Error]: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class WhatsAppTextPayload(BaseModel):
    text: str
    source: str
    timestamp: Optional[int] = None

@app.post("/api/ingest-whatsapp-text")
async def ingest_whatsapp_text(payload: WhatsAppTextPayload):
    try:
        return await process_whatsapp_text_payload(
            text=payload.text,
            source=payload.source,
            timestamp=payload.timestamp
        )
    except Exception as e:
        print(f"❌ [WhatsApp Ingest Error]: {e}")
        return {"status": "failed", "error": str(e)}

@app.post("/trigger")
async def trigger_agent():
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}
    initial_state = {"tickers": ["SYS", "HBL", "LUCK"]}
    
    await app.state.graph.ainvoke(initial_state, config=config)
    from portfolio_manager import publish_agent_event
    publish_agent_event('SYSTEM_ALERT', 'Manual Agent Trigger: SYS', 'Buy signal detected for SYS at optimal execution price. KMI-30 Compliant.', {'ticker': 'SYS'})
    return {"thread_id": thread_id, "status": "started", "message": "Triggered and published to agent feed"}

if __name__ == "__main__":
    import uvicorn
    import asyncio
    import os
    import sys

    if sys.platform == "win32":
        if sys.version_info < (3, 14):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        port = int(str(os.getenv("PORT", "8000")).strip().strip('"').strip("'"))
    except (ValueError, TypeError):
        port = 8000
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(config)
    asyncio.run(server.serve())
