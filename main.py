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
    print(f"FATAL: psycopg2-binary is missing: {e}")
    sys.exit(1)
# ---------------------------------------
from dotenv import load_dotenv
load_dotenv()

# --- API KEY ENVIRONMENT SANITIZATION ---
# Prevent Google GenAI and LangChain from spamming duplicate key warning
import os
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

bridge_dir = os.path.join(os.getcwd(), "whatsapp_bridge")
print("🚀 Booting Node.js WhatsApp Bridge concurrently in background...")

# Fast non-blocking boot (removed 3s sleep)
node_process = subprocess.Popen(
    ["node", "whatsapp_bridge.js"],
    cwd=bridge_dir, 
    shell=True
)

import psutil

def cleanup_node_process():
    """Ruthlessly hunts down and terminates the WhatsApp Node bridge on exit."""
    global node_process
    if node_process:
        print("🛑 Terminating background Node.js WhatsApp Bridge...")
        try:
            parent = psutil.Process(node_process.pid)
            for child in parent.children(recursive=True):
                child.kill()
            parent.kill()
        except psutil.NoSuchProcess:
            pass

atexit.register(cleanup_node_process)

import rate_limiter
rate_limiter.apply_global_rate_limits()

import urllib.parse

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, UploadFile, File, Form
from fastapi.responses import PlainTextResponse, HTMLResponse
from pydantic import BaseModel
from typing import Optional

from google import genai
raw_key = os.getenv("GOOGLE_API_KEY")
client = genai.Client(api_key=raw_key.strip() if raw_key else None, http_options={"timeout": 300000})

import agent
from langgraph.types import Command
import discord_bot
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

async def run_startup_orchestration():
    """
    Sequential Startup Orchestrator with Lazily Staggered Checks:
    Prevents boot stampede and quota exhaustion.
    1. WhatsApp polling & catch-up runs in background as soon as bridge is ready.
    2. At t+30s: Business Sweep runs ONLY IF last sweep was > 4 hours ago.
    3. At t+90s: YouTube Sweep runs ONLY IF last sweep was > 6 hours ago.
    """
    print("🚀 [Startup Orchestrator] Initializing non-blocking startup sequences...")

    # 1. Non-blocking WhatsApp Bridge polling task
    asyncio.create_task(poll_and_catchup_whatsapp())

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
    app.state.graph = await agent.get_agent()
    
    # Retrieve token
    token = os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
    if not token:
        print("❌ CRITICAL: DISCORD_TOKEN is missing from environment variables!", flush=True)
    else:
        print("🚀 Starting Discord bot in background task...", flush=True)
        asyncio.create_task(discord_bot.bot.start(token))

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
    
    # Trigger sequential startup orchestration in background task
    asyncio.create_task(run_startup_orchestration())
    yield
    # Shutdown
    print("🛑 Shutting down Discord bot...", flush=True)
    await discord_bot.bot.close()
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

# Microscopic health endpoint
@app.get("/ping", response_class=PlainTextResponse)
@app.head("/ping", response_class=PlainTextResponse)
async def ping():
    return "OK"

@app.get("/")
@app.head("/")
async def health_check():
    return {"status": "alive", "service": "Portfolio Agent Watchdog"}

@app.get("/whatsapp-qr", response_class=HTMLResponse)
async def whatsapp_qr_page():
    qr_data = None
    status = "INITIALIZING"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("http://127.0.0.1:3000/qr", timeout=aiohttp.ClientTimeout(total=3)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    status = data.get("status", "INITIALIZING")
                    qr_data = data.get("qr")
    except Exception:
        status = "CONNECTING"

    if status == "READY":
        content_html = """
        <div class="card">
            <div class="icon">✅</div>
            <h2>WhatsApp Bridge Connected</h2>
            <p class="subtitle">Live listener is actively authenticated and monitoring KTrade groups.</p>
            <div class="badge badge-ready">STATUS: READY & MONITORING</div>
        </div>
        """
        refresh_tag = '<meta http-equiv="refresh" content="30">'
    elif status == "WAITING_FOR_QR" and qr_data:
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

class WebhookPayload(BaseModel):
    thread_id: str
    action: str

@app.post("/webhook/discord")
async def discord_webhook(payload: WebhookPayload):
    config = {"configurable": {"thread_id": payload.thread_id}}
    await app.state.graph.ainvoke(Command(resume=payload.action), config=config)
    return {"status": "success", "action": payload.action}

async def process_webhook_alert(ticker: str, signal: str, price: float):
    import os
    import traceback
    
    print(f"⚡ [Webhook Task] Background worker started for {ticker}...")
    try:
        from discord_bot import bot, send_long_message
        from agent import get_agent
        
        channel_id_str = os.getenv("ALERT_CHANNEL_ID") or os.getenv("ADMIN_CHANNEL_ID")
        if not channel_id_str:
            print("❌ [Webhook Task] ERROR: ALERT_CHANNEL_ID or ADMIN_CHANNEL_ID is not set!")
            return
            
        channel = bot.get_channel(int(channel_id_str))
        if not channel:
            print(f"❌ [Webhook Task] ERROR: Bot could not find the Discord channel {channel_id_str}.")
            return

        print(f"✅ [Webhook Task] Channel found. Sending initial Discord alert...")
        await channel.send(f"🚨 **Real-Time Webhook Triggered:** `{ticker}` hit signal `{signal}` at price `Rs. {price}`. Initiating instant DCF valuation sweep...")

        prompt = f"Market Alert: {ticker} triggered a {signal} signal at Rs. {price}. Run a calculate_dcf_valuation for {ticker} and give me a strict tactical verdict comparing fair value to the current price."
        initial_state = {"messages": [("user", prompt)]}
        
        agent_app = await get_agent()
        config = {"configurable": {"thread_id": f"webhook_alert_{ticker}"}}
        ai_verdict = await agent_app.ainvoke(initial_state, config)
        
        print(f"✅ [Webhook Task] Agent analysis complete. Pushing to Discord...")
        await send_long_message(channel, ai_verdict)
        print(f"🚀 [Webhook Task] Webhook pipeline fully complete for {ticker}.")

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
    await discord_bot.send_alert(thread_id, "Buy signal detected for SYS at optimal execution price. KMI-30 Compliant.")
    return {"thread_id": thread_id, "status": "started", "message": "Awaiting human approval"}

if __name__ == "__main__":
    import uvicorn
    import asyncio
    import os
    import sys

    if sys.platform == "win32":
        if sys.version_info < (3, 14):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    port = int(os.getenv("PORT", 8000))
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(config)
    asyncio.run(server.serve())
