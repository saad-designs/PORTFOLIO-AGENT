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
    # Force it into sys.modules globally so any file can access it
    sys.modules['psycopg2'] = psycopg2
    sys.modules['psycopg2.extras'] = psycopg2.extras
except ImportError as e:
    print(f"FATAL: psycopg2-binary is missing: {e}")
    sys.exit(1)
# ---------------------------------------
from dotenv import load_dotenv
load_dotenv()

import subprocess
import atexit
import os
import time

bridge_dir = os.path.join(os.getcwd(), "whatsapp_bridge")
print("🚀 Booting Node.js WhatsApp Bridge as a background subprocess...")

# Pass cwd=bridge_dir so Node finds its dependencies and auth cache
node_process = subprocess.Popen(
    ["node", "whatsapp_bridge.js"],
    cwd=bridge_dir, 
    shell=True
    # Comment these two out:
    # stdout=subprocess.DEVNULL,
    # stderr=subprocess.STDOUT
)

# Give the Express server 3 seconds to bind to port 3000 before Python pings it
time.sleep(3)

import psutil

def cleanup_node_process():
    """Ruthlessly hunts down and terminates the WhatsApp Node bridge on exit."""
    global node_process
    if node_process:
        print("🛑 Terminating background Node.js WhatsApp Bridge...")
        try:
            # Cross-platform kill for the process and its children
            parent = psutil.Process(node_process.pid)
            for child in parent.children(recursive=True):
                child.kill()
            parent.kill()
        except psutil.NoSuchProcess:
            pass

atexit.register(cleanup_node_process)


import rate_limiter
rate_limiter.apply_global_rate_limits()

import psycopg2
import psycopg2.extras

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, UploadFile, File, Form
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from typing import Optional
import re
import json
if not os.getenv("GEMINI_API_KEY") and os.getenv("GOOGLE_API_KEY"):
    os.environ["GEMINI_API_KEY"] = os.getenv("GOOGLE_API_KEY")
from google import genai
raw_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
client = genai.Client(api_key=raw_key.strip() if raw_key else None, http_options={"timeout": 300000})
import agent
from langgraph.types import Command
import uuid
import os

import discord_bot
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from ingestion import sweep_rss_feeds, sweep_youtube_transcripts, process_whatsapp_media_payload, process_whatsapp_text_payload
from scheduler_config import scheduler
import aiohttp
from portfolio_manager import get_whatsapp_checkpoint, update_whatsapp_checkpoint, get_whatsapp_monitored_chats

async def run_startup_orchestration():
    """
    Sequential Startup Orchestrator:
    Prevents startup stampede and rate-limit exhaustion.
    1. Waits for WhatsApp Bridge to report READY (or skips cleanly if offline).
    2. Runs WhatsApp catch-up sweep with sanitized chat IDs (filters out 'XXXX' dummy IDs).
    3. Runs Business & Portfolio Sweep.
    4. Waits 10 seconds.
    5. Runs YouTube catch-up sweep ONCE.
    """
    print("🚀 [Startup Orchestrator] Initializing sequential startup sequence...")
    
    # --- 1. WhatsApp Bridge Catch-Up ---
    last_ts = get_whatsapp_checkpoint()
    raw_chats = get_whatsapp_monitored_chats()
    # Filter out dummy placeholder chat IDs containing 'xxxx'
    monitored_chats = [cid for cid in raw_chats if "xxxx" not in str(cid).lower()]
    
    if monitored_chats:
        bridge_ready = False
        print("📱 [Startup Orchestrator] Polling WhatsApp Bridge (http://127.0.0.1:3000/ping) for READY status...")
        async with aiohttp.ClientSession() as session:
            # Poll for up to 30 seconds
            for attempt in range(6):
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
                    print(f"🔄 [Startup Orchestrator] Executing WhatsApp catch-up for {len(monitored_chats)} monitored chats...")
                    async with session.post(
                        "http://127.0.0.1:3000/api/catchup",
                        json={"since_timestamp": last_ts, "target_chat_ids": monitored_chats},
                        timeout=aiohttp.ClientTimeout(total=120)
                    ) as resp:
                        data = await resp.json()
                        print(f"✅ [Startup Orchestrator] WhatsApp catch-up complete: {data.get('replayed', 0)} messages replayed.")
                except Exception as wa_err:
                    print(f"⚠️ [Startup Orchestrator] WhatsApp catch-up request failed: {wa_err}")
            else:
                print("ℹ️ [Startup Orchestrator] WhatsApp Bridge not ready or offline after 30s. Skipping startup catch-up.")
    else:
        print("ℹ️ [Startup Orchestrator] No valid monitored WhatsApp chats found in DB.")

    # --- 2. Business & Portfolio Sweep ---
    try:
        from schedulers import run_business_portfolio_sweep
        print("⚖️ [Startup Orchestrator] Running Business & Portfolio Sweep...")
        await run_business_portfolio_sweep()
        print("✅ [Startup Orchestrator] Business Sweep completed.")
    except Exception as bus_err:
        print(f"⚠️ [Startup Orchestrator] Business Sweep failed: {bus_err}")

    # --- 3. Buffer Sleep (10 seconds) ---
    print("⏳ [Startup Orchestrator] Waiting 10 seconds before YouTube sweep...")
    await asyncio.sleep(10)

    # --- 4. YouTube Catch-up Sweep ONCE ---
    try:
        from schedulers import run_youtube_sweep
        print("🕒 [Startup Orchestrator] Running YouTube catch-up sweep...")
        await run_youtube_sweep()
        print("✅ [Startup Orchestrator] YouTube catch-up sweep completed.")
    except Exception as yt_err:
        print(f"⚠️ [Startup Orchestrator] YouTube sweep failed: {yt_err}")

    print("🎯 [Startup Orchestrator] Sequential startup sequence completed successfully.")

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
        # Start Discord bot concurrently without blocking Uvicorn
        asyncio.create_task(discord_bot.bot.start(token))
        
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
    
    # Grab the active event loop
    loop = asyncio.get_running_loop()
    
    from schedulers import initialize_background_schedulers
    # Initialize background intelligence loops
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

# A dedicated, microscopic endpoint for cron-job.org
@app.get("/ping", response_class=PlainTextResponse)
@app.head("/ping", response_class=PlainTextResponse)
async def ping():
    return "OK"

@app.get("/")
@app.head("/")
async def health_check():
    return {"status": "alive", "service": "Portfolio Agent Watchdog"}

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
        from agent import get_agent  # FIX: Import the async getter, not the empty variable
        
        # 1. Verify Channel Access
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

        # 2. Force AI Execution
        print(f"🧠 [Webhook Task] Waking up LangGraph agent for {ticker}...")
        prompt = f"Market Alert: {ticker} triggered a {signal} signal at Rs. {price}. Run a calculate_dcf_valuation for {ticker} and give me a strict tactical verdict comparing fair value to the current price."
        initial_state = {"messages": [("user", prompt)]}
        
        # FIX: Ensure the graph is compiled and fetched before invoking
        agent_app = await get_agent()
        
        config = {"configurable": {"thread_id": f"webhook_alert_{ticker}"}}
        ai_verdict = await agent_app.ainvoke(initial_state, config)
        
        # 3. Route Output
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


    # Trigger agent background workflow using BackgroundTasks wrapped in our dispatcher
    thread_id = str(uuid.uuid4())
    
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
    import base64
    from portfolio_manager import supabase
    
    chat_id = payload.chat_id or "KTrade"
    sender = payload.sender or "KTrade"
    body = payload.body or ""
    source = payload.source or "KTrade"
    timestamp = payload.timestamp or int(time.time())

    # Optional archival to Supabase messages table
    if supabase:
        try:
            supabase.table("messages").insert({
                "chat_id": chat_id,
                "sender": sender,
                "body": body[:500] if body else f"[{payload.filename or 'Media Attachment'}]",
                "timestamp": timestamp,
                "is_group": payload.is_group
            }).execute()
        except Exception:
            pass

    # 1. Multimodal payload (image charts, PDF research, or voice notes)
    if payload.has_media and payload.media_base64:
        try:
            file_bytes = base64.b64decode(payload.media_base64)
            mimetype = payload.mime_type or "image/png"
            filename = payload.filename or "media_attachment"
            print(f"📥 [WhatsApp Webhook] Multimodal Ingestion: {filename} ({mimetype}, {len(file_bytes)} bytes) from {source}")
            result = await process_whatsapp_media_payload(
                file_bytes=file_bytes,
                filename=filename,
                mimetype=mimetype,
                caption=body,
                source=source,
                timestamp=timestamp
            )
            return {"status": "success", "multimodal": True, "result": result}
        except Exception as e:
            print(f"❌ [WhatsApp Webhook] Multimodal processing failed: {e}")
            return {"status": "error", "error": str(e)}

    # 2. Plain text intelligence payload
    elif body and len(body.strip()) > 0:
        result = await process_whatsapp_text_payload(
            text=body.strip(),
            source=source,
            timestamp=timestamp
        )
        return {"status": "success", "multimodal": False, "result": result}

    return {"status": "ignored", "reason": "empty body and no media"}

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
    
    # Trigger the bot to send the interactive ExecutionView card
    await discord_bot.send_alert(thread_id, "Buy signal detected for SYS at optimal execution price. KMI-30 Compliant.")
    
    return {"thread_id": thread_id, "status": "started", "message": "Awaiting human approval"}

if __name__ == "__main__":
    import uvicorn
    import asyncio
    import os
    import sys

    # Force SelectorEventLoop on Windows
    if sys.platform == "win32":
        import sys
        if sys.version_info < (3, 14):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    port = int(os.getenv("PORT", 8000))
    
    # Build the Uvicorn config, but DO NOT use uvicorn.run()
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(config)
    
    # We manually create the event loop and serve the app inside it
    asyncio.run(server.serve())
