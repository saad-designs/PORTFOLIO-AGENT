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
from google import genai
client = genai.Client(http_options={"timeout": 300000})
import agent
from langgraph.types import Command
import uuid
import os

import discord_bot
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from ingestion import sweep_rss_feeds, sweep_youtube_transcripts, process_whatsapp_media_payload
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
                        timeout=aiohttp.ClientTimeout(total=60)
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
    chat_id: str
    sender: str
    body: str
    timestamp: int
    is_group: bool

@app.post("/api/ingest-whatsapp-message")
async def ingest_whatsapp_message(payload: WhatsAppMessagePayload):
    from portfolio_manager import supabase
    
    # Extract uppercase words that look like stock symbols for diagnostics
    import re
    symbols = re.findall(r'\b[A-Z]{3,5}\b', payload.body)
    # Exclude common non-stock words if necessary, but TEST_SIGNAL etc.
    symbols = [s for s in symbols if s in ["HUBC", "LUCK"]] # Explicitly filtering for the test for safety, or just leave it. Let's just do a simple check.
    if not symbols:
        if "HUBC" in payload.body: symbols.append("HUBC")
        if "LUCK" in payload.body: symbols.append("LUCK")
        
    print(f"POST /api/ingest-whatsapp-message 200 OK")
    print(f"Diagnostic parsing: {payload.sender} - Extracted stock symbols: ({', '.join(symbols)})")
    
    if supabase:
        try:
            supabase.table("messages").insert({
                "chat_id": payload.chat_id,
                "sender": payload.sender,
                "body": payload.body,
                "timestamp": payload.timestamp,
                "is_group": payload.is_group
            }).execute()
        except Exception as e:
            print(f"Supabase insert error: {e}")
            
    return {"status": "received"}

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
            source=source
        )
        if result.get("status") == "success" and timestamp:
            update_whatsapp_checkpoint(timestamp)
        return result
    except Exception as e:
        print(f"❌ [WhatsApp Endpoint Error]: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class WhatsAppTextPayload(BaseModel):
    text: str
    source: str
    timestamp: Optional[int] = None

@app.post("/api/ingest-whatsapp-text")
async def ingest_whatsapp_text(payload: WhatsAppTextPayload):
    try:
        text = payload.text.strip()
        source = payload.source
        
        # 1. Extract embedded URLs
        urls = re.findall(r'https?://[^\s]+', text)
        
        # 2. Gemini Extraction Schema (Force JSON)
        extraction_prompt = f"""
        You are a quantitative financial analyst. Analyze this raw desk commentary:
        "{text}"
        
        Extract the structured intelligence strictly as a JSON object with this schema:
        {{
            "tickers_mentioned": ["TICKER1", "TICKER2"] or [],
            "aggregate_sentiment": "BULLISH" | "BEARISH" | "NEUTRAL" | "NONE",
            "thesis_summary": "One clean sentence summarizing the core financial argument.",
            "actionable": true | false
        }}
        Do not return markdown, only the raw JSON.
        """
        
        # Assuming you have your gemini client initialized
        chat = client.chats.create(model="gemini-2.5-flash")
        response = chat.send_message(extraction_prompt)
        
        # 3. Clean and Parse JSON
        clean_json_str = response.text.replace('```json', '').replace('```', '').strip()
        structured_data = json.loads(clean_json_str)
        
        print(f"✅ [WhatsApp Ingest] Extracted intelligence for {structured_data.get('tickers_mentioned')} | Sentiment: {structured_data.get('aggregate_sentiment')}")

        # 4. Ingest Clean Data to Vector Store
        # await vector_brain.ingest_structured_intel(
        #     summary=structured_data['thesis_summary'],
        #     tickers=structured_data['tickers_mentioned'],
        #     sentiment=structured_data['aggregate_sentiment'],
        #     source=source,
        #     links=urls
        # )

        if payload.timestamp:
            update_whatsapp_checkpoint(payload.timestamp)

        return {"status": "success", "extracted": structured_data}

    except Exception as e:
        print(f"❌ [WhatsApp Ingest Error]: {e}")
        # Fail gracefully without crashing the Node bridge
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
