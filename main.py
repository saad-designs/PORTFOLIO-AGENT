import sys
# Force UTF-8 stdout before ANY import that may print emojis (Windows cp1252 fix)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
import asyncio

# Force SelectorEventLoop on Windows to support psycopg async pools
if sys.platform == "win32":
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

import rate_limiter
rate_limiter.apply_global_rate_limits()

import psycopg2
import psycopg2.extras

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
import agent
from langgraph.types import Command
import uuid
import os

import discord_bot
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from ingestion import sweep_rss_feeds, sweep_youtube_transcripts
from scheduler_config import scheduler

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
    
    from schedulers import initialize_background_schedulers, run_youtube_sweep
    # Initialize background intelligence loops
    initialize_background_schedulers(loop)
    print("🚀 Portfolio Agent core running with autonomous background learning.")
    
    print("🚀 Triggering YouTube & Business Sweep catch-up...")
    asyncio.create_task(run_youtube_sweep())
    
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
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    port = int(os.getenv("PORT", 8000))
    
    # Build the Uvicorn config, but DO NOT use uvicorn.run()
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(config)
    
    # We manually create the event loop and serve the app inside it
    asyncio.run(server.serve())
