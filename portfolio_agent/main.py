from dotenv import load_dotenv
load_dotenv()

from contextlib import asynccontextmanager
from fastapi import FastAPI
from pydantic import BaseModel
import agent
from langgraph.types import Command
import uuid
import os
import discord_bot
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from ingestion import sweep_rss_feeds, sweep_youtube_transcripts

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    app.state.graph = agent.get_compiled_graph()
    
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
        hours=4
    )
    scheduler.add_job(
        sweep_youtube_transcripts, 
        'interval', 
        hours=24
    )
    scheduler.start()
    
    yield
    # Shutdown
    print("🛑 Shutting down Discord bot...", flush=True)
    await discord_bot.bot.close()
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

class WebhookPayload(BaseModel):
    thread_id: str
    action: str

@app.post("/webhook/discord")
async def discord_webhook(payload: WebhookPayload):
    config = {"configurable": {"thread_id": payload.thread_id}}
    app.state.graph.invoke(Command(resume=payload.action), config=config)
    return {"status": "success", "action": payload.action}

@app.post("/trigger")
async def trigger_agent():
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}
    initial_state = {"tickers": ["SYS", "HBL", "LUCK"]}
    
    app.state.graph.invoke(initial_state, config=config)
    
    # Trigger the bot to send the interactive ExecutionView card
    await discord_bot.send_alert(thread_id, "Buy signal detected for SYS at optimal execution price. KMI-30 Compliant.")
    
    return {"thread_id": thread_id, "status": "started", "message": "Awaiting human approval"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
