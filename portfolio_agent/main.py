from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from pydantic import BaseModel
import agent
from langgraph.types import Command
import uuid
import os
import discord_bot
import asyncio

app = FastAPI()

class WebhookPayload(BaseModel):
    thread_id: str
    action: str

@app.on_event("startup")
async def startup_event():
    app.state.graph = agent.get_compiled_graph()
    
    # Start bot in the same loop so bot.get_channel works when triggered
    token = os.getenv("DISCORD_BOT_TOKEN")
    if token:
        asyncio.create_task(discord_bot.bot.start(token))

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
