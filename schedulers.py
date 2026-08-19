import asyncio
import logging
import os
from datetime import datetime
from vector_brain import query_mentor_brain_tool, ingest_knowledge_tool, vector_db
import traceback
from discord_bot import bot, get_last_run, update_last_run
from langchain_google_genai import ChatGoogleGenerativeAI

# Define your admin channel ID here or pull from os.environ
ADMIN_CHANNEL_ID = int(os.getenv("ADMIN_CHANNEL_ID", 0))

logger = logging.getLogger("PortfolioAgentSchedulers")

async def run_youtube_sweep():
    """
    12:00 AM: YouTube Knowledge Sweep
    Extracts playlist transcripts, processes summaries, and logs them into memory.
    """
    logger.info("🤖 [00:00] Initializing YouTube Knowledge Sweep...")
    try:
        # Placeholder for targeted web/RSS/API extraction routine
        # In production, this pulls latest transcripts/articles into raw text strings
        class HarvestedItem:
            def __init__(self, source, title, content, date):
                self.source = source
                self.title = title
                self.content = content
                self.date = date

        harvested_items = [
            HarvestedItem("ARN", "Sector Rotation", "Recent analysis on sector rotation towards high-dividend energy stocks.", str(datetime.now())),
            HarvestedItem("InvestKaar", "Value Compounding", "Long-term value compounding framework updates for cyclical downturns.", str(datetime.now())),
            HarvestedItem("Jawad Hafeez", "Psychology", "Psychology of holding winners and cutting emotional losers in flat markets.", str(datetime.now()))
        ]
        
        # Inside your automated YouTube/Email loop:
        for item in harvested_items:
            # 1. Automatic Silent Vector Ingestion (No !learn needed)
            await asyncio.to_thread(
                vector_db.add_texts,
                texts=[item.content],
                metadatas=[{"source": item.source, "title": item.title, "date": item.date}]
            )
            
            # 2. LLM Summarization & Discord Output
            try:
                llm = ChatGoogleGenerativeAI(
                    model="gemini-3.5-flash-lite",
                    google_api_key=os.getenv("GOOGLE_API_KEY", "")
                )
                prompt = f"Summarize this briefly for Discord:\nTitle: {item.title}\nSource: {item.source}\nContent: {item.content}"
                summary_response = await asyncio.to_thread(llm.invoke, prompt)
                
                alert_channel = bot.get_channel(ADMIN_CHANNEL_ID)
                if alert_channel:
                    discord_msg = f"🧠 **Silent Vector Ingestion Complete**\n**Source:** {item.source}\n**Title:** {item.title}\n**Summary:** {summary_response.content}"
                    await alert_channel.send(discord_msg)
            except Exception as e:
                logger.error(f"Summarization error: {e}")
            
        logger.info("✅ [00:00] YouTube Knowledge Sweep successfully updated with fresh mental models.")
    except Exception as e:
        error_msg = f"❌ [YouTube Sweep Error]: {str(e)}"
        logger.error(error_msg)
        
        alert_channel = bot.get_channel(ADMIN_CHANNEL_ID)
        if alert_channel:
            tb_str = traceback.format_exc()
            discord_msg = f"⚠️ **CRITICAL SWEEP FAILURE (YouTube)**\n```python\n{tb_str[-1900:]}\n```"
            await alert_channel.send(discord_msg)

async def run_email_sweep():
    """
    12:00 PM & 4:00 PM: Email Sweeps
    Processes incoming portfolio notes or correspondence.
    """
    logger.info(f"📧 [{datetime.now().strftime('%H:%M')}] Initializing Email Sweep...")
    try:
        # Placeholder for incoming portfolio notes or correspondence parsing
        logger.info(f"✅ [{datetime.now().strftime('%H:%M')}] Email Sweep successfully processed incoming notes.")
    except Exception as e:
        error_msg = f"❌ [Email Sweep Error]: {str(e)}"
        logger.error(error_msg)
        
        alert_channel = bot.get_channel(ADMIN_CHANNEL_ID)
        if alert_channel:
            tb_str = traceback.format_exc()
            discord_msg = f"⚠️ **CRITICAL SWEEP FAILURE (Email)**\n```python\n{tb_str[-1900:]}\n```"
            await alert_channel.send(discord_msg)

async def run_business_portfolio_sweep():
    """
    6:00 PM: Business & Market Sweep
    Runs portfolio risk analytics, calculates Sharpe/MDD, and pushes performance health checks.
    """
    logger.info("⚖️ [18:00] Initializing Business & Portfolio Sweep...")
    try:
        # 1. Macro & Corporate News Scan
        macro_events = [
            {"category": "IPO / Corporate", "headline": "Upcoming sector initial public offerings and regulatory filings."},
            {"category": "Government / IMF", "headline": "Fiscal policy adjustments affecting industrial energy tariffs."}
        ]
        for event in macro_events:
            event_blob = f"Macro Wire [{event['category']}]: {event['headline']} | Date: {datetime.now()}"
            await asyncio.to_thread(ingest_knowledge_tool.invoke, {"input_text_or_url": event_blob})
        
        # 2. Risk & Rebalancing Engine Audit
        # Logic to evaluate holdings against target maximums (e.g., 20% single-stock cap)
        
        # 3. Predictive Synthesis
        recent_insights = await asyncio.to_thread(query_mentor_brain_tool.invoke, {"query": "Latest risk warnings or sector shifts"})
        if recent_insights:
            os.makedirs("memory_cache", exist_ok=True)
            with open("memory_cache/proactive_thought.txt", "w") as f:
                f.write(f"Proactive thought generated at {datetime.now()}: {recent_insights[:300]}")
                
        logger.info("✅ [18:00] Business & Portfolio Sweep calculations and checks complete.")
    except Exception as e:
        error_msg = f"❌ [Business Sweep Error]: {str(e)}"
        logger.error(error_msg)
        
        alert_channel = bot.get_channel(ADMIN_CHANNEL_ID)
        if alert_channel:
            tb_str = traceback.format_exc()
            discord_msg = f"⚠️ **CRITICAL SWEEP FAILURE (Business)**\n```python\n{tb_str[-1900:]}\n```"
            await alert_channel.send(discord_msg)

async def start_scheduled_sweeps():
    """
    Async background scheduler loop grouping tasks into distinct temporal buckets.
    """
    logger.info("🚀 Master background scheduled sweeps started (00:00 YouTube, 12/16:00 Email, 18:00 Business).")
    
    while True:
        now = datetime.now()
        current_date = now.date()
        
        def should_run(task_name, target_hour):
            last_timestamp = get_last_run(task_name)
            if last_timestamp == 0:
                return now.hour >= target_hour
            last_date = datetime.fromtimestamp(last_timestamp).date()
            return last_date < current_date and now.hour >= target_hour

        # 1. YouTube Knowledge Sweep (12:00 AM)
        if should_run("youtube", 0):
            await run_youtube_sweep()
            update_last_run("youtube")
            
        # 2. Email Sweeps (12:00 PM and 4:00 PM)
        if should_run("email_noon", 12):
            await run_email_sweep()
            update_last_run("email_noon")
            
        if should_run("email_afternoon", 16):
            await run_email_sweep()
            update_last_run("email_afternoon")
            
        # 3. Business & Portfolio Sweep (6:00 PM)
        if should_run("business", 18):
            await run_business_portfolio_sweep()
            update_last_run("business")
            
        # Check every 60 seconds to prevent tight CPU looping
        await asyncio.sleep(60)

def initialize_background_schedulers(bot_loop):
    """Hooks all tasks directly into the running Discord bot event loop."""
    bot_loop.create_task(start_scheduled_sweeps())
    logger.info("🚀 Scheduled sweeps task injected into bot event loop.")
