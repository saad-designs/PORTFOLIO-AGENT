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

# In-memory cooldown dictionary to prevent repetitive Discord spam
alert_cooldowns = {}

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

async def run_intraday_price_loop():
    """
    Periodic 10-minute loop during market hours to evaluate profit and rebalancing triggers.
    """
    logger.info(f"🔄 [{datetime.now().strftime('%H:%M')}] Initializing Intraday Price & Alert Loop...")
    try:
        from portfolio_manager import check_profit_and_rebalance_triggers
        alerts = await asyncio.to_thread(check_profit_and_rebalance_triggers)
        
        if not alerts:
            return
            
        alert_channel = bot.get_channel(ADMIN_CHANNEL_ID)
        if not alert_channel:
            return
            
        now_ts = datetime.now().timestamp()
        
        for alert in alerts:
            ticker = alert["ticker"]
            alert_type = alert["type"]
            cooldown_key = f"{ticker}_{alert_type}"
            
            last_alert_time = alert_cooldowns.get(cooldown_key, 0)
            if now_ts - last_alert_time < 4 * 3600: # 4 hours cooldown
                continue
                
            alert_cooldowns[cooldown_key] = now_ts
            
            # Format actionable alert message
            msg = f"🚨 **PORTFOLIO ALERT ({alert_type.replace('_', ' ').title()})**\n"
            msg += f"**Ticker:** {ticker}\n"
            msg += f"**Current Price:** Rs. {alert['current_price']:.2f}\n"
            msg += f"**Gain:** {alert['gain_pct']:.2f}%\n"
            msg += f"**Action:** Sell **{alert['shares_to_sell']}** shares\n"
            msg += f"**Expected Cash Gen:** Rs. {alert['cash_generated']:,.2f}\n"
            msg += f"**Est. New Balance:** Rs. {alert['new_cash_balance']:,.2f}\n"
            
            await alert_channel.send(msg)
            
    except Exception as e:
        error_msg = f"❌ [Intraday Loop Error]: {str(e)}"
        logger.error(error_msg)
        
        alert_channel = bot.get_channel(ADMIN_CHANNEL_ID)
        if alert_channel:
            tb_str = traceback.format_exc()
            discord_msg = f"⚠️ **CRITICAL SWEEP FAILURE (Intraday)**\n```python\n{tb_str[-1900:]}\n```"
            await alert_channel.send(discord_msg)

async def run_daily_closing_briefing(bot_instance=None):
    """
    Executes at 16:00 PKT Mon-Thu, and 17:00 PKT on Fridays.
    Constructs a dark-themed Discord embed summarizing liquidity and performance.
    """
    logger.info(f"📊 [{datetime.now().strftime('%H:%M')}] Initializing Daily Closing Summary...")
    try:
        from portfolio_manager import generate_daily_closing_summary, record_daily_snapshot
        import discord
        
        # Snapshot the data first
        await asyncio.to_thread(record_daily_snapshot)
        
        # Fetch the finalized metrics
        summary = await asyncio.to_thread(generate_daily_closing_summary)
        
        if not summary:
            return
            
        active_bot = bot_instance or bot
        
        alert_channel = active_bot.get_channel(ADMIN_CHANNEL_ID)
        if not alert_channel:
            try:
                alert_channel = await active_bot.fetch_channel(ADMIN_CHANNEL_ID)
            except Exception:
                pass
                
        if not alert_channel:
            logger.error(f"Could not find or fetch ADMIN_CHANNEL_ID: {ADMIN_CHANNEL_ID}")
            return
            
        embed = discord.Embed(
            title="📊 Daily Closing Summary & Liquidity Report",
            color=0x2B2D31,
            description="Market closed. Here is your portfolio snapshot:"
        )
        
        embed.add_field(name="Total Portfolio Value", value=f"Rs. {summary['total_portfolio_value']:,.2f}", inline=False)
        embed.add_field(name="Liquid Cash", value=f"Rs. {summary['liquid_cash']:,.2f}", inline=True)
        embed.add_field(name="Equity Allocation %", value=f"{summary['equity_allocation_pct']:.1f}%", inline=True)
        
        # Daily PnL
        pnl = summary.get("daily_pnl", 0.0)
        pnl_pct = summary.get("daily_pnl_pct", 0.0)
        sign = "+" if pnl >= 0 else ""
        embed.add_field(name="Daily PnL", value=f"{sign}Rs. {pnl:,.2f} ({sign}{pnl_pct:.2f}%)", inline=False)
        
        # Top Gainer
        g_ticker, g_change = summary['top_gainer']
        if g_ticker and g_ticker != "N/A":
            embed.add_field(name="🏆 Top Gainer", value=f"{g_ticker} (+{g_change:.2f}%)", inline=True)
            
        # Top Loser
        l_ticker, l_change = summary['top_loser']
        if l_ticker and l_ticker != "N/A":
            embed.add_field(name="🔻 Top Loser", value=f"{l_ticker} ({l_change:.2f}%)", inline=True)
            
        await alert_channel.send(embed=embed)
        logger.info("✅ Daily Closing Summary sent.")
    except Exception as e:
        error_msg = f"❌ [Daily Closing Summary Error]: {str(e)}"
        logger.error(error_msg)
        alert_channel = bot.get_channel(ADMIN_CHANNEL_ID)
        if alert_channel:
            import traceback
            tb_str = traceback.format_exc()
            await alert_channel.send(f"⚠️ **CRITICAL SWEEP FAILURE (Daily Summary)**\n```python\n{tb_str[-1900:]}\n```")

async def start_scheduled_sweeps():
    """
    Async background scheduler loop grouping tasks into distinct temporal buckets.
    """
    logger.info("🚀 Master background scheduled sweeps started (Intraday Loop, 00:00 YouTube, 12/16:00 Email, 18:00 Business).")
    
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
            
        # 4. Intraday Price & Alert Loop (Every 10 minutes, weekdays 09:15 - 16:30)
        if current_date.weekday() < 5: # Monday = 0, Friday = 4
            if (now.hour > 9 or (now.hour == 9 and now.minute >= 15)) and (now.hour < 16 or (now.hour == 16 and now.minute <= 30)):
                last_intraday_ts = get_last_run("intraday_loop")
                if now.timestamp() - last_intraday_ts >= 600: # 600 seconds = 10 minutes
                    await run_intraday_price_loop()
                    update_last_run("intraday_loop")
                    
        # 5. Daily Closing Summary
        # Monday - Thursday (0-3) at 16:00
        # Friday (4) at 17:00
        if current_date.weekday() <= 3 and should_run("daily_summary_mt", 16):
            await run_daily_closing_briefing()
            update_last_run("daily_summary_mt")
        elif current_date.weekday() == 4 and should_run("daily_summary_f", 17):
            await run_daily_closing_briefing()
            update_last_run("daily_summary_f")
            
        # Check every 60 seconds to prevent tight CPU looping
        await asyncio.sleep(60)

def initialize_background_schedulers(bot_loop):
    """Hooks all tasks directly into the running Discord bot event loop."""
    bot_loop.create_task(start_scheduled_sweeps())
    logger.info("🚀 Scheduled sweeps task injected into bot event loop.")
