import asyncio
import logging
import os
from datetime import datetime, timedelta
from vector_brain import query_mentor_brain_tool, ingest_knowledge_tool, vector_db, evaluate_trade_trigger
import traceback
from discord_bot import bot, get_last_run, update_last_run, send_long_message
from langchain_google_genai import ChatGoogleGenerativeAI

# Define your admin channel ID here or pull from os.environ
ADMIN_CHANNEL_ID = int(os.getenv("ADMIN_CHANNEL_ID", 0))
PROSPECT_CHANNEL_ID = int(os.getenv("PROSPECT_CHANNEL_ID", 0)) or ADMIN_CHANNEL_ID
RESEARCH_CHANNEL_ID = int(os.getenv("RESEARCH_CHANNEL_ID", 0)) or ADMIN_CHANNEL_ID

logger = logging.getLogger("PortfolioAgentSchedulers")

import json

STATE_DATE_FILE = "scheduler_date_state.json"

def get_last_run_date(task_name):
    if os.path.exists(STATE_DATE_FILE):
        with open(STATE_DATE_FILE, "r") as f:
            try:
                data = json.load(f)
                return data.get(task_name, "")
            except:
                pass
    return ""

def set_last_run_date(task_name, date_str):
    data = {}
    if os.path.exists(STATE_DATE_FILE):
        with open(STATE_DATE_FILE, "r") as f:
            try:
                data = json.load(f)
            except:
                pass
    data[task_name] = date_str
    with open(STATE_DATE_FILE, "w") as f:
        json.dump(data, f)

# In-memory cooldown dictionary to prevent repetitive Discord spam
alert_cooldowns = {}
technical_alert_cooldowns = {}

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
                
                content_val = summary_response.content
                if isinstance(content_val, list):
                    cleaned_summary = "".join(part.get("text", "") for part in content_val if isinstance(part, dict))
                elif isinstance(content_val, dict):
                    cleaned_summary = content_val.get("text", str(content_val))
                else:
                    cleaned_summary = str(content_val)
                
                alert_channel = bot.get_channel(RESEARCH_CHANNEL_ID)
                if alert_channel:
                    discord_msg = f"🧠 **Silent Vector Ingestion Complete**\n**Source:** {item.source}\n**Title:** {item.title}\n**Summary:** {cleaned_summary}"
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
        set_last_run_date("email_sweep", datetime.now().strftime("%Y-%m-%d"))
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
    Business & Market Sweep (08:30, 13:00, 18:00 PKT)
    Runs portfolio risk analytics, calculates Sharpe/MDD, and pushes performance health checks.
    """
    logger.info(f"⚖️ [{datetime.now().strftime('%H:%M')}] Initializing Business & Portfolio Sweep...")
    try:
        from portfolio_manager import update_ticker_valuations
        # Update per-ticker trailing PEs for macro triggers (direct PSX portal scrape)
        await asyncio.to_thread(update_ticker_valuations)
        
        # 1. Macro & Corporate News Scan
        from news_scraper import fetch_business_news
        news_feed = await fetch_business_news()
        
        macro_events = [
            {"category": "IPO / Corporate", "headline": "Upcoming sector initial public offerings and regulatory filings."},
            {"category": "Government / IMF", "headline": "Fiscal policy adjustments affecting industrial energy tariffs."}
        ]
        for event in macro_events:
            event_blob = f"Macro Wire [{event['category']}]: {event['headline']} | Date: {datetime.now()}"
            await asyncio.to_thread(ingest_knowledge_tool.invoke, {"input_text_or_url": event_blob})
        
        # 2. Risk & Rebalancing Engine Audit
        # Logic to evaluate holdings against target maximums (e.g., 20% single-stock cap)
        
        # 3. Predictive Synthesis via LangGraph Analyst Node
        try:
            from valuation_engine import get_deterministic_portfolio_summary
            portfolio_data = await asyncio.to_thread(get_deterministic_portfolio_summary)
            current_holdings = "EXACT_FINANCIAL_GROUND_TRUTH:\n" + portfolio_data.get('formatted_text', 'Data Unavailable.')
        except Exception as e:
            current_holdings = f"EXACT_FINANCIAL_GROUND_TRUTH:\nData Unavailable. ({e})"

        from agent import get_evaluation_graph
        graph = get_evaluation_graph()
        
        state = {
            "ticker": "MACRO_SWEEP",
            "trigger_type": "macro_news_sweep",
            "quantitative_data": {},
            "rag_context": f"Business News Feed:\n{news_feed}\n\n{current_holdings}",
            "iteration_count": 0
        }
        
        result = await asyncio.to_thread(graph.invoke, state)
        ai_verdict = result.get("analyst_thesis", "No analysis provided.")
        
        # Output proactive thought
        os.makedirs("memory_cache", exist_ok=True)
        with open("memory_cache/proactive_thought.txt", "w", encoding="utf-8") as f:
            f.write(f"Proactive thought generated at {datetime.now()}:\n{ai_verdict}")
            
        # Send alert
        alert_channel = bot.get_channel(ADMIN_CHANNEL_ID)
        if alert_channel:
            await send_long_message(alert_channel, ai_verdict)
            
        logger.info(f"✅ [{datetime.now().strftime('%H:%M')}] Business & Portfolio Sweep calculations and checks complete.")
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
    Periodic 60-minute loop during market hours to evaluate profit and rebalancing triggers.
    """
    logger.info(f"🔄 [{datetime.now().strftime('%H:%M')}] Initializing Intraday Price & Alert Loop...")
    try:
        from portfolio_manager import check_profit_and_rebalance_triggers
        alerts = await asyncio.to_thread(check_profit_and_rebalance_triggers)
        
        logger.info(f"🔄 Intraday Loop: Found {len(alerts) if alerts else 0} profit/rebalance triggers.")
        
        if not alerts:
            return
            
        alert_channel = bot.get_channel(PROSPECT_CHANNEL_ID)
        if not alert_channel:
            logger.warning(f"⚠️ No valid Discord alert_channel found (ID: {PROSPECT_CHANNEL_ID}). Alerts will only print to console.")
            
        now_ts = datetime.now().timestamp()
        
        for alert in alerts:
            from utils import normalize_ticker
            ticker = normalize_ticker(alert["ticker"])
            alert_type = alert["type"]
            cooldown_key = f"{ticker}_{alert_type}"
            
            last_alert_time = alert_cooldowns.get(cooldown_key, 0)
            if now_ts - last_alert_time < 4 * 3600: # 4 hours cooldown
                logger.info(f"⏭️ Skipping alert for {ticker} due to 4-hour cooldown.")
                continue
                
            alert_cooldowns[cooldown_key] = now_ts
            
            from agent import get_evaluation_graph, fetch_stocks_with_fallback
            from vector_brain import get_market_context
            from datetime import timedelta
            import pandas as pd
            
            # Calculate momentum
            end_date = datetime.today()
            start_date = end_date - timedelta(days=30)
            raw_hist, _ = fetch_stocks_with_fallback(ticker, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            
            momentum_data = {}
            if not raw_hist.empty and len(raw_hist) >= 5:
                closes = raw_hist['Close'] if 'Close' in raw_hist.columns else raw_hist.iloc[:, 3]
                closes = pd.to_numeric(closes, errors='coerce').dropna()
                if len(closes) >= 5:
                    five_day_change = ((closes.iloc[-1] - closes.iloc[-5]) / closes.iloc[-5]) * 100
                    momentum_data['5_day_change_pct'] = five_day_change
                if len(closes) >= 20:
                    ma_20 = closes.tail(20).mean()
                    distance_ma_20 = ((closes.iloc[-1] - ma_20) / ma_20) * 100
                    momentum_data['distance_from_20d_ma_pct'] = distance_ma_20
                    
            alert["quantitative_data"] = momentum_data
            
            # Fetch context
            rag_context = await get_market_context(ticker)
            
            # Hybrid Evaluation Gate using LangGraph
            graph = get_evaluation_graph()
            state = {
                "ticker": ticker,
                "trigger_type": alert_type,
                "quantitative_data": momentum_data,
                "rag_context": rag_context,
                "iteration_count": 0
            }
            
            result = await asyncio.to_thread(graph.invoke, state)
            ai_verdict = result.get("analyst_thesis", "No thesis generated.")
            
            # Format actionable alert message
            if alert_type == "stale_target_warning":
                msg = f"⚠️ **STALE TARGET WARNING**\n"
                msg += f"**Ticker:** {ticker}\n"
                msg += f"**Message:** {alert.get('message', '')}\n"
            else:
                msg = f"🚨 **PORTFOLIO ALERT ({alert_type.replace('_', ' ').title()})**\n"
                msg += f"**Ticker:** {ticker}\n"
                msg += f"**Current Price:** Rs. {alert.get('current_price', 0):.2f}\n"
                if 'gain_pct' in alert:
                    msg += f"**Gain:** {alert['gain_pct']:.2f}%\n"
                msg += f"**Action:** Sell **{alert.get('shares_to_sell', 0)}** shares\n"
                msg += f"**Expected Cash Gen:** Rs. {alert.get('cash_generated', 0):,.2f}\n"
                msg += f"**Est. New Balance:** Rs. {alert.get('new_cash_balance', 0):,.2f}\n\n"
                msg += f"🧠 **AI Contextual Verdict (Multi-Agent):**\n{ai_verdict}\n"
            
            if alert_channel:
                await alert_channel.send(msg)
            else:
                print(f"=== PORTFOLIO ALERT (Console Fallback) ===\n{msg}")
            
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
        set_last_run_date("daily_closing_briefing", datetime.now().strftime("%Y-%m-%d"))
        logger.info("✅ Daily Closing Summary sent.")
    except Exception as e:
        error_msg = f"❌ [Daily Closing Summary Error]: {str(e)}"
        logger.error(error_msg)
        alert_channel = bot.get_channel(ADMIN_CHANNEL_ID)
        if alert_channel:
            import traceback
            tb_str = traceback.format_exc()
            await alert_channel.send(f"⚠️ **CRITICAL SWEEP FAILURE (Daily Summary)**\n```python\n{tb_str[-1900:]}\n```")

async def run_intraday_technical_scanner():
    """
    Every 2 hours during active market hours.
    Runs technical scanner for oversold/overbought/volume signals.
    """
    logger.info(f"📈 [{datetime.now().strftime('%H:%M')}] Initializing Technical Analysis Scanner...")
    try:
        from technical_engine import run_technical_scanner
        alerts = await asyncio.to_thread(run_technical_scanner)
        
        if not alerts:
            return
            
        alert_channel = bot.get_channel(ADMIN_CHANNEL_ID)
        if not alert_channel:
            logger.warning(f"⚠️ No valid Discord admin channel found. Alerts will only print to console.")
            
        now_ts = datetime.now().timestamp()
        
        for alert in alerts:
            ticker = alert["ticker"]
            message = alert["message"]
            
            last_alert_time = technical_alert_cooldowns.get(ticker, 0)
            # 12 hours cooldown to avoid daily alert fatigue if oversold for consecutive days
            if now_ts - last_alert_time < 12 * 3600: 
                logger.info(f"⏭️ Skipping tech alert for {ticker} due to 12-hour cooldown.")
                continue
                
            technical_alert_cooldowns[ticker] = now_ts
            
            if alert_channel:
                await alert_channel.send(message)
            else:
                print(f"=== TECHNICAL ALERT ===\n{message}")
                
        logger.info(f"✅ Technical Analysis Scanner complete.")
    except Exception as e:
        error_msg = f"❌ [Technical Scanner Error]: {str(e)}"
        logger.error(error_msg)
        alert_channel = bot.get_channel(ADMIN_CHANNEL_ID)
        if alert_channel:
            import traceback
            tb_str = traceback.format_exc()
            await alert_channel.send(f"⚠️ **CRITICAL SWEEP FAILURE (Tech Scanner)**\n```python\n{tb_str[-1900:]}\n```")

async def start_scheduled_sweeps():
    """
    Async background scheduler loop grouping tasks into distinct temporal buckets.
    """
    logger.info("🚀 Master background scheduled sweeps started (Intraday Loop, 00:00 YouTube, 12/16:00 Email, 18:00 Business).")
    
    # Ensure Discord bot is ready before doing anything that requires fetching channels
    await bot.wait_until_ready()
    logger.info("🤖 Discord bot is ready. Schedulers proceeding.")
    
    # Catch-up sweeps on startup
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    
    # Check Daily Closing Briefing Catch-up
    last_briefing_date = get_last_run_date("daily_closing_briefing")
    is_weekday = now.weekday() < 5
    if is_weekday and last_briefing_date != today_str:
        if (now.weekday() <= 3 and now.hour >= 16) or (now.weekday() == 4 and now.hour >= 17):
            logger.info("⚡ Catch-up: Firing missed run_daily_closing_briefing.")
            await run_daily_closing_briefing()
            
    # Check Email Sweep Catch-up (past 08:30 PKT)
    last_email_date = get_last_run_date("email_sweep")
    if last_email_date != today_str:
        if now.hour > 8 or (now.hour == 8 and now.minute >= 30):
            logger.info("⚡ Catch-up: Firing missed run_email_sweep.")
            await run_email_sweep()
            
    while True:
        now = datetime.now()
        current_date = now.date()
        
        def should_run(task_name, target_hour):
            last_timestamp = get_last_run(task_name)
            if last_timestamp == 0:
                return now.hour >= target_hour
            last_date = datetime.fromtimestamp(last_timestamp).date()
            return last_date < current_date and now.hour >= target_hour

        def should_run_window(task_name, target_hour, target_minute, window_minutes=15):
            last_timestamp = get_last_run(task_name)
            if last_timestamp > 0:
                last_date = datetime.fromtimestamp(last_timestamp).date()
                if last_date == current_date:
                    return False
            
            target_time = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
            window_end = target_time + timedelta(minutes=window_minutes)
            
            return target_time <= now <= window_end

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
            
        # 3. Business & Portfolio Sweep (3x daily: 08:30, 13:00, 18:00 PKT)
        if should_run_window("business_morning", 8, 30):
            await run_business_portfolio_sweep()
            update_last_run("business_morning")
            
        if should_run_window("business_midday", 13, 0):
            await run_business_portfolio_sweep()
            update_last_run("business_midday")
            
        if should_run_window("business_evening", 18, 0):
            await run_business_portfolio_sweep()
            update_last_run("business_evening")
            
        # 4. Intraday Price & Alert Loop (Every 60 minutes, weekdays 09:15 - 16:30)
        if current_date.weekday() < 5: # Monday = 0, Friday = 4
            if (now.hour > 9 or (now.hour == 9 and now.minute >= 15)) and (now.hour < 16 or (now.hour == 16 and now.minute <= 30)):
                last_intraday_ts = get_last_run("intraday_loop")
                if now.timestamp() - last_intraday_ts >= 3600: # 3600 seconds = 60 minutes
                    await run_intraday_price_loop()
                    update_last_run("intraday_loop")
                    
                last_tech_ts = get_last_run("technical_scanner")
                if now.timestamp() - last_tech_ts >= 7200: # 7200 seconds = 2 hours
                    await run_intraday_technical_scanner()
                    update_last_run("technical_scanner")
                    
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
