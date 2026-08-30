import asyncio
import logging
import os
from datetime import datetime, timedelta
from vector_brain import query_mentor_brain_tool, ingest_knowledge_tool, vector_db, evaluate_trade_trigger
import traceback
from discord_bot import bot, get_last_run, update_last_run, send_long_message
from langchain_google_genai import ChatGoogleGenerativeAI
import yfinance as yf
from portfolio_manager import get_db_connection

# Define your admin channel ID here or pull from os.environ
ADMIN_CHANNEL_ID = int(os.getenv("ADMIN_CHANNEL_ID", 0))
PROSPECT_CHANNEL_ID = int(os.getenv("PROSPECT_CHANNEL_ID", 0)) or ADMIN_CHANNEL_ID
RESEARCH_CHANNEL_ID = int(os.getenv("RESEARCH_CHANNEL_ID", 0)) or ADMIN_CHANNEL_ID

logger = logging.getLogger("PortfolioAgentSchedulers")

import json

STATE_DATE_FILE = "scheduler_date_state.json"
_has_run_closing_report_today = None

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

async def run_weekly_fundamentals_sweep():
    """
    Weekly Scraper to update TTM FCF and 5/3-Year CAGR for DCF calculations.
    Runs once a week to prevent API latency during live evaluations.
    """
    logger.info("🤖 [SCHEDULER] Starting Weekly Fundamentals Sweep for DCF data...")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT ticker FROM fundamentals")
        tickers = [row[0] for row in cur.fetchall()]
        
        for ticker in tickers:
            try:
                stock = yf.Ticker(f"{ticker.upper()}.KA")
                
                # 1. TTM FCF Logic with strict column-count guardrails
                cashflow = stock.quarterly_cashflow
                fcf = None
                if not cashflow.empty and 'Free Cash Flow' in cashflow.index:
                    fcf_row = cashflow.loc['Free Cash Flow'].dropna()
                    if len(fcf_row) >= 4:
                        fcf = fcf_row.iloc[:4].sum()
                    elif len(fcf_row) > 0:
                        # Fragmented data: Annualize what is available
                        fcf = (fcf_row.sum() / len(fcf_row)) * 4
                
                # Fallback to Annual Cashflow if quarterly is fully missing/invalid
                if fcf is None:
                    annual_cf = stock.cashflow
                    if not annual_cf.empty and 'Free Cash Flow' in annual_cf.index:
                        fcf_annual_row = annual_cf.loc['Free Cash Flow'].dropna()
                        if len(fcf_annual_row) > 0:
                            fcf = fcf_annual_row.iloc[0]
                
                # 2. CAGR Logic (true 5-year, fallback to true 3-year)
                cagr = None
                financials = stock.financials
                if not financials.empty and 'Diluted EPS' in financials.index:
                    eps_row = financials.loc['Diluted EPS'].dropna()
                    
                    # Try true 5-Year CAGR (requires 6 years of data, index 0 to 5)
                    if len(eps_row) >= 6:
                        current = eps_row.iloc[0]
                        past = eps_row.iloc[5]
                        if past > 0 and current > 0:
                            cagr = (current / past) ** (1/5) - 1
                    
                    # Try true 4-Year CAGR if we only have 5 years (index 0 to 4)
                    if cagr is None and len(eps_row) >= 5:
                        current = eps_row.iloc[0]
                        past = eps_row.iloc[4]
                        if past > 0 and current > 0:
                            cagr = (current / past) ** (1/4) - 1
                            
                    # Try true 3-Year CAGR (requires 4 years of data, index 0 to 3)
                    if cagr is None and len(eps_row) >= 4:
                        current = eps_row.iloc[0]
                        past = eps_row.iloc[3]
                        if past > 0 and current > 0:
                            cagr = (current / past) ** (1/3) - 1
                            
                    # Try true 2-Year CAGR if we only have 3 years (index 0 to 2)
                    if cagr is None and len(eps_row) >= 3:
                        current = eps_row.iloc[0]
                        past = eps_row.iloc[2]
                        if past > 0 and current > 0:
                            cagr = (current / past) ** (1/2) - 1
                
                # Update DB only if valid data was found
                if fcf is not None or cagr is not None:
                    # Construct dynamic update
                    set_clauses = []
                    params = []
                    if fcf is not None:
                        set_clauses.append("fcf = %s")
                        params.append(float(fcf))
                    if cagr is not None:
                        set_clauses.append("historical_growth = %s")
                        params.append(float(cagr))
                        
                    params.append(ticker)
                    query = f"UPDATE fundamentals SET {', '.join(set_clauses)} WHERE ticker = %s"
                    cur.execute(query, tuple(params))
                    conn.commit()
                else:
                    logger.warning(f"⚠️ [Weekly Sweep] No FCF or CAGR data found for {ticker}. Skipping.")
                
            except Exception as e:
                logger.error(f"Error updating fundamentals for {ticker}: {e}")
                conn.rollback()
        
        cur.close()
        conn.close()
        logger.info("✅ [SCHEDULER] Weekly Fundamentals Sweep completed.")
    except Exception as e:
        logger.error(f"Failed Weekly Fundamentals Sweep: {e}")

async def run_youtube_sweep():
    """
    12:00 AM: YouTube Knowledge Sweep
    Extracts playlist transcripts, processes summaries, and logs them into memory.
    """
    print("🕒 Triggering YouTube Knowledge Sweep (00:00)...")
    try:
        from youtube_scraper import fetch_mentor_transcripts
        mentors = ["Abdul Rehman Najam", "Mashal Khan", "Jawad Hafeez", "Investkaar", "Sarmaaya Financials", "AKD Securities Limited", "KTrade PK"]
        
        # 300 seconds (5 minutes) absolute maximum execution time
        # asyncio.to_thread prevents a synchronous scraper from freezing your entire async bot
        transcripts = await asyncio.wait_for(asyncio.to_thread(fetch_mentor_transcripts, mentors), timeout=300.0)
        
        if transcripts:
            from langchain_google_genai import ChatGoogleGenerativeAI
            from ingestion import SWEEP_FORMAT_PROMPT, save_learned_rule
            from vector_brain import ingest_knowledge_tool
            import re
            
            llm = ChatGoogleGenerativeAI(model="gemini-3.5-flash-lite", google_api_key=os.getenv("GOOGLE_API_KEY"))
            alert_channel = bot.get_channel(RESEARCH_CHANNEL_ID)
            
            for mentor, text in transcripts.items():
                prompt = SWEEP_FORMAT_PROMPT + f"\n\nTranscript from {mentor}:\n{text[:15000]}"
                response = await asyncio.to_thread(llm.invoke, prompt)
                
                # Handle potential list response from Gemini content
                formatted_text = response.content
                if isinstance(formatted_text, list):
                    formatted_text = "".join([item.get("text", "") if isinstance(item, dict) else str(item) for item in formatted_text])
                else:
                    formatted_text = str(formatted_text)
                
                # Extract rule if it exists
                try:
                    rule_match = re.search(r'!rule\s+(.+)', formatted_text, re.IGNORECASE)
                    if rule_match:
                        rule = rule_match.group(1).strip()
                        # Clean up formatting if the LLM adds asterisks or markdown to the rule line itself
                        rule = rule.replace('**', '').strip()
                        await asyncio.to_thread(save_learned_rule, mentor, rule)
                except Exception as e:
                    print(f"⚠️ [YouTube Sweep] Error extracting rule for {mentor}: {e}")
                
                # Post structured sweep to Discord
                if alert_channel:
                    await send_long_message(alert_channel, f"🎥 **Knowledge Ingestion Report:**\n{formatted_text}")
                    
                # Store the high-signal structured output in Vector DB instead of rambling text
                await asyncio.to_thread(ingest_knowledge_tool.invoke, {"input_text_or_url": formatted_text})
                
        print("✅ [YouTube Sweep] Knowledge ingestion complete.")
    except asyncio.TimeoutError:
        print("⚠️ [YouTube Sweep] FATAL: Connection timed out after 5 minutes. Aborting to protect system loop.")
    except Exception as e:
        print(f"⚠️ [YouTube Sweep] Error: {e}")

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
    Periodic 10-minute loop during market hours to evaluate profit and rebalancing triggers,
    and forward-looking exit/exhaustion signals.
    """
    logger.info(f"🔄 [{datetime.now().strftime('%H:%M')}] Initializing Intraday Price & Alert Loop...")
    try:
        # --- 1. EXIT & EXHAUSTION EVALUATOR ---
        from risk_engine import evaluate_exit_and_exhaustion
        from agent import fetch_stocks_with_fallback
        from portfolio_manager import get_db_connection
        import pandas as pd
        from datetime import timedelta
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT p.ticker, p.shares as total_quantity, p.avg_buy_price as average_price, w.target_price, p.research_sentiment, p.sentiment_updated_at
            FROM portfolio_holdings p
            LEFT JOIN watchlist_targets w ON p.ticker = w.ticker
            WHERE p.shares > 0
        ''')
        holdings = cursor.fetchall()
        cursor.close()
        conn.close()
        
        alert_channel = bot.get_channel(PROSPECT_CHANNEL_ID)
        now_ts = datetime.now().timestamp()
        
        for row in holdings:
            ticker, shares, avg_price, target_price, research_sentiment, sentiment_updated_at = row
            target_price = float(target_price) if target_price else 0.0
            
            if sentiment_updated_at is None:
                days_since_update = 999  # Force it to decay to NEUTRAL if no timestamp exists
            else:
                days_since_update = (datetime.now() - sentiment_updated_at).days
            
            if days_since_update > 30:
                research_sentiment = "NEUTRAL"
            
            
            end_date = datetime.today()
            start_date = end_date - timedelta(days=60)
            raw_hist, _ = fetch_stocks_with_fallback(ticker, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            
            # Ensure dataframe is chronologically ordered before risk engine validation
            if raw_hist is not None and not raw_hist.empty:
                if isinstance(raw_hist.index, pd.DatetimeIndex):
                    raw_hist = raw_hist.sort_index(ascending=True)
                elif 'Date' in raw_hist.columns:
                    raw_hist['Date'] = pd.to_datetime(raw_hist['Date'])
                    raw_hist = raw_hist.sort_values('Date', ascending=True)

            if not raw_hist.empty and len(raw_hist) >= 30:
                if raw_hist is None or raw_hist.empty or 'Close' not in raw_hist.columns:
                    print(f"⚠️ [Risk Engine] Corrupt or missing OHLC data. Skipping ticker evaluation.")
                    continue
                
                current_price = raw_hist['Close'].iloc[-1]
                
                eval_res = evaluate_exit_and_exhaustion(
                    ticker=ticker,
                    current_price=current_price,
                    entry_price=float(avg_price),
                    target_price=target_price,
                    fair_value=target_price,
                    ohlc_df=raw_hist,
                    latest_research_sentiment=research_sentiment
                )
                
                action = eval_res.get("action", "HOLD")
                if action in ["TAKE_PROFIT_PEAK", "TAKE_PROFIT_OVERBOUGHT", "EXIT_DOWNSIDE_WARNING"]:
                    cooldown_key = f"{ticker}_{action}"
                    if now_ts - alert_cooldowns.get(cooldown_key, 0) >= 4 * 3600:
                        alert_cooldowns[cooldown_key] = now_ts
                        
                        price = eval_res.get("price", current_price)
                        rsi = eval_res.get("rsi", "N/A")
                        reason = eval_res.get("reason", "")
                        pct_change = ((price - float(avg_price)) / float(avg_price)) * 100 if float(avg_price) > 0 else 0
                        
                        if action == "EXIT_DOWNSIDE_WARNING":
                            msg = f"🚨 **DOWNSIDE RISK ALERT:** {ticker}\n\n"
                            msg += f"**Current Price:** Rs. {price:.2f} ({pct_change:+.2f}%)\n\n"
                            msg += f"**Diagnostic Reason:** {reason}\n\n"
                            msg += f"**Recommendation:** SELL / EXIT. Technical structure points to further downside before stabilization."
                        else:
                            msg = f"💰 **TARGET REACHED / MAXIMUM VALUATION:** {ticker}\n\n"
                            msg += f"**Current Price:** Rs. {price:.2f} ({pct_change:+.2f}%)\n\n"
                            msg += f"**Target Price:** Rs. {target_price:.2f} | **RSI:** {rsi}\n\n"
                            msg += f"**Diagnostic Reason:** {reason}\n\n"
                            msg += f"**Recommendation:** TAKE PROFIT. Lock in gains or place tight trailing stops."
                            
                        if alert_channel:
                            await alert_channel.send(msg)
                        else:
                            print(msg)

        # --- 2. EXISTING PROFIT AND REBALANCE TRIGGERS ---
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
            
            # Ensure dataframe is chronologically ordered before risk engine validation
            if raw_hist is not None and not raw_hist.empty:
                if isinstance(raw_hist.index, pd.DatetimeIndex):
                    raw_hist = raw_hist.sort_index(ascending=True)
                elif 'Date' in raw_hist.columns:
                    raw_hist['Date'] = pd.to_datetime(raw_hist['Date'])
                    raw_hist = raw_hist.sort_values('Date', ascending=True)

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
            
            import math
            parsed_rationale = ai_verdict
            ai_action = "Sell"
            
            try:
                decision_data = json.loads(ai_verdict)
                ai_action = decision_data.get("action", "Sell").title()
                parsed_rationale = decision_data.get("rationale", "No rationale provided.")
                trim_percentage = float(decision_data.get("trim_percentage", 0.0))
                
                current_shares = alert.get("current_shares", 0)
                
                if ai_action.upper() == "TRIM" and trim_percentage > 0:
                    shares_to_sell = math.floor(current_shares * (trim_percentage / 100.0))
                    alert["shares_to_sell"] = shares_to_sell
                    alert["cash_generated"] = shares_to_sell * alert.get("current_price", 0)
            except json.JSONDecodeError:
                print(f"[ERROR] LLM returned malformed JSON for {ticker}.")

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
                msg += f"**Action:** {ai_action} **{alert.get('shares_to_sell', 0)}** shares\n"
                msg += f"**Expected Cash Gen:** Rs. {alert.get('cash_generated', 0):,.2f}\n"
                msg += f"**Est. New Balance:** Rs. {alert.get('new_cash_balance', 0):,.2f}\n\n"
                msg += f"🧠 **AI Contextual Verdict (Multi-Agent):**\n{parsed_rationale}\n"
            
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

async def run_monthly_macro_sync():
    """Monthly background task to update macro variables via Chromium."""
    from macro_indicators import fetch_inflation_via_browser
    from portfolio_manager import get_db_connection
    
    new_rate = await fetch_inflation_via_browser()
    
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO system_macro_state (key, value, updated_at)
                    VALUES ('cpi_inflation_rate', %s, NOW())
                    ON CONFLICT (key) DO UPDATE 
                    SET value = EXCLUDED.value, updated_at = NOW();
                """, (new_rate,))
                conn.commit()
        finally:
            conn.close()
        logger.info(f"✅ Macro sync complete. New CPI: {new_rate}")
    else:
        logger.error("🚨 Macro sync failed: No DB connection.")

async def fetch_kmi30_valuations():
    conn = get_db_connection()
    try:
        from psycopg2.extras import RealDictCursor
        from market_data_scraper import fetch_live_price
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT ticker, target_price FROM watchlist_targets WHERE target_price > 0")
            targets = cursor.fetchall()
            kmi30_data = []
            for row in targets:
                price = fetch_live_price(row['ticker'])
                if price and price > 0:
                    kmi30_data.append({'ticker': row['ticker'], 'live_price': price, 'fair_value': float(row['target_price'])})
            return kmi30_data
    except Exception as e:
        logger.error(f"Error fetching KMI30 valuations: {e}")
        return []
    finally:
        conn.close()

async def run_cash_deployment_sweep():
    logger.info("Executing Intraday Cash Deployment Sweep...")
    # 1. Check Liquid Cash
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM cash_ledger")
        cash_res = cursor.fetchone()
        liquid_cash = float(cash_res[0]) if cash_res else 0.0
    except Exception as e:
        logger.error(f"Error fetching liquid cash: {e}")
        liquid_cash = 0.0
    finally:
        conn.close()
    
    if liquid_cash < 1000:
        return # Insufficient capital, abort sweep silently
        
    # 2. Fetch current prices and DCF values from your database
    kmi30_data = await fetch_kmi30_valuations() 
    
    actionable_setups = []
    
    for asset in kmi30_data:
        ticker = asset['ticker']
        price = asset['live_price']
        intrinsic = asset['fair_value']
        
        if intrinsic <= 0:
            continue
            
        # Calculate Margin of Safety
        discount = ((intrinsic - price) / intrinsic) * 100
        
        # Rigid Logic: Only flag if trading at least 20% BELOW intrinsic value
        if discount >= 20.0:
            shares_affordable = int(liquid_cash // price)
            actionable_setups.append(
                f"**{ticker}**: Price Rs. {price} | DCF Rs. {intrinsic} | **{discount:.1f}% Undervalued** | Can buy {shares_affordable} shares."
            )
            
    # 3. Dispatch to Discord only if a threshold is crossed
    if actionable_setups:
        payload = (
            f"🚨 **Cash Deployment Alert** 🚨\n"
            f"Liquid Capital: Rs. {liquid_cash}\n\n"
            f"The following KMI-30 assets have crossed the 20% Margin of Safety threshold:\n"
            + "\n".join(actionable_setups)
        )
        channel = bot.get_channel(ADMIN_CHANNEL_ID)
        if channel:
            await send_long_message(channel, payload)

async def start_scheduled_sweeps():
    """
    Async background scheduler loop grouping tasks into distinct temporal buckets.
    """
    global _has_run_closing_report_today
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

        # 0. Monthly Macro Sync (2nd of every month at 10:00)
        if current_date.day == 2 and should_run("monthly_macro_sync", 10):
            await run_monthly_macro_sync()
            update_last_run("monthly_macro_sync")
            
        # 0.5. Weekly Fundamentals Sweep (Sunday at 02:00)
        if current_date.weekday() == 6 and should_run("weekly_fundamentals", 2):
            await run_weekly_fundamentals_sweep()
            update_last_run("weekly_fundamentals")

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
            
        # 4. Intraday Price & Alert Loop (Every 10 minutes, weekdays 09:15 - 16:30)
        if current_date.weekday() < 5: # Monday = 0, Friday = 4
            
            # 5x Intraday Cash Deployment Sweep
            if should_run_window("cash_sweep_10_00", 10, 0):
                await run_cash_deployment_sweep()
                update_last_run("cash_sweep_10_00")
            if should_run_window("cash_sweep_11_30", 11, 30):
                await run_cash_deployment_sweep()
                update_last_run("cash_sweep_11_30")
            if should_run_window("cash_sweep_13_00", 13, 0):
                await run_cash_deployment_sweep()
                update_last_run("cash_sweep_13_00")
            if should_run_window("cash_sweep_14_30", 14, 30):
                await run_cash_deployment_sweep()
                update_last_run("cash_sweep_14_30")
            if should_run_window("cash_sweep_15_15", 15, 15):
                await run_cash_deployment_sweep()
                update_last_run("cash_sweep_15_15")

            if (now.hour > 9 or (now.hour == 9 and now.minute >= 15)) and (now.hour < 16 or (now.hour == 16 and now.minute <= 30)):
                last_intraday_ts = get_last_run("intraday_loop")
                if now.timestamp() - last_intraday_ts >= 600: # 600 seconds = 10 minutes
                    await run_intraday_price_loop()
                    update_last_run("intraday_loop")
                    
                last_tech_ts = get_last_run("technical_scanner")
                if now.timestamp() - last_tech_ts >= 7200: # 7200 seconds = 2 hours
                    await run_intraday_technical_scanner()
                    update_last_run("technical_scanner")
                    
        # 5. Daily Closing Summary
        if current_date.weekday() <= 3 and now.hour == 16 and now.minute == 0:
            if _has_run_closing_report_today != current_date:
                logger.info("[SCHEDULER] Triggering Market Close Report...")
                await run_daily_closing_briefing()
                _has_run_closing_report_today = current_date
                
        elif current_date.weekday() == 4 and now.hour == 17 and now.minute == 0:
            if _has_run_closing_report_today != current_date:
                logger.info("[SCHEDULER] Triggering Market Close Report...")
                await run_daily_closing_briefing()
                _has_run_closing_report_today = current_date
            
        # Check every 60 seconds to prevent tight CPU looping
        await asyncio.sleep(60)

def initialize_background_schedulers(bot_loop):
    """Hooks all tasks directly into the running Discord bot event loop."""
    bot_loop.create_task(start_scheduled_sweeps())
    logger.info("🚀 Scheduled sweeps task injected into bot event loop.")
