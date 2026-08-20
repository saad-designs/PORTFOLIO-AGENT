from dotenv import load_dotenv
load_dotenv()

import discord
from discord.ext import commands, tasks
from datetime import datetime, time, timezone, timedelta
from zoneinfo import ZoneInfo
import os
import uuid
import re
import requests
import aiohttp
import traceback
import base64
import imaplib
import email
from email.header import decode_header
from bs4 import BeautifulSoup
from langchain_core.messages import HumanMessage
from langchain_core.tools import tool
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_classic.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate
from agent import get_agent
from ingestion import sweep_rss_feeds, sweep_youtube_transcripts
import asyncio
from youtube_scraper import fetch_mentor_transcripts, get_channel_id, get_latest_video_id, get_transcript
from vector_brain import ingest_knowledge_tool, query_mentor_brain_tool
from googleapiclient.discovery import build
import contextvars
from langchain_core.prompts import MessagesPlaceholder
from langchain_core.chat_history import BaseChatMessageHistory
from langchain_community.chat_message_histories import ChatMessageHistory
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_core.messages import ToolMessage, AIMessage

# Create a ContextVar to store the current author ID
current_author_id = contextvars.ContextVar('current_author_id', default=None)
from google.api_core.exceptions import ResourceExhausted

from portfolio_manager import log_trade, get_portfolio_summary, set_target, get_live_price, get_watchlist_summary, scan_shariah_market, sync_portfolio_tool

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Ensure DISCORD_CHANNEL_ID is loaded from the environment
raw_channel_id = os.getenv("ADMIN_CHANNEL_ID") or os.getenv("DISCORD_CHANNEL_ID", "0")
CHANNEL_ID = int(raw_channel_id) if raw_channel_id else 0

async def log_error_to_discord(error_msg: str):
    log_channel_id = os.getenv("LOG_CHANNEL_ID")
    if not log_channel_id:
        return
    try:
        channel = bot.get_channel(int(log_channel_id))
        if channel:
            await channel.send(f"⚠️ **Background Task Error:**\n```\n{error_msg[:1900]}\n```")
    except Exception as e:
        print(f"Failed to send error log to Discord: {e}")

import json

STATE_FILE = "scheduler_state.json"
TASK_INTERVAL = 86400 

def update_last_run(task_name):
    """Saves the current timestamp when a task completes successfully."""
    data = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                pass
            
    data[task_name] = datetime.now().timestamp()
    
    with open(STATE_FILE, "w") as f:
        json.dump(data, f)

def get_last_run(task_name):
    """Retrieves the last execution timestamp of a task."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            try:
                data = json.load(f)
                return data.get(task_name, 0)
            except json.JSONDecodeError:
                pass
    return 0

DYNAMIC_PSX_TICKER_MAPPING = "{}"

async def update_psx_tickers():
    global DYNAMIC_PSX_TICKER_MAPPING
    print("🔄 Fetching live PSX tickers from market endpoint...")
    
    url = "https://dps.psx.com.pk/symbols"
    
    try:
        async with aiohttp.ClientSession() as session:
            # We add generic headers to prevent basic blocking
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9"
            }
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    try:
                        data = await response.json(content_type=None) # Disregard content type mismatch
                        mapping = {}
                        count = 0
                        if isinstance(data, list):
                            for item in data:
                                name = item.get("name", item.get("companyName", item.get("company_name", "")))
                                symbol = item.get("symbol", item.get("ticker", ""))
                                if name and symbol:
                                    mapping[name] = symbol
                            DYNAMIC_PSX_TICKER_MAPPING = json.dumps(mapping)
                            count = len(mapping)
                        elif isinstance(data, dict):
                            DYNAMIC_PSX_TICKER_MAPPING = json.dumps(data)
                            count = len(data)
                        else:
                            DYNAMIC_PSX_TICKER_MAPPING = str(data)
                            count = 1
                        print(f"✅ Loaded {count} active PSX tickers into memory.")
                    except Exception as e:
                        print(f"⚠️ Failed to parse ticker JSON: {e}")
                else:
                    print(f"⚠️ Failed to fetch tickers, HTTP {response.status}")
    except Exception as e:
        print(f"🚨 Ticker Fetch Error: {repr(e)}")
        traceback.print_exc()
# Set run times in UTC to align with 09:45 AM and 03:45 PM PKT


midnight_time = time(hour=0, minute=0, tzinfo=timezone.utc)

@tasks.loop(time=midnight_time)
async def midnight_mentor_sweep():
    try:
        # The strict list of Pakistani financial mentors you provided
        mentors = [
            "Abdul Rehman Najam",
            "Mashal Khan", 
            "Jawad Hafeez",
            "Investkaar",
            "Sarmaaya Financials"
        ]
        
        print("Starting Unfiltered Midnight Mentor Sweep...")
        transcripts_data = fetch_mentor_transcripts(mentors)
        
        if not transcripts_data:
            print("No new transcripts could be extracted tonight.")
            return

        # Loop through successful extractions and send them to your LLM agent
        for mentor, text in transcripts_data.items():
            prompt = f"""
            Analyze the following recent YouTube transcript from {mentor}.
            Extract the top 3 actionable investment or stock market insights. 
            Discard any promotional material, sponsor reads, or fluff.
            
            Also, extract any specific Pakistani stock/ticker mentions along with suggested buy targets and/or stop losses. 
            Format the targets strictly as Discord commands at the very end of your response, one per line:
            !target TICKER TARGET_PRICE STOP_LOSS
            (If no stop loss is mentioned, use 0)
            
            IMPORTANT: Do not guess the ticker. Use the following mapping dictionary to resolve company names to their exact PSX ticker symbols. If the exact ticker is not in this list, do your best to infer the correct 3-5 letter PSX symbol, but NEVER use the full company name in the !target command.
            {DYNAMIC_PSX_TICKER_MAPPING}
            
            Transcript: 
            {text}
            """            
            inputs = {"messages": [HumanMessage(content=prompt)]}
            config = {"configurable": {"thread_id": f"midnight_sweep_{mentor}"}}
            
            try:
                agent_app = await get_agent()
                response = await asyncio.to_thread(agent_app.invoke, inputs, config)
                raw_content = response["messages"][-1].content
                
                if isinstance(raw_content, list):
                    final_reply = "".join(
                        block.get("text", "") for block in raw_content 
                        if isinstance(block, dict) and block.get("type") == "text"
                    )
                else:
                    final_reply = str(raw_content)
                
                channel = bot.get_channel(CHANNEL_ID)
                if channel:
                    # Discord 2000 char limit slicing for safety
                    chunk_size = 1900
                    for i in range(0, len(final_reply), chunk_size):
                        await channel.send(f"🌙 **Midnight Insight: {mentor}**\n{final_reply[i:i+chunk_size]}")
                        
                print(f"Successfully generated summary for {mentor}")
            except Exception as e:
                print(f"Failed to generate summary for {mentor}: {e}")

        update_last_run("midnight_mentor_sweep")

    except Exception as e:
        print(f"Midnight Sweep Error: {e}")
        await log_error_to_discord(f"midnight_mentor_sweep:\n{str(e)}")

class ApprovalView(discord.ui.View):
    def __init__(self, thread_id: str = None):
        super().__init__(timeout=None)
        self.thread_id = thread_id

    @discord.ui.button(label="Approve Trade", style=discord.ButtonStyle.green, custom_id="approve_trade")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        t_id = self.thread_id or (interaction.message.embeds[0].footer.text if interaction.message.embeds else "unknown")
        # Retrieve the base URL from the environment
        base_url = os.getenv("RENDER_EXTERNAL_URL", "https://portfolio-agent-cn9g.onrender.com").strip().rstrip("/")
        
        # Force-sanitize the string to remove markdown artifacts
        if "](" in base_url:
            base_url = base_url.split("](")[-1].rstrip(")")
        
        # Construct the final webhook endpoint
        webhook_url = f"{base_url}/webhook/discord"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json={"thread_id": t_id, "action": "Approved"}) as resp:
                    pass
            await interaction.followup.send("Trade Approved! Resuming agent...", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"Error resuming agent: {e}", ephemeral=True)
        
    @discord.ui.button(label="Deny & Recalculate", style=discord.ButtonStyle.red, custom_id="deny_trade")
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        t_id = self.thread_id or (interaction.message.embeds[0].footer.text if interaction.message.embeds else "unknown")
        # Retrieve the base URL from the environment
        base_url = os.getenv("RENDER_EXTERNAL_URL", "https://portfolio-agent-cn9g.onrender.com").strip().rstrip("/")
        
        # Force-sanitize the string to remove markdown artifacts
        if "](" in base_url:
            base_url = base_url.split("](")[-1].rstrip(")")
        
        # Construct the final webhook endpoint
        webhook_url = f"{base_url}/webhook/discord"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json={"thread_id": t_id, "action": "Denied"}) as resp:
                    pass
            await interaction.followup.send("Trade Denied! Recalculating...", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"Error resuming agent: {e}", ephemeral=True)

async def send_alert(thread_id: str, message: str):
    raw_channel_id = os.getenv("ADMIN_CHANNEL_ID") or os.getenv("DISCORD_CHANNEL_ID")
    if not raw_channel_id:
        print("⚠️ Warning: Neither ADMIN_CHANNEL_ID nor DISCORD_CHANNEL_ID is set.")
        return
    channel_id = int(raw_channel_id)
    
    try:
        channel = await bot.fetch_channel(channel_id)
    except Exception as e:
        print(f"Error fetching channel: {e}")
        return
            
    if channel:
        embed = discord.Embed(title="Portfolio Action Required", description=message, color=discord.Color.blue())
        embed.set_footer(text=thread_id)
        await channel.send(embed=embed, view=ApprovalView(thread_id=thread_id))

# on_ready event moved to the unified handler below

async def force_ingest(ctx):
    await ctx.send("⚙️ Initializing manual data sweep. Fetching RSS feeds and generating vectors. This will take a moment...")
    try:
        # Run the synchronous ingestion sweep in an async thread to prevent blocking
        await asyncio.to_thread(sweep_rss_feeds)
        await ctx.send("✅ RSS Ingestion complete. Vectors stored in Supabase.")
    except Exception as e:
        await ctx.send(f"⚠️ Ingestion pipeline failed: {str(e)}")

@bot.command(name="briefing")
async def force_briefing(ctx):
    import schedulers
    # Assuming schedulers.py imports the bot instance and sends to ADMIN_CHANNEL_ID internally
    await schedulers.run_daily_closing_briefing(bot_instance=bot)
    await ctx.send("Manual briefing triggered.", delete_after=3)

@bot.command(name="force_run")
@commands.is_owner()
async def force_run_tasks(ctx):
    await ctx.send("⚙️ Forcing execution of all scheduled background tasks...")
    try:
        await midnight_mentor_sweep()
        await dividend_watchdog()
        await run_business_portfolio_sweep()
        await run_email_sweep()
        await ctx.send("✅ All tasks executed successfully. Check terminal logs for details.")
    except Exception as e:
        await ctx.send(f"❌ Execution failed: {str(e)}")


class TradeConfirmationView(discord.ui.View):
    def __init__(self, config, pending_tool_calls, author_id: int):
        super().__init__(timeout=60.0)
        self.config = config
        self.pending_tool_calls = pending_tool_calls
        self.author_id = author_id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.green, custom_id="hitl_confirm")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        if interaction.user.id != self.author_id:
            await interaction.followup.send("You are not authorized to confirm this trade.", ephemeral=True)
            return
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        self.stop()
        
        # Resume the graph WITHOUT a ToolMessage injection
        # This forces the graph to enter the ToolNode and execute the real Supabase backend logic
        agent_app = await get_agent()
        await asyncio.to_thread(agent_app.invoke, None, self.config)
        
        snapshot = await asyncio.to_thread(agent_app.get_state, self.config)
        last_msg = snapshot.values["messages"][-1]
        if isinstance(last_msg, AIMessage):
            final_text = str(last_msg.content)
            await interaction.followup.send(f"✅ Trade Executed.\n\n{final_text[:1800]}")
        else:
            await interaction.followup.send("✅ Trade Executed.")

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red, custom_id="hitl_cancel")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        if interaction.user.id != self.author_id:
            await interaction.followup.send("You are not authorized to cancel this trade.", ephemeral=True)
            return
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        self.stop()
        
        # Inject cancellation ToolMessage and act as_node="tools" so the DB write is safely bypassed
        tool_call_id = self.pending_tool_calls[0]['id']
        tool_message = ToolMessage(content="Trade Cancelled by User. Do not proceed.", tool_call_id=tool_call_id)
        
        agent_app = await get_agent()
        await asyncio.to_thread(agent_app.update_state, self.config, {"messages": [tool_message]}, as_node="tools")
        await asyncio.to_thread(agent_app.invoke, None, self.config)
        
        snapshot = await asyncio.to_thread(agent_app.get_state, self.config)
        last_msg = snapshot.values["messages"][-1]
        if isinstance(last_msg, AIMessage):
            final_text = str(last_msg.content)
            await interaction.followup.send(f"❌ Cancelled.\n\n{final_text[:1800]}")
        else:
            await interaction.followup.send("❌ Cancelled.")

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if hasattr(self, 'message'):
            try:
                await self.message.edit(view=self)
            except Exception:
                pass

# --- LangChain Agent Setup ---
@tool
async def log_trade_tool(action: str, ticker: str, quantity: int, price: float, fees: float = 0.0) -> str:
    """
    Logs a completed trade into the local portfolio database.
    action: Must be 'buy' or 'sell'
    ticker: The stock symbol (e.g., 'SYS', 'LUCK')
    quantity: Number of shares traded
    price: Execution price per share
    fees: Broker commissions and taxes applied to the trade (default 0.0)
    """
    try:
        channel = bot.get_channel(CHANNEL_ID)
        author_id = current_author_id.get()
        
        view = TradeConfirmationView(author_id=author_id)
        
        # 1. Output verification message
        confirm_msg = await channel.send(f"⚠️ **Confirm Database Write:** {action.upper()} {quantity} {ticker} @ Rs. {price} (Fees: Rs. {fees})?", view=view)
        view.message = confirm_msg
        
        # 2. Wait for verification button click
        await view.wait()
        
        if view.value is True:
            # 3. Execute the Supabase transaction
            result = await asyncio.to_thread(log_trade, ticker, action, quantity, price, fees)
            await confirm_msg.edit(content=f"✅ User confirmed {action.upper()} {quantity} {ticker}. Database updated.")
            return f"User confirmed. {result}"
        elif view.value is False:
            await confirm_msg.edit(content="❌ Transaction cancelled by user.")
            return "User denied the transaction. Database write aborted."
        else:
            await confirm_msg.edit(content="⏳ Transaction aborted: Verification timed out.")
            return "Transaction aborted: Verification timed out."
            
    except Exception as e:
        return f"Transaction failed during verification phase: {str(e)}"

@tool
def fetch_ktrade_emails() -> str:
    """Fetches unread corporate briefings and research reports from KTrade via the dedicated email inbox."""
    # Securely load credentials from your .env file
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASS = os.getenv("EMAIL_PASS")
    
    if not EMAIL_USER or not EMAIL_PASS:
        return "Tool Execution Error: Email credentials (EMAIL_USER or EMAIL_PASS) are not set in the .env file."
        
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(EMAIL_USER, EMAIL_PASS)
        mail.select("inbox")
        
        # Target specific sender and unread status to prevent data pollution
        # Note: Change "research@ktrade.pk" to the exact sender address KTrade uses
        status, messages = mail.search(None, '(UNSEEN FROM "research@ktrade.pk")')
        
        if status != "OK" or not messages[0]:
            mail.logout()
            return "No new unread emails found from KTrade."
            
        extracted_reports = []
        for num in messages[0].split():
            res, msg_data = mail.fetch(num, "(RFC822)")
            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    msg = email.message_from_bytes(response_part[1])
                    
                    # Safely decode the subject line
                    subject, encoding = decode_header(msg["Subject"])[0]
                    if isinstance(subject, bytes):
                        subject = subject.decode(encoding if encoding else "utf-8")
                        
                    body = clean_email_body(msg)
                    extracted_reports.append(f"Subject: {subject}\nBody: {body}\n---")
                    
        mail.logout()
        return "\n\n".join(extracted_reports)
        
    except Exception as e:
        # Returning string prevents the LangChain graph from crashing due to unhandled IMAP timeouts
        return f"Tool Execution Error: Failed to fetch emails. Details: {str(e)}"

@tool
def fetch_watchlist_tool() -> str:
    """Fetches the user's current stock watchlist and target prices, comparing them against live market prices. Use this when the user asks about their watchlist, targets, or potential new investments."""
    try:
        return get_watchlist_summary()
    except Exception as e:
        return f"Tool Execution Error: fetch_watchlist_tool failed. Details: {str(e)}"

@tool
def scan_shariah_market_tool() -> str:
    """Scans the open PSX market for Shariah-compliant equities, fetches live prices, and identifies mispriced buying opportunities based on fundamental valuation bands. Use this when the user asks to scan the market or find new Shariah stocks to buy."""
    try:
        return scan_shariah_market()
    except Exception as e:
        return f"Tool Execution Error: scan_shariah_market_tool failed. Details: {str(e)}"

@tool
def update_portfolio_tool(holdings_json: str) -> str:
    """Purges the old database and injects new portfolio holdings. Use only with valid JSON."""
    try:
        return sync_portfolio_tool(holdings_json)
    except Exception as e:
        return f"Tool Execution Error: update_portfolio_tool failed. Details: {str(e)}"

@tool
def fetch_portfolio_tool() -> str:
    """Fetches the user's active PSX stock portfolio and real-time PnL from the Supabase database."""
    try:
        return get_portfolio_summary()
    except Exception as e:
        return f"Tool Execution Error: fetch_portfolio_tool failed. Details: {str(e)}"



@tool
def visual_analyst_tool() -> str:
    """Use this tool when you need to formally analyze an uploaded image, chart, or document. The image is already provided in your context. Use this tool to structure your reasoning about the visual elements."""
    try:
        return "Visual data is available in your context. Proceed with analysis."
    except Exception as e:
        return f"Tool Execution Error: visual_analyst_tool failed. Details: {str(e)}"

# We are now utilizing the fully compiled, stateful LangGraph agent (agent_app) from agent.py.
# The isolated AgentExecutor and ephemeral memory dictionary have been removed.

@bot.event
async def on_message(message):
    # Prevent bot from replying to itself
    if message.author == bot.user:
        return

    current_author_id.set(message.author.id)

    # Trigger the agent to think, execute tools, and generate a response
    async with message.channel.typing():
        try:
            image_data = None
            if message.attachments:
                attachment = message.attachments[0]
                if any(attachment.filename.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.webp']):
                    image_bytes = await attachment.read()
                    image_b64 = base64.b64encode(image_bytes).decode('utf-8')
                    image_data = f"data:{attachment.content_type};base64,{image_b64}"

            if image_data:
                content = [
                    {"type": "text", "text": message.content or "Analyze this image against our portfolio and mentor rules."},
                    {"type": "image_url", "image_url": {"url": image_data}}
                ]
            else:
                content = message.content

            # We use ainvoke so it doesn't block the discord async loop
            # Route the channel ID directly into the existing PostgresSaver in agent.py
            session_id = str(message.channel.id)
            config = {"configurable": {"thread_id": session_id, "discord_user_id": str(message.author.id)}}
            inputs = {"messages": [HumanMessage(content=content)]}
            
            agent_app = await get_agent()
            response = await asyncio.to_thread(agent_app.invoke, inputs, config)
            
            # Loop to handle tool interruptions and auto-resume for safe tools
            while True:
                snapshot = await asyncio.to_thread(agent_app.get_state, config)
                
                if snapshot.next and "tools" in snapshot.next:
                    ai_message = snapshot.values["messages"][-1]
                    pending_tool_calls = ai_message.tool_calls
                    
                    needs_hitl = False
                    for tc in pending_tool_calls:
                        if tc["name"] in ["log_trade", "add_portfolio_position"]:
                            needs_hitl = True
                            break
                            
                    if needs_hitl:
                        for tc in pending_tool_calls:
                            if tc["name"] == "log_trade":
                                args = tc["args"]
                                action = args.get("action", "")
                                ticker = args.get("ticker", "")
                                qty = args.get("quantity", 0)
                                price = args.get("price", 0.0)
                                fees = args.get("fees", 0.0)
                                view = TradeConfirmationView(config, pending_tool_calls, message.author.id)
                                confirm_msg = await message.channel.send(f"⚠️ **Confirm Database Write:** {action.upper()} {qty} {ticker} @ Rs. {price} (Fees: Rs. {fees})?", view=view)
                                view.message = confirm_msg
                                return # Halt execution; UI button callbacks will resume
                        return
                    else:
                        # Auto-resume safe tools like web_search
                        response = await asyncio.to_thread(agent_app.invoke, None, config)
                else:
                    break
            
            # Extract final response
            snapshot = await asyncio.to_thread(agent_app.get_state, config)
            last_msg = snapshot.values["messages"][-1]
            if isinstance(last_msg, AIMessage):
                raw_output = last_msg.content
                if isinstance(raw_output, list):
                    final_text = "".join(
                        block.get("text", "") for block in raw_output 
                        if isinstance(block, dict) and block.get("type") == "text"
                    )
                else:
                    final_text = str(raw_output)
            else:
                final_text = ""
                
            # Send the response back seamlessly
            if final_text:
                chunk_size = 1900
                in_code_block = False
                
                for idx in range(0, len(final_text), chunk_size):
                    chunk = final_text[idx:idx + chunk_size]
                    
                    if in_code_block:
                        chunk = "```text\n" + chunk
                        
                    backtick_count = chunk.count("```")
                    if backtick_count % 2 != 0:
                        in_code_block = not in_code_block
                        if in_code_block:
                            chunk += "\n```"
                            
                    await message.channel.send(chunk)
        except Exception as e:
            import traceback
            traceback.print_exc()
            await message.channel.send(f"⚠️ **Agent Error:** `{str(e)}`")
import psycopg2
import psycopg2.extras
import pandas as pd
from agent import fetch_quote_with_fallback, clean_float

import pytz

@tasks.loop(minutes=15)
async def market_watchdog():
    try:
        # 1. Strict Market-Hours Time Gate
        tz_pkt = pytz.timezone('Asia/Karachi')
        now_pkt = datetime.now(tz_pkt)
        
        # Block weekends (5 = Saturday, 6 = Sunday)
        if now_pkt.weekday() >= 5:
            return 
            
        # Block outside 09:30 AM - 03:30 PM trading hours
        current_time = now_pkt.time()
        market_open = datetime.strptime("09:30", "%H:%M").time()
        market_close = datetime.strptime("15:30", "%H:%M").time()
        
        if not (market_open <= current_time <= market_close):
            return 

        # 1. Connect to DB and get all ACTIVE alerts
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT alert_id, discord_user_id, ticker, target_price, condition FROM price_alerts WHERE is_active = TRUE")
        alerts = cur.fetchall()

        if not alerts:
            cur.close()
            conn.close()
            return # Do nothing if no alerts are set

        # 2. Iterate and Check Prices
        for alert in alerts:
            # Re-use your fetch_quote_with_fallback function here to get the live price
            q, resolved_ticker = fetch_quote_with_fallback(alert['ticker'])
            if q is None:
                continue
                
            data = q.iloc[0].to_dict() if isinstance(q, pd.DataFrame) else q.to_dict()
            raw_price = data.get('price', data.get('current', data.get('close', 0)))
            live_price = clean_float(raw_price)
            
            if not live_price:
                continue
                
            trigger = False
            if alert['condition'] == 'ABOVE' and live_price >= alert['target_price']:
                trigger = True
            elif alert['condition'] == 'BELOW' and live_price <= alert['target_price']:
                trigger = True

            # 3. Fire the Alert & Deactivate it
            if trigger:
                user = await bot.fetch_user(int(alert['discord_user_id']))
                if user:
                    await user.send(f"🚨 **MARKET ALERT:** {alert['ticker']} has crossed your target of {alert['target_price']} PKR. Current price: **{live_price} PKR**.")
                
                # Mark alert as inactive so it doesn't spam the user every 15 minutes
                cur.execute("UPDATE price_alerts SET is_active = FALSE WHERE alert_id = %s", (alert['alert_id'],))
                conn.commit()

        cur.close()
        conn.close()

    except Exception as e:
        print("🔴 CRITICAL WATCHDOG TRACEBACK:")
        traceback.print_exc()
        await log_error_to_discord(f"market_watchdog:\n{traceback.format_exc()}")

import yfinance as yf
from datetime import datetime

def get_dividend_data(ticker: str):
    """
    Fetches the most recent cash dividend and its Ex-Date using Yahoo Finance.
    Returns a tuple: (dividend_amount, ex_date_string) or (0.0, None)
    """
    try:
        # Yahoo Finance tracks PSX tickers with the .KA suffix
        stock = yf.Ticker(f"{ticker.upper()}.KA")
        
        dividends = stock.dividends
        
        # If no dividend history exists, return safely
        if dividends.empty:
            return 0.0, None
            
        # Extract the most recent dividend payout
        latest_date = dividends.index[-1]
        dividend_amount = float(dividends.iloc[-1])
        
        # Format the date to match the '13 Aug 2026' string structure in your watchdog loop
        ex_date_text = latest_date.strftime('%d %b %Y')
        
        return dividend_amount, ex_date_text

    except Exception as e:
        print(f"yfinance failed for {ticker}: {str(e)}")
        return 0.0, None

async def test_scraper(ctx, ticker: str):
    await ctx.send(f"Scraping PSX portal for {ticker.upper()}...")
    
    dividend, ex_date = get_dividend_data(ticker)
    
    if dividend == 0.0 and ex_date is None:
        await ctx.send(f"❌ Scraper failed or no data found for {ticker.upper()}.")
    else:
        await ctx.send(f"✅ **{ticker.upper()} Data:**\nDividend: {dividend} PKR\nEx-Date: '{ex_date}'")


# Execute daily at 16:30 (4:30 PM) Pakistan Standard Time
tz_pkt = pytz.timezone('Asia/Karachi')
dividend_time = time(hour=16, minute=30, tzinfo=tz_pkt)

@tasks.loop(time=dividend_time)
async def dividend_watchdog():
    try:
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cur.execute("SELECT discord_user_id, ticker, quantity FROM portfolio_holdings WHERE quantity > 0")
        holdings = cur.fetchall()

        if not holdings:
            return

        unique_tickers = set(h['ticker'] for h in holdings)
        
        # Get today's date formatted to match PSX portal (e.g., '13 Aug 2026')
        today_str = datetime.now(tz_pkt).strftime('%d %b %Y')
        
        for ticker in unique_tickers:
            dividend_per_share, ex_date = get_dividend_data(ticker)
            
            # The brutal gate check: Does it pay out, and is TODAY the actual ex-date?
            if not dividend_per_share or ex_date != today_str:
                continue

            # Calculate and Distribute Cash
            for holding in holdings:
                if holding['ticker'] == ticker:
                    user_id = holding['discord_user_id']
                    quantity = holding['quantity']
                    payout = float(quantity) * float(dividend_per_share)
                    
                    # Inject into Cash Ledger
                    cur.execute(
                        '''INSERT INTO cash_ledger (discord_user_id, transaction_type, amount, ticker)
                           VALUES (%s, 'DIVIDEND', %s, %s)''',
                        (user_id, payout, ticker)
                    )
                    
                    user = await bot.fetch_user(int(user_id))
                    if user:
                        await user.send(f"💰 **DIVIDEND DEPOSIT:** {ticker} went Ex-Dividend today at {dividend_per_share} PKR per share. Based on your {quantity} shares, **{payout:.2f} PKR** has been deposited into your cash ledger.")

        conn.commit()
        cur.close()
        conn.close()
        update_last_run("dividend_watchdog")

    except Exception as e:
        print("🔴 CRITICAL WATCHDOG TRACEBACK:")
        traceback.print_exc()
        await log_error_to_discord(f"dividend_watchdog:\n{traceback.format_exc()}")

async def test_youtube(ctx, *, mentor_name: str):
    await ctx.send(f"🔍 Searching YouTube and extracting transcript for: **{mentor_name}**...")
    
    try:
        # The function expects a list, so we wrap the single string in brackets
        transcripts_data = fetch_mentor_transcripts([mentor_name])
        
        if not transcripts_data or mentor_name not in transcripts_data:
            await ctx.send(f"❌ **Failed.** No transcript extracted for {mentor_name}. Check your Render logs for API errors.")
            return
            
        raw_text = transcripts_data[mentor_name]
        
        # Cap the output to fit within Discord's 2000 character limit
        preview = raw_text[:1500] + "..." if len(raw_text) > 1500 else raw_text
        
        await ctx.send(f"✅ **Success!** Extracted {len(raw_text)} characters.\n\n**Transcript Preview:**\n```{preview}```")

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        # Discord has a 2000 character limit, so we slice the traceback to fit
        await ctx.send(f"❌ **Failed.** Crash details:\n```python\n{error_trace[:1900]}\n```")

async def debug_youtube(ctx):
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        await ctx.send("❌ Error: GOOGLE_API_KEY environment variable is missing.")
        return
        
    try:
        await ctx.send("Pinging Google's YouTube Data v3 API directly...")
        youtube = build('youtube', 'v3', developerKey=api_key)
        request = youtube.search().list(part="id", q="Investkaar", type="channel", maxResults=1)
        response = request.execute()
        
        channel_id = response['items'][0]['id']['channelId']
        await ctx.send(f"✅ **API Connection Successful!** Channel ID resolved: `{channel_id}`\nIf you see this, the API works and the failure is inside the transcript or audio fallback logic.")
        
    except Exception as e:
        await ctx.send(f"❌ **API Crash Details:**\n```python\n{str(e)}\n```")

# Configuration: We use the existing CHANNEL_ID from your .env
BRIEFING_CHANNEL_ID = CHANNEL_ID

# Create a timezone object for PKT (UTC+5)
pkt = timezone(timedelta(hours=5))
pkt_tz = ZoneInfo("Asia/Karachi")

async def run_business_portfolio_sweep():
    # 1. Isolate the memory so it never corrupts your Discord chat history
    unique_id = f"briefing_{uuid.uuid4().hex}"
    config = {
        "configurable": {
            "session_id": unique_id,
            "thread_id": unique_id
        }
    }
    
    portfolio_data = get_portfolio_summary()
    if "currently empty" in portfolio_data or not portfolio_data.strip():
        portfolio_context = 'Portfolio is empty. Please fall back to the default watchlist: ["SYS.KA", "HUBC.KA", "MEBL.KA", "ENGRO.KA"].'
    else:
        portfolio_context = f"Current Portfolio Context:\n{portfolio_data}"

    # 2. The Master Prompt: Forces scraping, database ingestion, and strict formatting
    prompt = f"""
    Execute your daily market sweep with the following strict sequence. 

    {portfolio_context}

    STEP 1: SCRAPE & INGEST
    - Use your search and scraping tools to find the latest macroeconomic updates, PSX notices, and new insights from InvestKaar, ARN, Jawad Hafeez, and Sarmaaya Financials over the last 24 hours.
    - IMMEDIATELY use your `ingest_knowledge_tool` to permanently save the core strategic metrics and macro facts you just found into your Vector Brain. Do not skip this step.

    STEP 2: GENERATE THE BRIEFING
    After the data is saved, format your final output EXACTLY like the structure below. Keep it compact, insightful, and brutally logical.

    # PSX Morning Mentor Sweep — {datetime.now().strftime('%A, %B %d, %Y')}
    Good morning. Here is your scheduled market intelligence sweep.

    ## 1. The 24-Hour Reality Check
    [Provide a concise update on new videos, strategic shifts, or macro news. If none, state that the analysis remains anchored to previous data.]

    ## 2. Macro Metrics & Surprises
    [Bullet points on critical macro data, e.g., trade deficits, services surplus, or sector impacts. Use bold headers for each bullet like "**The Headline Illusion:**" or "**The Market Impact:**"]

    ## 3. Sarmaaya Financials & PSX Data
    [List upcoming corporate board meetings for tracking, formal PSX notices, or holiday alerts. Analyze the Portfolio Context provided above and evaluate any immediate risks.]
    """
    
    try:
        print("⚙️ Executing isolated daily briefing and knowledge ingestion...")
        
        # 3. Invoke the agent with the isolated state
        inputs = {"messages": [HumanMessage(content=prompt)]}
        response = await asyncio.to_thread(agent_app.invoke, inputs, config)
        
        # --- NEW PARSING LOGIC ---
        messages = response.get("messages", [])
        if messages:
            final_output = messages[-1].content
        else:
            final_output = "⚠️ No analysis generated."
            
        # Check if LangChain returned a list of dictionaries
        if isinstance(final_output, list) and len(final_output) > 0 and isinstance(final_output[0], dict) and 'text' in final_output[0]:
            clean_message = final_output[0]['text']
        else:
            # Fallback if it's already a string
            clean_message = str(final_output)
            
        # Guard against empty string
        if not clean_message or clean_message.strip() == "[]":
            clean_message = "⚠️ Market sweep completed, but no report content was generated."

        # 4. Push the formatted brief to your specific Discord channel
        channel = bot.get_channel(CHANNEL_ID) 
        if channel:
            await channel.send(f"🏢 **Institutional Business Sweep (6:00 PM)**\n\n{clean_message}")
            
        # 5. Update the catch-up tracker so the bot knows it completed today's sweep
        update_last_run("run_business_portfolio_sweep")
        print("✅ Briefing complete and data ingested.")
        
    except Exception as e:
        print(f"❌ Briefing Execution Error: {str(e)}")
        await log_error_to_discord(f"run_daily_briefing:\n{str(e)}")

from youtube_transcript_api import YouTubeTranscriptApi
import yt_dlp



# Whitelisted institutional research senders
INSTITUTIONAL_DOMAINS = [
    "ktraderesearch.com",
    "ktrade.pk",
    "topline.com.pk",
    "arifhabibltd.com",
    "akdsecurities.net",
    "jsgcl.com",
    "sarmaaya.pk"
]

# Keywords that qualify an email as market research
RESEARCH_KEYWORDS = [
    "morning shout", "daily market", "pre-market", "closing bell",
    "equity market", "market view", "psx daily", "research report"
]

def clean_email_body(msg) -> str:
    """Extracts and strips HTML to plain text for LLM ingestion, prioritizing HTML payloads."""
    plain_text = ""
    html_text = ""
    
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            payload = part.get_payload(decode=True)
            if not payload:
                continue
            
            if content_type == "text/plain":
                plain_text += payload.decode(errors="ignore") + "\n"
            elif content_type == "text/html":
                soup = BeautifulSoup(payload.decode(errors="ignore"), "html.parser")
                html_text += soup.get_text(separator="\n", strip=True) + "\n"
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            if msg.get_content_type() == "text/html":
                soup = BeautifulSoup(payload.decode(errors="ignore"), "html.parser")
                html_text = soup.get_text(separator="\n", strip=True)
            else:
                plain_text = payload.decode(errors="ignore")
            
    # Prefer HTML text if plain text is just a short redirect wrapper (< 200 chars)
    if len(plain_text.strip()) < 200 and html_text.strip():
        final_text = html_text
    else:
        final_text = plain_text if plain_text.strip() else html_text
        
    return "\n".join([line.strip() for line in final_text.splitlines() if line.strip()])

async def run_email_sweep():
    print("📧 [Email Sweep] Checking inbox...")
    # 0 = Monday, 4 = Friday, 5 = Saturday, 6 = Sunday
    if datetime.now(pkt_tz).weekday() >= 5:
        return
        
    try:
        EMAIL_USER = os.getenv("EMAIL_USER")
        EMAIL_PASS = os.getenv("EMAIL_PASS")
        
        if not EMAIL_USER or not EMAIL_PASS:
            print("📧 [Email Sweep] Missing EMAIL_USER or EMAIL_PASS in .env")
            return
            
        print("📧 [Email Sweep] Authenticating with IMAP...")
        
        def fetch_emails():
            mail = imaplib.IMAP4_SSL("imap.gmail.com")
            mail.login(EMAIL_USER, EMAIL_PASS)
            mail.select("inbox")
            status, data = mail.search(None, '(UNSEEN)')
            
            if status != "OK" or not data[0]:
                mail.logout()
                return []
                
            msg_nums = data[0].split()
            print(f"📧 [Email Sweep] Found {len(msg_nums)} unread emails. Evaluating filters...")
            
            ingested_reports = []
            
            for num in msg_nums:
                res, msg_data = mail.fetch(num, "(RFC822)")
                for response_part in msg_data:
                    if isinstance(response_part, tuple):
                        msg = email.message_from_bytes(response_part[1])
                        
                        subject_header = decode_header(msg.get("Subject", ""))[0]
                        subject = subject_header[0]
                        encoding = subject_header[1]
                        if isinstance(subject, bytes):
                            subject = subject.decode(encoding or "utf-8", errors="ignore")
                        
                        sender = msg.get("From", "").lower()
                        
                        is_valid_sender = any(domain in sender for domain in INSTITUTIONAL_DOMAINS)
                        is_valid_subject = any(kw in subject.lower() for kw in RESEARCH_KEYWORDS)
                        
                        if is_valid_sender or is_valid_subject:
                            print(f"📧 [Email Sweep] Processing institutional report: '{subject}' from {sender}")
                            body_text = clean_email_body(msg)
                            ingested_reports.append((subject, sender, body_text))
                        else:
                            print(f"📧 [Email Sweep] Skipping non-research email: '{subject}'")
                            
            mail.logout()
            return ingested_reports

        # 1. Run the blocking network call in a separate thread
        ingested_reports = await asyncio.to_thread(fetch_emails)
        
        # 2. Halt execution if no new data is found
        if not ingested_reports:
            print("📧 [Email Sweep] 0 new research emails found. Inbox is clear.")
            return 
            
        print("📧 [Email Sweep] Sending data to LLM for summarization...")
        
        final_messages = []
        for subject, sender, body_text in ingested_reports:
            prompt = (
                f"Institutional Research Email Received.\n"
                f"Subject: {subject}\n"
                f"Sender: {sender}\n\n"
                f"Content:\n{body_text[:12000]}\n\n"
                f"Extract the core market outlook, key target stocks mentioned, and economic triggers."
            )
            inputs = {"messages": [HumanMessage(content=prompt)]}
            unique_id = f"email_{uuid.uuid4().hex}"
            config = {"configurable": {"session_id": unique_id, "thread_id": unique_id}}
            response = await asyncio.to_thread(agent_app.invoke, inputs, config)
            
            raw_output = response["messages"][-1].content
            if isinstance(raw_output, list):
                final_text = "".join(
                    block.get("text", "") for block in raw_output 
                    if isinstance(block, dict) and block.get("type") == "text"
                )
            else:
                final_text = str(raw_output)
            final_messages.append(f"📄 **{subject}**\n{final_text}")
        
        print("📧 [Email Sweep] Summarization complete. Sending to Discord.")
        
        channel = bot.get_channel(CHANNEL_ID)
        header = "📬 **Institutional Email Briefing**\n\n"
        full_content = header + "\n\n---\n\n".join(final_messages)
        
        # 4. Apply your existing structure-aware chunking loop
        chunk_size = 1900
        in_code_block = False
        for idx in range(0, len(full_content), chunk_size):
            chunk = full_content[idx:idx + chunk_size]
            if in_code_block:
                chunk = "```text\n" + chunk
            
            backtick_count = chunk.count("```")
            if backtick_count % 2 != 0:
                in_code_block = not in_code_block
                if in_code_block:
                    chunk += "\n```"
            await channel.send(chunk)
            
        print("📧 [Email Sweep] Email sweep finished successfully.")
            
    except Exception as e:
        import traceback
        print(f"❌ [Email Sweep Error]: {e}")
        traceback.print_exc()



async def start_scheduled_sweeps():
    await bot.wait_until_ready()
    print("⏳ Started central temporal bucket scheduler...")
    while True:
        now = datetime.now(pkt_tz)
        current_time = now.strftime("%H:%M")
        
        # 1. YouTube Knowledge Sweep (12:00 AM)
        if current_time == "00:00":
            print("🕒 Triggering YouTube Knowledge Sweep (00:00)...")
            await run_youtube_sweep()
            
        # 2. Email Sweeps (12:00 PM and 4:00 PM)
        elif current_time in ["12:00", "16:00"]:
            print(f"🕒 Triggering Email Sweep ({current_time})...")
            await run_email_sweep()
            
        # 3. Business & Portfolio Sweep (6:00 PM)
        elif current_time == "18:00":
            print("🕒 Triggering Business & Portfolio Sweep (18:00)...")
            await run_business_portfolio_sweep()
            
        # Check every 60 seconds to prevent tight CPU looping
        await asyncio.sleep(60)

@bot.event
async def on_ready():
    print(f"✅ Bot successfully connected to Discord as {bot.user} (ID: {bot.user.id})", flush=True)
    
    await update_psx_tickers()
    
    bot.add_view(ApprovalView())
    
    current_date = datetime.now().date()
    for task_name, task_func in [
        ("midnight_mentor_sweep", midnight_mentor_sweep),
        ("dividend_watchdog", dividend_watchdog),
        ("run_business_portfolio_sweep", run_business_portfolio_sweep),
        ("run_email_sweep", run_email_sweep)
    ]:
        last_run_timestamp = get_last_run(task_name)
        if last_run_timestamp == 0 or datetime.fromtimestamp(last_run_timestamp).date() < current_date:
            print(f"⚠️ Missed task detected! Running {task_name} catch-up...")
            await task_func()

    bot.loop.create_task(start_scheduled_sweeps())


    print("Automated briefing schedule initialized.", flush=True)

    if not market_watchdog.is_running():
        market_watchdog.start()
    if not dividend_watchdog.is_running():
        dividend_watchdog.start()
    if not check_price_targets.is_running():
        check_price_targets.start()
        print("✅ Background price check loop started successfully.")

async def buy_stock(ctx, ticker: str, quantity: float, price: float):
    """Command format: !buy [TICKER] [QTY] [PRICE]"""
    response = log_trade(ticker, "BUY", quantity, price)
    await ctx.send(response)

async def sell_stock(ctx, ticker: str, quantity: float, price: float):
    """Command format: !sell [TICKER] [QTY] [PRICE]"""
    response = log_trade(ticker, "SELL", quantity, price)
    await ctx.send(response)

async def view_portfolio(ctx):
    """Command format: !portfolio"""
    response = get_portfolio_summary()
    await ctx.send(response)

async def add_target(ctx, ticker: str, target: float, stop_loss: float = 0.0):
    """Command format: !target [TICKER] [TARGET_PRICE] [STOP_LOSS]"""
    response = set_target(ticker, target, stop_loss)
    await ctx.send(response)

@tasks.loop(minutes=60) # The agent will check the PSX every 60 minutes
async def check_price_targets():
    # Wait until the bot is fully booted before checking
    await bot.wait_until_ready()
    
    # Grab the channel ID from your .env file
    raw_channel_id = os.getenv("ADMIN_CHANNEL_ID") or os.getenv("DISCORD_CHANNEL_ID")
    if not raw_channel_id:
        print("⚠️ Warning: Neither ADMIN_CHANNEL_ID nor DISCORD_CHANNEL_ID is set.")
        return
    channel_id = int(raw_channel_id)
    channel = bot.get_channel(channel_id)
    
    if not channel:
        print("🚨 Alert Task Failed: Could not find the Discord channel.")
        return

    try:
        # Open a quick connection to Supabase
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cursor = conn.cursor()
        
        # Fetch all active targets
        cursor.execute("SELECT ticker, target_price, stop_loss FROM watchlist_targets")
        targets = cursor.fetchall()
        
        for row in targets:
            ticker = row[0]
            target_price = float(row[1]) if row[1] else 0.0
            stop_loss = float(row[2]) if row[2] else 0.0
            
            live_price = get_live_price(ticker)
            if live_price == 0.0:
                continue # Skip if the PSX site is temporarily down
                
            # Check if Target is Hit
            if target_price > 0 and live_price >= target_price:
                await channel.send(f"🚨 **PROFIT TARGET HIT:** {ticker} has reached Rs. {live_price}! (Target: Rs. {target_price})")
                # Delete the target so the bot doesn't spam you every hour
                cursor.execute("DELETE FROM watchlist_targets WHERE ticker = %s", (ticker,))
                conn.commit()
                
            # Check if Stop Loss is Hit
            elif stop_loss > 0 and live_price <= stop_loss:
                await channel.send(f"⚠️ **STOP LOSS TRIGGERED:** {ticker} has dropped to Rs. {live_price}! (Stop Loss: Rs. {stop_loss})")
                cursor.execute("DELETE FROM watchlist_targets WHERE ticker = %s", (ticker,))
                conn.commit()
                
        cursor.close()
        conn.close()
        print("✅ DIAGNOSTIC: Background price check completed.")
        
    except Exception as e:
        print(f"🚨 Background Task Error: {e}")
        await log_error_to_discord(f"check_price_targets:\n{str(e)}")

# Background loop started in on_ready

def run_bot():
    token = os.getenv("DISCORD_BOT_TOKEN")
    if token:
        bot.run(token)
    else:
        print("DISCORD_BOT_TOKEN not found.")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    run_bot()
