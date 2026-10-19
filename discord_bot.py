from dotenv import load_dotenv
load_dotenv()

import discord
from discord.ext import commands, tasks
from datetime import datetime, time, timezone, timedelta
from zoneinfo import ZoneInfo
import os
import ast
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

import httpx
from google import genai
from google.genai import types, errors

client = genai.Client(http_options={"timeout": 300000})





from portfolio_manager import log_trade, get_portfolio_summary, set_target, get_live_price, get_watchlist_summary, scan_shariah_market, sync_portfolio_tool

import psycopg2.pool
DB_POOL = psycopg2.pool.ThreadedConnectionPool(
    minconn=1,
    maxconn=20,
    dsn=os.getenv("SUPABASE_DB_URL")
)

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

# Ensure DISCORD_CHANNEL_ID is loaded from the environment
raw_channel_id = os.getenv("ADMIN_CHANNEL_ID") or os.getenv("DISCORD_CHANNEL_ID", "0")
CHANNEL_ID = int(raw_channel_id) if raw_channel_id else 0
ADMIN_CHANNEL_ID = CHANNEL_ID
PROSPECT_CHANNEL_ID = int(os.getenv("PROSPECT_CHANNEL_ID", "0")) or CHANNEL_ID
RESEARCH_CHANNEL_ID = int(os.getenv("RESEARCH_CHANNEL_ID", "0")) or CHANNEL_ID

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

async def send_long_message(channel, raw_message, max_chars: int = 1800):
    """
    Parses complex objects into clean text, using Regex as an absolute 
    failsafe against stringified Google GenAI/LangGraph dictionaries.
    """
    # 1. Standard Object Extraction
    if isinstance(raw_message, list):
        message_text = "\n\n".join(str(item) for item in raw_message)
    elif isinstance(raw_message, dict):
        if "messages" in raw_message and hasattr(raw_message["messages"][-1], "content"):
            message_text = str(raw_message["messages"][-1].content)
        elif "text" in raw_message:
            message_text = str(raw_message["text"])
        else:
            message_text = str(raw_message)
    elif hasattr(raw_message, "content") and not isinstance(raw_message.content, (list, dict)):
        message_text = str(raw_message.content)
    elif hasattr(raw_message, "text") and not isinstance(raw_message.text, (list, dict)):
        message_text = str(raw_message.text)
    else:
        message_text = str(raw_message)

    # 2. THE REGEX SLEDGEHAMMER
    # If the string STILL contains the Gemini dictionary wrapper, rip the text out manually
    if "{'type':" in message_text and "'text':" in message_text:
        # Matches everything between 'text': " and ", 'extras':
        match = re.search(r"'text':\s*([\"'])(.*?)(?<!\\)\1\s*,\s*'extras':", message_text, re.DOTALL)
        if match:
            message_text = match.group(2)
            # Convert literal string '\n' back into actual Discord line breaks
            message_text = message_text.replace('\\n', '\n')

    if not message_text or not message_text.strip():
        return

    # 3. ACCURATE LENGTH CHECK
    if len(message_text) <= max_chars:
        await channel.send(message_text)
        return

    # 4. CHUNKING LOGIC
    chunks = []
    current_chunk = ""
    
    for line in message_text.split("\n"):
        if len(line) > max_chars:
            if current_chunk.strip():
                chunks.append(current_chunk.strip())
                current_chunk = ""
            while len(line) > max_chars:
                chunks.append(line[:max_chars])
                line = line[max_chars:]
            current_chunk = line + "\n"
            continue

        if len(current_chunk) + len(line) + 1 > max_chars:
            if current_chunk.strip():
                chunks.append(current_chunk.strip())
            current_chunk = line + "\n"
        else:
            current_chunk += line + "\n"
            
    if current_chunk.strip():
        chunks.append(current_chunk.strip())

    # 5. SAFE DISPATCH
    for msg_chunk in chunks:
        if msg_chunk.strip():
            await channel.send(msg_chunk)

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
        from market_data_scraper import fetch_psx_data_robust
        data = await fetch_psx_data_robust(url, is_json=True)
        if data:
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
                
            if count < 200:
                raise ValueError(f"PSX API payload abnormally small ({count} items), assuming cache poisoning risk.")
                
            print(f"✅ Loaded {count} active PSX tickers into memory.")
            # Save to cache
            try:
                with open('psx_tickers_cache.json', 'w') as f:
                    f.write(DYNAMIC_PSX_TICKER_MAPPING)
            except Exception as cache_err:
                print(f"⚠️ Failed to write ticker cache: {cache_err}")
        else:
            raise Exception("fetch_psx_data_robust returned None")
    except Exception as e:
        print(f"🚨 Ticker Fetch Error: {repr(e)}")
        import traceback
        traceback.print_exc()
        # Fallback to cache
        try:
            if os.path.exists('psx_tickers_cache.json'):
                with open('psx_tickers_cache.json', 'r') as f:
                    DYNAMIC_PSX_TICKER_MAPPING = f.read()
                print("✅ Loaded PSX tickers from local cache fallback.")
            else:
                print("⚠️ No local PSX ticker cache available.")
        except Exception as cache_err:
            print(f"⚠️ Failed to load ticker cache: {cache_err}")
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
            "Sarmaaya Financials",
            "AKD Securities Limited",
            "KTrade PK"
        ]
        
        print("Starting Unfiltered Midnight Mentor Sweep...")
        # Offload yt-dlp / transcript fetch
        transcripts_data, failed_mentors = await fetch_mentor_transcripts(mentors)
        
        if failed_mentors:
            alert_channel = bot.get_channel(ADMIN_CHANNEL_ID)
            if alert_channel:
                msg = f"⚠️ **PARTIAL SWEEP FAILURE**: The following mentors failed all fallback extraction tiers: {', '.join(failed_mentors)}"
                await alert_channel.send(msg)
                
        if not transcripts_data:
            print("No new transcripts could be extracted tonight.")
            return

        # 1. Fetch current portfolio
        def get_portfolio_weights():
            from portfolio_manager import get_db_connection
            conn = get_db_connection()
            try:
                cur = conn.cursor()
                cur.execute("SELECT ticker, shares FROM portfolio_holdings WHERE shares > 0")
                holdings = cur.fetchall()
                
                h_str = []
                for row in holdings:
                    ticker, qty = row[0], float(row[1])
                    h_str.append(f"{ticker} ({qty} shares)")
                
                res = "Current Holdings: " + ", ".join(h_str) if h_str else "No active holdings."
                cur.close()
                return res
            except Exception as e:
                print("Error fetching holdings for sweep:", e)
                return "Could not fetch holdings due to DB error."
            finally:
                conn.close()
                
        # Offload DB call
        holdings_str = await asyncio.to_thread(get_portfolio_weights)

        # Loop through successful extractions and send them to your LLM agent
        for mentor, text in transcripts_data.items():
            from ingestion import SWEEP_FORMAT_PROMPT
            prompt = SWEEP_FORMAT_PROMPT.format(mentor_name=mentor) + f"\n\nTranscript:\n{text}"
            inputs = {"messages": [HumanMessage(content=prompt)]}
            config = {"configurable": {"thread_id": f"midnight_sweep_{mentor}"}}
            
            try:
                agent_app = await get_agent()
                response = await agent_app.ainvoke(inputs, config)
                raw_content = response["messages"][-1].content
                
                if isinstance(raw_content, list):
                    final_reply = "".join(
                        block.get("text", "") for block in raw_content 
                        if isinstance(block, dict) and block.get("type") == "text"
                    )
                else:
                    final_reply = str(raw_content)
                
                # Split the text at the internal system markers to clean the Discord message
                discord_friendly_message = final_reply
                if "EXTRACTED TICKERS" in discord_friendly_message:
                    discord_friendly_message = discord_friendly_message.split("EXTRACTED TICKERS")[0]
                if "EXTRACTED TACTICAL RULE" in discord_friendly_message:
                    discord_friendly_message = discord_friendly_message.split("EXTRACTED TACTICAL RULE")[0]
                
                discord_friendly_message = discord_friendly_message.strip()
                
                channel = bot.get_channel(RESEARCH_CHANNEL_ID)
                if channel:
                    embed = discord.Embed(
                        title=f"🌙 Midnight Insight: {mentor}",
                        description=discord_friendly_message[:4096],
                        color=16766720 # Gold color
                    )
                    await channel.send(embed=embed)
                        
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
        await agent_app.ainvoke(None, self.config)
        
        snapshot = await agent_app.aget_state(self.config)
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
        await agent_app.aupdate_state(self.config, {"messages": [tool_message]}, as_node="tools")
        await agent_app.ainvoke(None, self.config)
        
        snapshot = await agent_app.aget_state(self.config)
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

from google import genai
from google.genai import types

CORE_DIRECTIVE = """
You are an autonomous quantitative value investing agent managing a live PSX portfolio. 
Your core mandate is capital preservation and deploying cash strictly based on mathematical intrinsic value and the provided mental models.

CRITICAL RULES:
1. NO TECHNICAL ANALYSIS: You are strictly forbidden from referencing Moving Averages, RSI, MACD, 'momentum', or 'chart patterns'. 
2. DCF IS LAW: Base all accumulation and trim decisions purely on the provided DCF Intrinsic Value, Margin of Safety, and concentration limits.
3. BAN ON SUNK COST FALLACY: Never base a hold/sell decision on the portfolio's 'average cost' or 'unrealized P/L'. The market does not care what price we bought at. Refusing to realize a loss is emotional weakness. Base sells on valuation, not cost basis.
4. BAN ON THE DISPOSITION EFFECT: Never suggest selling a winning asset simply because it is in the green. Winners are meant to compound. Base sells strictly on valuation exhaustion (price exceeding DCF).
5. APPLY INGESTED MENTAL MODELS: Always apply the principles from the latest vector ingestions (e.g., cutting emotional losers) to justify moves.
"""

# 1. Define the rigid tools the LLM is allowed to use
import os
import psycopg2
import psycopg2.extras

def trigger_research(ticker: str = "KMI30") -> str:
    """Scans the entire KMI-30 universe, evaluating live prices against hardened DCF intrinsic values and active cash levels."""
    try:
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Pull all fundamentals and latest calculated metrics across the board
        cur.execute("""
            SELECT ticker, fcf, shares_outstanding, historical_growth, total_cash, total_debt 
            FROM fundamentals
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        return f"Database error during universe scan: {str(e)}"

    if not rows:
        return "No fundamental records found in database."

    evaluation_summary = []
    
    # Simple DCF evaluation loop across the universe
    for r in rows:
        t = r["ticker"]
        fcf = float(r.get("fcf", 0) or 0)
        shares = float(r.get("shares_outstanding", 0) or 0)
        growth = float(r.get("historical_growth", 0) or 0)
        cash = float(r.get("total_cash", 0) or 0)
        debt = float(r.get("total_debt", 0) or 0)
        
        if fcf <= 0 or shares <= 0:
            continue
            
        # Simplified execution of your hardened DCF model for screening
        discount_rate = 0.12 + 0.07 # Normalized RFR (12%) + ERP (7%)
        terminal_growth = 0.04
        
        # 5-year tapered projection simulation
        current_fcf = fcf
        pv_fcf = 0
        for i in range(1, 6):
            tapered = growth - ((growth - terminal_growth) * (i / 5))
            current_fcf *= (1 + tapered)
            pv_fcf += current_fcf / ((1 + discount_rate) ** i)
            
        terminal_val = (current_fcf * (1 + terminal_growth)) / (discount_rate - terminal_growth)
        pv_terminal = terminal_val / ((1 + discount_rate) ** 5)
        
        equity_val = (pv_fcf + pv_terminal) + cash - debt
        fair_value = round(equity_val / shares, 2)
        
        evaluation_summary.append(f"- **{t}**: Intrinsic DCF Fair Value = Rs. {fair_value}")

    # Pull the latest active rules from Active Playbook
    try:
        from google import genai
        client = genai.Client(http_options={"timeout": 300000})
        
        # 1. Define the semantic context of what you are researching
        research_context = f"Analyzing {ticker} for fundamental valuation, dividend yield, macro sensitivity, and cyclical margin expansion."
        
        # 2. Embed the context
        result = client.models.embed_content(
            model='gemini-embedding-001',
            contents=research_context,
            config={'output_dimensionality': 768}
        )
        query_embedding = result.embeddings[0].values
        
        # 3. Pull the top 5 most semantically relevant rules (threshold 0.5)
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor()
        cur.execute(
            "SELECT source, rule FROM match_playbook_rules(%s::vector, 0.5, 5)",
            (query_embedding,)
        )
        active_rules = cur.fetchall()
        cur.close()
        conn.close()
        
        # 4. Format for the LLM
        if active_rules:
            rules_formatted = "\n".join([f"- [{r[0]}]: {r[1]}" for r in active_rules])
        else:
            rules_formatted = "Standard value compounding frameworks apply."
    except Exception as e:
        rules_formatted = f"Error fetching playbook rules: {e}"

    return (
        f"=== KMI-30 UNIVERSE QUANTITATIVE SCREEN ===\n"
        f"Liquid Capital Available: Rs. 5,225.16\n\n"
        f"Calculated Intrinsic Values:\n" + "\n".join(evaluation_summary) + "\n\n"
        f"=== ACTIVE INVESTMENT PLAYBOOK (LEARNED TACTICS) ===\n"
        f"{rules_formatted}\n\n"
        f"MANDATE FOR RESEARCH:\n"
        f"You must evaluate the target asset against these learned tactics and your DCF valuations. In your response, explicitly state which learned rule guided your verdict."
    )

def force_cash_sweep() -> str:
    """Forces the cash deployment sweep to run immediately."""
    import schedulers
    import asyncio
    asyncio.create_task(schedulers.run_cash_deployment_sweep())
    return "Cash sweep triggered manually."

# 2. Bind them to the model
agent_tools = [trigger_research, force_cash_sweep]
gemini_client = genai.Client(http_options={"timeout": 300000})

@bot.event
async def on_message(message):
    # 🛑 Ignore messages from the bot itself
    if message.author.bot:
        return

    # Direct Mentions or DMs logic for Gemini tool calling
    if bot.user in message.mentions or not message.guild:
        from agent import get_portfolio_status
        mock_config = {"configurable": {"discord_user_id": str(message.author.id)}}
        current_portfolio_state = await get_portfolio_status.ainvoke(mock_config)
        
        injected_prompt = (
            f"CURRENT PORTFOLIO STATE:\n{current_portfolio_state}\n\n"
            f"USER MESSAGE:\n{message.content}"
        )

        chat = gemini_client.chats.create(
            model='gemini-3.8-flash',
            config=types.GenerateContentConfig(
                system_instruction=CORE_DIRECTIVE
            )
        )
        response = chat.send_message(injected_prompt, config=types.GenerateContentConfig(tools=agent_tools))
        await message.channel.send(response.text)
        return

    # 🚀 THE FULL-FLEDGED FORCE TEST TRIGGER
    if message.content.strip() == "!force_test":
        await message.channel.send("🚀 **Initiating Full System Sweep Test...**")
        
        # Import all dependencies at the top before any checks
        import schedulers
        import youtube_scraper
        import portfolio_manager
        import asyncio

        try:
            # Step 1: Email Sweep
            await message.channel.send("📧 **Step 1:** Running Multimodal Email Sweep...")
            await run_email_sweep() 
            
            # Step 2: YouTube Sweep
            await message.channel.send("🎥 **Step 2:** Running YouTube Ingestion Sweep...")
            if hasattr(schedulers, "run_youtube_sweep"):
                await schedulers.run_youtube_sweep()
            elif hasattr(youtube_scraper, "run_youtube_sweep"):
                await youtube_scraper.run_youtube_sweep()
            elif hasattr(youtube_scraper, "scrape_and_process_videos"):
                await asyncio.to_thread(youtube_scraper.scrape_and_process_videos)
            
            # Step 3: Business & Portfolio Sweep
            await message.channel.send("💼 **Step 3:** Running Business & Portfolio Sweep...")
            await schedulers.run_business_portfolio_sweep()
            
            # Step 4: Daily Snapshot
            await message.channel.send("📊 **Step 4:** Recording Daily Database Snapshot...")
            await asyncio.to_thread(portfolio_manager.record_daily_snapshot)
            
            await message.channel.send("✅ **All forced sweeps executed successfully!**")
        except Exception as e:
            await message.channel.send(f"⚠️ **Test Failed at some point:** `{e}`")
            
        return # Stop execution here so it doesn't trigger the LangGraph AI loop

    # INSTANT TYPING: Fire this before ANY database or LangGraph operations
    async with message.channel.typing():
        try:
            current_author_id.set(message.author.id)

            # --- Phase 4 Conversational Ledger Parsing ---
            if message.channel.id == CHANNEL_ID and not message.content.startswith("!"):
                from trade_parser import extract_trade_intent
                from valuation_engine import execute_buy_fifo, execute_sell_fifo
                
                trade_intent = extract_trade_intent(message.content)
                if trade_intent:
                    user_id = str(message.author.id)
                    try:
                        if trade_intent.action == "BUY":
                            result = execute_buy_fifo(user_id, trade_intent.ticker, trade_intent.shares, trade_intent.price)
                        elif trade_intent.action == "SELL":
                            result = execute_sell_fifo(user_id, trade_intent.ticker, trade_intent.shares, trade_intent.price)
                        else:
                            result = {"status": "error", "message": "Unknown trade action."}
                            
                        await message.channel.send(result.get("message", "Trade executed."))
                    except Exception as e:
                        await message.channel.send(f"🚨 Execution Error: {str(e)}")
                    return # Stop execution so it doesn't trigger the main agent
            # ---------------------------------------------

            # Trigger the agent to think, execute tools, and generate a response
            from agent import get_portfolio_status
            mock_config = {"configurable": {"discord_user_id": str(message.author.id)}}
            current_portfolio_state = await get_portfolio_status.ainvoke(mock_config)
            
            base_text = message.content or "Analyze these images against our portfolio and mentor rules."
            injected_prompt = (
                f"CURRENT PORTFOLIO STATE:\n{current_portfolio_state}\n\n"
                f"USER MESSAGE:\n{base_text}"
            )
            
            content = []
            has_images = False
            for attachment in message.attachments:
                if any(attachment.filename.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.webp']):
                    image_bytes = await attachment.read()
                    image_b64 = base64.b64encode(image_bytes).decode('utf-8')
                    image_data = f"data:{attachment.content_type};base64,{image_b64}"
                    content.append({"type": "image_url", "image_url": {"url": image_data}})
                    has_images = True

            if has_images:
                content.insert(0, {"type": "text", "text": injected_prompt})
            else:
                content = injected_prompt

            # We use ainvoke so it doesn't block the discord async loop
            # Route the channel ID directly into the existing PostgresSaver in agent.py
            session_id = str(message.channel.id)
            config = {"configurable": {"thread_id": session_id, "discord_user_id": str(message.author.id)}}
            inputs = {"messages": [HumanMessage(content=content)]}
            
            agent_app = await get_agent()
            response = await agent_app.ainvoke(inputs, config)
            
            # Loop to handle tool interruptions and auto-resume for safe tools
            while True:
                snapshot = await agent_app.aget_state(config)
                
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
                        response = await agent_app.ainvoke(None, config)
                else:
                    break
            
            # Extract final response
            snapshot = await agent_app.aget_state(config)
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
                await send_long_message(message.channel, final_text)

        except Exception as e:
            import traceback
            traceback.print_exc()
            await message.channel.send(f"⚠️ **Agent Error:** `{str(e)}`")
import psycopg2
import psycopg2.extras
import pandas as pd
from agent import fetch_quote_with_fallback, clean_float

import pytz

@tasks.loop(minutes=5) # Balanced scanning interval
async def market_watchdog():
    try:
        # 1. Strict Market-Hours Time Gate
        tz_pkt = pytz.timezone('Asia/Karachi')
        now_pkt = datetime.now(tz_pkt)
        
        if now_pkt.weekday() >= 5:
            return 
            
        current_time = now_pkt.time()
        market_open = datetime.strptime("09:30", "%H:%M").time()
        market_close = datetime.strptime("15:30", "%H:%M").time()
        
        if not (market_open <= current_time <= market_close):
            return 

        def fetch_market_alerts_sync():
            import psycopg2.extras
            import pandas as pd
            from agent import fetch_quote_with_fallback, clean_float
            conn = DB_POOL.getconn()
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                cur.execute("SELECT alert_id, discord_user_id, ticker, target_price, condition FROM price_alerts WHERE is_active = TRUE")
                alerts = cur.fetchall()
                
                if not alerts:
                    cur.close()
                    return []
                    
                triggered_alerts = []
                for alert in alerts:
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

                    if trigger:
                        triggered_alerts.append((alert, live_price))
                        cur.execute("UPDATE price_alerts SET is_active = FALSE WHERE alert_id = %s", (alert['alert_id'],))
                        
                conn.commit()
                cur.close()
                return triggered_alerts
            finally:
                DB_POOL.putconn(conn)

        # Offload blocking database and API calls to a thread
        triggered_alerts = await asyncio.to_thread(fetch_market_alerts_sync)

        # 2. Fire the Alerts back in the async loop and WAKE UP LANGGRAPH
        for alert, live_price in triggered_alerts:
            # A. Notify the user via DM
            user = await bot.fetch_user(int(alert['discord_user_id']))
            if user:
                await user.send(f"🚨 **MARKET ALERT:** {alert['ticker']} has crossed your target of {alert['target_price']} PKR. Current price: **{live_price} PKR**.")

            # B. Route directly to the DCF Agent in the main channel
            channel_id_str = os.getenv("ALERT_CHANNEL_ID") or os.getenv("ADMIN_CHANNEL_ID")
            if channel_id_str:
                channel = bot.get_channel(int(channel_id_str))
                if channel:
                    await channel.send(f"🚨 **Internal Watchdog Triggered:** `{alert['ticker']}` crossed `{alert['condition']}` target of Rs. {alert['target_price']}. Live price: `Rs. {live_price}`. Initiating instant DCF valuation sweep...")
                    
                    from agent import get_agent
                    agent_app = await get_agent()
                    prompt = f"Market Alert: {alert['ticker']} triggered a {alert['condition']} signal at Rs. {live_price}. Run a calculate_dcf_valuation for {alert['ticker']} and give me a strict tactical verdict comparing fair value to the current price."
                    initial_state = {"messages": [("user", prompt)]}
                    config = {"configurable": {"thread_id": f"watchdog_alert_{alert['ticker']}_{alert['alert_id']}"}}
                    
                    ai_verdict = await agent_app.ainvoke(initial_state, config)
                    await send_long_message(channel, ai_verdict)

    except Exception as e:
        print("🔴 CRITICAL WATCHDOG TRACEBACK:")
        import traceback
        traceback.print_exc()
        await log_error_to_discord(f"market_watchdog:\n{traceback.format_exc()}")

RECENT_OPPORTUNITY_ALERTS = {}

@tasks.loop(hours=4) # Scans every 4 hours to preserve API tokens while catching daily market shifts
async def passive_opportunity_scanner():
    try:
        import os
        import asyncio
        from agent import get_screened_opportunities, get_agent
        
        candidates = await asyncio.to_thread(get_screened_opportunities)
        
        channel_id_str = os.getenv("ADMIN_CHANNEL_ID")
        if not channel_id_str:
            return
            
        channel = bot.get_channel(int(channel_id_str))

        # Zero hallucination escape hatch
        if not candidates:
            print("Scanner Pass: 0 tickers passed quantitative hurdles. Aborting pass.")
            return

        # Construct the strictly quantified payload
        dataset_summary = "\n".join([
            f"- {c['ticker']} ({c['thesis_type']}): Price = Rs. {c['live_price']}, "
            f"MoS = {c['margin_of_safety_pct']}%, Div Yield = {c['dividend_yield_pct']}%, ROIC = {c['roic_pct']}%"
            for c in candidates
        ])

        analysis_prompt = f"""
The following {len(candidates)} stock(s) have passed all strict quantitative hurdles (ROIC >= 12%, Earnings Yield >= 15%, MoS >= 25%):

{dataset_summary}

TASK:
1. Review recent market news, sector tailwinds, and earnings momentum for these candidates.
2. Filter out value traps (e.g., one-off earnings spikes, impending regulatory fines).
3. If a strong thesis exists, select the SINGLE best asymmetric opportunity and output the entry, intrinsic target, and strategic thesis.
4. If none present a compelling near-term catalyst, respond with strictly "NO_ACTION".
"""
        agent_app = await get_agent()
        state = {"messages": [("user", analysis_prompt)]}
        verdict = await agent_app.ainvoke(state, {"configurable": {"thread_id": "market_wide_scanner"}})
        
        raw_content = verdict["messages"][-1].content
        if isinstance(raw_content, list):
            final_text = " ".join([item.get("text", "") if isinstance(item, dict) else str(item) for item in raw_content]).strip()
        else:
            final_text = str(raw_content).strip()

        # 3. Only notify Discord if the AI proves the thesis
        if "NO_ACTION" not in final_text:
            await channel.send("🚨 **AUTONOMOUS RESEARCH OPPORTUNITY DETECTED** 🚨\nExtracting quantitative parameters...")
            
            from pydantic import BaseModel
            from langchain_google_genai import ChatGoogleGenerativeAI
            
            class OpportunityParser(BaseModel):
                ticker: str
                price: float
                intrinsic: float
            
            from model_router import get_langchain_llm, TASK_FAST_INGESTION
            llm = get_langchain_llm(TASK_FAST_INGESTION)
            parser = llm.with_structured_output(OpportunityParser)
            try:
                parsed_op = parser.invoke(f"Extract the exact ticker, current price, and target/intrinsic price from this thesis:\n{final_text}")
                op = {"ticker": parsed_op.ticker, "price": float(parsed_op.price), "intrinsic": float(parsed_op.intrinsic)}
            except Exception as e:
                await channel.send(f"⚠️ **Parsing Error:** Could not extract trade metrics from the thesis. {e}")
                op = None
            
            if op:
                from risk_engine import calculate_kelly_position_size
                from portfolio_manager import generate_daily_closing_summary

                # Pull fresh portfolio state
                summary = generate_daily_closing_summary()
                tot_val = summary.get("total_portfolio_value", 34000.0)
                liq_cash = summary.get("liquid_cash", 5225.16)
                current_holdings = summary.get("current_holdings", [])

                entry_price = float(op['price'])
                target_price = float(op['intrinsic'])
                stop_loss = round(entry_price * 0.85, 2)  # Strict 15% stop loss

                projected_upside_pct = (target_price - entry_price) / entry_price
                from macro_indicators import passes_real_yield_hurdle
                if not passes_real_yield_hurdle(projected_upside_pct):
                    await channel.send("🚫 **Trade blocked.** Nominal yield does not exceed annualized currency devaluation.")
                else:
                    # Calculate Quarter-Kelly Sizing
                    sizing = calculate_kelly_position_size(
                        entry_price=entry_price,
                        target_price=target_price,
                        stop_loss=stop_loss,
                        total_portfolio_value=tot_val,
                        liquid_cash=liq_cash
                    )
    
                    from risk_engine import check_sector_concentration
                    sector_safe, sector_msg = check_sector_concentration(
                        ticker=op['ticker'],
                        proposed_allocation_pkr=sizing['allocated_capital'],
                        total_portfolio_value=tot_val,
                        current_holdings=current_holdings
                    )
                    
                    if not sector_safe:
                        await channel.send(sector_msg)
                    else:
                        # Format Execution Payload
                        sizing_header = (
                            f"🎯 **QUANTITATIVE EXECUTION DIRECTIVE: {op['ticker']}**\n"
                            f"• **Entry**: Rs. {entry_price:,.2f} | **Target**: Rs. {target_price:,.2f} | **Stop-Loss**: Rs. {stop_loss:,.2f}\n"
                            f"• **Payoff Ratio**: {sizing.get('payoff_ratio', 'N/A')}x | **Kelly Weight**: {sizing.get('fractional_kelly_pct', 0)}%\n"
                            f"• **Recommended Action**: Buy **{sizing['recommended_shares']} shares** (Rs. {sizing['allocated_capital']:,.2f})\n"
                            f"• **Portfolio Impact**: Allocates {sizing['portfolio_weight_pct']}% of total net worth\n"
                            "--------------------------------------------------"
                        )
        
                        await channel.send(sizing_header)
                
            from discord_bot import send_long_message # Ensure import is available
            await send_long_message(channel, final_text)

    except Exception as e:
        print(f"🔴 SCANNER ERROR: {e}")



import yfinance as yf
from datetime import datetime

def get_dividend_data(ticker: str):
    """
    Fetches the most recent cash dividend and its Ex-Date using Yahoo Finance.
    Returns a tuple: (dividend_amount, ex_date_string) or (0.0, None)
    """
    dividend_amount = 0.0
    ex_date_text = None
    success = False
    
    try:
        # Yahoo Finance tracks PSX tickers with the .KA suffix
        stock = yf.Ticker(f"{ticker.upper()}.KA")
        
        dividends = stock.dividends
        
        if dividends is not None and not dividends.empty:
            # Extract the most recent dividend payout
            latest_date = dividends.index[-1]
            dividend_amount = float(dividends.iloc[-1])
            ex_date_text = latest_date.strftime('%d %b %Y')
            success = True
            
            # Cache the successful result in portfolio_holdings
            conn = DB_POOL.getconn()
            try:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE portfolio_holdings SET dividend_amount = %s, ex_dividend_date = %s WHERE ticker = %s",
                    (dividend_amount, ex_date_text, ticker.upper())
                )
                conn.commit()
                cur.close()
            except Exception as db_e:
                print(f"Failed to cache dividend for {ticker}: {db_e}")
            finally:
                DB_POOL.putconn(conn)

    except Exception as e:
        print(f"yfinance failed for {ticker}: {str(e)}")
        
    if not success:
        # Fallback to local cache in portfolio_holdings
        try:
            conn = DB_POOL.getconn()
            try:
                import psycopg2.extras
                cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                cur.execute("SELECT dividend_amount, ex_dividend_date FROM portfolio_holdings WHERE ticker = %s LIMIT 1", (ticker.upper(),))
                row = cur.fetchone()
                if row and row['dividend_amount'] and row['ex_dividend_date']:
                    dividend_amount = float(row['dividend_amount'])
                    ex_date_text = row['ex_dividend_date']
                cur.close()
            finally:
                DB_POOL.putconn(conn)
        except Exception as cache_e:
            print(f"Cache fallback failed for {ticker}: {cache_e}")

    return dividend_amount, ex_date_text

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
        tz_pkt = pytz.timezone('Asia/Karachi')
        today_str = datetime.now(tz_pkt).strftime('%d %b %Y')

        def fetch_and_distribute_dividends_sync(t_str):
            import psycopg2.extras
            conn = DB_POOL.getconn()
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                
                cur.execute("SELECT ticker, shares FROM portfolio_holdings WHERE shares > 0")
                holdings = cur.fetchall()
                
                if not holdings:
                    cur.close()
                    return []
                    
                unique_tickers = set(h['ticker'] for h in holdings)
                messages = []
                
                for ticker in unique_tickers:
                    dividend_per_share, ex_date = get_dividend_data(ticker)
                    
                    # The brutal gate check: Does it pay out, and is TODAY the actual ex-date?
                    if not dividend_per_share or ex_date != t_str:
                        continue

                    # Calculate and Distribute Cash
                    for holding in holdings:
                        if holding['ticker'] == ticker:
                            quantity = holding['shares']
                            payout = float(quantity) * float(dividend_per_share)
                            
                            # Inject into Cash Ledger
                            cur.execute(
                                '''INSERT INTO cash_ledger (transaction_type, amount, ticker)
                                   VALUES ('DIVIDEND', %s, %s)''',
                                (payout, ticker)
                            )
                            
                            messages.append((ticker, dividend_per_share, quantity, payout))

                conn.commit()
                cur.close()
                return messages
            finally:
                DB_POOL.putconn(conn)

        # Offload blocking database and API calls to a thread
        messages_to_send = await asyncio.to_thread(fetch_and_distribute_dividends_sync, today_str)

        channel = bot.get_channel(PROSPECT_CHANNEL_ID)
        if channel:
            for ticker, dividend_per_share, quantity, payout in messages_to_send:
                msg = f"💰 **DIVIDEND DEPOSIT:** {ticker} went Ex-Dividend today at {dividend_per_share} PKR per share. Based on our {quantity} shares, **{payout:.2f} PKR** has been deposited into the cash ledger."
                if len(msg) > 1950:
                    for chunk in [msg[i:i+1950] for i in range(0, len(msg), 1950)]:
                        await channel.send(chunk)
                else:
                    await channel.send(msg)

        update_last_run("dividend_watchdog")

    except Exception as e:
        print("🔴 CRITICAL WATCHDOG TRACEBACK:")
        traceback.print_exc()
        await log_error_to_discord(f"dividend_watchdog:\n{traceback.format_exc()}")

async def test_youtube(ctx, *, mentor_name: str):
    await ctx.send(f"🔍 Searching YouTube and extracting transcript for: **{mentor_name}**...")
    
    try:
        # The function expects a list, so we wrap the single string in brackets
        transcripts_data, failed_mentors = await fetch_mentor_transcripts([mentor_name])
        
        if failed_mentors or not transcripts_data or mentor_name not in transcripts_data:
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
BRIEFING_CHANNEL_ID = PROSPECT_CHANNEL_ID

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
You are a ruthless, cynical senior equity analyst managing a proprietary PSX portfolio.
Review the market data, news, and our current portfolio holdings (SYS, HUBC, LUCK, FFC, EFERT, MEBL, MARI).

Write a sharp, aggressive End-of-Day Desk Note.
NO robotic headers, NO numbered lists (1, 2, 3), and NO corporate fluff. 
Format exactly like this:

**⚡ End-of-Day Desk Note**
Write 2-3 punchy sentences on the macro reality today. What actually moved the needle? Is it noise or a structural shift?

**🎯 Portfolio Impact & Action**
Call out ONLY the holdings that matter today. Are we bleeding on SYS? Is MEBL carrying the weight? Give me the brutal truth and state the desk's immediate action (e.g., hold cash, buy the dip, cut losers).

{portfolio_context}
    """
    
    try:
        print("⚙️ Executing isolated daily briefing and knowledge ingestion...")
        
        # 3. Invoke the agent with the isolated state
        agent_app = await get_agent()
        inputs = {"messages": [HumanMessage(content=prompt)]}
        response = await agent_app.ainvoke(inputs, config)
        
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
        channel = bot.get_channel(PROSPECT_CHANNEL_ID) 
        if channel:
            msg = f"🏢 **Institutional Business Sweep (6:00 PM)**\n\n{clean_message}"
            if len(msg) > 1950:
                for chunk in [msg[i:i+1950] for i in range(0, len(msg), 1950)]:
                    await channel.send(chunk)
            else:
                await channel.send(msg)
            
        # 5. Update the catch-up tracker so the bot knows it completed today's sweep
        update_last_run("run_business_portfolio_sweep")
        print("✅ Briefing complete and data ingested.")
        
    except Exception as e:
        print(f"❌ Briefing Execution Error: {str(e)}")
        await log_error_to_discord(f"run_daily_briefing:\n{str(e)}")

# Institutional research imports & constants



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
    body = ""
    
    # Walk through the email parts recursively
    for part in msg.walk():
        content_type = part.get_content_type()
        disposition = str(part.get("Content-Disposition"))

        # Skip attachments
        if "attachment" in disposition:
            continue

        # Prioritize HTML: institutional newsletters store the actual data here
        if content_type == "text/html":
            raw_html = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')
            soup = BeautifulSoup(raw_html, 'html.parser')
            # Extract text and preserve paragraph spacing
            body = soup.get_text(separator='\n\n', strip=True)
            break  # Exit loop once the rich HTML is found and parsed
            
        # Fallback to plain text ONLY if HTML hasn't been found yet
        elif content_type == "text/plain" and not body:
            body = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')

    return body

from email_scraper import run_email_sweep

_booted = False

@bot.event
async def on_ready():
    global _booted
    if not _booted:
        print(f"✅ Bot successfully connected to Discord as {bot.user} (ID: {bot.user.id})")
        await bot.tree.sync()
        
        # Boot Sequence: Execute ONLY once
        await update_psx_tickers()
        from agent import update_kmi30_tickers
        await update_kmi30_tickers()
        
        bot.add_view(ApprovalView())
        _booted = True
    else:
        print("⚠️ Bot reconnected to Discord WebSocket. Skipping redundant boot sequence.")
    


    bot.loop.create_task(start_scheduled_sweeps())


    print("Automated briefing schedule initialized.", flush=True)

    if not market_watchdog.is_running():
        market_watchdog.start()
    if not dividend_watchdog.is_running():
        dividend_watchdog.start()
    if not check_price_targets.is_running():
        check_price_targets.start()
        print("✅ Background price check loop started successfully.")
    if not passive_opportunity_scanner.is_running():
        passive_opportunity_scanner.start()
        print("✅ Autonomous passive opportunity scanner started successfully.")
    if not psx_filing_watchdog.is_running():
        psx_filing_watchdog.start()
        print("✅ Autonomous PSX filing watchdog started successfully.")
    if not system_heartbeat.is_running():
        system_heartbeat.start()
        print("✅ System heartbeat started successfully.")

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
    channel_id = PROSPECT_CHANNEL_ID
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

import time
import psutil

BOT_START_TIME = time.time()

async def get_whatsapp_bridge_status():
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            # The bridge is running locally on port 3000
            async with session.get("http://127.0.0.1:3000/api/trigger-sweep", timeout=5) as response:
                return "🟢 WhatsApp Bridge: Online"
    except Exception:
        return "🔴 WhatsApp Bridge: Offline/Unreachable"

@tasks.loop(hours=12)
async def system_heartbeat():
    try:
        import os
        channel_id = os.getenv("ADMIN_CHANNEL_ID")
        if not channel_id:
            return
            
        channel = bot.get_channel(int(channel_id))
        if not channel:
            return
            
        uptime_hours = round((time.time() - BOT_START_TIME) / 3600, 2)
        
        try:
            mem = psutil.virtual_memory()
            mem_str = f"{mem.percent}%"
        except Exception:
            mem_str = "OK"
            
        bridge_status = await get_whatsapp_bridge_status()
        msg = f"🟢 [SYSTEM HEARTBEAT] Dedicated Node Online. Memory {mem_str}. Uptime: {uptime_hours} hours.\n{bridge_status}"
        await channel.send(msg)
    except Exception as e:
        print(f"Heartbeat Error: {e}")


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


from discord import app_commands

class PnlPaginationView(discord.ui.View):
    def __init__(self, user_id: str, data: list, macro_stats: dict):
        super().__init__(timeout=300)
        self.user_id = user_id
        self.data = data
        self.macro_stats = macro_stats
        self.current_page = 0
        self.max_page = max(0, (len(data) - 1) // 5)
        self.update_buttons()

    def update_buttons(self):
        self.prev_btn.disabled = self.current_page == 0
        self.next_btn.disabled = self.current_page == self.max_page

    def generate_embed(self) -> discord.Embed:
        gross = self.macro_stats['gross']
        cgt = self.macro_stats['cgt']
        net = self.macro_stats['net']
        
        embed = discord.Embed(
            title="📊 Realized PnL & Tax Ledger",
            color=0x1E1E24,
            description=f"**Gross Realized PnL:** Rs.{gross:,.2f}\n**Total CGT Deducted:** Rs.{cgt:,.2f}\n**Net Realized Gain:** Rs.{net:,.2f}"
        )
        
        start_idx = self.current_page * 5
        end_idx = start_idx + 5
        page_data = self.data[start_idx:end_idx]
        
        for row in page_data:
            ticker = row['ticker']
            qty = float(row['sell_quantity'])
            price = float(row['sell_price'])
            net_pnl = float(row['net_realized_pnl'])
            date = row['sell_date'].strftime('%Y-%m-%d')
            
            pnl_sign = "+" if net_pnl >= 0 else ""
            embed.add_field(
                name=f"{ticker} | {date}",
                value=f"Sold {qty:,.0f} @ Rs.{price:,.2f}\nNet PnL: {pnl_sign}Rs.{net_pnl:,.2f}",
                inline=False
            )
            
        embed.set_footer(text=f"Page {self.current_page + 1} of {self.max_page + 1}")
        return embed

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary, custom_id="prev_btn")
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != self.user_id:
            return await interaction.response.send_message("You cannot control this pagination.", ephemeral=True)
            
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.generate_embed(), view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.primary, custom_id="next_btn")
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != self.user_id:
            return await interaction.response.send_message("You cannot control this pagination.", ephemeral=True)
            
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.generate_embed(), view=self)

@bot.tree.command(name="realized_pnl", description="View your realized PnL and CGT tax deductions.")
async def realized_pnl(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    
    conn = DB_POOL.getconn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT COALESCE(SUM(gross_realized_pnl), 0) as gross,
                       COALESCE(SUM(cgt_deducted), 0) as cgt,
                       COALESCE(SUM(net_realized_pnl), 0) as net
                FROM realized_cgt_ledger
                WHERE discord_user_id = %s
            """, (user_id,))
            macro_row = cur.fetchone()
            macro_stats = {
                'gross': float(macro_row['gross']),
                'cgt': float(macro_row['cgt']),
                'net': float(macro_row['net'])
            }
            
            cur.execute("""
                SELECT ticker, sell_date, sell_quantity, sell_price, net_realized_pnl
                FROM realized_cgt_ledger
                WHERE discord_user_id = %s
                ORDER BY sell_date DESC
            """, (user_id,))
            
            data = cur.fetchall()
            
            if not data:
                return await interaction.response.send_message("You have no realized trades in your ledger yet.", ephemeral=True)
                
            view = PnlPaginationView(user_id, data, macro_stats)
            await interaction.response.send_message(embed=view.generate_embed(), view=view)
            
    except Exception as e:
        await interaction.response.send_message(f"🚨 Error generating report: {e}", ephemeral=True)
    finally:
        DB_POOL.putconn(conn)
