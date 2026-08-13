import discord
from discord.ext import commands, tasks
import datetime
import os
import re
import requests
import aiohttp
import traceback
import base64
from langchain_core.messages import HumanMessage
from agent import graph as agent_app
from ingestion import sweep_rss_feeds, sweep_youtube_transcripts
import asyncio

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Ensure DISCORD_CHANNEL_ID is loaded from the environment
CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))

# Set run times in UTC to align with 09:45 AM and 03:45 PM PKT
market_times = [
    datetime.time(hour=4, minute=45, tzinfo=datetime.timezone.utc),
    datetime.time(hour=10, minute=45, tzinfo=datetime.timezone.utc)
]

@tasks.loop(time=market_times)
async def daily_market_briefing():
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        return
        
    # Prompt the agent to utilize its live tools
    inputs = {"messages": [("user", "Fetch the live market data for the KSE-100 benchmark. Summarize the current PSX market state, key drivers, and volume. Keep it strictly concise.")]}
    
    # Use a dedicated thread ID for background tasks to prevent polluting your chat memory
    config = {"configurable": {"thread_id": "automated_daily_briefing_thread"}}
    
    try:
        response = await asyncio.to_thread(agent_app.invoke, inputs, config)
        
        raw_content = response["messages"][-1].content
        if isinstance(raw_content, list):
            final_reply = "".join(
                block.get("text", "") for block in raw_content 
                if isinstance(block, dict) and block.get("type") == "text"
            )
        else:
            final_reply = str(raw_content)
            
        await channel.send(f"📊 **Automated PSX Market Briefing**\n\n{final_reply}")
    except Exception as e:
        await channel.send(f"⚠️ **Briefing Execution Error:**\n`{str(e)}`")

class ApprovalView(discord.ui.View):
    def __init__(self, thread_id: str = None):
        super().__init__(timeout=None)
        self.thread_id = thread_id

    @discord.ui.button(label="Approve Trade", style=discord.ButtonStyle.green, custom_id="approve_trade")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        t_id = self.thread_id or (interaction.message.embeds[0].footer.text if interaction.message.embeds else "unknown")
        api_base_url = os.getenv("API_URL", "http://127.0.0.1:8000")
        webhook_url = f"{api_base_url}/webhook/discord"
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
        api_base_url = os.getenv("API_URL", "http://127.0.0.1:8000")
        webhook_url = f"{api_base_url}/webhook/discord"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json={"thread_id": t_id, "action": "Denied"}) as resp:
                    pass
            await interaction.followup.send("Trade Denied! Recalculating...", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"Error resuming agent: {e}", ephemeral=True)

async def send_alert(thread_id: str, message: str):
    channel_id = int(os.getenv("DISCORD_CHANNEL_ID"))
    
    try:
        channel = await bot.fetch_channel(channel_id)
    except Exception as e:
        print(f"Error fetching channel: {e}")
        return
            
    if channel:
        embed = discord.Embed(title="Portfolio Action Required", description=message, color=discord.Color.blue())
        embed.set_footer(text=thread_id)
        await channel.send(embed=embed, view=ApprovalView(thread_id=thread_id))

@bot.event
async def on_ready():
    bot.add_view(ApprovalView())
    daily_market_briefing.start()
    print(f"✅ Bot successfully connected to Discord as {bot.user} (ID: {bot.user.id})", flush=True)
    print("Automated briefing schedule initialized.", flush=True)

@bot.command(name="ingest")
async def force_ingest(ctx):
    await ctx.send("⚙️ Initializing manual data sweep. Fetching RSS feeds and generating vectors. This will take a moment...")
    try:
        # Run the synchronous ingestion sweep in an async thread to prevent blocking
        await asyncio.to_thread(sweep_rss_feeds)
        await ctx.send("✅ RSS Ingestion complete. Vectors stored in Supabase.")
    except Exception as e:
        await ctx.send(f"⚠️ Ingestion pipeline failed: {str(e)}")

@bot.event
async def on_message(message):
    # Ignore messages sent by the bot itself
    if message.author == bot.user:
        return

    try:
        # Allow bot commands to bypass the LangGraph agent
        await bot.process_commands(message)

        if not message.content.startswith(bot.command_prefix):
            # Entry point debug log (forces immediate output in Render console)
            print(f"📩 [DISCORD EVENT] Received message from {message.author}: '{message.content}'", flush=True)

            async with message.channel.typing():
                try:
                    # Map Discord Channel ID to PostgreSQL Checkpointer thread_id
                    thread_id = str(message.channel.id)
                    author_id = str(message.author.id)
                    config = {"configurable": {"thread_id": thread_id, "user_id": author_id}}
                    
                    print(f"🔄 Invoking LangGraph agent for thread_id: {thread_id}...", flush=True)
                    
                    content_payload = []

                    # 1. Add text prompt if provided
                    if message.content:
                        content_payload.append({"type": "text", "text": message.content})

                    # 2. Extract image attachments and convert to Base64
                    for attachment in message.attachments:
                        if attachment.content_type and attachment.content_type.startswith("image/"):
                            image_bytes = await attachment.read()
                            base64_image = base64.b64encode(image_bytes).decode("utf-8")
                            content_payload.append({
                                "type": "image_url",
                                "image_url": {"url": f"data:{attachment.content_type};base64,{base64_image}"}
                            })

                    # Return early if message contains neither text nor images
                    if not content_payload:
                        return

                    # 3. If image present without text, add default instruction
                    if message.attachments and not message.content:
                        content_payload.insert(0, {"type": "text", "text": "Extract and process the text/data from this image."})

                    # Pass structured input array or text string to the agent
                    prompt_input = content_payload if message.attachments else message.content

                    inputs = {"messages": [HumanMessage(content=prompt_input)]}
                    
                    import asyncio
                    # Execute state machine asynchronously via to_thread (since checkpointer is sync)
                    response = await asyncio.to_thread(agent_app.invoke, inputs, config)
                    
                    # 1. Parse the LLM's raw output
                    raw_content = response["messages"][-1].content
                    if isinstance(raw_content, list):
                        final_reply = "".join(
                            block.get("text", "") for block in raw_content 
                            if isinstance(block, dict) and block.get("type") == "text"
                        )
                    else:
                        final_reply = str(raw_content)
            
                    # 2. Intercept the file path (catches [FILE: /tmp/...] OR Markdown ![alt](/tmp/...))
                    file_path = None
                    match = re.search(r'/tmp/[\w_.-]+\.png', final_reply)

                    if match:
                        file_path = match.group(0)
                        # Strip markdown image syntax or [FILE: ...] tags from the output text
                        final_reply = re.sub(r'(!\[.*?\]\(/tmp/[\w_.-]+\.png\)|\[FILE:\s*/tmp/[\w_.-]+\.png\])', '', final_reply).strip()
            
                    # 3. Send text in chunks
                    if final_reply:
                        chunk_size = 1900
                        for i in range(0, len(final_reply), chunk_size):
                            await message.channel.send(final_reply[i:i + chunk_size])
            
                    # 4. Upload the image and clear the temporary file
                    if file_path and os.path.exists(file_path):
                        await message.channel.send(file=discord.File(file_path))
                        os.remove(file_path)

                except Exception as e:
                    # This catches any graph-level crash and tells you immediately in Discord
                    await message.channel.send(f"⚠️ **I ran into a system error processing that:**\n`{str(e)}`")
    except Exception as e:
        # Prevent silent crashes by reporting the exact exception to Discord
        await message.channel.send(f"⚠️ **Critical Handler Error:** `{str(e)}`")

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
        now_pkt = datetime.datetime.now(tz_pkt)
        
        # Block weekends (5 = Saturday, 6 = Sunday)
        if now_pkt.weekday() >= 5:
            return 
            
        # Block outside 09:30 AM - 03:30 PM trading hours
        current_time = now_pkt.time()
        market_open = datetime.datetime.strptime("09:30", "%H:%M").time()
        market_close = datetime.datetime.strptime("15:30", "%H:%M").time()
        
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
        print(f"Watchdog Error: {str(e)}")

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

@bot.command(name="test_scraper")
async def test_scraper(ctx, ticker: str):
    await ctx.send(f"Scraping PSX portal for {ticker.upper()}...")
    
    dividend, ex_date = get_dividend_data(ticker)
    
    if dividend == 0.0 and ex_date is None:
        await ctx.send(f"❌ Scraper failed or no data found for {ticker.upper()}.")
    else:
        await ctx.send(f"✅ **{ticker.upper()} Data:**\nDividend: {dividend} PKR\nEx-Date: '{ex_date}'")


# Execute daily at 16:30 (4:30 PM) Pakistan Standard Time
tz_pkt = pytz.timezone('Asia/Karachi')
dividend_time = datetime.time(hour=16, minute=30, tzinfo=tz_pkt)

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
        today_str = datetime.datetime.now(tz_pkt).strftime('%d %b %Y')
        
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

    except Exception as e:
        print(f"Dividend Watchdog Error: {str(e)}")

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user}')
    if not market_watchdog.is_running():
        market_watchdog.start()
    if not dividend_watchdog.is_running():
        dividend_watchdog.start()

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
