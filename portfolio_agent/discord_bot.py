import discord
from discord.ext import commands, tasks
import datetime
import os
import requests
import aiohttp
import traceback
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

    # Allow bot commands to bypass the LangGraph agent
    await bot.process_commands(message)

    if not message.content.startswith(bot.command_prefix):
        # Entry point debug log (forces immediate output in Render console)
        print(f"📩 [DISCORD EVENT] Received message from {message.author}: '{message.content}'", flush=True)

        async with message.channel.typing():
            try:
                # Map Discord Channel ID to PostgreSQL Checkpointer thread_id
                thread_id = str(message.channel.id)
                config = {"configurable": {"thread_id": thread_id}}
                
                print(f"🔄 Invoking LangGraph agent for thread_id: {thread_id}...", flush=True)
                
                inputs = {"messages": [("user", message.content)]}
                
                import asyncio
                # Execute state machine asynchronously via to_thread (since checkpointer is sync)
                response = await asyncio.to_thread(agent_app.invoke, inputs, config)
                
                # Extract the last AI content message generated by the model
                raw_content = response["messages"][-1].content

                # Handle cases where LangChain returns a list of dictionaries
                if isinstance(raw_content, list):
                    final_reply = "".join(
                        block.get("text", "") for block in raw_content 
                        if isinstance(block, dict) and block.get("type") == "text"
                    )
                else:
                    final_reply = str(raw_content)
                print(f"📤 Agent response generated successfully. Sending to Discord...", flush=True)
                
                # Discord imposes a strict 2000-character limit per message
                chunk_size = 1900 # 100-character buffer for safety
                for i in range(0, len(final_reply), chunk_size):
                    await message.channel.send(final_reply[i:i + chunk_size])

            except Exception as e:
                # This catches any graph-level crash and tells you immediately in Discord
                await message.channel.send(f"⚠️ **I ran into a system error processing that:**\n`{str(e)}`")

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
