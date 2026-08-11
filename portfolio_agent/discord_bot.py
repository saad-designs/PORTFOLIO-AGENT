import discord
from discord.ext import commands
import os
import requests
import aiohttp
import traceback
from langchain_core.messages import HumanMessage
from agent import graph as agent_app

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

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
    print(f"✅ Bot successfully connected to Discord as {bot.user} (ID: {bot.user.id})", flush=True)

@bot.event
async def on_message(message):
    # Ignore messages sent by the bot itself
    if message.author == bot.user:
        return

    # Entry point debug log (forces immediate output in Render console)
    print(f"📩 [DISCORD EVENT] Received message from {message.author}: '{message.content}'", flush=True)

    async with message.channel.typing():
        try:
            # Map Discord User ID to PostgreSQL Checkpointer thread_id
            thread_id = str(message.author.id)
            config = {"configurable": {"thread_id": thread_id}}
            
            print(f"🔄 Invoking LangGraph agent for thread_id: {thread_id}...", flush=True)
            
            inputs = {"messages": [HumanMessage(content=message.content)]}
            
            # Execute state machine asynchronously
            response = await agent_app.ainvoke(inputs, config=config)
            
            # Extract reply
            final_reply = response["messages"][-1].content
            print(f"📤 Agent response generated successfully. Sending to Discord...", flush=True)
            
            await message.channel.send(final_reply)

        except Exception as e:
            import traceback
            full_traceback = traceback.format_exc()
            error_msg = f"⚠️ **Agent Execution Error:** Check Render logs for the full stack trace."
            
            # Print the full traceback to Render console
            print(f"❌ [GRAPH ERROR] Full traceback:\n{full_traceback}", flush=True)
            
            await message.channel.send(error_msg)

    await bot.process_commands(message)

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
