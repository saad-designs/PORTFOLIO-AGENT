import discord
from discord.ext import commands
import os
import requests
import aiohttp

intents = discord.Intents.default()
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
    print(f"Logged in as {bot.user}")

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
