import sys
import re

with open("discord_bot.py", "r", encoding="utf-8") as f:
    content = f.read()

# Add await bot.tree.sync() in on_ready if not there
if "bot.tree.sync()" not in content:
    pattern_ready = re.compile(r"(async def on_ready\(\):.*?print\(f\"✅ Bot successfully connected.*?\))", re.DOTALL)
    content = pattern_ready.sub(r"\1\n    await bot.tree.sync()", content)

# Append the slash command and the view class at the end of the file
slash_command_code = '''

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
            description=f"**Gross Realized PnL:** Rs.{gross:,.2f}\\n**Total CGT Deducted:** Rs.{cgt:,.2f}\\n**Net Realized Gain:** Rs.{net:,.2f}"
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
                value=f"Sold {qty:,.0f} @ Rs.{price:,.2f}\\nNet PnL: {pnl_sign}Rs.{net_pnl:,.2f}",
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
'''

# We need to make sure we don't append it multiple times
if "class PnlPaginationView" not in content:
    content += slash_command_code

with open("discord_bot.py", "w", encoding="utf-8") as f:
    f.write(content)

print("discord_bot.py patched with realized_pnl command")
