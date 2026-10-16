import requests
import json
import datetime
import os
from dotenv import load_dotenv

# Try to load webhook from environment or fallback to placeholder
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "YOUR_DISCORD_WEBHOOK_URL_HERE")
USER_ID = os.getenv("DISCORD_ADMIN_USER_ID", "YOUR_DISCORD_USER_ID")

def send_macro_reminder():
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    
    payload = {
        "content": f"<@{USER_ID}> ⚠️ **ACTION REQUIRED: Update Macro Risk Config**",
        "embeds": [
            {
                "title": "🏦 Monthly SBP & Inflation Update",
                "description": "It is time to update the macroeconomic hurdle rates for the quantitative engine. The bot relies on these to calculate the real yield of dividend assets.",
                "color": 16711680, # Red
                "fields": [
                    {
                        "name": "Step 1: Check the State Bank of Pakistan Rate",
                        "value": "Verify the current risk-free policy rate.\n[Check SBP Rate Here](https://www.sbp.org.pk/)",
                        "inline": False
                    },
                    {
                        "name": "Step 2: Check the Consumer Price Index (CPI)",
                        "value": "Verify the latest YoY inflation data from the Pakistan Bureau of Statistics.\n[Check CPI Here](https://www.pbs.gov.pk/cpi)",
                        "inline": False
                    },
                    {
                        "name": "Step 3: Update `macro_config.json`",
                        "value": f"Open your project directory and update the `sbp_policy_rate` and `cpi_inflation_rate` decimals. Update the `last_updated` date to `{current_date}`.",
                        "inline": False
                    },
                    {
                        "name": "Step 4: Save & Close",
                        "value": "The risk engine will automatically parse the new rates on its next execution loop. No bot restart is required.",
                        "inline": False
                    }
                ],
                "footer": {
                    "text": "Quantitative Risk Layer • Automated Scheduled Task"
                }
            }
        ]
    }
    
    if WEBHOOK_URL == "YOUR_DISCORD_WEBHOOK_URL_HERE":
        print("[WARNING] Please update WEBHOOK_URL before running this script.")
        return

    response = requests.post(WEBHOOK_URL, json=payload)
    if response.status_code == 204:
        print("[SUCCESS] Macro reminder dispatched to Discord.")
    else:
        print(f"[ERROR] Failed to send reminder: {response.status_code} {response.text}")

if __name__ == "__main__":
    send_macro_reminder()
