import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import asyncio
import urllib.request
import xml.etree.ElementTree as ET
import json
from portfolio_manager import get_checkpoint, update_checkpoint, add_pending_signal
# Import your new discord brief function
from portfolio_manager import send_learning_brief 

def fetch_psx_news():
    """Pulls top financial news via limitless RSS."""
    print("🔄 [News Pipeline] Scanning macro news via RSS...")
    
    checkpoint_raw = get_checkpoint("last_news_title")
    last_news_title = None
    if checkpoint_raw:
        try:
            parsed = json.loads(checkpoint_raw) if isinstance(checkpoint_raw, str) else checkpoint_raw
            last_news_title = parsed.get("title") if isinstance(parsed, dict) else parsed
        except Exception:
            last_news_title = checkpoint_raw
    
    # Direct RSS feed for PSX and State Bank news
    url = "https://news.google.com/rss/search?q=Pakistan+Stock+Exchange+OR+State+Bank+Pakistan+finance&hl=en-PK&gl=PK&ceid=PK:en"
    
    try:
        # Spoof a standard browser User-Agent
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            xml_data = response.read()
            
        root = ET.fromstring(xml_data)
        
        results = []
        for item in root.findall('.//item')[:5]:
            title = item.find('title').text
            link = item.find('link').text
            pub_date = item.find('pubDate').text
            results.append({
                'title': title, 
                'url': link, 
                'body': f"Published: {pub_date}", 
                'source': 'Google News RSS'
            })
            
    except Exception as e:
        print(f"❌ [News Scraper Error] Failed to fetch RSS: {e}")
        return []

    # Watermark logic
    new_articles = []
    for article in results:
        if last_news_title and article['title'] == last_news_title:
            break
        new_articles.append(article)
    
    if new_articles:
        # Ensure the title is valid JSON before writing to the database
        update_checkpoint("last_news_title", json.dumps({"title": new_articles[0]['title']}))
        
    return new_articles

# Alias to satisfy schedulers.py legacy imports
fetch_business_news = fetch_psx_news

async def run_news_sweep():
    articles = await asyncio.to_thread(fetch_psx_news)
    
    for article in articles:
        # 1. Send the educational brief to Discord
        await send_learning_brief(
            source=article['source'],
            title=article['title'],
            summary=f"{article['body']}\n\n[Read more]({article['url']})"
        )
        
        # 2. Push to Supabase for the Desk Manager to trade on
        add_pending_signal(
            source="Macro News",
            payload={"title": article['title'], "body": article['body'], "url": article['url']}
        )
    return articles

if __name__ == "__main__":
    print("🚀 [Diagnostics] Forcing manual News sweep...")
    asyncio.run(run_news_sweep())
    print("✅ [Diagnostics] Manual sweep complete.")
