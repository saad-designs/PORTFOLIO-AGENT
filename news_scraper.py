import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import asyncio
import time
import requests
import feedparser
import xml.etree.ElementTree as ET
import json
import re
from datetime import datetime
from portfolio_manager import get_checkpoint, update_checkpoint, add_pending_signal, send_learning_brief

PRIMARY_RSS_FEEDS = [
    {
        "name": "Google News PSX",
        "url": "https://news.google.com/rss/search?q=PSX+OR+%22Pakistan+Stock+Exchange%22+OR+KSE100+when:2d&hl=en-PK&gl=PK&ceid=PK:en"
    },
    {
        "name": "Business Recorder",
        "url": "https://www.brecorder.com/feeds/latest-news"
    },
    {
        "name": "Profit Pakistan Today",
        "url": "https://profit.pakistantoday.com.pk/feed/"
    },
    {
        "name": "Dawn Business",
        "url": "https://www.dawn.com/feeds/business/"
    }
]

def search_duckduckgo_fallback(query: str, max_results: int = 5) -> list[dict]:
    """
    Cloud-Safe DuckDuckGo fallback with a strict 8-second timeout.
    Catches 202 Ratelimits and network errors immediately without blocking the pipeline.
    """
    try:
        from duckduckgo_search import DDGS
        with DDGS(timeout=8) as ddgs:
            results = list(ddgs.news(query, region="pk-en", max_results=max_results))
            clean_results = []
            for r in results:
                clean_results.append({
                    "title": r.get("title", ""),
                    "url": r.get("url") or r.get("link", ""),
                    "body": r.get("body", "")[:300],
                    "source": "DuckDuckGo News"
                })
            return clean_results
    except Exception as ddg_err:
        print(f"ℹ️ [DuckDuckGo Fallback] DDG rate-limited or unavailable on datacenter IP: {ddg_err}. Skipping.")
        return []

def fetch_psx_news(max_articles: int = 10) -> list[dict]:
    """
    Pulls top macro & PSX financial news via direct Pakistani RSS feeds.
    Cloud-Safe: Completely immune to datacenter IP blocks.
    """
    print("🔄 [News Pipeline] Scanning macro news via Pakistani RSS feeds & Google News...")
    
    checkpoint_raw = get_checkpoint("last_news_title")
    last_news_title = None
    if checkpoint_raw:
        try:
            parsed = json.loads(checkpoint_raw) if isinstance(checkpoint_raw, str) else checkpoint_raw
            last_news_title = parsed.get("title") if isinstance(parsed, dict) else parsed
        except Exception:
            last_news_title = checkpoint_raw

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.8'
    }

    discovered = []
    seen_titles = set()

    for feed_info in PRIMARY_RSS_FEEDS:
        feed_name = feed_info["name"]
        feed_url = feed_info["url"]
        try:
            resp = requests.get(feed_url, headers=headers, timeout=8)
            if resp.status_code == 200 and resp.content:
                parsed = feedparser.parse(resp.content)
                for entry in parsed.entries[:6]:
                    title = getattr(entry, "title", "").strip()
                    if not title or title in seen_titles:
                        continue
                    seen_titles.add(title)
                    link = getattr(entry, "link", "")
                    pub_date = getattr(entry, "published", "") or getattr(entry, "updated", "")
                    summary = getattr(entry, "summary", "")
                    # Strip any HTML tags from summary
                    clean_summary = re.sub(r'<[^>]+>', '', summary).strip()
                    discovered.append({
                        "title": title,
                        "url": link,
                        "body": clean_summary[:350] or f"Published: {pub_date}",
                        "source": feed_name,
                        "published_at": pub_date
                    })
        except Exception as feed_err:
            print(f"⚠️ [News Pipeline] Failed fetching {feed_name}: {feed_err}")

    # If all RSS feeds returned empty, attempt quick DDG fallback with 8s cap
    if not discovered:
        print("ℹ️ [News Pipeline] RSS feeds returned 0 articles. Attempting 8s DuckDuckGo fallback...")
        discovered = search_duckduckgo_fallback("PSX Pakistan Stock Exchange KSE100", max_results=max_articles)

    # Watermark logic to only yield un-processed new articles
    new_articles = []
    for article in discovered:
        if last_news_title and article['title'] == last_news_title:
            break
        new_articles.append(article)
        if len(new_articles) >= max_articles:
            break

    if new_articles:
        update_checkpoint("last_news_title", json.dumps({"title": new_articles[0]['title']}))
        print(f"✅ [News Pipeline] Discovered {len(new_articles)} fresh macro news stories.")
    else:
        print("ℹ️ [News Pipeline] No new articles since last sweep watermark.")
        
    return new_articles

# Legacy alias
fetch_business_news = fetch_psx_news

async def run_news_sweep():
    articles = await asyncio.to_thread(fetch_psx_news)
    
    for article in articles:
        await send_learning_brief(
            source=article['source'],
            title=article['title'],
            summary=f"{article['body']}\n\n[Read more]({article['url']})"
        )
        add_pending_signal(
            source="Macro News",
            payload={"title": article['title'], "body": article['body'], "url": article['url']}
        )
    return articles

if __name__ == "__main__":
    print("🚀 [Diagnostics] Forcing manual News sweep...")
    asyncio.run(run_news_sweep())
    print("✅ [Diagnostics] Manual sweep complete.")
