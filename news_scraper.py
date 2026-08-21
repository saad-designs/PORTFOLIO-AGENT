import feedparser
from bs4 import BeautifulSoup
from datetime import datetime
from langchain_text_splitters import RecursiveCharacterTextSplitter
from vector_brain import vector_db
from tenacity import retry, wait_exponential, stop_after_attempt
import logging
import re
import asyncio

async def fetch_business_news() -> str:
    targets = ['https://www.dawn.com/feeds/home', 'https://www.brecorder.com/feeds/latest-news']
    headlines = []
    
    for url in targets:
        try:
            feed = await asyncio.to_thread(feedparser.parse, url)
            headlines.append(f"=== Source: {url} ===")
            for entry in feed.entries[:10]:
                title = entry.get('title', 'No Title')
                published = entry.get('published', 'No Date')
                headlines.append(f"[{published}] {title}")
        except Exception as e:
            headlines.append(f"Error fetching {url}: {e}")
            
    return "\n".join(headlines)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("NewsScraper")

# Hardcoded dictionary of RSS feed URLs for Pakistani financial news
RSS_FEEDS = {
    "Business Recorder": "https://www.brecorder.com/feeds/latest-news/",
    "Profit.pk": "https://profit.pakistantoday.com.pk/feed/",
    "Dawn Business": "https://www.dawn.com/feeds/business/"
}

TICKER_MAP = {
    "SYS": [r"\bSYS\b", r"(?i)Systems Limited"],
    "HUBC": [r"\bHUBC\b", r"(?i)Hub Power"],
    "LUCK": [r"\bLUCK\b", r"(?i)Lucky Cement"],
    "MARI": [r"\bMARI\b", r"(?i)Mari Petroleum"],
    "FFC": [r"\bFFC\b", r"(?i)Fauji Fertilizer"],
    "EFERT": [r"\bEFERT\b", r"(?i)Engro Fertilizers"],
    "MEBL": [r"\bMEBL\b", r"(?i)Meezan Bank"]
}

def clean_html(raw_html: str) -> str:
    """Removes HTML tags from a string."""
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, "html.parser")
    return soup.get_text(separator=" ", strip=True)

def extract_tickers(text: str) -> list[str]:
    """Matches text against predefined regex patterns and returns a list of matched tickers."""
    from utils import normalize_ticker
    matched_tickers = set()
    
    for ticker, patterns in TICKER_MAP.items():
        for pattern in patterns:
            if re.search(pattern, text):
                matched_tickers.add(normalize_ticker(ticker))
                break # Move to next ticker if a match is found
                
    return list(matched_tickers)

def scrape_feeds():
    """Loops through the feeds, extracts data, and filters by target tickers."""
    import time
    from email.utils import parsedate_to_datetime
    articles_to_ingest = []
    
    for source, url in RSS_FEEDS.items():
        logger.info(f"Parsing feed: {source}")
        feed = feedparser.parse(url)
        
        for entry in feed.entries:
            title = entry.get("title", "")
            link = entry.get("link", "")
            
            # published_parsed is a time.struct_time object, or fallback to string
            try:
                if hasattr(entry, "published"):
                    dt = parsedate_to_datetime(entry.published)
                    published_date = int(dt.timestamp())
                elif hasattr(entry, "published_parsed"):
                    published_date = int(time.mktime(entry.published_parsed))
                else:
                    published_date = int(time.time())
            except Exception as e:
                logger.warning(f"Failed to parse timestamp, using current epoch: {e}")
                published_date = int(time.time())
                
            raw_description = entry.get("description", "")
            description = clean_html(raw_description)
            
            combined_text = f"{title}\n\n{description}"
            
            matched_tickers = extract_tickers(combined_text)
            
            if matched_tickers:
                articles_to_ingest.append({
                    "title": title,
                    "description": description,
                    "link": link,
                    "published_date": published_date,
                    "source": source,
                    "tickers": matched_tickers,
                    "combined_text": combined_text
                })
                
    return articles_to_ingest

def inject_to_vector_db(articles: list[dict]):
    """Chunks articles and inserts them into ChromaDB."""
    if not articles:
        logger.info("No articles matched our target tickers. Skipping ingestion.")
        return
        
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
    total_chunks = 0
    
    for article in articles:
        chunks = text_splitter.split_text(article["combined_text"])
        if not chunks:
            continue
            
        metadatas = [
            {
                "source": "rss", 
                "feed_source": article["source"],
                "timestamp": article["published_date"], 
                "tickers": ",".join(article["tickers"]),
                "link": article["link"]
            }
            for _ in chunks
        ]
        
        @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
        def _add_texts():
            vector_db.add_texts(texts=chunks, metadatas=metadatas)
            
        try:
            _add_texts()
            total_chunks += len(chunks)
            logger.info(f"Ingested article: '{article['title']}' | Tickers: {article['tickers']}")
        except Exception as e:
            logger.error(f"Failed to ingest article '{article['title']}': {e}")
            
    logger.info(f"Successfully ingested {total_chunks} chunks into Vector DB.")

def run_manual_news_sweep():
    """Manual trigger to execute the news scraping and vector ingestion pipeline."""
    logger.info("Starting manual news sweep...")
    try:
        articles = scrape_feeds()
        logger.info(f"Found {len(articles)} relevant articles across feeds.")
        inject_to_vector_db(articles)
        logger.info("Manual news sweep completed.")
    except Exception as e:
        logger.error(f"Error during manual news sweep: {e}")

if __name__ == "__main__":
    run_manual_news_sweep()
