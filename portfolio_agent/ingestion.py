import os
import feedparser
from googleapiclient.discovery import build

def fetch_macro_rss(url: str) -> list[str]:
    """Scrape macroeconomic RSS feeds."""
    feed = feedparser.parse(url)
    return [entry.title for entry in feed.entries]

def fetch_youtube_transcripts(channel_id: str) -> list[str]:
    """Scrape mentor channels using YouTube Data API v3."""
    youtube = build('youtube', 'v3', developerKey=os.getenv('YOUTUBE_API_KEY'))
    # In a full implementation, you would extract full transcripts.
    # This is scaffolded for YouTube Data API interaction as requested.
    request = youtube.search().list(part="snippet", channelId=channel_id, maxResults=5)
    response = request.execute()
    return [item['snippet']['title'] for item in response.get('items', [])]
