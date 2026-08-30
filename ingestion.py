import os
import re
import feedparser
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import TextFormatter
import logging
from bs4 import BeautifulSoup
from langchain_google_genai import GoogleGenerativeAIEmbeddings
import psycopg

logger = logging.getLogger("YouTubeScraper")

# Strict Prompt Template for Sweep Summarization
SWEEP_FORMAT_PROMPT = """
You are a senior quantitative portfolio strategist. Analyze the provided transcript/content.
Format the output cleanly using Markdown with exact line breaks and bold labels. 

Structure your response into exactly four sections:

🎙️ **Source & Core Topic**
- [Name of Mentor / Channel] | [Core subject of discussion]

🧠 **Tactical Method / Framework Learned**
- Break down the underlying investment tactic, framework, or lesson clearly in 2–3 bullet points so the user can learn the mechanics.

💼 **Portfolio Application**
- Evaluate how this directly applies to our holdings (SYS, HUBC, LUCK, FFC, EFERT, MEBL, MARI) or our cash reserve. State clearly whether this validates holding, warns against accumulation, or signals rotation.

🎯 **What This Research Means for Us**
- 1–2 crisp, actionable sentences defining our strategic stance moving forward (e.g., "Keep cash dry until Q3 earnings confirm margin expansion" or "Disregard short-term political noise; fundamentals remain intact").

EXTRACTED TICKERS (Strict format):
!target TICKER FAIR_VALUE STOP_LOSS (Use 0 0 if not explicitly stated)

EXTRACTED TACTICAL RULE (Strict format):
!rule [Write exactly one concise, actionable sentence summarizing the core tactical lesson to be saved to the database playbook]
"""

def save_learned_rule(source: str, rule: str):
    import psycopg2
    import os
    try:
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO active_playbook (source, rule, created_at) VALUES (%s, %s, NOW())",
            (source, rule)
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"[Playbook] Successfully saved rule from {source}")
    except Exception as e:
        print(f"[Playbook] Error saving rule: {e}")

def get_embeddings_model():
    return GoogleGenerativeAIEmbeddings(
        model="models/gemini-embedding-001", 
        google_api_key=os.getenv("GEMINI_API_KEY")
    )

def chunk_text(text: str, chunk_size: int = 1000, overlap: int = 100) -> list[str]:
    """Split raw text into overlapping chunks."""
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        start += chunk_size - overlap
    return chunks

def insert_knowledge(source_type: str, source_url: str, text: str):
    """Chunk, embed, and insert into agent_knowledge if not duplicate."""
    db_url = os.getenv("SUPABASE_DB_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
    try:
        with psycopg.connect(db_url, autocommit=True) as conn:
            # Check if source_url exists to prevent duplicates
            res = conn.execute("SELECT id FROM agent_knowledge WHERE source_url = %s", (source_url,)).fetchone()
            if res:
                return # Already exists
            
            embeddings = get_embeddings_model()
            chunks = chunk_text(text)
            for chunk in chunks:
                if not chunk.strip():
                    continue
                embedding = embeddings.embed_query(chunk)
                conn.execute("""
                    INSERT INTO agent_knowledge (source_type, source_url, content, embedding)
                    VALUES (%s, %s, %s, %s)
                """, (source_type, source_url, chunk, str(embedding)))
    except Exception as e:
        print(f"Error inserting knowledge: {e}")

def sweep_rss_feeds(urls: list[str] = None):
    """Fetch macro news from RSS URLs, chunk, embed, and insert."""
    if not urls:
        urls = [
            "https://www.brecorder.com/trends/rss",
            "https://www.dawn.com/feeds/home/",
            "https://tribune.com.pk/feed/business"
        ]
        
    for url in urls:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                link = getattr(entry, 'link', url)
                title = getattr(entry, 'title', '')
                summary = getattr(entry, 'summary', '')
                content = f"{title}\n\n{summary}"
                # Extract tags and strip HTML for cleaner text if necessary
                content = re.sub(r'<[^>]+>', '', content)
                insert_knowledge("rss", link, content)
        except Exception as e:
            print(f"Could not fetch RSS feed {url}: {e}")

def sweep_youtube_transcripts(video_ids: list[str] = None):
    """Pull transcripts for video IDs, chunk, embed, and insert."""
    if not video_ids:
        # Provide some dummy video_ids or empty list if no arguments are provided
        video_ids = []
        
    for video_id in video_ids:
        url = f"https://www.youtube.com/watch?v={video_id}"
        try:
            transcript_list = YouTubeTranscriptApi.get_transcript(video_id)
            text = " ".join([t['text'] for t in transcript_list])
            insert_knowledge("youtube", url, text)
        except Exception as e:
            print(f"Could not fetch transcript for {video_id}: {e}")

def extract_video_id(url: str) -> str:
    """Extracts YouTube video ID from standard, shortened, or live URLs."""
    pattern = r"(?:v=|\/|youtu\.be\/|embed\/|shorts\/|live\/)([0-9A-Za-z_-]{11})"
    match = re.search(pattern, url)
    return match.group(1) if match else ""

def get_youtube_transcript(video_url: str) -> str:
    """Extracts timestamp-free transcript text with multi-language fallback."""
    try:
        video_id = extract_video_id(video_url)
        if not video_id:
            logger.error(f"Invalid YouTube URL format: {video_url}")
            return ""

        import os
        
        # Check if cookies file exists and pass it to bypass Cloud IP bans
        cookie_path = "cookies.txt"
        if os.path.exists(cookie_path):
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id, cookies=cookie_path)
        else:
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        
        try:
            # Try finding manually created or auto-generated transcripts in target languages
            transcript = transcript_list.find_transcript(['ur', 'en', 'hi', 'en-US'])
        except Exception:
            # Fall back to whatever primary transcript is available and translate if needed
            transcript = next(iter(transcript_list))

        raw_data = transcript.fetch()
        formatter = TextFormatter()
        clean_text = formatter.format_transcript(raw_data)
        
        return clean_text

    except Exception as e:
        logger.warning(f"Fast transcript extraction failed for {video_url}: {str(e)}")
        logger.info(f"Triggering yt-dlp fallback for {video_id}...")
        try:
            from youtube_scraper import get_transcript_fallback
            fallback_text = get_transcript_fallback(video_id)
            if fallback_text:
                return fallback_text
        except Exception as fallback_e:
            logger.error(f"Fallback extraction failed for {video_id}: {str(fallback_e)}")
            
        return ""

def extract_email_body(msg) -> str:
    """Extracts email body, prioritizing HTML payloads in multipart MIME emails."""
    body = ""
    
    # Walk through the email parts recursively
    for part in msg.walk():
        content_type = part.get_content_type()
        disposition = str(part.get("Content-Disposition"))

        # Skip attachments
        if "attachment" in disposition:
            continue

        # Prioritize HTML: institutional newsletters store the actual data here
        if content_type == "text/html":
            raw_html = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')
            soup = BeautifulSoup(raw_html, 'html.parser')
            # Extract text and preserve paragraph spacing
            body = soup.get_text(separator='\n\n', strip=True)
            break  # Exit loop once the rich HTML is found and parsed
            
        # Fallback to plain text ONLY if HTML hasn't been found yet
        elif content_type == "text/plain" and not body:
            body = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')

    return body
