import os
import re
import feedparser
from youtube_scraper import get_transcript
import logging
from bs4 import BeautifulSoup
from langchain_google_genai import GoogleGenerativeAIEmbeddings
import psycopg

logger = logging.getLogger("YouTubeScraper")

SWEEP_FORMAT_PROMPT = """
You are my veteran PSX co-investor and portfolio partner. We currently hold: SYS, HUBC, LUCK, FFC, EFERT, MEBL, and MARI.
You just reviewed this market update from: {mentor_name}.

Do not give me a generic summary, corporate jargon, or robotic lists. Speak to me directly and candidly like a real investment partner.
Tell me exactly what this means for OUR money and OUR specific holdings. 

Crucially: If the content is purely educational (e.g., a new valuation metric, psychological discipline, or market framework), DO NOT dismiss it. Break the lesson down clearly so we both learn from it, and explain how we will apply this new thinking to our strategy moving forward.

Format your response exactly like this:

**Partner's Verdict: {mentor_name}**
Write a sharp, 2-3 sentence candid takeaway. Are they spotting a real catalyst, warning of a macro risk, or teaching us a valuable investment principle? If it's educational, summarize the core lesson simply and sharply.

**Portfolio Action & Application**
Name ONLY our specific holdings that are affected and state our stance. 
If none of our stocks are directly impacted, but the video is educational, tell me exactly how we are going to apply this new rule, method, or mindset to our future trades and market analysis. 

EXTRACTED TICKERS (Strict format):
!target TICKER FAIR_VALUE STOP_LOSS (If no specific stock tickers and numeric targets are mentioned in the text, you must output exactly 'None'. Do not invent placeholder zeros. ONLY extract if it directly impacts our watchlist.)

EXTRACTED TACTICAL RULE (Strict format):
!rule [Extract exactly one timeless quantitative, educational, or risk-management principle from this update to store in our active playbook for future evaluation. This is how our system remembers the lesson.]
"""

def save_learned_rule(source: str, rule: str):
    import psycopg2
    import os
    from google import genai
    
    try:
        client = genai.Client()
        
        # Generate the 768-dimensional embedding for the rule
        result = client.models.embed_content(
            model='gemini-embedding-001',
            contents=rule,
            config={'output_dimensionality': 768}
        )
        embedding = result.embeddings[0].values

        # Persist the rule and its embedding to Supabase
        conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO active_playbook (source, rule, embedding, created_at) 
            VALUES (%s, %s, %s::vector, NOW())
            """,
            (source, rule, embedding)
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
            import asyncio
            text = asyncio.run(get_transcript(video_id))
            if text:
                insert_knowledge("youtube", url, text)
        except Exception as e:
            print(f"Could not fetch transcript for {video_id}: {e}")

def extract_video_id(url: str) -> str:
    """Extracts YouTube video ID from standard, shortened, or live URLs."""
    pattern = r"(?:v=|\/|youtu\.be\/|embed\/|shorts\/|live\/)([0-9A-Za-z_-]{11})"
    match = re.search(pattern, url)
    return match.group(1) if match else ""

def get_youtube_transcript(video_url: str) -> str:
    """Extracts timestamp-free transcript text."""
    try:
        video_id = extract_video_id(video_url)
        if not video_id:
            logger.error(f"Invalid YouTube URL format: {video_url}")
            return ""

        import asyncio
        transcript = asyncio.run(get_transcript(video_id))
        return transcript or ""

    except Exception as e:
        logger.warning(f"Audio extraction failed for {video_url}: {str(e)}")
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

import uuid
import asyncio
from google import genai
from google.genai import types

async def generate_with_fallback(client: genai.Client, contents: list) -> str:
    """
    Executes Gemini generation prioritizing the most efficient 3.8 architecture.
    Cascades down to legacy 3.5 models instantly if a 429 quota wall is hit.
    """
    models_to_try = [
        "gemini-1.5-flash",       # Burns your 20 high-tier free requests first
        "gemini-1.5-flash-8b",    # Takes over the remaining thousands of background sweeps
        "gemini-1.5-flash"        # Final safety net
    ]
    
    for model_name in models_to_try:
        try:
            chat = client.chats.create(model=model_name)
            response = await asyncio.to_thread(
                chat.send_message,
                contents
            )
            return response.text.strip()
            
        except Exception as e:
            error_str = str(e).lower()
            # Now catches Rate Limits (429), Missing Models (404), and Server Overloads (503/500)
            if any(x in error_str for x in ["429", "quota", "resource_exhausted", "404", "not_found", "503", "unavailable", "500"]):
                print(f"⚠️ [Fallback] {model_name} unavailable. Cascading down...")
                continue 
            else:
                raise e
                
    raise Exception("❌ CRITICAL: All 3.8 and 3.5 tier background models have exhausted their rate limits.")

WHATSAPP_PARTNER_PROMPT = """
You are my veteran PSX co-investor and portfolio partner. We currently hold: SYS, HUBC, LUCK, FFC, EFERT, MEBL, and MARI.
You just intercepted this research chart, graphic, or PDF document from the KTrade WhatsApp channel. 
Caption provided: {caption}
Source: {source}

Do not give me a generic summary, corporate jargon, or robotic lists. Speak to me directly and candidly.
Filter out all the retail noise and tell me exactly what this data means for OUR money and OUR specific holdings. 
If it's an educational graphic or framework, break down the lesson so we both internalize it.

Format your response exactly like this:

**Partner's Verdict: WhatsApp ({source})**
Write a sharp, 2-3 candid sentences on what this chart or document actually signals for valuations, margins, or market sentiment. 

**Portfolio Action & Application**
Name ONLY our specific holdings affected and state our stance. If none of our stocks are directly impacted, explain how we apply this new data or rule to our future trades.

EXTRACTED TICKERS (Strict format):
!target TICKER FAIR_VALUE STOP_LOSS (If no specific stock tickers and numeric targets are mentioned in the text, you must output exactly 'None'. Do not invent placeholder zeros. ONLY extract if it directly impacts our watchlist.)

EXTRACTED TACTICAL RULE (Strict format):
!rule [Extract exactly one timeless quantitative or risk-management principle from this asset to store in our active playbook vector database.]

<SENTIMENT_MAPPING>
{"TICKER": "BULLISH/BEARISH/NEUTRAL"}
</SENTIMENT_MAPPING>
"""

async def process_whatsapp_media_payload(file_bytes: bytes, filename: str, mimetype: str, caption: str, source: str):
    """Processes uploaded media, sends it to Gemini Multi-Modal, extracts !rule, and posts to Discord."""
    temp_filename = f"temp_wa_{uuid.uuid4().hex[:8]}_{filename}"
    
    with open(temp_filename, "wb") as f:
        f.write(file_bytes)

    try:
        # Initialize Gemini Client
        client = genai.Client()
        
        # Upload file for multimodal processing
        uploaded_file = await asyncio.to_thread(
            client.files.upload,
            file=temp_filename,
            config=types.UploadFileConfig(mime_type=mimetype)
        )

        prompt = WHATSAPP_PARTNER_PROMPT.format(caption=caption or "N/A", source=source or "KTrade WhatsApp")

        analysis_text = await generate_with_fallback(client, [uploaded_file, prompt])

        # Extract !rule and persist to active_playbook
        if "!rule " in analysis_text:
            try:
                rule_match = re.search(r"!rule\s+(.*)", analysis_text)
                if rule_match:
                    rule_content = rule_match.group(1).strip()
                    save_learned_rule(f"WhatsApp: {source}", rule_content)
                    print(f"🧠 [Active Playbook] Saved rule from {source}: {rule_content}")
            except Exception as e:
                print(f"⚠️ [Active Playbook] Failed to save rule: {e}")

        import discord
        
        # Clean the text for Discord (Remove SENTIMENT_MAPPING and any EXTRACTED blocks if they exist)
        discord_friendly_message = re.sub(r'<SENTIMENT_MAPPING>.*?</SENTIMENT_MAPPING>', '', analysis_text, flags=re.DOTALL).strip()
        if "EXTRACTED TICKERS (Strict format):" in discord_friendly_message:
            discord_friendly_message = discord_friendly_message.split("EXTRACTED TICKERS (Strict format):")[0].strip()

        # Extract Sentiment Mapping and persist to institutional_reports
        match = re.search(r"<SENTIMENT_MAPPING>\s*(\{.*?\})\s*</SENTIMENT_MAPPING>", analysis_text, re.DOTALL)
        if match:
            try:
                import json
                from portfolio_manager import get_db_connection
                sentiment_mapping = json.loads(match.group(1).strip())
                summary_text = discord_friendly_message
                
                conn = get_db_connection()
                cur = conn.cursor()
                for tick, sent in sentiment_mapping.items():
                    cur.execute("""
                        INSERT INTO institutional_reports (source, ticker, subject, sentiment, summary)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (f"WhatsApp: {source}", tick, caption or "Media Payload", sent, summary_text))
                conn.commit()
                cur.close()
                conn.close()
                print(f"📥 Saved WhatsApp report sentiments for {list(sentiment_mapping.keys())}")
            except Exception as e:
                print(f"⚠️ Failed to parse/save WhatsApp sentiment mapping: {e}")

        # Post clean summary to Discord research channel
        from discord_bot import bot, RESEARCH_CHANNEL_ID
        channel = bot.get_channel(RESEARCH_CHANNEL_ID)
        if channel:
            embed = discord.Embed(
                title=f"📱 Automated Research Ingestion: {source}",
                description=discord_friendly_message[:4096],
                color=3447003
            )
            await channel.send(embed=embed)

        return {"status": "success", "summary": analysis_text}

    finally:
        # Windows-safe file cleanup with retries
        for _ in range(3):
            try:
                if os.path.exists(temp_filename):
                    os.remove(temp_filename)
                break
            except PermissionError:
                await asyncio.sleep(1)
