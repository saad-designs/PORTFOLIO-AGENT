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
        client = genai.Client(http_options={"timeout": 300000})
        
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
from model_router import (
    generate_with_fallback,
    get_candidate_models,
    record_model_error,
    is_financial_or_signal_text,
    TASK_FAST_INGESTION,
    TASK_DEEP_REASONING
)

def transcribe_mentor_audio(audio_file_path: str, mime_type: str = "audio/mp4") -> str:
    """
    Transcribes downloaded YouTube audio using Gemini via Files API.
    Polls the processing state until ACTIVE (loop time.sleep(3)) and guarantees remote file deletion.
    """
    import time
    if not os.path.exists(audio_file_path):
        print(f"❌ [Audio Transcribe] Audio file not found: {audio_file_path}")
        return ""

    raw_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    client = genai.Client(api_key=raw_key.strip() if raw_key else None, http_options={"timeout": 300000})
    uploaded_file = None
    try:
        print(f"🎙️ [Audio Transcribe] Uploading {audio_file_path} to Gemini...")
        uploaded_file = client.files.upload(file=audio_file_path)
        
        # Poll client.files.get until state is ACTIVE (max 240s)
        start_time = time.time()
        while True:
            uploaded_file = client.files.get(name=uploaded_file.name)
            if uploaded_file.state.name == "ACTIVE":
                print("✅ [Audio Transcribe] File state is ACTIVE.")
                break
            elif uploaded_file.state.name == "FAILED":
                raise RuntimeError(f"Audio file processing failed on Google servers: {uploaded_file.error}")
            
            if time.time() - start_time > 240:
                raise TimeoutError("Audio file processing exceeded 240-second cap.")
            time.sleep(3)

        models_to_try = get_candidate_models(TASK_FAST_INGESTION)
        last_error = None
        for model in models_to_try:
            try:
                response = client.models.generate_content(
                    model=model,
                    contents=[
                        uploaded_file,
                        "Watch and listen to the audio of this video (which may be in Urdu, Hindi, or English). Provide a comprehensive, detailed English transcript and breakdown of everything said, capturing every PSX stock ticker, price target, support/resistance level, and macroeconomic insight."
                    ]
                )
                if response.text and response.text.strip():
                    print(f"✅ [Audio Transcribe] Transcription successful using {model}.")
                    return response.text.strip()
            except Exception as e:
                last_error = e
                record_model_error(model, e)
                print(f"⚠️ [Audio Transcribe] {model} failed: {e}")
                continue

        raise RuntimeError(f"All models failed transcription. Last error: {repr(last_error)}")
    finally:
        if uploaded_file:
            try:
                client.files.delete(name=uploaded_file.name)
                print(f"🧹 [Audio Transcribe] Deleted remote file {uploaded_file.name}")
            except Exception as del_err:
                print(f"⚠️ [Audio Transcribe] Could not delete remote file: {del_err}")

# Specialized Multimodal Prompts for KTrade WhatsApp Intelligence
WHATSAPP_IMAGE_PROMPT = """
You are an elite quantitative PSX (Pakistan Stock Exchange) technical and fundamental analyst.
Analyze this KTrade image chart, technical call card, or graphic.
Caption provided: {caption}
Source: {source}

Extract the following intelligence strictly and comprehensively:
1. PSX Ticker Symbol(s) (e.g., SYS, HUBC, LUCK, FFC, MEBL, OGDC, MARI, etc.)
2. Stance / Trade Action (BUY, ACCUMULATE, SELL, HOLD, NEUTRAL)
3. Entry / Buy Zone (exact numeric price range or entry level)
4. Target Price(s) (TP1, TP2, fundamental targets)
5. Stop-Loss Level (SL price)
6. Timeframe (Intraday, Short-term, Medium-term, Long-term)
7. Technical Indicators & Patterns (RSI, MACD, Moving Averages, Breakouts, Support/Resistance levels)
8. Strategic Partner Takeaway: A candid 2-3 sentence verdict on what this setup signals for our capital.

EXTRACTED TICKERS (Strict format):
!target TICKER FAIR_VALUE STOP_LOSS (If no specific tickers and targets, output 'None'. E.g.: !target SYS 520.0 420.0)

EXTRACTED TACTICAL RULE (Strict format):
!rule [Extract one timeless technical or risk-management rule from this setup to memorize into active playbook.]

<SENTIMENT_MAPPING>
{{"TICKER": "BULLISH/BEARISH/NEUTRAL"}}
</SENTIMENT_MAPPING>
"""

WHATSAPP_PDF_PROMPT = """
You are a senior institutional equity research analyst for the Pakistan Stock Exchange (PSX).
Analyze this KTrade institutional research report (PDF).
Caption provided: {caption}
Source: {source}

Extract the following structured intelligence:
1. Core Macroeconomic & Sector Thesis (Inflation, SBP Policy rate, FX reserves, Sector catalysts/headwinds)
2. All Covered PSX Tickers with:
   - Investment Stance (BUY, ACCUMULATE, HOLD, SELL)
   - Target Price(s) and Expected Return / Upside %
   - EPS & Dividend Estimates (current FY and forward FY)
   - Valuation Multiples (Forward P/E, P/B, Dividend Yield)
   - Core Catalysts and Downside Risks
3. Direct Portfolio Impact: Specifically highlight any impact on our core holdings (SYS, HUBC, LUCK, FFC, EFERT, MEBL, MARI).
4. Partner's Verdict: Candid institutional summary for our investment partnership.

EXTRACTED TICKERS (Strict format):
!target TICKER FAIR_VALUE STOP_LOSS (Format: !target TICKER TARGET_PRICE STOP_LOSS. If no SL given, use 0. If no tickers, output 'None'.)

EXTRACTED TACTICAL RULE (Strict format):
!rule [Extract one fundamental valuation or risk-management principle from this report.]

<SENTIMENT_MAPPING>
{{"TICKER": "BULLISH/BEARISH/NEUTRAL"}}
</SENTIMENT_MAPPING>
"""

WHATSAPP_AUDIO_PROMPT = """
You are an expert bilingual Urdu/English PSX market intelligence analyst.
Listen closely to this WhatsApp voice note / audio commentary from the KTrade analyst desk.
Caption provided: {caption}
Source: {source}

Extract and analyze the following:
1. Full Bilingual English Transcription & Translation: Comprehensive, faithful translation of the commentary spoken in Urdu/English.
2. PSX Market Intelligence:
   - Stock tickers discussed and analyst sentiment
   - Trading activity, institutional flow, foreign selling/buying mentioned
   - Any price levels, buying zones, target prices, or profit-taking warnings given
   - Key macro/catalyst triggers (SBP monetary policy, IMF review, corporate earnings)
3. Partner's Takeaway: Candid assessment of whether this is actionable desk intel or market noise.

EXTRACTED TICKERS (Strict format):
!target TICKER FAIR_VALUE STOP_LOSS (If no specific tickers and targets, output 'None')

EXTRACTED TACTICAL RULE (Strict format):
!rule [Extract one key risk or trading principle mentioned.]

<SENTIMENT_MAPPING>
{{"TICKER": "BULLISH/BEARISH/NEUTRAL"}}
</SENTIMENT_MAPPING>
"""

async def process_whatsapp_media_payload(
    file_bytes: bytes,
    filename: str,
    mimetype: str,
    caption: str = "",
    source: str = "KTrade",
    timestamp: int = None
):
    """
    Multimodal processor for KTrade WhatsApp attachments (charts, PDFs, voice notes).
    Uses google.genai.types.Part.from_bytes for direct attachment feeding, with fallback to Files API.
    Routes extracted intelligence to trade_parser, vector_brain (PGVector), active_playbook, and Discord.
    """
    import os
    import re
    import json
    import asyncio
    import uuid
    from google import genai
    from google.genai import types

    raw_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    client = genai.Client(api_key=raw_key.strip() if raw_key else None, http_options={"timeout": 300000})

    mime = (mimetype or "").lower()
    fname = (filename or "").lower()

    if mime.startswith("image/"):
        prompt_template = WHATSAPP_IMAGE_PROMPT
        media_category = "Chart / Call Card"
    elif mime == "application/pdf" or fname.endswith(".pdf"):
        prompt_template = WHATSAPP_PDF_PROMPT
        media_category = "Research Report (PDF)"
    elif mime.startswith("audio/") or any(ext in mime for ext in ["ogg", "mpeg", "mp3", "mp4", "aac", "wav"]) or any(fname.endswith(ext) for ext in [".ogg", ".mp3", ".m4a", ".wav", ".aac"]):
        prompt_template = WHATSAPP_AUDIO_PROMPT
        media_category = "Voice Note / Audio"
    else:
        prompt_template = WHATSAPP_IMAGE_PROMPT
        media_category = "Media Attachment"

    prompt_text = prompt_template.format(caption=caption or "N/A", source=source or "KTrade WhatsApp")

    analysis_text = ""
    # 1. Primary Extraction: Direct inline Part.from_bytes
    try:
        print(f"🧠 [Multimodal] Passing {filename} ({mimetype}, {len(file_bytes)} bytes) directly via Part.from_bytes to Gemini...")
        media_part = types.Part.from_bytes(data=file_bytes, mime_type=mimetype)
        contents = [media_part, prompt_text]
        analysis_text = await generate_with_fallback(client, contents, task=TASK_FAST_INGESTION)
    except Exception as inline_err:
        print(f"⚠️ [Multimodal] Inline Part.from_bytes threw ({inline_err}). Falling back to Files API upload...")
        temp_filename = f"temp_wa_{uuid.uuid4().hex[:8]}_{filename}"
        try:
            with open(temp_filename, "wb") as f:
                f.write(file_bytes)
            uploaded_file = await asyncio.to_thread(
                client.files.upload,
                file=temp_filename,
                config=types.UploadFileConfig(mime_type=mimetype)
            )
            try:
                analysis_text = await generate_with_fallback(client, [uploaded_file, prompt_text], task=TASK_FAST_INGESTION)
            finally:
                try:
                    client.files.delete(name=uploaded_file.name)
                except Exception:
                    pass
        finally:
            if os.path.exists(temp_filename):
                try:
                    os.remove(temp_filename)
                except Exception:
                    pass

    if not analysis_text:
        return {"status": "failed", "error": "Multimodal analysis returned empty text."}

    # Combine extracted multimodal intelligence with caption
    combined_intel = (
        f"📱 [KTrade WhatsApp Intel] Source: {source} | Category: {media_category} | File: {filename}\n"
        f"Caption: {caption or 'None'}\n\n"
        f"Multimodal Analysis & Extracted Intelligence:\n{analysis_text}"
    )

    # 2. Downstream Integration: Trade Signal Parser (trade_parser.py)
    try:
        from trade_parser import extract_trade_intent
        trade_intent = extract_trade_intent(combined_intel)
        if trade_intent and trade_intent.is_trade_execution:
            print(f"🎯 [WhatsApp Trade Signal Detected]: {trade_intent.action} {trade_intent.shares} {trade_intent.ticker} @ {trade_intent.price}")
            try:
                from discord_bot import bot, ALERT_CHANNEL_ID
                if ALERT_CHANNEL_ID:
                    alert_chan = bot.get_channel(int(ALERT_CHANNEL_ID))
                    if alert_chan:
                        await alert_chan.send(
                            f"🚨 **WhatsApp Trade Executed/Detected:** `{trade_intent.action}` `{trade_intent.shares}` `{trade_intent.ticker}` @ `Rs. {trade_intent.price}` (Source: {source})"
                        )
            except Exception as alert_err:
                print(f"⚠️ [Trade Alert Error]: {alert_err}")
    except Exception as tp_err:
        print(f"⚠️ [WhatsApp] Trade parser check: {tp_err}")

    # 3. Downstream Integration: PGVector Knowledge Store (vector_brain.py)
    try:
        from vector_brain import ingest_knowledge_tool
        await ingest_knowledge_tool.ainvoke(combined_intel)
        print(f"🧠 [Vector Brain] Successfully ingested WhatsApp {media_category} intelligence into PGVector.")
    except Exception as vb_err:
        print(f"⚠️ [Vector Brain] Ingestion failed: {vb_err}")

    # 4. Downstream Integration: Active Playbook Rule Extraction
    if "!rule " in analysis_text:
        try:
            rule_match = re.search(r"!rule\s+(.*)", analysis_text)
            if rule_match:
                rule_content = rule_match.group(1).strip()
                save_learned_rule(f"WhatsApp: {source}", rule_content)
                print(f"🧠 [Active Playbook] Saved rule from {source}: {rule_content}")
        except Exception as rule_err:
            print(f"⚠️ [Active Playbook] Failed to save rule: {rule_err}")

    # 5. Downstream Integration: Institutional Sentiment Mapping
    discord_friendly_message = re.sub(r'<SENTIMENT_MAPPING>.*?</SENTIMENT_MAPPING>', '', analysis_text, flags=re.DOTALL).strip()
    if "EXTRACTED TICKERS (Strict format):" in discord_friendly_message:
        discord_friendly_message = discord_friendly_message.split("EXTRACTED TICKERS (Strict format):")[0].strip()

    match = re.search(r"<SENTIMENT_MAPPING>\s*(\{.*?\})\s*</SENTIMENT_MAPPING>", analysis_text, re.DOTALL)
    if match:
        try:
            from portfolio_manager import get_db_connection
            sentiment_mapping = json.loads(match.group(1).strip())
            summary_text = discord_friendly_message
            conn = get_db_connection()
            cur = conn.cursor()
            for tick, sent in sentiment_mapping.items():
                cur.execute("""
                    INSERT INTO institutional_reports (source, ticker, subject, sentiment, summary)
                    VALUES (%s, %s, %s, %s, %s)
                """, (f"WhatsApp: {source}", tick, caption or filename, sent, summary_text))
            conn.commit()
            cur.close()
            conn.close()
            print(f"📥 Saved WhatsApp report sentiments for {list(sentiment_mapping.keys())}")
        except Exception as sent_err:
            print(f"⚠️ Failed to parse/save WhatsApp sentiment mapping: {sent_err}")

    # 6. Downstream Integration: Discord Research Channel
    try:
        import discord
        from discord_bot import bot, RESEARCH_CHANNEL_ID
        channel = bot.get_channel(RESEARCH_CHANNEL_ID)
        if channel:
            embed = discord.Embed(
                title=f"📱 KTrade Multimodal Intel: {source} ({media_category})",
                description=discord_friendly_message[:4096],
                color=3447003
            )
            embed.set_footer(text=f"Attachment: {filename} | Ingested to PGVector & Playbook")
            await channel.send(embed=embed)
    except Exception as dc_err:
        print(f"⚠️ [Discord Channel Dispatch]: {dc_err}")

    # 7. Checkpoint Update
    if timestamp:
        try:
            from portfolio_manager import update_whatsapp_checkpoint
            update_whatsapp_checkpoint(timestamp)
        except Exception as cp_err:
            print(f"⚠️ [Checkpoint Update Error]: {cp_err}")

    return {"status": "success", "category": media_category, "summary": analysis_text}

async def process_whatsapp_text_payload(text: str, source: str = "KTrade", timestamp: int = None):
    """
    Processes plain text WhatsApp commentary with Gemini extraction,
    trade parser verification, PGVector knowledge memorization, and checkpoint updating.
    """
    import os
    import re
    import json
    from google import genai
    from portfolio_manager import update_whatsapp_checkpoint

    raw_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    client = genai.Client(api_key=raw_key.strip() if raw_key else None, http_options={"timeout": 300000})

    # Zero-Cost Pre-LLM Noise Filter (Filters chitchat < 30 chars with no tickers or financial keywords)
    if not is_financial_or_signal_text(text, has_media=False):
        print(f"ℹ️ [Noise Filter] Ignored short non-financial commentary (<30 chars, no tickers/keywords): '{text}'")
        if timestamp:
            update_whatsapp_checkpoint(timestamp)
        return {"status": "ignored", "reason": "chitchat_noise_filter"}

    extraction_prompt = f"""
    You are an elite quantitative PSX financial analyst. Analyze this KTrade WhatsApp commentary:
    "{text}"
    Source: {source}

    Extract the structured intelligence strictly as a JSON object with this schema:
    {{
        "tickers_mentioned": ["TICKER1", "TICKER2"] or [],
        "aggregate_sentiment": "BULLISH" | "BEARISH" | "NEUTRAL" | "NONE",
        "thesis_summary": "One clean sentence summarizing the core financial argument.",
        "actionable": true | false,
        "rule": "One timeless market principle if present, else null"
    }}
    Do not return markdown, only the raw JSON.
    """

    analysis_json_str = await generate_with_fallback(client, [extraction_prompt], task=TASK_FAST_INGESTION)
    clean_json_str = analysis_json_str.replace('```json', '').replace('```', '').strip()
    try:
        structured_data = json.loads(clean_json_str)
    except Exception:
        structured_data = {
            "tickers_mentioned": [],
            "aggregate_sentiment": "NEUTRAL",
            "thesis_summary": text[:200],
            "actionable": False,
            "rule": None
        }

    combined_intel = f"📱 [KTrade WhatsApp Text] Source: {source}\nIntel: {text}\nSummary: {structured_data.get('thesis_summary')}"

    # 1. Trade parser check
    try:
        from trade_parser import extract_trade_intent
        trade_intent = extract_trade_intent(text)
        if trade_intent and trade_intent.is_trade_execution:
            print(f"🎯 [WhatsApp Trade Intent Detected]: {trade_intent}")
            try:
                from discord_bot import bot, ALERT_CHANNEL_ID
                if ALERT_CHANNEL_ID:
                    alert_chan = bot.get_channel(int(ALERT_CHANNEL_ID))
                    if alert_chan:
                        await alert_chan.send(
                            f"🚨 **WhatsApp Trade Executed/Detected:** `{trade_intent.action}` `{trade_intent.shares}` `{trade_intent.ticker}` @ `Rs. {trade_intent.price}` (Source: {source})"
                        )
            except Exception:
                pass
    except Exception as tp_err:
        print(f"⚠️ [WhatsApp] Trade parser check: {tp_err}")

    # 2. PGVector Brain ingestion
    try:
        from vector_brain import ingest_knowledge_tool
        await ingest_knowledge_tool.ainvoke(combined_intel)
        print("🧠 [Vector Brain] Ingested WhatsApp text intel into PGVector.")
    except Exception as vb_err:
        print(f"⚠️ [Vector Brain] Ingestion failed: {vb_err}")

    # 3. Active Playbook rule
    rule_candidate = structured_data.get("rule")
    if rule_candidate:
        save_learned_rule(f"WhatsApp: {source}", rule_candidate)

    # 4. Checkpoint
    if timestamp:
        update_whatsapp_checkpoint(timestamp)

    return {"status": "success", "extracted": structured_data}

