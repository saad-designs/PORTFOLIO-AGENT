import os
import sys
# Force UTF-8 stdout on Windows cp1252 consoles
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

import time
import re
import asyncio
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

from google import genai
from google.genai import types

# Pulling keys from your local environment
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
if YOUTUBE_API_KEY:
    YOUTUBE_API_KEY = YOUTUBE_API_KEY.strip().strip('"').strip("'")
raw_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
clean_key = raw_key.strip().strip('"').strip("'") if raw_key else None
client = None
try:
    if clean_key:
        client = genai.Client(api_key=clean_key, http_options={"timeout": 300000})
except Exception as c_err:
    print(f"⚠️ [YouTube Scraper Warning] genai.Client init: {c_err}", flush=True)

def get_channel_id(channel_name: str) -> str | None:
    """Cloud-safe channel ID lookup using YouTube Data API v3 over googleapis.com."""
    print(f"DIAGNOSTIC: Searching for channel ID: {channel_name}")
    api_key = os.getenv("YOUTUBE_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        from model_router import get_active_google_api_key
        api_key = get_active_google_api_key()
    if not api_key:
        return None
    try:
        url = "https://www.googleapis.com/youtube/v3/search"
        params = {
            "part": "id",
            "q": channel_name,
            "type": "channel",
            "maxResults": 1,
            "key": api_key.strip()
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("items", [])
            if items:
                return items[0]["id"].get("channelId")
    except Exception as e:
        print(f"DIAGNOSTIC CRASH (get_channel_id): {e}")
    return None

def get_latest_videos(channel_id: str, max_results: int = 5) -> list[dict]:
    """
    Two-Stage Cloud-Safe Channel Discovery (Zero yt-dlp, Zero Bot Blocks):
    Stage A (Official YouTube Atom XML Feed):
      Fetches https://www.youtube.com/feeds/videos.xml?channel_id={channel_id} with browser User-Agent.
      Extracts video_id, title, published_at, and full <media:description> in ~50ms.
    Stage B (YouTube Data API v3 Fallback):
      If XML feed ever fails, converts channel_id 'UC...' to uploads playlist 'UU...' and queries
      https://www.googleapis.com/youtube/v3/playlistItems via googleapis.com (never blocked on cloud).
    Returns: List of dicts: [{'video_id': ..., 'title': ..., 'published_at': ..., 'description': ...}]
    """
    print(f"DIAGNOSTIC: Searching for latest videos on channel: {channel_id}")
    
    # --- STAGE A: Official YouTube Atom XML Feed ---
    try:
        xml_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/atom+xml,application/xml,text/xml;q=0.9,*/*;q=0.8"
        }
        resp = None
        try:
            resp = requests.get(xml_url, headers=headers, timeout=10)
        except requests.exceptions.SSLError:
            # Handle Windows local certificate store quirk gracefully
            resp = requests.get(xml_url, headers=headers, timeout=10, verify=False)

        if resp and resp.status_code == 200 and b"<entry>" in resp.content:
            root = ET.fromstring(resp.content)
            ns = {
                'atom': 'http://www.w3.org/2005/Atom',
                'yt': 'http://www.youtube.com/xml/schemas/2015',
                'media': 'http://search.yahoo.com/mrss/'
            }
            entries = root.findall('atom:entry', ns)
            videos = []
            for entry in entries[:max_results]:
                vid_elem = entry.find('yt:videoId', ns)
                vid = vid_elem.text.strip() if vid_elem is not None and vid_elem.text else None
                if not vid:
                    id_elem = entry.find('atom:id', ns)
                    if id_elem is not None and id_elem.text and "yt:video:" in id_elem.text:
                        vid = id_elem.text.split("yt:video:")[1].strip()
                if not vid:
                    continue

                title_elem = entry.find('atom:title', ns)
                title = title_elem.text.strip() if title_elem is not None and title_elem.text else "Unknown Title"

                pub_elem = entry.find('atom:published', ns)
                pub_str = pub_elem.text.strip() if pub_elem is not None and pub_elem.text else None
                published_at = int(time.time())
                if pub_str:
                    try:
                        clean_pub = pub_str.replace("Z", "+00:00")
                        published_at = int(datetime.fromisoformat(clean_pub).timestamp())
                    except Exception:
                        pass

                desc_elem = entry.find('.//media:description', ns)
                description = desc_elem.text.strip() if desc_elem is not None and desc_elem.text else ""

                videos.append({
                    "video_id": vid,
                    "title": title,
                    "published_at": published_at,
                    "description": description
                })

            if videos:
                print(f"✅ [Channel Discovery - Stage A (XML)] Found {len(videos)} videos for channel {channel_id}.")
                return videos
    except Exception as xml_err:
        print(f"ℹ️ [Channel Discovery - Stage A XML] XML feed unavailable for {channel_id} ({xml_err}). Cascading to Stage B (Data API v3)...")

    # --- STAGE B: YouTube Data API v3 Fallback (googleapis.com) ---
    try:
        api_key = os.getenv("YOUTUBE_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not api_key:
            from model_router import get_active_google_api_key
            api_key = get_active_google_api_key()

        if api_key:
            uploads_id = "UU" + channel_id[2:] if channel_id.startswith("UC") else channel_id
            url = "https://www.googleapis.com/youtube/v3/playlistItems"
            params = {
                "part": "snippet",
                "playlistId": uploads_id,
                "maxResults": max_results,
                "key": api_key.strip()
            }
            resp = requests.get(url, params=params, timeout=12)
            if resp.status_code == 200:
                data = resp.json()
                videos = []
                for item in data.get("items", []):
                    snippet = item.get("snippet", {})
                    res_id = snippet.get("resourceId", {})
                    vid = res_id.get("videoId") or snippet.get("videoId")
                    if not vid:
                        continue
                    title = snippet.get("title", "Unknown Title")
                    desc = snippet.get("description", "")
                    pub_str = snippet.get("publishedAt")
                    published_at = int(time.time())
                    if pub_str:
                        try:
                            clean_pub = pub_str.replace("Z", "+00:00")
                            published_at = int(datetime.fromisoformat(clean_pub).timestamp())
                        except Exception:
                            pass
                    videos.append({
                        "video_id": vid,
                        "title": title,
                        "published_at": published_at,
                        "description": desc
                    })
                if videos:
                    print(f"✅ [Channel Discovery - Stage B (Data API v3)] Found {len(videos)} videos for channel {channel_id}.")
                    return videos
            else:
                print(f"⚠️ [Channel Discovery - Stage B] Data API returned HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as api_err:
        print(f"❌ [Channel Discovery - Stage B] YouTube Data API v3 fallback failed for {channel_id}: {api_err}")

    return []

def synthesize_groq_psx_summary(content_text: str, mentor_name: str = "Mentor", title: str = "") -> str | None:
    """
    Offload post-transcription or metadata synthesis strictly to Groq active fast text models.
    Formats response using SWEEP_FORMAT_PROMPT with 0 Gemini quota consumed.
    """
    from model_router import generate_text_groq, is_any_groq_text_model_available
    from ingestion import SWEEP_FORMAT_PROMPT

    if not is_any_groq_text_model_available():
        return None

    try:
        user_prompt = f"Video Title: {title}\nMentor: {mentor_name}\n\nContent/Transcript:\n{content_text[:15000]}"
        system_prompt = SWEEP_FORMAT_PROMPT.format(mentor_name=mentor_name)
        groq_result = generate_text_groq(prompt=user_prompt, system_prompt=system_prompt)
        if groq_result and len(groq_result.strip()) > 50:
            print(f"✅ [Groq Synthesis] Generated structured PSX analysis via Groq llama-3.3-70b-versatile ({len(groq_result)} chars).")
            return groq_result.strip()
    except Exception as g_err:
        print(f"⚠️ [Groq Synthesis] Groq completion failed: {g_err}")

    return None

def _tier1_native_gemini(video_id: str, title: str = "", mentor_name: str = "Mentor") -> str:
    """
    Tier 1 (Primary): Google Backbone Native URL Ingestion + Groq Synthesis.
    Passes file_uri directly to Gemini via generativelanguage.googleapis.com
    (Google internal servers stream the video; cloud server never connects to YouTube).
    
    Model Order: Always tries gemini-3.5-flash-lite FIRST, then gemini-3.5-flash, then gemini-3.8-flash.
    On 503 UNAVAILABLE: Sleeps 5 seconds and retries up to 2 times before escalating.
    Once raw transcript is returned: Offloads structured PSX analysis to Groq llama-3.3-70b-versatile
    so only 1 Gemini request is spent per video.
    """
    youtube_url = f"https://www.youtube.com/watch?v={video_id}"
    content = types.Content(
        parts=[
            types.Part(file_data=types.FileData(file_uri=youtube_url, mime_type="video/mp4")),
            types.Part(text="Watch and listen to the audio of this video (which may be in Urdu, Hindi, or English). Provide a comprehensive, detailed English transcript and breakdown of everything said, capturing every PSX stock ticker, price target, support/resistance level, and macroeconomic insight.")
        ]
    )
    
    from model_router import (
        is_model_available,
        record_model_error,
        get_active_google_api_key,
        get_candidate_models,
        TASK_MULTIMODAL
    )
    
    # Enforce strictly: gemini-3.5-flash-lite -> gemini-3.5-flash -> gemini-3.8-flash
    preferred_order = ["gemini-3.5-flash-lite", "gemini-3.5-flash", "gemini-3.8-flash"]
    models_to_try = [m for m in preferred_order if is_model_available(m)]
    if not models_to_try:
        models_to_try = get_candidate_models(TASK_MULTIMODAL)

    active_key = get_active_google_api_key()
    active_client = genai.Client(api_key=active_key, http_options={"timeout": 300000}) if active_key else client
    last_err = None

    for model_name in models_to_try:
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                print(f"🎬 [Tier 1] Querying Gemini ({model_name}, attempt {attempt+1}/{max_attempts}) with file_uri: {youtube_url}...")
                response = active_client.models.generate_content(
                    model=model_name,
                    contents=content
                )
                if response.text and len(response.text.strip()) > 50:
                    raw_transcript = response.text.strip()
                    print(f"✅ [Tier 1] Gemini {model_name} extracted transcript ({len(raw_transcript)} chars). Offloading structured analysis to Groq...")
                    
                    # Offload structured PSX analysis to Groq (1 Gemini call total!)
                    groq_summary = synthesize_groq_psx_summary(raw_transcript, mentor_name=mentor_name, title=title)
                    if groq_summary:
                        return groq_summary
                    return raw_transcript
                else:
                    print(f"⚠️ [Tier 1] Model {model_name} returned short output ({len(response.text.strip()) if response.text else 0} chars). Trying next model...")
                    break
            except Exception as e:
                last_err = e
                err_str = str(e).lower()
                
                # Check for 503 UNAVAILABLE
                if "503" in err_str or "unavailable" in err_str:
                    if attempt < max_attempts - 1:
                        print(f"⏳ [Tier 1] 503 UNAVAILABLE on {model_name}. Sleeping 5s before retry (attempt {attempt+2}/{max_attempts})...")
                        time.sleep(5)
                        continue
                
                record_model_error(model_name, e)
                print(f"⚠️ [Tier 1] Model {model_name} failed: {e}")
                break

    raise RuntimeError(f"Tier 1 native Gemini failed for {video_id}: {last_err}")

def _tier2_download_and_transcribe(video_id: str) -> str:
    """
    Tier 2 (Fallback 1): Audio Extraction via yt-dlp + Transcription.
    STRICT CLOUD SAFEGUARD:
    Only runs if NOT in CLOUD_ENV and local cookies.txt exists.
    On cloud servers (or if cookies.txt is missing), skips yt-dlp immediately (0ms)
    to prevent datacenter IP bot-check blocks.
    """
    is_cloud = os.getenv("CLOUD_ENV", "false").lower() == "true"
    cookie_path = os.path.abspath("cookies.txt")
    has_cookies = os.path.exists(cookie_path)
    
    if is_cloud or not has_cookies:
        reason = "CLOUD_ENV=true" if is_cloud else "missing cookies.txt"
        print(f"☁️ [Tier 2] Cloud environment / datacenter IP safeguard triggered ({reason}). Skipping yt-dlp to prevent 403 block.")
        return ""

    import yt_dlp
    from ingestion import transcribe_mentor_audio as _transcribe_audio
    temp_audio_file = None
    
    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': f'temp_{video_id}.%(ext)s',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'm4a',
            'preferredquality': '24',
        }],
        'postprocessor_args': ['-ac', '1', '-ar', '16000'],
        'extractor_args': {'youtube': {'player_client': ['android', 'ios', 'tv']}},
        'socket_timeout': 30,
        'quiet': True,
        'no_warnings': True,
        'cookiefile': cookie_path,
        'http_headers': {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    }
        
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f'https://www.youtube.com/watch?v={video_id}'])
            
        for file in os.listdir('.'):
            if file.startswith(f'temp_{video_id}') and (file.endswith('.m4a') or file.endswith('.mp3')):
                temp_audio_file = file
                break
                
        if temp_audio_file and os.path.exists(temp_audio_file):
            print(f"🎙️ [Tier 2] Audio downloaded: {temp_audio_file} ({os.path.getsize(temp_audio_file)} bytes). Passing to transcribe_mentor_audio...")
            return _transcribe_audio(temp_audio_file)
        else:
            return ""
    except Exception as ydl_err:
        print(f"⚠️ [Tier 2] Local yt-dlp download failed: {ydl_err}")
        return ""
    finally:
        if temp_audio_file and os.path.exists(temp_audio_file):
            try:
                time.sleep(1)
                os.remove(temp_audio_file)
                print(f"🧹 [Tier 2] Cleaned up temporary audio file: {temp_audio_file}")
            except Exception as cleanup_err:
                print(f"⚠️ [Tier 2] Could not clean up {temp_audio_file}: {cleanup_err}")

def _tier3_metadata_extraction(video_id: str, title: str = "", description: str = "", mentor_name: str = "Mentor") -> str:
    """
    Tier 3 (Fallback 2): Zero-yt-dlp XML/API Metadata + Groq PSX Synthesis.
    Uses pre-fetched title and full <media:description> from Task 1.
    If missing, fetches metadata via googleapis.com (never blocked).
    Passes metadata to Groq llama-3.3-70b-versatile to extract PSX tickers and macro takeaways.
    """
    print(f"📄 [Tier 3] Cloud-Safe Metadata Extraction for {video_id}...")
    
    # If title and description weren't passed, fetch via googleapis.com (whitelisted)
    if not title and not description:
        try:
            api_key = os.getenv("YOUTUBE_API_KEY") or os.getenv("GOOGLE_API_KEY")
            if api_key:
                v_url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet&id={video_id}&key={api_key.strip()}"
                v_resp = requests.get(v_url, timeout=10)
                if v_resp.status_code == 200:
                    v_items = v_resp.json().get("items", [])
                    if v_items:
                        snip = v_items[0].get("snippet", {})
                        title = snip.get("title", "")
                        description = snip.get("description", "")
        except Exception as meta_fetch_err:
            print(f"⚠️ [Tier 3] Could not fetch video metadata from Data API: {meta_fetch_err}")

    if not title and not description:
        print(f"❌ [Tier 3] No metadata available for {video_id}.")
        return ""

    combined_metadata = f"Title: {title}\nDescription: {description}"
    print(f"📄 [Tier 3] Pre-fetched metadata captured ({len(combined_metadata)} chars). Synthesizing via Groq...")

    groq_summary = synthesize_groq_psx_summary(combined_metadata, mentor_name=mentor_name, title=title)
    if groq_summary:
        return groq_summary

    # Minimal text fallback
    return combined_metadata

def transcribe_mentor_audio(audio_file_path: str, mime_type: str = "audio/mp4") -> str:
    """Delegates to ingestion.transcribe_mentor_audio."""
    from ingestion import transcribe_mentor_audio as _transcribe
    return _transcribe(audio_file_path, mime_type)

async def get_transcript(video_id: str, title: str = "", description: str = "", mentor_name: str = "Mentor") -> str:
    """
    3-Tier Cloud-Native Non-Blocking Transcript Pipeline (Zero yt-dlp on Cloud):
    1. Tier 1: Native Gemini YouTube URL Ingestion (Google Backbone, 180s timeout) + Groq Synthesis
    2. Tier 2: Compressed Mono Audio (ONLY on local machine if cookies.txt exists; skipped on cloud)
    3. Tier 3: Pre-Fetched XML/API Metadata + Groq Synthesis (Zero yt-dlp, zero 403 blocks)
    """
    loop = asyncio.get_running_loop()
    
    # --- TIER 1: Native Gemini YouTube URL Ingestion ---
    try:
        print(f"🎬 [Tier 1] Attempting native Gemini URL ingestion for {video_id}...")
        transcript = await asyncio.wait_for(
            loop.run_in_executor(None, _tier1_native_gemini, video_id, title, mentor_name),
            timeout=180.0
        )
        if transcript and len(transcript) > 50:
            print(f"✅ [Tier 1] Successfully processed {video_id} directly via Gemini URL ({len(transcript)} chars).")
            return transcript
        print(f"⚠️ [Tier 1] Output too short for {video_id} ({len(transcript) if transcript else 0} chars). Escalating to Tier 2...")
    except Exception as e:
        print(f"⚠️ [Tier 1] Native Gemini URL ingestion failed for {video_id}: {e}. Escalating to Tier 2...")

    # --- TIER 2: Compressed Mono Audio (Local with cookies only) ---
    try:
        transcript = await asyncio.wait_for(
            loop.run_in_executor(None, _tier2_download_and_transcribe, video_id),
            timeout=120.0
        )
        if transcript and len(transcript) > 50:
            print(f"✅ [Tier 2] Successfully transcribed {video_id} via local audio ({len(transcript)} chars).")
            return transcript
    except Exception as e:
        print(f"⚠️ [Tier 2] Audio extraction/transcription skipped/failed for {video_id}: {e}. Escalating to Tier 3...")

    # --- TIER 3: Pre-Fetched XML/API Metadata + Groq Synthesis (Zero yt-dlp) ---
    try:
        print(f"📄 [Tier 3] Synthesizing pre-fetched XML/API metadata for {video_id}...")
        return await loop.run_in_executor(None, _tier3_metadata_extraction, video_id, title, description, mentor_name)
    except Exception as e:
        print(f"❌ [Tier 3] Metadata synthesis failed for {video_id}: {e}")
        return ""

MENTOR_CHANNEL_MAP = {
    "Abdul Rehman Najam": "UCU_s2lRotb65VMvAtFVnDuw",
    "Mashal Khan": "UCKDp90OW0427E9zXO8YOlBQ",
    "Jawad Hafeez": "UCsvI0Vt4IEY25Y_WUQOjfyA",
    "Investkaar": "UCfTbnFryszAD4dvIw05Svog",
    "Sarmaaya Financials": "UC_6aNQkpEKgl9azddaN-QyQ",
    "AKD Securities Limited": "UCNj1eySxkI_FBP3g3FT0qbw",
    "KTrade PK": "UCfXgbAAHzN_VxGJg2agyAYA"
}

LATEST_SWEEP_TIMESTAMP = 0

async def fetch_mentor_transcripts(mentor_names: list) -> tuple[dict, list]:
    from portfolio_manager import get_checkpoint, update_checkpoint
    from datetime import datetime
    
    transcripts = {}
    failed_mentors = []
    
    last_timestamp = get_checkpoint("youtube_last_timestamp")
    highest_timestamp = last_timestamp
    total_processed_videos = 0
    MAX_VIDEOS_PER_SWEEP = 2
    
    for name in mentor_names:
        if total_processed_videos >= MAX_VIDEOS_PER_SWEEP:
            print(f"🛑 [YouTube Scraper] Reached max cap of {MAX_VIDEOS_PER_SWEEP} new videos for this sweep. Halting mentor loop.")
            break
        print(f"\n--- Processing Mentor: {name} ---")
        try:
            channel_id = MENTOR_CHANNEL_MAP.get(name)
            if not channel_id:
                print(f"⚠️ [YouTube Scraper] Unknown mentor: {name}")
                failed_mentors.append(name)
                continue
                
            videos = get_latest_videos(channel_id, max_results=5)
            if not videos:
                failed_mentors.append(name)
                continue
                
            new_videos = [v for v in videos if v["published_at"] > last_timestamp]
            new_videos.sort(key=lambda x: x["published_at"])
            
            if not new_videos:
                print(f"No new videos for {name} since {last_timestamp}. Skipping.")
                continue
                
            mentor_transcripts = []
            for video in new_videos:
                if total_processed_videos >= MAX_VIDEOS_PER_SWEEP:
                    print(f"🛑 [YouTube Scraper] Reached max cap of {MAX_VIDEOS_PER_SWEEP} videos. Stopping video loop.")
                    break
                video_id = video["video_id"]
                published_at = video["published_at"]
                title = video.get("title", "")
                description = video.get("description", "")
                
                transcript = await get_transcript(video_id, title=title, description=description, mentor_name=name)
                total_processed_videos += 1
                if transcript:
                    if isinstance(transcript, list):
                        clean_text = " ".join([entry.get("text", "") for entry in transcript])
                    elif isinstance(transcript, str):
                        clean_text = transcript.strip()
                    else:
                        clean_text = str(transcript)

                    if len(clean_text) >= 50:
                        mentor_transcripts.append(f"[{datetime.fromtimestamp(published_at)}] {clean_text}")
                        highest_timestamp = max(highest_timestamp, published_at)
                    else:
                        print(f"ℹ️ [YouTube Scraper] Transcript output too short ({len(clean_text)} chars) for {name}. Updating checkpoint anyway.")
                        highest_timestamp = max(highest_timestamp, published_at)
                else:
                    print(f"DIAGNOSTIC: Failed to fetch transcript for {name} ({video_id}). Updating checkpoint anyway to avoid loop trap.")
                    highest_timestamp = max(highest_timestamp, published_at)
            
            if mentor_transcripts:
                transcripts[name] = "\n\n---\n\n".join(mentor_transcripts)
            else:
                failed_mentors.append(name)
                
        except Exception as e:
            print(f"[ERROR] Unhandled error extracting transcript for {name}: {e}. Skipping mentor.")
            failed_mentors.append(name)
            continue
            
    global LATEST_SWEEP_TIMESTAMP
    LATEST_SWEEP_TIMESTAMP = highest_timestamp
    return transcripts, failed_mentors

async def run_youtube_sweep(max_videos: int = 2) -> dict:
    """
    Executes live YouTube Mentor sweep across primary mentor channels.
    Pulls recent videos, summarizes using Tier 3/Tier 1, and posts to agent_feed.
    Returns structured audit metrics: status, latency_ms, processed_videos, and mentor summaries.
    """
    import time
    from portfolio_manager import send_learning_brief, add_pending_signal, publish_agent_event
    t0 = time.perf_counter()
    transcripts, failed = await fetch_mentor_transcripts(list(MENTOR_CHANNEL_MAP.keys())[:3])

    saved_items = []
    if transcripts:
        for mentor, text in transcripts.items():
            await send_learning_brief(
                source="YouTube Mentor",
                title=f"Update from {mentor}",
                summary=f"Key Takeaways:\n{text[:500]}..."
            )
            add_pending_signal(source="YouTube", payload={"mentor": mentor, "report": text})
            saved_items.append({"mentor": mentor, "summary": text[:200]})

    lat_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    return {
        "status": "PASS" if (transcripts or not failed) else "WARN",
        "latency_ms": lat_ms,
        "mentors_scanned": 3,
        "transcripts_found": len(transcripts),
        "failed_mentors": failed,
        "sample": saved_items[:2] if saved_items else [{"info": "No new videos since last watermark"}]
    }


if __name__ == "__main__":
    import asyncio
    from portfolio_manager import send_learning_brief, add_pending_signal
    print("🚀 [Diagnostics] Forcing manual YouTube sweep...")
    
    async def manual_test():
        transcripts, failed = await fetch_mentor_transcripts(list(MENTOR_CHANNEL_MAP.keys())) 
        
        if transcripts:
            for mentor, text in transcripts.items():
                print(f"✅ Found data: {mentor}")
                await send_learning_brief(
                    source="YouTube Mentor",
                    title=f"Update from {mentor}",
                    summary=f"**Key Takeaways:**\n{text[:500]}..."
                )
                add_pending_signal(source="YouTube", payload={"mentor": mentor, "report": text})
        else:
            print("⚠️ No new videos or transcripts found.")

    asyncio.run(manual_test())