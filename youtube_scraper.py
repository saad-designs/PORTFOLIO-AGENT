import os
from dotenv import load_dotenv

load_dotenv()
import re
import asyncio
import yt_dlp
from google import genai
from google.genai import types
from googleapiclient.discovery import build
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, wait_chain, wait_fixed
from requests.exceptions import Timeout, ConnectionError

# Pulling keys from your local environment
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY") 
# Load key and forcefully strip any hidden spaces or newlines from the .env file
raw_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
if not raw_key:
    raise ValueError("No API key found in environment variables.")
    
clean_key = raw_key.strip()
client = genai.Client(api_key=clean_key, http_options={"timeout": 300000})

@retry(
    retry=retry_if_exception_type((Timeout, ConnectionError)),
    wait=wait_exponential(multiplier=1, min=2, max=10), 
    stop=stop_after_attempt(3)
)
def get_channel_id(channel_name):
    print(f"DIAGNOSTIC: Searching for channel ID: {channel_name}")
    try:
        with build('youtube', 'v3', developerKey=YOUTUBE_API_KEY) as youtube:
            request = youtube.search().list(part="id", q=channel_name, type="channel", maxResults=1)
            response = request.execute()
            
            if not response.get('items'):
                return None
                
            return response['items'][0]['id']['channelId']
    except Exception as e:
        print(f"DIAGNOSTIC CRASH (get_channel_id): {str(e)}")
        return None

@retry(
    retry=retry_if_exception_type((Timeout, ConnectionError)),
    wait=wait_exponential(multiplier=1, min=2, max=10), 
    stop=stop_after_attempt(3)
)
def get_latest_videos(channel_id, max_results=5):
    print(f"DIAGNOSTIC: Searching for latest videos on channel: {channel_id}")
    try:
        with build('youtube', 'v3', developerKey=YOUTUBE_API_KEY) as youtube:
            # 1. Fetch the 'Uploads' playlist ID (Cost: 1 unit)
            channel_res = youtube.channels().list(
                part="contentDetails", 
                id=channel_id
            ).execute()
            
            if not channel_res.get('items'):
                return []
                
            uploads_playlist_id = channel_res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            
            # 2. Fetch the newest video from that playlist (Cost: 1 unit)
            playlist_res = youtube.playlistItems().list(
                part="contentDetails,snippet", 
                playlistId=uploads_playlist_id, 
                maxResults=max_results
            ).execute()
            
            if not playlist_res.get('items'):
                return []
                
            from datetime import datetime
            videos = []
            for item in playlist_res['items']:
                video_id = item['contentDetails']['videoId']
                published_at_str = item['contentDetails'].get('videoPublishedAt')
                if not published_at_str:
                    continue
                published_at = int(datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%SZ").timestamp())
                videos.append({"video_id": video_id, "published_at": published_at})
                
            return videos
    except Exception as e:
        print(f"❌ [YouTube API Error in get_latest_videos]: {e}")
        return []

def _tier1_native_gemini(video_id: str) -> str:
    """
    Tier 1 (Primary): Gemini Native Direct YouTube Video/Audio Watching.
    Passes YouTube URL directly to Gemini without downloading locally.
    """
    youtube_url = f"https://www.youtube.com/watch?v={video_id}"
    content = types.Content(
        parts=[
            types.Part(file_data=types.FileData(file_uri=youtube_url, mime_type="video/mp4")),
            types.Part(text="Watch and listen to the audio of this video (which may be in Urdu, Hindi, or English). Provide a comprehensive, detailed English transcript and breakdown of everything said, capturing every PSX stock ticker, price target, support/resistance level, and macroeconomic insight.")
        ]
    )
    models_to_try = ["gemini-2.5-flash", "gemini-3.8-flash", "gemini-3.5-flash-lite"]
    last_err = None
    for model_name in models_to_try:
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=content
            )
            if response.text and len(response.text.strip()) > 50:
                return response.text.strip()
        except Exception as e:
            last_err = e
            print(f"⚠️ [Tier 1] Model {model_name} failed: {e}")
            continue
    raise RuntimeError(f"Tier 1 native Gemini failed for {video_id}: {last_err}")

def _tier2_download_and_transcribe(video_id: str) -> str:
    """
    Tier 2 (Fallback 1): Compressed 24kbps Mono Audio via yt-dlp + Gemini File API.
    Forces 16kHz mono audio so even long videos are compact (~3MB).
    """
    from ingestion import transcribe_mentor_audio as _transcribe_audio
    cookie_path = os.path.abspath("cookies.txt")
    temp_audio_file = None
    
    ydl_opts = {
        'format': 'worstaudio[ext=m4a]/worstaudio/bestaudio',
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
        'http_headers': {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    }
    
    if os.path.exists(cookie_path):
        ydl_opts['cookiefile'] = cookie_path
        
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f'https://www.youtube.com/watch?v={video_id}'])
            
        # Locate the downloaded audio file
        for file in os.listdir('.'):
            if file.startswith(f'temp_{video_id}') and (file.endswith('.m4a') or file.endswith('.mp3')):
                temp_audio_file = file
                break
                
        if temp_audio_file and os.path.exists(temp_audio_file):
            print(f"🎙️ [Tier 2] Audio downloaded: {temp_audio_file} ({os.path.getsize(temp_audio_file)} bytes). Passing to transcribe_mentor_audio...")
            return _transcribe_audio(temp_audio_file)
        else:
            raise FileNotFoundError(f"Audio file for {video_id} not found after download.")
    finally:
        # Guarantee cleanup regardless of success or failure
        if temp_audio_file and os.path.exists(temp_audio_file):
            try:
                import time
                time.sleep(1)
                os.remove(temp_audio_file)
                print(f"🧹 [Tier 2] Cleaned up temporary audio file: {temp_audio_file}")
            except Exception as cleanup_err:
                print(f"⚠️ [Tier 2] Could not clean up {temp_audio_file}: {cleanup_err}")

def _tier3_metadata_extraction(video_id: str) -> str:
    """
    Tier 3 (Fallback 2): Multi-Language Subtitle & Metadata Extraction via yt-dlp.
    Extracts title, full description, chapters, and auto-generated subtitles.
    """
    cookie_path = os.path.abspath("cookies.txt")
    ydl_opts_meta = {
        'skip_download': True,
        'quiet': True,
        'no_warnings': True,
        'socket_timeout': 30,
        'subtitleslangs': ['ur', 'hi', 'en'],
        'writeautomaticsub': True,
    }
    if os.path.exists(cookie_path):
        ydl_opts_meta['cookiefile'] = cookie_path
        
    try:
        with yt_dlp.YoutubeDL(ydl_opts_meta) as ydl:
            info = ydl.extract_info(f'https://www.youtube.com/watch?v={video_id}', download=False)
            
        title = info.get('title', 'Unknown Title')
        description = info.get('description', 'No description available')
        tags = info.get('tags', [])
        chapters = info.get('chapters', [])
        
        meta_lines = [
            f"Title: {title}",
            f"Description: {description}",
        ]
        if chapters:
            chapter_text = "; ".join([c.get('title', '') for c in chapters if c.get('title')])
            if chapter_text:
                meta_lines.append(f"Chapters: {chapter_text}")
        if tags:
            meta_lines.append(f"Tags: {tags}")
            
        print("📄 [Tier 3] Successfully extracted metadata via Tier 3 fallback.")
        return "\n".join(meta_lines)
    except Exception as meta_err:
        print(f"❌ [Tier 3] Metadata fallback failed: {meta_err}")
        return ""

def transcribe_mentor_audio(audio_file_path: str, mime_type: str = "audio/mp4") -> str:
    """Delegates to ingestion.transcribe_mentor_audio."""
    from ingestion import transcribe_mentor_audio as _transcribe
    return _transcribe(audio_file_path, mime_type)

async def get_transcript(video_id: str) -> str:
    """
    3-Tier Non-Blocking Transcript Pipeline:
    1. Tier 1: Native Gemini YouTube URL Ingestion (zero local download, 180s timeout)
    2. Tier 2: Compressed 24kbps Mono Audio via yt-dlp + File API (240s timeout)
    3. Tier 3: Multi-Language Subtitle & Metadata Extraction Fallback
    """
    loop = asyncio.get_running_loop()
    
    # --- TIER 1: Native Gemini YouTube URL Ingestion ---
    try:
        print(f"🎬 [Tier 1] Attempting native Gemini URL ingestion for {video_id}...")
        transcript = await asyncio.wait_for(
            loop.run_in_executor(None, _tier1_native_gemini, video_id),
            timeout=180.0
        )
        if transcript and len(transcript) > 50:
            print(f"✅ [Tier 1] Successfully transcribed {video_id} directly via Gemini URL ({len(transcript)} chars).")
            return transcript
        print(f"⚠️ [Tier 1] Output too short for {video_id} ({len(transcript) if transcript else 0} chars). Escalating to Tier 2...")
    except Exception as e:
        print(f"⚠️ [Tier 1] Native Gemini URL ingestion failed for {video_id}: {e}. Escalating to Tier 2...")

    # --- TIER 2: Compressed 24kbps Mono Audio via yt-dlp + File API ---
    try:
        print(f"🎙️ [Tier 2] Attempting compressed 24kbps mono audio extraction for {video_id}...")
        transcript = await asyncio.wait_for(
            loop.run_in_executor(None, _tier2_download_and_transcribe, video_id),
            timeout=240.0
        )
        if transcript and len(transcript) > 50:
            print(f"✅ [Tier 2] Successfully transcribed {video_id} via compressed audio ({len(transcript)} chars).")
            return transcript
        print(f"⚠️ [Tier 2] Output too short for {video_id}. Escalating to Tier 3...")
    except Exception as e:
        print(f"⚠️ [Tier 2] Audio extraction/transcription failed for {video_id}: {e}. Escalating to Tier 3...")

    # --- TIER 3: Multi-Language Subtitle & Metadata Extraction Fallback ---
    try:
        print(f"📄 [Tier 3] Extracting metadata fallback for {video_id}...")
        return await loop.run_in_executor(None, _tier3_metadata_extraction, video_id)
    except Exception as e:
        print(f"❌ [Tier 3] Metadata extraction failed for {video_id}: {e}")
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

async def fetch_mentor_transcripts(mentor_names: list) -> tuple[dict, list]:
    from portfolio_manager import get_checkpoint, update_checkpoint
    from datetime import datetime
    
    transcripts = {}
    failed_mentors = []
    
    last_timestamp = get_checkpoint("youtube_last_timestamp")
    highest_timestamp = last_timestamp
    
    for name in mentor_names:
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
                video_id = video["video_id"]
                published_at = video["published_at"]
                
                transcript = await get_transcript(video_id)
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
            
    if highest_timestamp > last_timestamp:
        update_checkpoint("youtube_last_timestamp", highest_timestamp)
            
    return transcripts, failed_mentors

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