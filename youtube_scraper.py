import os
from dotenv import load_dotenv

load_dotenv()
import re
import asyncio
import yt_dlp
from google import genai
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
client = genai.Client(api_key=clean_key)

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

async def get_transcript(video_id):
    """
    Primary Entry Point (Async)
    Bypasses captions entirely, extracts audio, and forces Gemini to transcribe.
    """
    print(f"DIAGNOSTIC: Commencing direct audio extraction & AI transcription for {video_id}")
    try:
        # Enforces a strict 60-second timeout. If it hangs, the system forcefully unblocks.
        return await asyncio.wait_for(_run_audio_pipeline(video_id), timeout=60.0)
    except asyncio.TimeoutError:
        print(f"[ERROR] Audio extraction for {video_id} timed out after 60 seconds. System unblocked.")
        return await _metadata_failsafe(video_id)
    except Exception as e:
        print(f"[ERROR] Direct transcription failed: {str(e)}")
        return await _metadata_failsafe(video_id)

async def _run_audio_pipeline(video_id):
    """
    Offloads the synchronous yt-dlp download to a background thread.
    This prevents the download from freezing the main FastAPI event loop.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _download_and_transcribe, video_id)

def _download_and_transcribe(video_id):
    """Synchronous audio extraction via yt-dlp."""
    cookie_path = os.path.abspath("cookies.txt")
    temp_audio_file = None
    
    ydl_opts = {
        'format': 'm4a/bestaudio/best',
        'outtmpl': f'temp_{video_id}.%(ext)s',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'm4a',
        }],
        'quiet': True,
        'no_warnings': True,
        'socket_timeout': 15,  # CRITICAL: Prevents internal yt-dlp socket deadlocks
        'extractor_args': {'youtube': {'player_client': ['android', 'ios']}},
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
                
        if temp_audio_file:
            print(f"DIAGNOSTIC: Audio downloaded successfully. Passing {temp_audio_file} to Gemini...")
            # Passes to your existing transcription function
            return transcribe_mentor_audio(temp_audio_file)
        else:
            raise Exception("Audio download failed or file not found")
            
    finally:
        # Guarantee cleanup regardless of success or failure
        if temp_audio_file and os.path.exists(temp_audio_file):
            print(f"DIAGNOSTIC: Cleaning up local temporary file {temp_audio_file}...")
            try:
                import time
                time.sleep(1)  # Force OS to release the file handle
                os.remove(temp_audio_file)
            except Exception as cleanup_err:
                print(f"[WARNING] Could not clean up {temp_audio_file}: {cleanup_err}")



@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_chain(*[wait_fixed(300), wait_fixed(900), wait_fixed(1800)]),
    stop=stop_after_attempt(3)
)
def transcribe_mentor_audio(audio_file_path: str, mime_type: str = "audio/mp4") -> str:
    """
    Transcribes downloaded YouTube audio using Gemini 3.5 Transcribe.
    """
    if not os.path.exists(audio_file_path):
        print(f"[ERROR] Audio file not found: {audio_file_path}")
        return ""

    print(f"[DIAGNOSTIC] Uploading {audio_file_path} to Gemini 3.5 Transcribe...")
    
    try:
        # 1. Upload the audio payload
        uploaded_file = client.files.upload(file=audio_file_path)
        
        # 2. Call the Generate Content API for transcription
        chat = client.chats.create(model="gemini-1.5-flash-latest")
        response = chat.send_message(
            [uploaded_file, "Accurately transcribe this financial market commentary in Urdu/English."]
        )
        
        # 3. Ruthlessly purge the remote file to prevent cloud storage bloat
        try:
            client.files.delete(name=uploaded_file.name)
        except Exception as del_err:
            print(f"[WARNING] Could not delete remote file: {del_err}")
        
        print("[DIAGNOSTIC] Transcription successful.")
        
        # MUST EXPLICITLY RETURN THE STRING
        return response.text

    except Exception as e:
        print(f"❌ [DIAGNOSTIC] Fallback transcription failed: {e}")
        return None

async def _metadata_failsafe(video_id):
    """Tier 2 Fallback: If audio completely fails or times out, grab the metadata."""
    print(f"DIAGNOSTIC: Triggering Tier 2 Fallback (Metadata Extraction) for {video_id}...")
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _extract_metadata_sync, video_id)

def _extract_metadata_sync(video_id):
    cookie_path = os.path.abspath("cookies.txt")
    ydl_opts_meta = {
        'skip_download': True,
        'quiet': True,
        'no_warnings': True,
        'socket_timeout': 15
    }
    if os.path.exists(cookie_path):
        ydl_opts_meta['cookiefile'] = cookie_path
        
    try:
        with yt_dlp.YoutubeDL(ydl_opts_meta) as ydl:
            info = ydl.extract_info(f'https://www.youtube.com/watch?v={video_id}', download=False)
            
        title = info.get('title', 'Unknown Title')
        description = info.get('description', 'No description available')
        tags = info.get('tags', [])
        
        print("DIAGNOSTIC: Successfully extracted metadata via Tier 2 fallback.")
        return f"Title: {title}\nDescription: {description}\nTags: {tags}"
    except Exception as meta_err:
        print(f"DIAGNOSTIC CRASH (Tier 2 Metadata Fallback): {str(meta_err)}")
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
                    # Normalize YouTubeTranscriptApi (list) vs Gemini Audio (str)
                    if isinstance(transcript, list):
                        clean_text = " ".join([entry.get("text", "") for entry in transcript])
                    elif isinstance(transcript, str):
                        clean_text = transcript.strip()
                    else:
                        clean_text = str(transcript)

                    if len(clean_text) > 100 and not clean_text.startswith("Title: "):
                        mentor_transcripts.append(f"[{datetime.fromtimestamp(published_at)}] {clean_text}")
                        highest_timestamp = max(highest_timestamp, published_at)
                    else:
                        print(f"DIAGNOSTIC: Audio ingestion yielded no speech or only metadata for {name}. Skipping LLM.")
                else:
                    print(f"DIAGNOSTIC: Failed to fetch transcript for {name} ({video_id}).")
            
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