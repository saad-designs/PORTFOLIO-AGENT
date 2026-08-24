import os
from dotenv import load_dotenv

load_dotenv()
import re
import yt_dlp
from google import genai
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
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
        youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
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
def get_latest_video_id(channel_id):
    print(f"DIAGNOSTIC: Searching for latest video on channel: {channel_id}")
    try:
        youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
        request = youtube.search().list(part="id", channelId=channel_id, order="date", type="video", maxResults=1)
        response = request.execute()
        
        if not response.get('items'):
            return None
            
        return response['items'][0]['id']['videoId']
    except Exception as e:
        print(f"DIAGNOSTIC CRASH (get_latest_video_id): {str(e)}")
        return None

@retry(
    retry=retry_if_exception_type((Timeout, ConnectionError)),
    wait=wait_exponential(multiplier=1, min=2, max=10), 
    stop=stop_after_attempt(3)
)
def get_transcript(video_id):
    print(f"DIAGNOSTIC: Attempting to pull transcript for video: {video_id}")
    try:
        # Primary Attempt: Fetch transcripts in preferred language order
        print("DIAGNOSTIC: Attempting fast anonymous pull...")
        data = YouTubeTranscriptApi.get_transcript(video_id, languages=['en', 'ur', 'hi', 'en-US'])
        transcript = " ".join([entry['text'] for entry in data])
        
        print("DIAGNOSTIC: Transcript pulled successfully via fast API!")
        return transcript

    except Exception as e:
        print(f"Fast pull failed ({str(e)}). IP might still be blocked or captions disabled.")
        print("DIAGNOSTIC: Triggering Tier 1 Fallback (Gemini Audio Ingestion)...")
        try:
            return get_transcript_fallback(video_id)
        except Exception as yt_err:
            print(f"⚠️ Fallback pipeline failed for {video_id}: {yt_err}")
            return ""


@retry(
    retry=retry_if_exception_type((Timeout, ConnectionError)),
    wait=wait_exponential(multiplier=1, min=2, max=10), 
    stop=stop_after_attempt(3)
)
def get_transcript_fallback(video_id):
    cookie_path = os.path.abspath("cookies.txt")
    temp_audio_file = None
    
    try:
        # Tier 1: Gemini Direct Audio Comprehension
        ydl_opts = {
            'format': 'm4a/bestaudio/best',
            'outtmpl': f'temp_{video_id}.%(ext)s',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'm4a',
            }],
            'quiet': True,
            'no_warnings': True,
            'http_headers': {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        }
        
        if os.path.exists(cookie_path):
            ydl_opts['cookiefile'] = cookie_path
            
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f'https://www.youtube.com/watch?v={video_id}'])
            
        # Locate the downloaded audio file
        for file in os.listdir('.'):
            if file.startswith(f'temp_{video_id}') and (file.endswith('.m4a') or file.endswith('.mp3')):
                temp_audio_file = file
                break
                
        if temp_audio_file:
            print(f"DIAGNOSTIC: Uploading {temp_audio_file} to Gemini...")
            gemini_file = client.files.upload(file=temp_audio_file)
            
            try:
                prompt = "Listen to this market analysis video audio carefully (often in Urdu/English). Transcribe and summarize the key stock market commentary, macroeconomic observations, mentioned PSX ticker symbols, and the core strategic takeaway."
                response = client.models.generate_content(
                    model='gemini-3.6-flash',
                    contents=[prompt, gemini_file]
                )
                
                print("DIAGNOSTIC: Successfully generated audio summary with Gemini.")
                return response.text
            finally:
                print("DIAGNOSTIC: Purging audio file from Gemini...")
                client.files.delete(name=gemini_file.name)
        else:
            raise Exception("Audio download failed or file not found")
            
    except Exception as e:
        print(f"DIAGNOSTIC: Tier 1 Audio Fallback failed ({str(e)}). Triggering Tier 2 Fallback (Metadata Extraction)...")
        # Tier 2: Metadata Extraction (Failsafe)
        try:
            ydl_opts_meta = {
                'skip_download': True,
                'quiet': True,
                'no_warnings': True
            }
            if os.path.exists(cookie_path):
                ydl_opts_meta['cookiefile'] = cookie_path
                
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
            
    finally:
        # Cleanup local audio file
        if temp_audio_file and os.path.exists(temp_audio_file):
            print(f"DIAGNOSTIC: Cleaning up local temporary file {temp_audio_file}...")
            os.remove(temp_audio_file)

def fetch_mentor_transcripts(mentor_names: list) -> dict:
    transcripts = {}
    
    for name in mentor_names:
        print(f"\n--- Processing Mentor: {name} ---")
        try:
            channel_id = get_channel_id(name)
            if not channel_id:
                continue
                
            video_id = get_latest_video_id(channel_id)
            if not video_id:
                continue
                
            transcript = get_transcript(video_id)
            if transcript:
                transcripts[name] = transcript
        except Exception as e:
            print(f"⚠️ Unhandled error extracting transcript for {name}: {e}. Skipping mentor.")
            continue
            
    return transcripts