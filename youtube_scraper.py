import os
import re
import yt_dlp
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from requests.exceptions import Timeout, ConnectionError

# Pulling keys from your local environment
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY") 

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
        # Primary Attempt: Fast Anonymous Pull
        print("DIAGNOSTIC: Attempting fast anonymous pull...")
        if not hasattr(YouTubeTranscriptApi, 'get_transcript'):
            api = YouTubeTranscriptApi()
            result = api.fetch(video_id)
            transcript_list = result.to_raw_data() if hasattr(result, 'to_raw_data') else result
        else:
            transcript_list = YouTubeTranscriptApi.get_transcript(video_id, languages=['en', 'ur', 'hi'])

        transcript = " ".join([t['text'] for t in transcript_list])
        print("DIAGNOSTIC: Transcript pulled successfully via fast API!")
        return transcript

    except Exception as e:
        print(f"Fast pull failed ({str(e)}). IP might still be blocked.")
        print("DIAGNOSTIC: Triggering robust yt-dlp cookie fallback...")
        return get_transcript_fallback(video_id)


@retry(
    retry=retry_if_exception_type((Timeout, ConnectionError)),
    wait=wait_exponential(multiplier=1, min=2, max=10), 
    stop=stop_after_attempt(3)
)
def get_transcript_fallback(video_id):
    cookie_path = os.path.abspath("cookies.txt")
    
    # yt-dlp configuration to extract ONLY subtitles (no video download)
    ydl_opts = {
        'cookiefile': 'cookies.txt',          # Keep your working cookie file
        'skip_download': True,                # CRITICAL: Do not try to download the video
        'writesubtitles': True,               # Download manual subtitles
        'writeautomaticsub': True,            # Download auto-generated subtitles
        'subtitleslangs': ['en', 'ur', 'hi'],       # Look for English, Urdu, and Hindi
        'outtmpl': '%(id)s.%(ext)s',          # Temporary save format
        'quiet': True,
        'no_warnings': True,
        # This prevents the "format not available" crash:
        'format': 'bestaudio/best',
    }
    
    if os.path.exists(cookie_path):
        ydl_opts['cookiefile'] = cookie_path

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f'https://www.youtube.com/watch?v={video_id}'])

        # Locate the downloaded subtitle file
        vtt_file = None
        for file in os.listdir('.'):
            if file.startswith(video_id) and file.endswith('.vtt'):
                vtt_file = file
                break

        if not vtt_file:
            print("CRITICAL CRASH: yt-dlp could not extract subtitles.")
            return None

        # Read the file and immediately delete it
        with open(vtt_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        os.remove(vtt_file)

        # Clean VTT timestamps and HTML formatting
        text_lines = []
        for line in lines:
            if '-->' in line or line.startswith('WEBVTT') or line.startswith('Kind:') or line.startswith('Language:'):
                continue
            clean_line = re.sub(r'<[^>]+>', '', line).strip()
            if clean_line and not clean_line.isdigit():
                text_lines.append(clean_line)

        # Remove duplicate lines common in auto-generated subs
        deduped = []
        for line in text_lines:
            if not deduped or deduped[-1] != line:
                deduped.append(line)

        transcript = " ".join(deduped)
        print("DIAGNOSTIC: Transcript pulled successfully via yt-dlp fallback!")
        return transcript

    except Exception as e:
        print(f"CRITICAL CRASH (yt-dlp fallback): {str(e)}")
        return None

def fetch_mentor_transcripts(mentor_names: list) -> dict:
    transcripts = {}
    
    for name in mentor_names:
        print(f"\n--- Processing Mentor: {name} ---")
        channel_id = get_channel_id(name)
        if not channel_id:
            continue
            
        video_id = get_latest_video_id(channel_id)
        if not video_id:
            continue
            
        transcript = get_transcript(video_id)
        if transcript:
            transcripts[name] = transcript
            
    return transcripts