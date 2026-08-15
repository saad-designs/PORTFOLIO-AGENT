import os
import glob
import yt_dlp
from google import genai
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi

# Grab the new GCP key from Render's environment
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY") 
client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

def get_channel_id(channel_name):
    print(f"DIAGNOSTIC: Searching for channel ID: {channel_name}")
    try:
        youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
        request = youtube.search().list(part="id", q=channel_name, type="channel", maxResults=1)
        response = request.execute()
        
        if not response.get('items'):
            print(f"DIAGNOSTIC: Google API returned 0 items for {channel_name}")
            return None
            
        channel_id = response['items'][0]['id']['channelId']
        print(f"DIAGNOSTIC: Found Channel ID: {channel_id}")
        return channel_id
        
    except Exception as e:
        print(f"DIAGNOSTIC CRASH (get_channel_id): {str(e)}")
        return None

def get_latest_video_id(channel_id):
    print(f"DIAGNOSTIC: Searching for latest video on channel: {channel_id}")
    try:
        youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
        request = youtube.search().list(part="id", channelId=channel_id, order="date", type="video", maxResults=1)
        response = request.execute()
        
        if not response.get('items'):
            print("DIAGNOSTIC: No videos found for this channel.")
            return None
            
        video_id = response['items'][0]['id']['videoId']
        print(f"DIAGNOSTIC: Found latest video ID: {video_id}")
        return video_id
        
    except Exception as e:
        print(f"DIAGNOSTIC CRASH (get_latest_video_id): {str(e)}")
        return None

def get_transcript(video_id):
    print(f"DIAGNOSTIC: Attempting to pull transcript for video: {video_id}")
    try:
        transcript_list = YouTubeTranscriptApi.get_transcript(video_id)
        transcript = " ".join([t['text'] for t in transcript_list])
        print("DIAGNOSTIC: Transcript pulled successfully!")
        return transcript
    except Exception as e:
        print(f"DIAGNOSTIC CRASH (get_transcript): {str(e)}")
        return None

def extract_audio_with_gemini(video_id: str, mentor_name: str) -> str:
    print(f"⚠️ Subtitles disabled for {mentor_name}. Triggering audio extraction fallback...")
    
    # 1. Target Render's specific Secret File mount path
    render_secret_path = "/etc/secrets/cookies.txt"
    local_dev_path = os.path.abspath("cookies.txt")
    
    # 2. Dynamically assign the cookie path based on the environment
    if os.path.exists(render_secret_path):
        active_cookie_path = render_secret_path
        print(f"✅ DIAGNOSTIC: Using Render Secret cookies at {active_cookie_path}")
    elif os.path.exists(local_dev_path):
        active_cookie_path = local_dev_path
        print(f"✅ DIAGNOSTIC: Using Local dev cookies at {active_cookie_path}")
    else:
        active_cookie_path = None
        print("🚨 CRITICAL: NO COOKIE FILE FOUND IN /etc/secrets/ OR ROOT.")

    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': f'{video_id}.%(ext)s',
    }
    
    # Only inject the cookie parameter if the file physically exists
    if active_cookie_path:
        ydl_opts['cookiefile'] = active_cookie_path
    
    audio_filename = None
    uploaded_file = None
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            print("DIAGNOSTIC: Commencing yt-dlp download...")
            ydl.download([f'https://www.youtube.com/watch?v={video_id}'])
            
        search_files = glob.glob(f'{video_id}.*')
        if not search_files:
            return None
        audio_filename = search_files[0]
        
        print(f"Uploading {audio_filename} to Gemini for processing...")
        uploaded_file = client.files.upload(file=audio_filename)
        
        prompt = f"Listen to this audio from {mentor_name}. Transcribe the core financial and stock market insights into a detailed text summary. Ignore fluff and promotional content."
        
        response = client.models.generate_content(model="gemini-1.5-flash", contents=[prompt, uploaded_file])
        
        return response.text

    except Exception as e:
        print(f"Audio extraction failed for {video_id}: {str(e)}")
        return None
        
    finally:
        if uploaded_file:
            try:
                client.files.delete(name=uploaded_file.name)
            except Exception:
                pass
        if audio_filename and os.path.exists(audio_filename):
            os.remove(audio_filename)

def fetch_mentor_transcripts(mentor_names: list) -> dict:
    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    transcripts = {}
    
    for name in mentor_names:
        print(f"Locating channel for: {name}...")
        channel_id = get_channel_id(name)
        
        if not channel_id:
            continue
            
        video_id = get_latest_video_id(channel_id)
        if not video_id:
            continue
            
        # First attempt: The fast, cheap text transcript
        transcript = get_transcript(video_id)
        
        if transcript:
            transcripts[name] = transcript
        else:
            # Second attempt: The audio fallback
            audio_summary = extract_audio_with_gemini(video_id, name)
            if audio_summary:
                transcripts[name] = audio_summary
            
    return transcripts