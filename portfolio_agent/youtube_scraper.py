import os
import glob
import yt_dlp
from google import genai
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi

YOUTUBE_API_KEY = os.getenv("GOOGLE_API_KEY") 
client = genai.Client(api_key=YOUTUBE_API_KEY)

def get_channel_id(youtube, channel_name: str) -> str:
    """Dynamically searches for a channel by name and returns its official ID."""
    try:
        request = youtube.search().list(
            part="id",
            q=channel_name,
            type="channel",
            maxResults=1
        )
        response = request.execute()
        if response.get('items'):
            return response['items'][0]['id']['channelId']
        return None
    except Exception as e:
        print(f"Failed to find channel '{channel_name}': {str(e)}")
        return None

def get_latest_video_id(youtube, channel_id: str) -> str:
    """Queries the YouTube API for the most recently uploaded video ID."""
    try:
        request = youtube.search().list(
            part="id",
            channelId=channel_id,
            maxResults=1,
            order="date",
            type="video"
        )
        response = request.execute()
        
        if response.get('items'):
            return response['items'][0]['id']['videoId']
        return None
    except Exception as e:
        print(f"Failed to fetch video for channel {channel_id}: {str(e)}")
        return None

def get_video_transcript(video_id: str) -> str:
    """Extracts the transcript and joins it into a single text block."""
    try:
        # We attempt to fetch English, Urdu, or auto-generated transcripts
        transcript_list = YouTubeTranscriptApi.get_transcript(video_id, languages=['en', 'ur'])
        full_text = " ".join([fragment['text'] for fragment in transcript_list])
        return full_text
    except Exception as e:
        print(f"Transcript Error for video {video_id}: {str(e)}")
        return None

def extract_audio_with_gemini(video_id: str, mentor_name: str) -> str:
    """
    Fallback mechanism: Downloads the audio stream and uses Gemini 1.5 Flash 
    to transcribe and summarize it when YouTube subtitles are disabled.
    """
    print(f"⚠️ Subtitles disabled for {mentor_name}. Triggering audio extraction fallback...")
    
    # yt-dlp config: Download ONLY the lowest memory audio format directly. No video.
    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': f'{video_id}.%(ext)s',
        'quiet': True,
        'no_warnings': True
    }
    
    audio_filename = None
    uploaded_file = None
    
    try:
        # 1. Download the audio file to the Render server
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f'https://www.youtube.com/watch?v={video_id}'])
            
        # Locate the downloaded file (extensions vary by YouTube's backend delivery)
        search_files = glob.glob(f'{video_id}.*')
        if not search_files:
            return None
        audio_filename = search_files[0]
        
        # 2. Upload to Google's servers
        print(f"Uploading {audio_filename} to Gemini for processing...")
        uploaded_file = client.files.upload(file=audio_filename)
        
        # 3. Prompt the model to extract the data natively from the audio
        prompt = f"Listen to this audio from {mentor_name}. Transcribe the core financial and stock market insights into a detailed text summary. Ignore fluff and promotional content."
        
        response = client.models.generate_content(model="gemini-1.5-flash", contents=[prompt, uploaded_file])
        
        return response.text

    except Exception as e:
        print(f"Audio extraction failed for {video_id}: {str(e)}")
        return None
        
    finally:
        # 4. CRITICAL: Prevent server disk space exhaustion by deleting the files
        if uploaded_file:
            client.files.delete(name=uploaded_file.name)
        if audio_filename and os.path.exists(audio_filename):
            os.remove(audio_filename)

def fetch_mentor_transcripts(mentor_names: list) -> dict:
    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    transcripts = {}
    
    for name in mentor_names:
        print(f"Locating channel for: {name}...")
        channel_id = get_channel_id(youtube, name)
        
        if not channel_id:
            continue
            
        video_id = get_latest_video_id(youtube, channel_id)
        if not video_id:
            continue
            
        # First attempt: The fast, cheap text transcript
        transcript = get_video_transcript(video_id)
        
        if transcript:
            transcripts[name] = transcript
        else:
            # Second attempt: The audio fallback
            audio_summary = extract_audio_with_gemini(video_id, name)
            if audio_summary:
                transcripts[name] = audio_summary
            
    return transcripts
