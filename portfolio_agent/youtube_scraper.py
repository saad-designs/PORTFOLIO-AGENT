import os
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi

YOUTUBE_API_KEY = os.getenv("GOOGLE_API_KEY") 

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

def fetch_mentor_transcripts(mentor_names: list) -> dict:
    """
    Takes a list of mentor names, resolves their channels, 
    and returns a dictionary mapping the mentor to their latest video transcript.
    """
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
            
        transcript = get_video_transcript(video_id)
        if transcript:
            transcripts[name] = transcript
            
    return transcripts
