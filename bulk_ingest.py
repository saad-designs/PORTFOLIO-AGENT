import asyncio
import yt_dlp
from vector_brain import ingest_knowledge_tool

CHANNEL_HANDLE = "@InvestKaar"
TARGET_ENDPOINTS = [
    f"https://www.youtube.com/{CHANNEL_HANDLE}/videos",
    f"https://www.youtube.com/{CHANNEL_HANDLE}/shorts",
    f"https://www.youtube.com/{CHANNEL_HANDLE}/streams"
]

async def run_historical_backfill():
    print("==================================================")
    print(f"   STARTING FULL CATALOG BACKFILL: {CHANNEL_HANDLE}")
    print("   (Includes Videos, Shorts, and Live Streams)")
    print("==================================================")
    
    ydl_opts = {
        'extract_flat': 'in_playlist',
        'quiet': True,
        'no_warnings': True,
    }
    
    all_videos = {}  # {video_id: full_url} to ensure deduplication
    
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for endpoint in TARGET_ENDPOINTS:
            tab_name = endpoint.split('/')[-1].capitalize()
            print(f"Fetching {tab_name} catalog...")
            try:
                info = ydl.extract_info(endpoint, download=False)
                if 'entries' in info:
                    for entry in info['entries']:
                        if entry and entry.get('id'):
                            vid_id = entry['id']
                            # Normalize all URLs to standard watch format
                            all_videos[vid_id] = f"https://www.youtube.com/watch?v={vid_id}"
            except Exception as e:
                print(f"Warning: Could not fetch {tab_name}: {e}")

    video_list = list(all_videos.values())
    total_videos = len(video_list)
    print(f"\nTotal unique items found (Videos + Shorts): {total_videos}\n")
    
    if total_videos == 0:
        print("No items extracted.")
        return

    # Ingest one by one with rate limiting
    for i, url in enumerate(video_list):
        print(f"[{i+1}/{total_videos}] Ingesting: {url}")
        
        try:
            # Call your single video ingestion tool
            await asyncio.to_thread(ingest_knowledge_tool.invoke, {"input_text_or_url": url})
            print("   -> Ingestion processed.")
        except Exception as e:
            print(f"   FAILED on {url}: {e}")
            
        # Mandatory delay to respect Gemini (15 RPM) and YouTube limits
        if i < total_videos - 1:
            print("   -> Waiting 10 seconds before next item...\n")
            await asyncio.sleep(10)

    print("\n==================================================")
    print("   HISTORICAL BACKFILL (VIDEOS + SHORTS) COMPLETE  ")
    print("==================================================")

if __name__ == "__main__":
    asyncio.run(run_historical_backfill())
