import os
import time
import json
import logging
import random
import urllib3
from dotenv import load_dotenv
from typing import Dict, List
import googleapiclient.discovery
import googleapiclient.errors

# --- Logging setup ---
logging.basicConfig(
    filename="fetch_channel_videos.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
# Disable SSL warnings (for development; configure SSL properly in production)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Load environment & initialize YouTube API client ---
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY or API_KEY == "YOUR_API_KEY":
    logging.error("YOUTUBE_API_KEY 환경변수가 설정되지 않았습니다.")
    print("경고: 'YOUTUBE_API_KEY' 환경변수 또는 API 키를 설정하세요.")
    youtube = None
else:
    try:
        youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API_KEY)
        logging.info("YouTube API client initialized successfully.")
    except Exception as e:
        logging.error(f"YouTube API initialization failed: {e}")
        youtube = None

# --- Configuration ---
MIN_VIEW_COUNT = 50000
PUBLISHED_AFTER_DATE = "2023-01-01T00:00:00Z"
CACHE_FILE = "US_shows.json"
API_CALL_DELAY = 1.5
MAX_API_RETRIES = 5


def load_cache() -> Dict:
    """Load cache from disk (returns empty if file missing or invalid)."""
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.warning(f"Failed to load cache: {e}")
        return {}


def save_cache(cache: Dict) -> None:
    """Persist cache to disk."""
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        logging.info("Cache saved successfully.")
    except IOError as e:
        logging.error(f"Failed to save cache: {e}")


def get_videos_from_channel(channel_id: str, cache: Dict) -> Dict[str, List[Dict]]:
    """
    Fetches all uploads via the channel's uploads playlist, then filters videos
    published after PUBLISHED_AFTER_DATE with at least MIN_VIEW_COUNT views.
    Splits results into 'kept' and 'skipped'.
    """
    cache_key = f"{channel_id}|views={MIN_VIEW_COUNT}|after={PUBLISHED_AFTER_DATE}"
    if cache_key in cache:
        logging.info(f"Using cached results for {channel_id}.")
        return cache[cache_key]

    if youtube is None:
        logging.error("YouTube API client not initialized.")
        return {"kept": [], "skipped": []}

    # 1) Get the uploads playlist ID
    try:
        ch_resp = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        ).execute()
        uploads_pl = (
            ch_resp["items"][0]
                ["contentDetails"]
                ["relatedPlaylists"]
                ["uploads"]
        )
    except Exception as e:
        logging.error(f"Failed to retrieve uploads playlist for {channel_id}: {e}")
        return {"kept": [], "skipped": []}

    # 2) Page through all playlist items to collect every video ID
    all_video_ids: List[str] = []
    next_token = None
    while True:
        try:
            time.sleep(API_CALL_DELAY)
            pl_req = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=uploads_pl,
                maxResults=50,
                pageToken=next_token
            ).execute()
            for item in pl_req.get("items", []):
                vid = item["contentDetails"]["videoId"]
                all_video_ids.append(vid)
            next_token = pl_req.get("nextPageToken")
            if not next_token:
                break
        except Exception as e:
            logging.error(f"Error fetching playlist items for {channel_id}: {e}")
            break

    logging.info(f"Channel {channel_id}: found {len(all_video_ids)} total uploads.")

    # 3) Batch-fetch stats and apply filters
    kept: List[Dict] = []
    skipped: List[Dict] = []
    for i in range(0, len(all_video_ids), 50):
        batch = all_video_ids[i : i + 50]
        try:
            time.sleep(API_CALL_DELAY)
            stats_resp = youtube.videos().list(
                part="snippet,statistics",
                id=",".join(batch)
            ).execute()
            for item in stats_resp.get("items", []):
                vid = item["id"]
                sn = item["snippet"]
                stats = item.get("statistics", {})
                pub = sn.get("publishedAt", "")
                vc = int(stats.get("viewCount", 0))
                info = {
                    "video_id": vid,
                    "url": f"https://youtu.be/{vid}",
                    "title": sn.get("title", ""),
                    "description": sn.get("description", ""),
                    "channel_id": sn.get("channelId", ""),
                    "channel_name": sn.get("channelTitle", ""),
                    "published_at": pub,
                    "view_count": vc,
                }
                if pub >= PUBLISHED_AFTER_DATE and vc >= MIN_VIEW_COUNT:
                    kept.append(info)
                else:
                    skipped.append(info)
        except Exception as e:
            logging.error(f"Error fetching stats for batch starting at {batch[0]}: {e}")
            continue

    # Cache results and return
    result = {"kept": kept, "skipped": skipped}
    cache[cache_key] = result
    save_cache(cache)
    logging.info(f"Channel {channel_id}: {len(kept)} kept, {len(skipped)} skipped.")
    return result


def main():
    if youtube is None:
        print("YouTube API client not initialized. Check your API key.")
        return

    # Example channel IDs
    trusted_channel_ids = [
        "UC8-Th83bH_thdKZDJCrn88g",  # Jimmy Fallon
        "UCMtFAi84ehTSYSE9XoHefig",  # Stephen Colbert
        "UCa6vGFO9ty8v5KZJXQxdhaw",  # Jimmy Kimmel
        "UCVTyTA7-g9nopHeHbeuvpRA",  # Seth Meyers
        "UCJ0uqCI0Vqr2Rrt1HseGirg",  # James Corden
        "UCzQUP1qoWDoEbmsQxvdjxgQ",  # Joe Rogan
    ]

    cache = load_cache()
    all_kept: Dict[str, List[Dict]] = {}
    all_skipped: Dict[str, List[Dict]] = {}

    for cid in trusted_channel_ids:
        print(f"Processing channel {cid}...")
        videos = get_videos_from_channel(cid, cache)
        all_kept[cid] = videos["kept"]
        all_skipped[cid] = videos["skipped"]

    # Save final JSON outputs
    with open("US_shows_kept.json", "w", encoding="utf-8") as f:
        json.dump(all_kept, f, ensure_ascii=False, indent=2)
    with open("US_shows_skipped.json", "w", encoding="utf-8") as f:
        json.dump(all_skipped, f, ensure_ascii=False, indent=2)

    print("All channels processed. Kept/skipped data saved.")

if __name__ == "__main__":
    main()