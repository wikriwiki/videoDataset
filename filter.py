import json
import logging
import os
import csv
from typing import List, Dict
import pandas as pd

# Logging configuration
logging.basicConfig(
    filename="person_search.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def find_persons_in_json(json_file: str, person_list: List[str]) -> List[Dict]:
    if not os.path.exists(json_file):
        logging.error(f"JSON file {json_file} does not exist.")
        print(f"Error: JSON file {json_file} does not exist.")
        return []

    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        logging.info(f"Successfully loaded JSON file {json_file}.")
    except json.JSONDecodeError:
        logging.error(f"JSON file {json_file} has an invalid format.")
        print(f"Error: JSON file {json_file} has an invalid format.")
        return []
    except Exception as e:
        logging.error(f"Error loading JSON file {json_file}: {e}")
        print(f"Error loading JSON file {json_file}: {e}")
        return []

    matched_videos = []

    if isinstance(data, dict):  # dict 구조 대응
        for channel_id, videos in data.items():
            logging.info(f"Processing videos for channel {channel_id}...")
            for video in videos:
                matched_videos.extend(_match_video(video, person_list))
    elif isinstance(data, list):  # list 구조 대응
        for video in data:
            matched_videos.extend(_match_video(video, person_list))
    else:
        logging.warning("Unsupported JSON structure.")
        return []

    logging.info(f"Found persons in {len(matched_videos)} videos from {json_file}.")
    return matched_videos


def _match_video(video: Dict, person_list: List[str]) -> List[Dict]:
    """Helper function to match persons in a single video entry."""
    title = video.get("title", "")
    description = video.get("description", "")
    content = (title + " " + description).lower()
    found_persons = [person for person in person_list if person.lower() in content]

    if found_persons:
        return [{
            "video_id": video.get("video_id", ""),
            "url": video.get("url", ""),
            "title": title,
            "channel_name": video.get("channel_name", ""),
            "published_at": video.get("published_at", ""),
            "view_count": video.get("view_count", 0),
            "persons_found": ",".join(sorted(found_persons)),
            "total_persons": len(found_persons)
        }]
    return []


def save_results_to_csv(results: List[Dict], csv_file: str) -> None:
    csv_fields = ["video_id", "url", "title", "channel_name", "published_at", "view_count", "persons_found", "total_persons"]
    sorted_results = sorted(results, key=lambda x: x["total_persons"], reverse=True)

    file_exists = os.path.isfile(csv_file)

    try:
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=csv_fields)

            if not file_exists:
                writer.writeheader()

            writer.writerows(sorted_results)

        logging.info(f"Appended {len(sorted_results)} results to {csv_file}.")
        print(f"Appended {len(sorted_results)} results to {csv_file}")
    except IOError as e:
        logging.error(f"Failed to write to CSV file {csv_file}: {e}")
        print(f"Error: Failed to write to CSV file {csv_file}: {e}")


def main():
    json_files = [
        "Al_Jazeera_English_kept.json",
        "BBC_News_kept.json",
        "Bloomberg_Television_kept.json",
        "CBC_kept.json",
        "CNBC_kept.json",
        "CNN_kept.json",
        "Fox_News_kept.json",
        "Guardian_News_kept.json",
        "UK_Parliament_kept.json"
    ]

    csv_file = "person_search_results.csv"
    person_csv = "final.csv"

    # Load person list
    try:
        df = pd.read_csv(person_csv)
        if "name" not in df.columns:
            raise ValueError("'name' column not found in final.csv")
        person_list = df["name"].dropna().astype(str).tolist()
        logging.info(f"Loaded {len(person_list)} person names from {person_csv}.")
    except Exception as e:
        logging.error(f"Failed to load person list from {person_csv}: {e}")
        print(f"Error: Failed to load person list from {person_csv}: {e}")
        return

    # Optional: delete previous CSV if fresh start needed
    # if os.path.exists(csv_file):
    #     os.remove(csv_file)

    # Process each JSON file
    for json_file in json_files:
        print(f"\n Processing {json_file}...")
        results = find_persons_in_json(json_file, person_list)
        if results:
            save_results_to_csv(results, csv_file)
        else:
            print("No matching videos found.")


if __name__ == "__main__":
    main()
