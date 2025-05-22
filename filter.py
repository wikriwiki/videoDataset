import json
import sys
import logging
import time
import os
import csv
import openai
import pandas as pd
from typing import List, Dict

# Logging configuration
logging.basicConfig(
    filename="person_search.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

openai.api_key = os.getenv("OPENAI_API_KEY")

def detect_persons(text: str, person_list: List[str], model: str = "gpt-4o", retry_limit: int =3) -> List[str]:
    """Uses GPT to detect which persons from person_list appear in the given text. Retruns a list of matched names exactly in person_list"""
    prompt = ("Given the following list of famous individuals:\n" + json.dumps(person_list, ensure_ascii=False, indent=2) +
    "\n\nAnd the following text (title and description):\n" + text +
    "\n\nIdentify which names from the list are mentioned or referred to.\n"
    "Respond with a JSON array of the matched names, exactly as they appear in the input list."
    )

    messages = [
        {
            "role": "system",
            "content": (
                "You are a knowledgeable assistant that extracts names from text."
            )
        },
        {
            "role": "user",
            "content": prompt
        }
    ]

    for attempt in range(1, retry_limit+1):
        try:
            response = openai.chat.completions.create(
                model=model,
                messages=messages,
                temperature =0.0
            )
            content = response.choices[0].message.content.strip()
            #Now parse the JSON array from assistant reply
            matches = json.loads(content)
            if isinstance(matches, list):
                return matches
            else:
                logging.warning(f"Invalid response format, expected list but got: {matches}")
        except Exception as e:
            logging.warning(f"ChatGPT attempt {attempt} failed: {e}")
            time.sleep(2 ** attempt)
        #Return empty if all retries fail
    return []


def find_persons_in_json(json_file: str, person_list: List[str], model: str = "gpt-4o") -> List[Dict]:
    """
    Reads video data from a JSON file and searches for specified persons in titles and descriptions.
    
    Args:
        json_file (str): Path to the JSON file (e.g., channel_videos_cache.json)
        person_list (List[str]): List of person names to search for
    
    Returns:
        List[Dict]: List of video information where persons were found. Each video includes
                   video_id, url, title, channel_name, published_at, view_count, persons_found,
                   and total_persons.
    """
    # Check if JSON file exists
    if not os.path.exists(json_file):
        logging.error(f"JSON file {json_file} does not exist.")
        print(f"Error: JSON file {json_file} does not exist.")
        return []

    # Load JSON file
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        logging.info(f"Successfully loaded JSON file {json_file}.")
    except Exception as e:
        logging.error(f"Error loading JSON file {json_file}: {e}")
        print(f"Error loading JSON file {json_file}: {e}")
        return []

    # List to store results
    matched_videos = []
    total_videos = sum(len(videos) for videos in data.values() if isinstance(videos,list))
    processed = 0

    # JSON data has channel IDs as keys and lists of videos as values
    for channel_id, videos in data.items():
        if not isinstance(videos, list):
            continue
        for video in videos:
            processed+= 1
            title = video.get('title', '')
            description = video.get('description', '')
            text = title + "\n" + description

            persons = detect_persons(text, person_list, model=model)

            # Add to results
            if persons:
                video_info = {
                    'video_id': video.get('video_id', ''),
                    'url': video.get('url', ''),
                    'title': title,
                    'channel_name': video.get('channel_name', ''),
                    'published_at': video.get('published_at', ''),
                    'view_count': video.get('view_count', 0),
                    'persons_found': ",".join(sorted(persons)),
                    "total_persons": len(persons)  # Number of persons found
                }
                matched_videos.append(video_info)
                logging.info(f"[{processed}/{total_videos}] Matched {persons} in video {video_info['video_id']}")
            # throttle to avoid rate limits
            time.sleep(0.5)

    logging.info(f"Found persons in {len(matched_videos)} out of {total_videos} videos.")
    return matched_videos

def save_results_to_csv(results: List[Dict], csv_file: str) -> None:
    """
    Saves search results to a CSV file, sorted by total_persons in descending order.
    
    Args:
        results (List[Dict]): List of video information to save
        csv_file (str): Path to the CSV file
    """
    # CSV fields
    fields = ['video_id', 'url', 'title', 'channel_name', 'published_at', 'view_count', 'persons_found', 'total_persons']
    
    # Sort results by total_persons in descending order
    sorted_results = sorted(results, key=lambda x: x['total_persons'], reverse=True)
    
    try:
        with open(csv_file, "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(sorted_results)
        logging.info(f"Saved {len(sorted_results)} records to {csv_file}.")
        print(f"Saved results to {csv_file}. Total {len(sorted_results)} videos.")
    except Exception as e:
        logging.error(f"Failed to write CSV file {csv_file}: {e}")
        print(f"Error: Failed to save CSV file {csv_file}: {e}")

def main():
    """Main function to perform person search in JSON file."""
    json_file = 'channel_videos_cache.json'  # Input JSON file
    csv_file = 'person_search_results.csv'   # Output CSV file
    df = pd.read_csv('final.csv')
    person_list = df['name']  # List of persons to search for
    # Perform person search
    results = find_persons_in_json(json_file, person_list)
    
    # Save to CSV if results exist
    if results:
        save_results_to_csv(results, csv_file)
    else:
        logging.info("No matches found.")
        print("No videos found with specified persons.")

if __name__ == "__main__":
    main()