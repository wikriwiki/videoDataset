import googleapiclient.discovery
import googleapiclient.errors
import json
import os
import time
import logging
import urllib3
import random
from typing import List, Dict

# 로깅 설정
logging.basicConfig(
    filename="fetch_channel_videos.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# SSL 경고 비활성화 (개발용; 운영 환경에서는 SSL 인증서 설정 권장)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# YouTube API 설정
API_KEY =  os.getenv("YOUTUBE_API_KEY") # 여기에 실제 YouTube Data API v3 키를 입력하세요
youtube = None
if API_KEY == "YOUR_API_KEY":
    logging.warning("API_KEY가 설정되지 않았습니다. 'YOUR_API_KEY'를 실제 API 키로 교체하세요.")
    print("경고: 'YOUR_API_KEY'를 실제 YouTube Data API v3 키로 교체하세요.")
else:
    try:
        youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API_KEY)
        logging.info("YouTube API 클라이언트가 성공적으로 초기화되었습니다.")
    except Exception as e:
        logging.error(f"YouTube API 클라이언트 초기화 실패: {e}")
        print(f"YouTube API 클라이언트 초기화 실패: {e}. API 키를 확인하세요.")

# 설정
MIN_VIEW_COUNT = 50000  # 최소 조회수 기준
PUBLISHED_AFTER_DATE = "2023-01-01T00:00:00Z"  # RFC3339 형식 (UTC)
CACHE_FILE = "US shows.json"  # 캐시 파일 경로
API_CALL_DELAY = 1.5  # API 호출 간 지연 시간 (초)
MAX_API_RETRIES = 5  # 최대 API 재시도 횟수

def load_cache() -> Dict:
    """캐시 파일에서 데이터를 로드합니다."""
    if not os.path.exists(CACHE_FILE):
        return {}
    
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        logging.warning(f"캐시 파일 {CACHE_FILE}이 손상되었습니다. 빈 캐시로 시작합니다.")
        return {}
    except Exception as e:
        logging.error(f"캐시 파일 {CACHE_FILE} 로드 중 오류: {e}")
        return {}

def save_cache(cache: Dict) -> None:
    """데이터를 캐시 파일에 저장합니다."""
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        logging.info("캐시가 성공적으로 저장되었습니다.")
    except IOError as e:
        logging.error(f"캐시 파일 {CACHE_FILE} 저장 실패: {e}")

def get_videos_from_channel(channel_id: str, cache: Dict) -> List[Dict]:
    """
    지정된 채널에서 기준(게시 날짜, 최소 조회수)을 만족하는 영상을 가져옵니다.
    
    Args:
        channel_id (str): YouTube 채널 ID
        cache (Dict): 캐시 데이터
    
    Returns:
        List[Dict]: 영상 정보 리스트 (video_id, url, title, description, channel_id,
                   channel_name, published_at, view_count 포함)
    """
    # 캐시 확인
    if channel_id in cache:
        logging.info(f"채널 {channel_id}의 캐시 데이터 사용")
        return cache[channel_id]

    if youtube is None:
        logging.error("YouTube API 클라이언트가 초기화되지 않았습니다.")
        return []

    logging.info(f"채널 {channel_id}의 영상을 {PUBLISHED_AFTER_DATE} 이후로 가져오는 중")
    videos_details = []
    next_page_token = None

    while True:
        for attempt in range(MAX_API_RETRIES):
            try:
                time.sleep(API_CALL_DELAY)
                search_request = youtube.search().list(
                    part="snippet",
                    channelId=channel_id,
                    type="video",
                    publishedAfter=PUBLISHED_AFTER_DATE,
                    order="date",
                    maxResults=50,
                    pageToken=next_page_token
                )
                search_response = search_request.execute()
                break
            except googleapiclient.errors.HttpError as e:
                logging.error(f"채널 {channel_id} 검색 API 오류 (시도 {attempt+1}/{MAX_API_RETRIES}): {e}")
                if e.resp.status in [403, 500, 503] and attempt < MAX_API_RETRIES - 1:
                    wait_time = (2 ** attempt) + random.random()
                    logging.info(f"{wait_time:.2f}초 후 재시도...")
                    time.sleep(wait_time)
                    continue
                logging.error(f"채널 {channel_id} 검색 결과 가져오기 실패 (최대 재시도 초과)")
                return []
            except Exception as e:
                logging.error(f"검색 API 호출 중 예기치 않은 오류 (채널 {channel_id}): {e}")
                return []

        video_ids = [item["id"]["videoId"] for item in search_response.get("items", []) 
                    if item["id"]["kind"] == "youtube#video"]

        if not video_ids:
            logging.info(f"채널 {channel_id}에서 더 이상 영상을 찾을 수 없습니다.")
            break

        # 영상 상세 정보 가져오기
        for i in range(0, len(video_ids), 50):
            batch_video_ids = video_ids[i:i+50]
            try:
                time.sleep(API_CALL_DELAY)
                video_stats_request = youtube.videos().list(
                    part="snippet,statistics",
                    id=",".join(batch_video_ids)
                )
                video_stats_response = video_stats_request.execute()

                for item in video_stats_response.get("items", []):
                    view_count = item.get("statistics", {}).get("viewCount")
                    if view_count is None:
                        logging.warning(f"영상 {item['id']}에 조회수 정보가 없습니다. 건너뜁니다.")
                        continue
                    try:
                        view_count = int(view_count)
                        if view_count >= MIN_VIEW_COUNT:
                            videos_details.append({
                                "video_id": item["id"],
                                "url": f"https://www.youtube.com/watch?v={item['id']}",
                                "title": item["snippet"]["title"],
                                "description": item["snippet"].get("description", ""),
                                "channel_id": item["snippet"]["channelId"],
                                "channel_name": item["snippet"]["channelTitle"],
                                "published_at": item["snippet"]["publishedAt"],
                                "view_count": view_count
                            })
                            logging.info(f"영상 {item['id']} 수집됨 ({view_count} 조회수)")
                        else:
                            logging.info(f"영상 {item['id']} 조회수 {view_count}는 기준 미달")
                    except ValueError:
                        logging.warning(f"영상 {item['id']}의 조회수 '{view_count}' 변환 실패. 건너뜁니다.")
            except googleapiclient.errors.HttpError as e:
                logging.error(f"영상 목록 API 오류 (채널 {channel_id}, 배치 시작 ID {batch_video_ids[0]}): {e}")
                continue
            except Exception as e:
                logging.error(f"영상 목록 API 호출 중 예기치 않은 오류 (채널 {channel_id}): {e}")
                continue

        next_page_token = search_response.get("nextPageToken")
        if not next_page_token:
            logging.info(f"채널 {channel_id}의 추가 페이지 없음")
            break

    # 캐시 업데이트
    cache[channel_id] = videos_details
    save_cache(cache)
    logging.info(f"채널 {channel_id}에서 {len(videos_details)}개 영상 수집 완료")
    return videos_details

def main():
    """지정된 채널에서 영상을 가져오는 메인 함수."""
    if youtube is None:
        print("YouTube API 클라이언트가 초기화되지 않았습니다. API 키를 확인하세요.")
        return

    # 테스트용 채널 ID 목록
    trusted_channel_ids = [
        ### US shows
        "UC8-Th83bH_thdKZDJCrn88g",  # Jimmy Fallon Show
        "UCMtFAi84ehTSYSE9XoHefig",  # Stephen Colbert
        "UCa6vGFO9ty8v5KZJXQxdhaw",  # Jimmy Kimmel Live
        "UCVTyTA7-g9nopHeHbeuvpRA",  # Seth Meyers
        "UCJ0uqCI0Vqr2Rrt1HseGirg",  # James Corden Show
        "UCzQUP1qoWDoEbmsQxvdjxgQ",  # Joe Rogan Podcast

        # ## UK Shows
        # "UCe5q9904G9h0m_YfO_4uSg",   # Graham Norton Show
        # "UCgurmV2nVq_1DUb2pvGOKmg",  # Johnathan Ross Show
        # "UC_MlPthVWFLMl4Lajr1F3KA",  # Sunday Brunch
        # "UCuzIhL00Didg_5-w1BOtjXg",  # Piers Morgan

        # ### Other Entertainment stuff
        # "UCftwRNsjfRo08xYE31tkiyw",  #Wired
        # "UCIsbLox_y9dCIMLd8tdC6qg",  #Vanity Fair
        # "UCsEukrAd64fqA7FjwkmZ_Dw",  #GQ
        # "UClWCQNaggkMW7SDtS3BkEBg",  #Entertainment Weekly
        # "UCPD_bxCRGpmmeQcbe2kpPaA",  #First we Feast
        # "UCGbQJy-531_5vfphay-rChQ",  #People
        # "UCgRQHK8Ttr1j9xCEpCAlgbQ",  #Variety
        # "UC9ZmFEgVwLieP6FZYtAsy9g",  #Billboard Music Awards
        # "UCgXYV8asjAC9iLPnpe43-GQ",  #Oscar Awards

        # ### Global News
        # "UCupvZG-5ko_eiXAupbDfxWw",  #CNN
        # "UC16niRr50-MSBwiO3YDb3RA",  #BBC NEWS
        # "UCqnbDFdCpuN8CMEg0VuEBqA",  #The New York Times
        # "UChqUTb7kYRX8-EiaN3XFrSQ",  #Reuters
        # "UCHd62-u_v4DvJ8TCFtpi4GA",  #The Washington Post
        # "UCNye-wNBqNL5ZzHSJj3l8Bg",  #Al Jazeera English
        # "UCIRYBXDze5krPDzAEOxFGVA",  #The Guardian News
        # "UCHTK-2W11Vh1V4uwofOfR4w",  # Associated Press
        # "UCeY0bbntWzzVIaj2z3QigXg",  # NBC News
        # "UCXIJgqnII2ZOINSWNOGFThA",  # Fox News Channel
        # "UCIALMKvObZNtJ6AmdCLP7Lg",  # Bloomberg Television
        # "UCvJJ_dzjViJCoLf5uKUTwoA",  # CNBC
        # "UC6ZFN9Tx6xh-skXCuRHCDpQ",  # PBS NewsHour
        # "UCuFFtHWoLl5fauMMD5Ww2jA",  # CBC
        # "UChLtXXpo4Ge1ReTEboVvTDg",  # Global news
        # "UC5aNPmKYwbudeNngDMTY3lw",  # CTV news
        # "UCI_4WZYDHY9Dvb_zUq1_4HQ"   # UK Parliament

    ]

    cache = load_cache()
    all_videos = {}

    for channel_id in trusted_channel_ids:
        print(f"채널 처리 중: {channel_id}")
        logging.info(f"채널 처리 시작: {channel_id}")
        videos = get_videos_from_channel(channel_id, cache)
        all_videos[channel_id] = videos
        logging.info(f"채널 {channel_id} 처리 완료: {len(videos)}개 영상")

    # 결과 캐시 파일에 저장 (중복 저장 방지)
    save_cache(all_videos)
    print(f"모든 채널 처리 완료. 결과는 {CACHE_FILE}에 저장되었습니다.")

if __name__ == "__main__":
    if API_KEY == "YOUR_API_KEY":
        print("\n===== 중요: API 키 설정 필요 =====")
        print("'YOUR_API_KEY'를 실제 YouTube Data API v3 키로 교체하세요.")
        print("API 키 없이는 스크립트가 동작하지 않습니다.")
        print("===================================")
    else:
        main()