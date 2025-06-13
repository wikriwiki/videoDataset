from __future__ import annotations
import os
import sys
import threading
import time

def _schedule_restart(interval_min: int = 30):
    """백그라운드 스레드로 interval_min마다 프로세스를 재실행(execl)"""
    def _worker():
        time.sleep(interval_min * 60)
        # 현재 파이썬 인터프리터로 자기 자신 재실행
        os.execv(sys.executable, [sys.executable] + sys.argv)
    t = threading.Thread(target=_worker, daemon=True)
    t.start()

# 30분마다 스크립트를 완전 재실행
_schedule_restart(30)


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
google_trend_crawl_270.py   (2025-06-12, full standalone)

기능
────
1. final.csv 의 name 열을 읽어 2023-01-01 ~ 2025-05-31(UTC) 구간을
   270 일+1 일 중복 청크로 나눠 일간 트렌드(TIMESERIES) 수집
2. 겹치는 1 일 값을 이용해 체인-링킹(CHAIN-LINK) 방식으로 스케일 조정
   - 시작·끝 값이 0 이면 스케일 불가 → unlinkable_keywords.csv 기록
   - **마지막 청크의 종료일 0 값은 허용**
3. 결과 CSV
   • data/raw_trends.csv        name,date,value,season
   • data/scaled_trends.csv     name,date,value,season,scaled_value
   • data/unlinkable_keywords.csv name,reason

실행
────
pip install python-dotenv pandas python-dateutil serpapi tqdm
python google_trend_crawl_270.py
"""


import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple

import pandas as pd
from dotenv import load_dotenv
from serpapi import GoogleSearch       # pip install google-search-results
from tqdm import tqdm

# ────────────────────────────── 설정
START_DATE   = datetime(2023, 1, 1, tzinfo=timezone.utc)
END_DATE     = datetime(2025, 5, 31, tzinfo=timezone.utc)
CHUNK_DAYS   = 270           # 270-day window (inclusive)
OVERLAP_DAYS = 1             # overlap size

RAW_CSV    = Path("data/raw_trends.csv")
SCALED_CSV = Path("data/scaled_trends.csv")
BAD_CSV    = Path("data/unlinkable_keywords.csv")

LOG_FILE   = Path("logs/trend_crawler.log")
CACHE_DIR  = Path("cache/trends_json")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ────────────────────────────── 로깅
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

file_handler = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(fmt)

console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

logger = logging.getLogger()
if not logger.handlers:
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console)
else:
    # 중복 실행 시 콘솔 핸들러 중복 추가 방지
    if not any(isinstance(h, logging.StreamHandler) and h.stream is sys.stdout
               for h in logger.handlers):
        logger.addHandler(console)

# ────────────────────────────── API 키
load_dotenv()
API_KEY = os.getenv("SERPAPI_KEY")
if not API_KEY:
    logger.error("SERPAPI_KEY 환경변수가 없습니다. .env에 추가하세요.")
    sys.exit(1)

# ────────────────────────────── 유틸
def generate_chunks(start: datetime, end: datetime) -> List[Tuple[datetime, datetime]]:
    """270-day + 1-day overlap 구간 리스트 반환."""
    chunks: List[Tuple[datetime, datetime]] = []
    cur_start = start
    while cur_start <= end:
        cur_end = min(cur_start + timedelta(days=CHUNK_DAYS - 1), end)
        chunks.append((cur_start, cur_end))
        if cur_end >= end:
            break
        cur_start = cur_end - timedelta(days=OVERLAP_DAYS - 1)
    return chunks


def parse_value(val_raw) -> int:
    """'16'·'1,234'·'<1' 등 문자열을 int로 변환, 오류는 0."""
    if val_raw in ("<1", "", None):
        return 0
    if isinstance(val_raw, (int, float)):
        return int(val_raw)
    try:
        return int(str(val_raw).replace(",", "").strip())
    except Exception:  # pragma: no cover
        return 0


def timeline_item_to_row(item: dict) -> dict | None:
    """timeline 항목을 dict(date,value)로 변환."""
    # 날짜
    if "date" in item and item["date"]:
        date_dt = pd.to_datetime(item["date"], utc=True, errors="coerce")
    else:
        date_dt = pd.NaT
    if pd.isna(date_dt) and "timestamp" in item:
        date_dt = pd.to_datetime(int(item["timestamp"]), unit="s", utc=True, errors="coerce")
    if pd.isna(date_dt):
        return None
    # 값
    if "value" in item:
        val_int = parse_value(item["value"])
    else:
        values_arr = item.get("values", [])
        if values_arr and isinstance(values_arr[0], dict):
            val_int = parse_value(values_arr[0].get("value", 0))
        else:
            val_int = 0
    return {"date": date_dt, "value": val_int}


def fetch_chunk(keyword: str, begin: datetime, finish: datetime) -> pd.DataFrame:
    """SerpApi로 한 청크 조회 → DataFrame(date,value)"""
    cache_fp = CACHE_DIR / f"{keyword}_{begin:%Y%m%d}_{finish:%Y%m%d}.json"
    if cache_fp.exists():
        data = json.loads(cache_fp.read_text(encoding="utf-8"))
    else:
        params = {
            "api_key": API_KEY,
            "engine": "google_trends",
            "q": keyword,
            "date": f"{begin:%Y-%m-%d} {finish:%Y-%m-%d}",  # UTC
            "data_type": "TIMESERIES",
            "output": "json",
        }
        try:
            data = GoogleSearch(params).get_dict()
        except Exception as e:
            logger.error("SerpApi 요청 실패 (%s): %s", keyword, e)
            raise
        cache_fp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    timeline = (
        data.get("interest_over_time", {}).get("timeline")
        or data.get("interest_over_time", {}).get("timeline_data")
        or []
    )
    rows = []
    for item in timeline:
        row = timeline_item_to_row(item)
        if row:
            rows.append(row)
    if not rows:
        logger.warning("%s: No data %s → %s", keyword, begin.date(), finish.date())
    return pd.DataFrame(rows)


def has_bad_edges(frames: List[pd.DataFrame]) -> tuple[bool, str]:
    """
    시작·끝(단, 마지막 청크 끝 제외)·overlap 값이 0이면 True, reason 반환.
    """
    if not frames:
        return True, "empty frames"
    # 1) 청크 시작·끝 값
    for idx, df in enumerate(frames):
        if df.empty:
            return True, f"empty chunk {idx+1}"
        start_zero = df.iloc[0]["value"] == 0
        end_zero   = df.iloc[-1]["value"] == 0 and idx != len(frames) - 1
        if start_zero or end_zero:
            return True, "zero start/end"
    # 2) overlap 값
    for i in range(1, len(frames)):
        overlap_val_prev = frames[i-1].iloc[-1]["value"]
        overlap_val_curr = frames[i  ].iloc[0 ]["value"]
        if overlap_val_prev == 0 or overlap_val_curr == 0:
            return True, "zero overlap"
    return False, ""


def rescale_chunks(frames: List[pd.DataFrame]) -> List[pd.DataFrame]:
    """Chain-link scaling (B → A)"""
    if not frames:
        return []
    scaled = [frames[0].copy()]
    scaled[0]["scaled_value"] = scaled[0]["value"]
    for i in range(1, len(frames)):
        prev = scaled[-1]
        curr = frames[i].copy()
        overlap_day = curr.iloc[0]["date"]
        prev_val = prev.loc[prev["date"] == overlap_day, "scaled_value"].values[0]
        curr_val = curr.loc[curr["date"] == overlap_day, "value"].values[0]
        ratio = prev_val / curr_val if curr_val else 1.0
        curr["scaled_value"] = curr["value"] * ratio
        scaled.append(curr)
    return scaled


def append_csv(path: Path, df: pd.DataFrame):
    header = not path.exists()
    df.to_csv(path, mode="a", header=header, index=False)


# ────────────────────────────── main
def main():
    key_csv = Path("final.csv")
    if not key_csv.exists():
        logger.error("final.csv 가 없습니다.")
        return
    names = pd.read_csv(key_csv, usecols=["name"])["name"].dropna().unique().tolist()
    logger.info("Loaded %d keywords", len(names))

    # 스케줄 & CSV 헤더 준비
    schedule = generate_chunks(START_DATE, END_DATE)
    logger.info("Chunk schedule: %d chunks (270d, 1d overlap)", len(schedule))

    RAW_CSV.parent.mkdir(parents=True, exist_ok=True)
    for p in (RAW_CSV, SCALED_CSV, BAD_CSV):
        if not p.exists():
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("")  # 헤더는 append_csv에서 처리

    for kw in tqdm(names, desc="Keywords"):
        raw_frames: List[pd.DataFrame] = []
        for idx, (s, e) in enumerate(schedule):
            logger.debug("%s | chunk %02d: %s → %s", kw, idx + 1, s.date(), e.date())
            df = fetch_chunk(kw, s, e)
            df["season"] = idx + 1
            raw_frames.append(df)

        # edge 0 검증
        bad, reason = has_bad_edges(raw_frames)
        if bad:
            logger.info("⏭  %s skipped  – %s", kw, reason)
            append_csv(BAD_CSV, pd.DataFrame([{"name": kw, "reason": reason}]))
            continue

        raw_all = pd.concat(raw_frames, ignore_index=True)
        raw_all.insert(0, "name", kw)
        append_csv(RAW_CSV, raw_all)

        scaled_frames = rescale_chunks(raw_frames)
        scaled_all = pd.concat(scaled_frames, ignore_index=True)
        scaled_all.insert(0, "name", kw)
        append_csv(SCALED_CSV, scaled_all)

        logger.info("✔︎  %s 완료 (%d rows)", kw, len(raw_all))

    logger.info("All done → %s / %s / %s", RAW_CSV, SCALED_CSV, BAD_CSV)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Exit.")
