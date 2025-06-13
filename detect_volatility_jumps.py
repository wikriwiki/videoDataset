#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
detect_volatility_jumps.py
────────────────────────────────────────────────────────────
1. scaled_trends.csv  (name,date,scaled_value,…) 로드
2. 하루 변화율(pct_change) → 14일 롤링 표준편차(변동성) 계산
3. 변동성의 절대 증가량이 개별 키워드별 MAD×3 초과하면 vol_jump = True
4. 결과 CSV:
   • data/volatility_jumps.csv   (점프 발생 행만)
   • data/volatility_full.csv    (전체 행 + vol_jump 열)
"""

from pathlib import Path
import numpy as np
import pandas as pd

# ───────────────────────── 파일 경로
DATA_DIR = Path("data")
SRC  = DATA_DIR / "scaled_trends.csv"
OUT1 = DATA_DIR / "volatility_jumps.csv"
OUT2 = DATA_DIR / "volatility_full.csv"

# ───────────────────────── 하이퍼파라미터
WIN  = 14      # 롤링 윈도우(일) – 변동성 계산용
MULT = 8       # MAD 배수 – 임계값 민감도

def mad(arr: pd.Series) -> float:
    """Median Absolute Deviation – outlier에 강인한 분산 척도"""
    return np.median(np.abs(arr - np.median(arr)))

def main():
    if not SRC.exists():
        raise FileNotFoundError(f"{SRC} not found. 먼저 크롤링 스크립트를 실행하세요.")

    # 1) 데이터 로드 & 정렬
    df = pd.read_csv(SRC, parse_dates=["date"])
    df.sort_values(["name", "date"], inplace=True)

    # 2) 하루 변화율(전일 대비 %)
    eps = 1e-6
    df["pct_change"] = df.groupby("name")["scaled_value"] \
                         .transform(lambda s: (s - s.shift(1)) / (s.shift(1) + eps))

    # 3) 롤링 변동성(표준편차) – 최근 WIN일
    df["vol"] = df.groupby("name")["pct_change"] \
                  .transform(lambda s: s.rolling(WIN).std())

    # 4) 변동성 증가량
    df["dvol"] = df.groupby("name")["vol"].diff().abs()

    # 5) 키워드별 MAD×MULT 임계값
    df["thr"] = df.groupby("name")["dvol"] \
                  .transform(lambda s: MULT * mad(s.dropna()))

    # 6) 급변 플래그
    df["vol_jump"] = df["dvol"] > df["thr"]

    # 7) 저장
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    jumps = df[df["vol_jump"]].copy()
    jumps.to_csv(OUT1, index=False)
    df.to_csv(OUT2, index=False)

    print(f"✔  저장 완료\n   • {OUT1} ({len(jumps)} rows)\n   • {OUT2} ({len(df)} rows)")

if __name__ == "__main__":
    main()
