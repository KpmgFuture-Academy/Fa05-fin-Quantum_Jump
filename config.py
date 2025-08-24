# config.py (누락된 설정들 추가)

import os
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# FastAPI 설정
API_TITLE = "오르다 투자 학습 플랫폼 API"
API_VERSION = "1.0.0"
API_DESCRIPTION = "MySQL + Docker + 백그라운드 자동 파이프라인을 사용한 뉴스 분석 및 모의투자 시스템"

# CORS 설정
CORS_ALLOW_ORIGINS = ["*"]  # 프로덕션에서는 구체적인 도메인으로 제한

# MySQL 데이터베이스 설정
DATABASE_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3308")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "password"),
    "database": os.getenv("MYSQL_DATABASE", "orda_db"),
    "charset": "utf8mb4",
    "autocommit": True
}

# yfinance 종목 코드 설정 (추가)
YFINANCE_TICKER_SUFFIX_KOSPI = ".KS"     # 코스피 종목 접미사
YFINANCE_TICKER_SUFFIX_KOSDAQ = ".KQ"    # 코스닥 종목 접미사

# OpenAI 설정
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")

# Pinecone 설정  
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "ordaproject")

# 파일 경로 설정
DATA_DIR = "data"
DATA2_DIR = "data2"
STATIC_DIR = "static"

# 로깅 설정
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = "application.log"

# 백그라운드 파이프라인 설정
PIPELINE_SCHEDULE_MINUTES = int(os.getenv("PIPELINE_SCHEDULE_MINUTES", "60"))
PIPELINE_ISSUES_PER_CATEGORY = int(os.getenv("PIPELINE_ISSUES_PER_CATEGORY", "10"))
PIPELINE_TARGET_FILTERED_COUNT = int(os.getenv("PIPELINE_TARGET_FILTERED_COUNT", "5"))

# 크롤링 설정
CRAWLING_HEADLESS = os.getenv("CRAWLING_HEADLESS", "true").lower() == "true"
CRAWLING_TIMEOUT = int(os.getenv("CRAWLING_TIMEOUT", "30"))

# 시뮬레이션 설정
SIMULATION_MIN_INVESTMENT = int(os.getenv("SIMULATION_MIN_INVESTMENT", "10000"))      # 최소 1만원
SIMULATION_MAX_INVESTMENT = int(os.getenv("SIMULATION_MAX_INVESTMENT", "100000000"))  # 최대 1억원
SIMULATION_MAX_PERIOD_MONTHS = int(os.getenv("SIMULATION_MAX_PERIOD_MONTHS", "24"))   # 최대 24개월

print(f"✅ Config 로드 완료 - MySQL 포트: {DATABASE_CONFIG['port']}")
print(f"📊 yfinance 설정 - KOSPI: {YFINANCE_TICKER_SUFFIX_KOSPI}, KOSDAQ: {YFINANCE_TICKER_SUFFIX_KOSDAQ}")