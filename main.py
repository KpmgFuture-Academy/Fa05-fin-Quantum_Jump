# main.py - 최종 통합 버전
import uvicorn
import threading
import time
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

# --- 설정, API 라우터, 서비스 임포트 ---
# config.py에서 API 기본 정보와 CORS 설정값을 가져옵니다.
from config import API_TITLE, API_VERSION, API_DESCRIPTION, CORS_ALLOW_ORIGINS

# api 폴더에 정의된 각 기능별 라우터들을 모두 임포트합니다.
from api import (
    health_api, 
    analysis_api, 
    database_api, 
    news_api, 
    simulation_api, 
    pipeline_api
)

# services 폴더에서 데이터베이스 서비스 모듈을 임포트합니다.
from services import database_service

# 백그라운드에서 주기적으로 데이터 파이프라인을 실행할 클래스를 임포트합니다.
from background_pipeline import BackgroundPipelineExecutor

# --- 전역 변수 ---
# 백그라운드 파이프라인 실행기와 스레드를 관리하기 위한 전역 변수입니다.
pipeline_executor: BackgroundPipelineExecutor = None
pipeline_thread: threading.Thread = None

# --- 백그라운드 작업 함수 ---
def run_background_pipeline():
    """백그라운드 스레드에서 파이프라인을 주기적으로 실행하는 함수"""
    global pipeline_executor
    
    try:
        print("🔄 백그라운드 파이프라인 스레드 시작...")
        pipeline_executor = BackgroundPipelineExecutor()
        
        # 서버 시작 시, 최신 데이터를 즉시 사용할 수 있도록 파이프라인을 1회 실행합니다.
        print("🎬 서버 시작 시 초기 파이프라인 실행...")
        pipeline_executor.run_once()
        
        # 30분(1800초)마다 파이프라인을 반복 실행하는 루프입니다.
        print("⏰ 30분 간격 스케줄러 시작...")
        while True:
            time.sleep(1800)
            print("🔔 30분 경과 - 파이프라인 재실행...")
            try:
                pipeline_executor.run_once()
            except Exception as e:
                print(f"❌ 스케줄 실행 중 오류 발생: {e}")
                # 오류가 발생하더라도 스케줄링은 중단되지 않고 계속됩니다.
                continue
    except Exception as e:
        print(f"❌ 백그라운드 파이프라인 스레드를 시작하지 못했습니다: {e}")

# --- FastAPI Lifespan 이벤트 ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI 앱의 시작과 종료 시점에 실행될 작업을 정의합니다."""
    global pipeline_thread
    
    # === 서버 시작 시 실행 (Startup) ===
    print("🚀 서버 시작: 서비스 및 백그라운드 작업을 초기화합니다...")
    
    # 1. 각종 서비스 초기화 (DB 연결 등)
    try:
        from services import initialize_all_services
        if initialize_all_services():
            print("✅ 모든 서비스 초기화 완료")
        else:
            print("⚠️ 일부 서비스 초기화 실패 - 백그라운드에서 재시도됩니다.")
    except Exception as e:
        print(f"⚠️ 서비스 초기화 중 심각한 오류 발생: {e}")

    # 2. 백그라운드 파이프라인 스레드 시작
    pipeline_thread = threading.Thread(target=run_background_pipeline, daemon=True)
    pipeline_thread.start()
    print("✅ 백그라운드 파이프라인 스레드 시작됨")
    
    yield  # 이 시점에서 실제 FastAPI 서버가 실행됩니다.
    
    # === 서버 종료 시 실행 (Shutdown) ===
    print("👋 서버를 종료합니다...")
    if pipeline_executor:
        try:
            pipeline_executor.shutdown()
            print("✅ 백그라운드 파이프라인 정상 종료")
        except Exception as e:
            print(f"⚠️ 백그라운드 파이프라인 종료 중 오류 발생: {e}")
    
    print("✅ 서버 종료 완료")

# --- FastAPI 앱 생성 및 설정 ---
app = FastAPI(
    title=API_TITLE,
    version=API_VERSION,
    description=API_DESCRIPTION,
    lifespan=lifespan  # 시작/종료 이벤트를 처리할 lifespan 함수를 등록합니다.
)

# CORS 미들웨어 설정 (다른 도메인에서의 API 요청을 허용)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- API 라우터 등록 ---
# 각 기능별로 분리된 API 라우터들을 메인 앱에 등록합니다.
app.include_router(health_api.router, tags=["Health Check"])
app.include_router(news_api.router, prefix="/api/news", tags=["News API"])
app.include_router(analysis_api.router, prefix="/api/analysis", tags=["Analysis API"])
app.include_router(simulation_api.router, prefix="/api/simulation", tags=["Simulation API"])
app.include_router(database_api.router, prefix="/api/database", tags=["Database API"])
app.include_router(pipeline_api.router, prefix="/api/pipeline", tags=["Pipeline API"])

# --- 정적 파일 및 루트 경로 설정 ---
# 'static' 폴더를 정적 파일 디렉토리로 지정하여 HTML, CSS, JS 파일을 서비스합니다.
static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=static_dir, html=True), name="static")

# 루트 경로 ("/")로 접속 시 메인 페이지로 이동시킵니다.
@app.get("/")
async def root():
    return RedirectResponse(url="/static/index.html")

@app.get("/game", response_class=FileResponse, include_in_schema=False)
async def serve_game():
    """Game 메뉴 클릭 시 game.html 파일을 서비스하는 라우트"""
    return "static/game.html"

# --- 서버 실행 ---
if __name__ == "__main__":
    print("=" * 50)
    print(f"🎯 {API_TITLE} (v{API_VERSION})")
    print("=" * 50)
    print("🌐 서버 주소: http://localhost:8000")
    print("📋 API 문서: http://localhost:8000/docs")
    print("🏠 프론트엔드: http://localhost:8000/static/index.html")
    print("🔄 백그라운드 파이프라인: 30분마다 자동 실행")
    print("=" * 50)
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,  # 백그라운드 스레드와 충돌을 피하기 위해 reload는 False로 설정
        log_level="info"
    )
