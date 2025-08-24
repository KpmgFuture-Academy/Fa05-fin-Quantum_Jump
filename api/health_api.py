# api/health_api.py
from fastapi import APIRouter
from datetime import datetime
from typing import Dict

from services.database_service import get_database_service

# 누락된 router 정의 추가
router = APIRouter()

@router.get("/health")
async def health_check():
    """API 서버와 데이터베이스 상태를 확인합니다."""
    
    components = {}
    
    # 데이터베이스 상태 확인
    try:
        db_service = get_database_service()
        db_service.test_connection()  # await 제거 (동기 함수)
        components["mysql_database"] = {
            "status": "ok",
            "message": "MySQL 연결 정상"
        }
    except Exception as e:
        components["mysql_database"] = {
            "status": "error",
            "message": f"MySQL 연결 실패: {e}"
        }
    
    # 백그라운드 파이프라인 상태 확인
    try:
        db_service = get_database_service()
        latest_log = db_service.get_latest_pipeline_log()  # await 제거
        
        if latest_log and latest_log.get("final_status") == "success":
            components["background_pipeline"] = {
                "status": "ok",
                "message": f"최근 실행 성공: {latest_log.get('completed_at')}"
            }
        else:
            components["background_pipeline"] = {
                "status": "warning",
                "message": "최근 실행 로그 없음 또는 실패"
            }
    except Exception as e:
        components["background_pipeline"] = {
            "status": "error",
            "message": f"파이프라인 상태 확인 실패: {e}"
        }
    
    # 전체 상태 결정
    all_ok = all(comp["status"] == "ok" for comp in components.values())
    overall_status = "ok" if all_ok else "degraded"
    
    return {
        "status": overall_status,
        "timestamp": datetime.now().isoformat(),
        "components": components,
        "message": "모든 서비스 정상" if all_ok else "일부 서비스에 문제가 있습니다."
    }