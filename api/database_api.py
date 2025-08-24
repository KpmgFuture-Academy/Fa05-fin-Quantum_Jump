# api/database_api.py
from fastapi import APIRouter, HTTPException
from typing import Optional, List

from services import database_service
from models.schemas import PastIssue, Industry, DatabaseStats

# router 정의 추가
router = APIRouter()

@router.get("/past-issues", response_model=List[PastIssue])
async def get_past_issues(limit: int = 20, search: Optional[str] = None, industry: Optional[str] = None):
    """SQLite DB에서 과거 뉴스 기록을 조회합니다."""
    if not database_service.is_initialized():
        raise HTTPException(status_code=503, detail="데이터베이스 서비스가 준비되지 않았습니다.")
    
    try:
        return await database_service.db_api.get_past_news(limit, search, industry)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/industries", response_model=List[Industry])
async def get_industries(search: Optional[str] = None, limit: int = 50):
    """SQLite DB에서 산업 분류 정보를 조회합니다."""
    if not database_service.is_initialized():
        raise HTTPException(status_code=503, detail="데이터베이스 서비스가 준비되지 않았습니다.")
    
    try:
        return await database_service.db_api.get_industries(search, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stats", response_model=DatabaseStats)
async def get_db_stats():
    """SQLite DB의 통계 정보를 반환합니다."""
    if not database_service.is_initialized():
        raise HTTPException(status_code=503, detail="데이터베이스 서비스가 준비되지 않았습니다.")
    
    try:
        return database_service.orda_db.get_database_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))