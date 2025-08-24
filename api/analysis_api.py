# api/analysis_api.py
from fastapi import APIRouter, HTTPException

from models.schemas import AnalysisRequest, FullAnalysisResponse
from services import rag_service

# router 정의 추가
router = APIRouter()

@router.post("/analysis", response_model=FullAnalysisResponse)
async def analyze_news_issue(request: AnalysisRequest):
    """
    하나의 뉴스 이슈에 대해 RAG 기반의 종합 분석을 수행합니다.
    - 관련 과거 이슈 및 산업 검색 (Vector Search)
    - LLM을 통한 종합 분석 및 신뢰도 평가
    """
    if not rag_service.is_initialized():
        raise HTTPException(status_code=503, detail="분석 서비스가 준비되지 않았습니다.")
    
    try:
        analysis_result = await rag_service.comprehensive_analysis(
            current_news=request.content,
            max_past_issues=3,
            max_industries=3
        )
        return analysis_result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"뉴스 분석 중 오류 발생: {e}")