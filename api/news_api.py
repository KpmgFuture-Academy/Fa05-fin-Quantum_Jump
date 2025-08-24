# api/news_api.py (안전한 버전)
from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Optional
from datetime import datetime
import json
from pathlib import Path
import pandas as pd

from services.database_service import get_database_service

router = APIRouter()

CSV_FILE_PATH = Path(__file__).parent.parent / "data" / "Past_news.csv"
df_past_news = None

def load_csv_data():
    """서버 시작 시 CSV 파일을 안전하게 로드하고, 컬럼명을 표준화하는 함수"""
    global df_past_news
    try:
        if not CSV_FILE_PATH.is_file():
            raise FileNotFoundError(f"{CSV_FILE_PATH} 파일을 찾을 수 없습니다.")
        
        df = pd.read_csv(CSV_FILE_PATH)
        
        print("✅ Past_news.csv에서 원본 그대로 읽은 컬럼명:", df.columns.tolist())

        # --- ▼▼▼ 핵심 수정 부분: 실제 CSV 컬럼명을 코드 표준 컬럼명으로 매핑 ▼▼▼ ---
        # 실제 CSV 파일의 컬럼명을 왼쪽에, 코드에서 사용할 이름을 오른쪽에 적습니다.
        column_mapping = {
            'ID': 'id',
            'Issue_name': 'title',
            'Contents': 'summary',
            'Contentes(Spec)': 'content', # CSV 파일의 오타('Contentes')를 그대로 반영
            'Start_date': 'start_date',
            'Fin_date': 'end_date',
            '근거자료': 'evidence_source',
            '카테고리': 'related_industries'
        }
        df.rename(columns=column_mapping, inplace=True)
        # --- ▲▲▲ 핵심 수정 부분 끝 ▲▲▲ ---
        
        df = df.fillna('')
        if 'id' not in df.columns or df['id'].astype(str).duplicated().any():
            df['id'] = df.index.astype(str)
        
        # 'source' 컬럼이 없다면 기본값으로 생성
        if 'source' not in df.columns:
            df['source'] = '과거 이슈 DB'

        df_past_news = df
        print(f"✅ Past_news.csv 데이터 표준화 및 로드 성공. 총 {len(df_past_news)}개 뉴스.")
        print("   -> 코드에서 사용할 컬럼명:", df_past_news.columns.tolist())

    except Exception as e:
        df_past_news = pd.DataFrame()
        print(f"❌ Past_news.csv 파일 로드/처리 실패: {e}")

load_csv_data()

@router.get("/latest")
async def get_latest_news_issues():
    """최신 뉴스 이슈들을 MySQL에서 조회하고 RAG 분석 상세 정보를 포함합니다."""
    try:
        db_service = get_database_service()
        
        news_issues = await db_service.get_latest_news_issues()
        
        if not news_issues:
            # MySQL에 데이터가 없으면 fallback: 최신 JSON 파일에서 로드
            fallback_data = _load_fallback_data()
            if fallback_data:
                # 백업 데이터에도 상세 정보 추가
                enriched_fallback = _enrich_with_rag_details(fallback_data)
                return {
                    "success": True,
                    "data": {
                        "issues": enriched_fallback,
                        "count": len(enriched_fallback),
                        "source": "파일 백업 데이터",
                        "last_updated": "백그라운드 업데이트 대기 중"
                    }
                }
            else:
                return {
                    "success": True,
                    "data": {
                        "issues": [],
                        "count": 0,
                        "source": "데이터 없음",
                        "message": "백그라운드 파이프라인이 첫 실행을 완료할 때까지 기다려주세요."
                    }
                }
        
        # MySQL 데이터에 RAG 상세 정보 추가
        enriched_issues = _enrich_with_rag_details(news_issues)
        
        return {
            "success": True,
            "data": {
                "issues": enriched_issues,
                "count": len(enriched_issues),
                "source": "MySQL 실시간 데이터",
                "last_updated": news_issues[0].get("updated_at") if news_issues else None,
                # 추가: RAG 분석 메타데이터
                "rag_metadata": {
                    "verification_enabled": True,
                    "confidence_calculation": "multi_dimensional",
                    "scoring_method": "hybrid_vector_ai"
                }
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"뉴스 조회 실패: {e}")

@router.get("/past", summary="과거 뉴스 목록 조회(CSV 기반)", description="data/Past_news.csv 파일에서 과거 뉴스 데이터를 조회합니다.")
async def get_past_news(
    limit: int = 100,
    search: Optional[str] = Query(None, description="뉴스 제목 또는 내용에서 검색할 키워드"),
    industry: Optional[str] = Query(None, description="관련 산업별로 필터링")
):
    global df_past_news
    
    if df_past_news is None or df_past_news.empty:
        raise HTTPException(status_code=500, detail="서버에 과거 뉴스 데이터(CSV)가 로드되지 않았습니다.")
    
    try:
        df_filtered = df_past_news.copy()

        if search:
            search_term = search.lower()
            df_filtered = df_filtered[
                df_filtered['title'].str.lower().str.contains(search_term, na=False) |
                df_filtered['summary'].str.lower().str.contains(search_term, na=False)
            ]

        if industry:
            df_filtered = df_filtered[
                df_filtered['related_industries'].str.contains(industry, na=False)
            ]

        total_count = len(df_filtered)
        df_result = df_filtered.head(limit)
        data_to_return = df_result.to_dict(orient='records')

        return {
            "success": True,
            "total": total_count,
            "data": data_to_return
        }
    except Exception as e:
        print(f"❌ 과거 뉴스 처리 중 오류 발생: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def _enrich_with_rag_details(issues: List[Dict]) -> List[Dict]:
    """이슈 데이터에 RAG 분석의 상세 정보를 추가합니다."""
    enriched = []
    
    for issue in issues:
        enriched_issue = issue.copy()
        
        # 🔥 안전한 관련 산업 상세 정보 추가
        raw_industries = issue.get("관련산업", [])
        if isinstance(raw_industries, list):
            detailed_industries = []
            for industry in raw_industries:
                if isinstance(industry, dict):
                    detailed_industry = {
                        "name": industry.get("name", "산업명 없음"),
                        "final_score": industry.get("final_score", 0),
                        "vector_score": industry.get("vector_score", 0),
                        "ai_score": industry.get("ai_score", 0),
                        "ai_reason": industry.get("ai_reason", ""),
                        "description": industry.get("description", ""),
                        # 검증 정보 안전하게 추가
                        "verification": industry.get("verification", {
                            "is_grounded": False,
                            "supporting_quote": ""
                        }),
                        # 점수 구성 상세
                        "score_breakdown": {
                            "vector_weight": 0.3,
                            "ai_weight": 0.7,
                            "penalty_applied": not industry.get("verification", {}).get("is_grounded", True)
                        }
                    }
                    detailed_industries.append(detailed_industry)
                else:
                    # 문자열이나 다른 형태인 경우 기본 구조로 변환
                    detailed_industries.append({
                        "name": str(industry),
                        "final_score": 0,
                        "vector_score": 0,
                        "ai_score": 0,
                        "ai_reason": "구조 변환됨",
                        "description": "",
                        "verification": {"is_grounded": False, "supporting_quote": ""},
                        "score_breakdown": {"vector_weight": 0.3, "ai_weight": 0.7, "penalty_applied": True}
                    })
            enriched_issue["관련산업_상세"] = detailed_industries
        
        # 🔥 안전한 관련 과거 이슈 상세 정보 추가
        raw_past_issues = issue.get("관련과거이슈", [])
        if isinstance(raw_past_issues, list):
            detailed_past_issues = []
            for past_issue in raw_past_issues:
                if isinstance(past_issue, dict):
                    detailed_past_issue = {
                        "name": past_issue.get("name", "이슈명 없음"),
                        "final_score": past_issue.get("final_score", 0),
                        "vector_score": past_issue.get("vector_score", 0),
                        "ai_score": past_issue.get("ai_score", 0),
                        "ai_reason": past_issue.get("ai_reason", ""),
                        "description": past_issue.get("description", ""),
                        "period": past_issue.get("period", "N/A"),
                        # 검증 정보 안전하게 추가
                        "verification": past_issue.get("verification", {
                            "is_grounded": False,
                            "supporting_quote": ""
                        }),
                        # 점수 구성 상세
                        "score_breakdown": {
                            "vector_weight": 0.3,
                            "ai_weight": 0.7,
                            "penalty_applied": not past_issue.get("verification", {}).get("is_grounded", True)
                        }
                    }
                    detailed_past_issues.append(detailed_past_issue)
                else:
                    # 문자열이나 다른 형태인 경우 기본 구조로 변환
                    detailed_past_issues.append({
                        "name": str(past_issue),
                        "final_score": 0,
                        "vector_score": 0,
                        "ai_score": 0,
                        "ai_reason": "구조 변환됨",
                        "description": "",
                        "period": "N/A",
                        "verification": {"is_grounded": False, "supporting_quote": ""},
                        "score_breakdown": {"vector_weight": 0.3, "ai_weight": 0.7, "penalty_applied": True}
                    })
            enriched_issue["관련과거이슈_상세"] = detailed_past_issues
        
        # 🔥 안전한 RAG 신뢰도 상세 정보 추가
        rag_confidence = issue.get("RAG분석신뢰도", {})
        if isinstance(rag_confidence, dict):
            consistency_score = rag_confidence.get("consistency_score", 0)
            peak_relevance_score = rag_confidence.get("peak_relevance_score", 0)
        elif isinstance(rag_confidence, (int, float)):
            # 구 버전 호환
            consistency_score = float(rag_confidence)
            peak_relevance_score = float(rag_confidence)
        else:
            consistency_score = 0
            peak_relevance_score = 0
        
        enriched_issue["RAG분석신뢰도_상세"] = {
            "consistency_score": consistency_score,
            "peak_relevance_score": peak_relevance_score,
            "calculation_method": "평균 일관성 + 최고 연관도",
            "total_verified_items": sum(1 for ind in detailed_industries 
                                      if ind.get("verification", {}).get("is_grounded", False)) +
                                  sum(1 for past in detailed_past_issues 
                                      if past.get("verification", {}).get("is_grounded", False))
        }
        
        enriched.append(enriched_issue)
    
    return enriched

def _load_fallback_data():
    """JSON 파일에서 백업 데이터를 로드합니다 (안전한 버전)."""
    try:
        data_dir = Path("data2")
        if not data_dir.exists():
            return []
        
        # 가장 최근 파이프라인 결과 파일 찾기
        pipeline_files = list(data_dir.glob("*Pipeline_Results.json"))
        if not pipeline_files:
            return []
        
        latest_file = max(pipeline_files, key=lambda p: p.stat().st_mtime)
        
        with open(latest_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 🔥 다양한 파일 구조 처리
        issues = []
        
        # 새로운 구조: {"selected_issues": [...]}
        if "selected_issues" in data:
            issues = data["selected_issues"]
        # 구 API 구조: {"api_ready_data": {"data": {"selected_issues": [...]}}}
        elif "api_ready_data" in data:
            api_data = data.get("api_ready_data", {})
            issues = api_data.get("data", {}).get("selected_issues", [])
        
        return issues if isinstance(issues, list) else []
    
    except Exception as e:
        print(f"백업 데이터 로드 실패: {e}")
        return []

@router.get("/pipeline-status")
async def get_pipeline_status():
    """백그라운드 파이프라인의 최근 실행 상태를 조회합니다."""
    try:
        db_service = get_database_service()
        latest_log = await db_service.get_latest_pipeline_log()
        
        return {
            "success": True,
            "data": latest_log or {
                "status": "대기 중",
                "message": "백그라운드 파이프라인이 아직 실행되지 않았습니다."
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"파이프라인 상태 조회 실패: {e}")