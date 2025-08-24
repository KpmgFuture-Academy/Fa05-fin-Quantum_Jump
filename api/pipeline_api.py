# api/pipeline_api.py (수정된 버전)
from fastapi import APIRouter, BackgroundTasks, HTTPException
from typing import List

from services import pipeline_service
from models.schemas import CurrentIssue

router = APIRouter()

@router.get("/today-issues", response_model=List[CurrentIssue])
async def get_today_issues():
    """
    오늘의 주요 이슈 5개를 RAG 분석 결과와 함께 반환합니다.
    캐시된 최신 데이터를 반환하며, 데이터가 없으면 파이프라인을 실행합니다.
    """
    try:
        # 🔥 수정: get_latest_analyzed_issues는 async 함수가 아님
        issues = pipeline_service.get_latest_analyzed_issues()
        if not issues:
            return []
        return issues
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"이슈 조회 실패: {e}")

@router.post("/refresh-issues")
async def refresh_all_issues(background_tasks: BackgroundTasks):
    """
    백그라운드에서 전체 데이터 파이프라인을 실행하여 오늘의 이슈를 새로고침합니다.
    (크롤링 -> 필터링 -> 분석)
    """
    try:
        # 🔥 수정: pipeline_service 인스턴스를 얻어서 올바른 메서드 호출
        from services.pipeline_service import PipelineService
        
        def run_pipeline():
            try:
                pipeline = PipelineService()
                result = pipeline.execute_full_pipeline()
                print(f"✅ 백그라운드 파이프라인 완료: {result.get('pipeline_id', 'unknown')}")
                return result
            except Exception as e:
                print(f"❌ 백그라운드 파이프라인 실패: {e}")
                raise e
        
        background_tasks.add_task(run_pipeline)
        
        return {
            "success": True,
            "message": "오늘의 이슈 데이터 새로고침을 시작합니다. 약 3~5분 소요됩니다.",
            "status": "파이프라인이 백그라운드에서 실행 중입니다."
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"파이프라인 시작 실패: {e}")

@router.get("/status")
async def get_pipeline_status():
    """파이프라인 실행 상태 조회"""
    try:
        # 최신 파이프라인 결과 파일 확인
        from pathlib import Path
        import json
        
        data_dir = Path("data2")
        pipeline_files = list(data_dir.glob("*Pipeline_Results.json"))
        
        if pipeline_files:
            latest_file = max(pipeline_files, key=lambda f: f.stat().st_mtime)
            
            with open(latest_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return {
                "success": True,
                "data": {
                    "last_execution": data.get("pipeline_metadata", {}).get("pipeline_id", "unknown"),
                    "status": data.get("pipeline_metadata", {}).get("final_status", "unknown"),
                    "file_path": str(latest_file),
                    "issues_count": data.get("total_issues", 0),
                    "average_confidence": data.get("average_confidence", 0)
                }
            }
        else:
            return {
                "success": True,
                "data": {
                    "status": "no_executions",
                    "message": "아직 실행된 파이프라인이 없습니다."
                }
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"상태 조회 실패: {e}")