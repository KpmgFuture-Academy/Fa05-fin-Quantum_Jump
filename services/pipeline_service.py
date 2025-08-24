"""
파이프라인 통합 서비스 - 크롤링, 필터링, RAG 분석을 연결
integrated_pipeline.py의 IntegratedNewsPipeline 로직 이관 (수정 사항 반영)
완전 안전 버전 - 다양한 데이터 구조 처리
"""

import json
import traceback  # 오류 추적을 위해 추가
from pathlib import Path
from typing import Dict, List
from datetime import datetime
from .crawling_service import CrawlingService
from .rag_service import RAGService

class PipelineService:
    """전체 파이프라인 통합 서비스"""
    
    def __init__(self, data_dir: str = "data2", headless: bool = True):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # 서비스 초기화
        self.crawling_service = CrawlingService(str(self.data_dir), headless)
        self.rag_service = RAGService()
        
        print("✅ 파이프라인 서비스 초기화 완료")
    
    def execute_full_pipeline(self, 
                                issues_per_category: int = 10,
                                target_filtered_count: int = 5) -> Dict:
        """전체 파이프라인 실행: 크롤링 → 필터링 → RAG 분석 (오류 처리 강화)"""
        
        pipeline_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        started_at = datetime.now()
        
        print(f"🚀 파이프라인 실행 시작 (ID: {pipeline_id})")
        print(f"📋 설정: 카테고리별 {issues_per_category}개, 최종 선별 {target_filtered_count}개")
        
        result = {
            "pipeline_id": pipeline_id,
            "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "completed_at": None,
            "execution_time": None,
            "final_status": "running",
            "steps_completed": [],
            "errors": []
        }
        
        try:
            # Step 1: 크롤링 + 필터링
            print(f"\n{'='*60}")
            print(f"📡 Step 1: 크롤링 + 주식시장 필터링")
            print(f"{'='*60}")
            
            crawling_result = self.crawling_service.crawl_and_filter_news(
                issues_per_category, target_filtered_count
            )
            
            result["crawling_result"] = {
                "total_crawled": len(crawling_result.get("all_issues", [])),
                "filtered_count": len(crawling_result.get("filtered_issues", []))
            }
            result["steps_completed"].append("crawling_and_filtering")
            
            # Step 2: RAG 분석 (완전 안전 버전)
            print(f"\n{'='*60}")
            print(f"🔍 Step 2: RAG 분석 (산업 + 과거 이슈)")
            print(f"{'='*60}")
            
            # 🔥 크롤링 결과에서 필터링된 이슈 추출 (다양한 구조 처리)
            raw_filtered = crawling_result.get("filtered_issues", [])
            print(f"📊 원본 필터링 결과 타입: {type(raw_filtered)}")
            print(f"📊 원본 필터링 결과 길이: {len(raw_filtered) if hasattr(raw_filtered, '__len__') else 'N/A'}")
            
            # 🔥 다양한 구조 처리
            if isinstance(raw_filtered, dict) and "selected_issues" in raw_filtered:
                # StockFiltered 파일 구조: {"selected_issues": [...]}
                filtered_issues = raw_filtered["selected_issues"]
                print(f"📋 selected_issues에서 추출: {len(filtered_issues)}개")
            elif isinstance(raw_filtered, list) and raw_filtered:
                if isinstance(raw_filtered[0], dict):
                    # 이미 올바른 딕셔너리 배열: [{"제목": "...", "내용": "..."}, ...]
                    filtered_issues = raw_filtered
                    print(f"📋 올바른 딕셔너리 배열: {len(filtered_issues)}개")
                    print(f"📋 첫 번째 이슈 키들: {list(raw_filtered[0].keys())}")
                else:
                    # 문자열 배열 → 딕셔너리 배열 변환: ["텍스트1", "텍스트2", ...]
                    print("⚙️ 문자열 배열을 딕셔너리 배열로 변환...")
                    filtered_issues = []
                    for i, text in enumerate(raw_filtered):
                        filtered_issues.append({
                            "이슈번호": i + 1,
                            "제목": f"필터링된 이슈 {i+1}",
                            "내용": str(text),
                            "원본내용": str(text),
                            "카테고리": "자동변환",
                            "추출시간": datetime.now().isoformat(),
                            "주식시장_관련성_점수": 5.0,
                            "rank": i + 1
                        })
                    print(f"✅ {len(filtered_issues)}개 이슈 변환 완료")
            else:
                raise ValueError("필터링된 이슈가 없거나 형식이 올바르지 않습니다.")
            
            if not filtered_issues:
                raise ValueError("변환된 필터링 이슈가 없습니다.")
            
            # RAG 분석 실행
            print(f"🔍 RAG 분석 시작 - {len(filtered_issues)}개 이슈 처리...")
            enriched_issues = self.rag_service.analyze_issues_with_rag(filtered_issues)
            print(f"✅ RAG 분석 완료 - {len(enriched_issues)}개 이슈 처리됨")
            
            # 🔥 [수정] 안전한 평균 신뢰도 계산
            try:
                average_confidence = self._calculate_average_confidence(enriched_issues)
                print(f"✅ 평균 신뢰도 계산 완료: {average_confidence}")
            except Exception as conf_error:
                print(f"⚠️ 평균 신뢰도 계산 실패: {conf_error}")
                average_confidence = 0.0

            result["rag_result"] = {
                "analyzed_count": len(enriched_issues),
                "average_confidence": average_confidence
            }
            result["steps_completed"].append("rag_analysis")
            
            # Step 3: API용 데이터 준비
            print(f"\n{'='*60}")
            print(f"🌐 Step 3: API 응답 데이터 준비")
            print(f"{'='*60}")
            
            api_data = self._prepare_api_data(crawling_result, enriched_issues)
            result["api_ready_data"] = api_data
            result["steps_completed"].append("api_preparation")
            
            # 파이프라인 완료
            completed_at = datetime.now()
            execution_time = completed_at - started_at
            
            result.update({
                "completed_at": completed_at.strftime("%Y-%m-%d %H:%M:%S"),
                "execution_time": str(execution_time),
                "final_status": "success",
                # 🔥 추가: 최종 결과 요약
                "final_summary": {
                    "total_issues": len(enriched_issues),
                    "average_confidence": average_confidence,
                    "processing_details": {
                        "crawled": result["crawling_result"]["total_crawled"],
                        "filtered": result["crawling_result"]["filtered_count"],
                        "analyzed": len(enriched_issues)
                    }
                }
            })
            
            # 결과 저장
            saved_file = self._save_pipeline_result(result, enriched_issues)
            result["saved_file"] = saved_file
            
            print(f"\n🎉 파이프라인 실행 완료!")
            print(f"⏰ 실행 시간: {execution_time}")
            print(f"📊 최종 결과: {len(enriched_issues)}개 이슈 분석 완료")
            print(f"💾 결과 저장: {saved_file}")
            
            return result
            
        except Exception as e:
            error_msg = f"파이프라인 실행 실패: {e}"
            print(f"❌ {error_msg}")
            
            # 🔥 [수정] 스택 트레이스 로깅
            print("스택 트레이스:")
            traceback.print_exc()

            result.update({
                "completed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "final_status": "failed",
                "errors": [error_msg, traceback.format_exc()]
            })
            
            # 에러를 발생시켜 상위 호출자에게 전파
            raise Exception(error_msg)

    def _prepare_api_data(self, crawling_result: Dict, enriched_issues: List[Dict]) -> Dict:
        """API 응답용 데이터 구성"""
        
        api_data = {
            "success": True,
            "data": {
                "total_crawled": len(crawling_result.get("all_issues", [])),
                "selected_count": len(enriched_issues),
                "selection_criteria": "주식시장 영향도 + RAG 분석",
                "selected_issues": []
            },
            "metadata": {
                "crawled_at": crawling_result.get("crawling_metadata", {}).get("timestamp", ""),
                "categories_processed": crawling_result.get("crawling_metadata", {}).get("categories_processed", []),
                "ai_filter_applied": True,
                "rag_analysis_applied": True,
                "filter_model": "gpt-4o-mini",
                "rag_model": "gpt-4o-mini",
                "rag_confidence": self._calculate_average_confidence(enriched_issues)
            }
        }
        
        # 이슈 데이터 변환
        for issue in enriched_issues:
            api_issue = {
                "이슈번호": issue.get("이슈번호", 0),
                "제목": issue.get("제목", ""),
                "내용": issue.get("원본내용", issue.get("내용", "")),
                "카테고리": issue.get("카테고리", ""),
                "추출시간": issue.get("추출시간", ""),
                "주식시장_관련성_점수": issue.get("주식시장_관련성_점수", 0),
                "순위": issue.get("rank", 0),
                
                # RAG 분석 결과
                "관련산업": issue.get("관련산업", []),
                "관련과거이슈": issue.get("관련과거이슈", []),
                # 🔥 [수정] RAG 신뢰도 기본값을 dict로 변경하여 안정성 확보
                "RAG분석신뢰도": issue.get("RAG분석신뢰도", {"consistency_score": 0.0, "peak_relevance_score": 0.0}),
            }
            api_data["data"]["selected_issues"].append(api_issue)
        
        # 순위별 정렬
        api_data["data"]["selected_issues"].sort(key=lambda x: x.get("순위", 999))
        
        return api_data
    
    # 🔥 [교체] 새로운 _calculate_average_confidence 메서드
    def _calculate_average_confidence(self, enriched_issues: List[Dict]) -> float:
        """전체 이슈들의 평균 RAG 신뢰도 계산 (오류 수정됨)"""
        if not enriched_issues:
            return 0.0
        
        confidences = []
        
        for issue in enriched_issues:
            rag_confidence = issue.get("RAG분석신뢰도")
            
            if rag_confidence is None:
                continue
            
            # 🔥 오류 수정: 딕셔너리와 숫자 타입 모두 처리
            if isinstance(rag_confidence, dict):
                # 새로운 다차원 신뢰도 구조
                consistency_score = rag_confidence.get("consistency_score", 0)
                confidences.append(float(consistency_score))
            elif isinstance(rag_confidence, (int, float)):
                # 기존 단일 숫자 구조
                confidences.append(float(rag_confidence))
            else:
                # 예상치 못한 타입은 건너뛰기
                print(f"⚠️ 예상치 못한 RAG 신뢰도 타입: {type(rag_confidence)}, 값: {rag_confidence}")
                continue
        
        if not confidences:
            return 0.0
            
        return round(sum(confidences) / len(confidences), 2)

    def _save_pipeline_result(self, result: Dict, enriched_issues: List[Dict]) -> str:
        """파이프라인 실행 결과 저장 (향상된 버전)"""
        try:
            timestamp = datetime.now().strftime("%Y.%m.%d_%H.%M.%S")
            filename = f"{timestamp}_Pipeline_Results.json"
            filepath = self.data_dir / filename
            
            # 🔥 향상된 저장 데이터 구조
            save_data = {
                "timestamp": datetime.now().isoformat(),
                "total_issues": len(enriched_issues),
                "selected_issues": enriched_issues,  # 핵심: enriched_issues 직접 저장
                "average_confidence": self._calculate_average_confidence(enriched_issues),
                "processing_time": result.get("execution_time", ""),
                "pipeline_metadata": {
                    "pipeline_id": result.get("pipeline_id", ""),
                    "steps_completed": result.get("steps_completed", []),
                    "final_status": result.get("final_status", ""),
                    "version": "PipelineService_v1.2_SafeVersion"
                },
                "file_info": {
                    "filename": filename,
                    "created_at": datetime.now().isoformat(),
                    "format_version": "2.0"
                }
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(save_data, f, ensure_ascii=False, indent=2)
            
            print(f"💾 파이프라인 결과 저장: {filepath}")
            return str(filepath)
            
        except Exception as e:
            print(f"⚠️ 결과 저장 실패: {e}")
            import traceback
            traceback.print_exc()
            return ""

    def get_latest_analyzed_issues(self) -> List[Dict]:
        """최신 분석된 이슈들 조회 (API용) - 향상된 버전"""
        try:
            # 1. MySQL에서 먼저 조회 시도
            try:
                from .database_service import DatabaseService
                db_service = DatabaseService()
                
                if db_service.is_initialized():
                    mysql_data = db_service.get_latest_news_issues()
                    if mysql_data:
                        print(f"📊 MySQL에서 {len(mysql_data)}개 이슈 조회")
                        return mysql_data
            except Exception as db_error:
                print(f"⚠️ MySQL 조회 실패: {db_error}")
            
            # 2. MySQL에 데이터가 없으면 최신 파일에서 조회
            pipeline_files = list(self.data_dir.glob("*_Pipeline_Results.json"))
            if pipeline_files:
                latest_file = max(pipeline_files, key=lambda f: f.stat().st_mtime)
                
                with open(latest_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # 🔥 다양한 파일 구조 처리
                issues = []
                
                # 새로운 구조: {"selected_issues": [...]}
                if "selected_issues" in data:
                    issues = data["selected_issues"]
                    print(f"📂 selected_issues에서 조회: {len(issues)}개")
                # 구 API 구조: {"api_ready_data": {"data": {"selected_issues": [...]}}}
                elif "api_ready_data" in data:
                    api_data = data.get("api_ready_data", {})
                    issues = api_data.get("data", {}).get("selected_issues", [])
                    print(f"📂 api_ready_data에서 조회: {len(issues)}개")
                
                if issues:
                    print(f"📂 파일에서 이슈 조회 성공: {latest_file.name}")
                    return issues
            
            print("⚠️ 분석된 이슈 데이터가 없습니다.")
            return []
            
        except Exception as e:
            print(f"❌ 최신 이슈 조회 실패: {e}")
            import traceback
            traceback.print_exc()
            return []