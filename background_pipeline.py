"""
백그라운드 뉴스 파이프라인 - services 조합 버전 (안정성 개선)
integrated_pipeline.py 제거 후 services들을 조합해서 실행
통합 환경에서의 크롤링 안정성 향상
"""

import os
import sys
import time
import schedule
import logging
import asyncio
import signal
from datetime import datetime
from pathlib import Path
from typing import Optional

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    from services.pipeline_service import PipelineService
    from services.database_service import DatabaseService
except ImportError as e:
    print(f"❌ 서비스 import 실패: {e}")
    print(f"현재 경로: {os.getcwd()}")
    print(f"프로젝트 루트: {project_root}")
    sys.exit(1)

# 로깅 설정
log_file = project_root / 'background_pipeline.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class BackgroundPipelineExecutor:
    """백그라운드 파이프라인 실행기 - services 조합 버전 (안정성 개선)"""
    
    def __init__(self):
        self.pipeline_service = None
        self.db_service = None
        self.is_running = False
        self.current_loop = None
        
        try:
            # 항상 headless 모드로 실행 (백그라운드 환경)
            self.pipeline_service = PipelineService(headless=True)
            self.db_service = DatabaseService()
            
            # 데이터베이스 초기화
            self.db_service.initialize()
            
            logger.info("✅ 백그라운드 파이프라인 실행기 초기화 완료")
            
        except Exception as e:
            logger.error(f"❌ 초기화 실패: {e}")
            raise
    
    async def run_scheduled_update(self):
        """30분마다 실행되는 뉴스 업데이트 (안정성 개선)"""
        if self.is_running:
            logger.warning("⚠️ 이미 파이프라인이 실행 중입니다. 스킵합니다.")
            return
            
        self.is_running = True
        
        try:
            logger.info("🔄 백그라운드 파이프라인 시작 (services 버전)")
            start_time = datetime.now()
            
            # 🔥 수정: 파이프라인 실행 시 파라미터 제거 (execute_full_pipeline에 파라미터가 없음)
            result = self.pipeline_service.execute_full_pipeline()
            
            execution_time = datetime.now() - start_time
            
            if result.get("final_status") == "success":
                # MySQL에 결과 저장 시도
                try:
                    await self.db_service.save_pipeline_result(result)
                    logger.info(f"✅ MySQL 저장 완료")
                except Exception as db_error:
                    logger.warning(f"⚠️ MySQL 저장 실패 (파일은 저장됨): {db_error}")
                
                logger.info(f"✅ 백그라운드 파이프라인 완료: {result.get('pipeline_id', 'unknown')} (소요시간: {execution_time})")
                
                # 🔥 추가: 결과 파일 경로 로깅
                saved_file = result.get("saved_file", "")
                if saved_file:
                    logger.info(f"💾 결과 파일 저장: {saved_file}")
                
                # 🔥 추가: 분석 결과 요약 로깅
                final_summary = result.get("final_summary", {})
                if final_summary:
                    processing_details = final_summary.get("processing_details", {})
                    logger.info(f"📊 처리 요약: 크롤링 {processing_details.get('crawled', 0)}개 → 필터링 {processing_details.get('filtered', 0)}개 → 분석 {processing_details.get('analyzed', 0)}개")
                    logger.info(f"📊 평균 신뢰도: {final_summary.get('average_confidence', 0)}")
                
            else:
                logger.error(f"❌ 파이프라인 실행 실패: {result.get('errors', [])}")
                
        except Exception as e:
            logger.error(f"❌ 백그라운드 파이프라인 실패: {e}")
            # 스택 트레이스도 로깅
            import traceback
            logger.error(f"스택 트레이스:\n{traceback.format_exc()}")
            
        finally:
            self.is_running = False
    
    def run_once(self):
        """즉시 1회 실행 (테스트용) - 이벤트 루프 안전 처리"""
        logger.info("🚀 백그라운드 파이프라인 즉시 실행 (services 버전)")
        
        try:
            # 기존 이벤트 루프가 있는지 확인
            try:
                loop = asyncio.get_running_loop()
                logger.info("기존 이벤트 루프 감지됨")
                # 이미 실행 중인 루프에서는 task로 실행
                task = asyncio.create_task(self.run_scheduled_update())
                return task
            except RuntimeError:
                # 실행 중인 루프가 없으면 새로 생성
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                self.current_loop = loop
                
                try:
                    loop.run_until_complete(self.run_scheduled_update())
                finally:
                    loop.close()
                    self.current_loop = None
                    
        except Exception as e:
            logger.error(f"❌ 즉시 실행 실패: {e}")
            import traceback
            logger.error(f"스택 트레이스:\n{traceback.format_exc()}")
    
    def shutdown(self):
        """안전한 종료 처리"""
        logger.info("🛑 백그라운드 파이프라인 종료 중...")
        
        if self.current_loop and not self.current_loop.is_closed():
            try:
                self.current_loop.stop()
            except Exception as e:
                logger.warning(f"⚠️ 이벤트 루프 종료 실패: {e}")
        
        # 🔥 추가: 크롤링 서비스 안전 종료
        try:
            if self.pipeline_service and hasattr(self.pipeline_service, 'crawling_service'):
                crawling_service = self.pipeline_service.crawling_service
                if hasattr(crawling_service, 'cleanup'):
                    crawling_service.cleanup()
                    logger.info("✅ 크롤링 서비스 정리 완료")
        except Exception as e:
            logger.warning(f"⚠️ 크롤링 서비스 정리 실패: {e}")
        
        logger.info("✅ 백그라운드 파이프라인 종료 완료")

def signal_handler(signum, frame):
    """시그널 핸들러 (Ctrl+C 등)"""
    logger.info(f"🔔 종료 시그널 수신: {signum}")
    sys.exit(0)

def run_scheduled_wrapper(executor: BackgroundPipelineExecutor):
    """스케줄 실행을 위한 래퍼 함수"""
    try:
        # 새로운 이벤트 루프에서 실행
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(executor.run_scheduled_update())
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"❌ 스케줄 실행 실패: {e}")

def main():
    """메인 실행 함수"""
    
    # 시그널 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        executor = BackgroundPipelineExecutor()
    except Exception as e:
        logger.error(f"❌ 실행기 초기화 실패: {e}")
        sys.exit(1)
    
    # 명령행 인자 처리
    if len(sys.argv) > 1 and sys.argv[1] == "once":
        # 즉시 1회 실행
        try:
            executor.run_once()
        except KeyboardInterrupt:
            logger.info("🔔 사용자에 의해 중단됨")
        except Exception as e:
            logger.error(f"❌ 즉시 실행 실패: {e}")
        finally:
            executor.shutdown()
        return
    
    # 🔥 추가: 크롤링 횟수 제한 확인
    if "--test-mode" in sys.argv:
        logger.info("🧪 테스트 모드: 크롤링 횟수 제한됨")
    
    # 스케줄 실행
    logger.info("📅 백그라운드 파이프라인 스케줄러 시작 (30분 간격, services 버전)")
    
    # 30분마다 실행 스케줄링
    schedule.every(30).minutes.do(lambda: run_scheduled_wrapper(executor))
    
    # 시작 시 즉시 1회 실행 (선택사항)
    if "--no-initial-run" not in sys.argv:
        logger.info("🎬 시작 시 초기 실행...")
        try:
            executor.run_once()
        except Exception as e:
            logger.error(f"❌ 초기 실행 실패: {e}")
    
    # 스케줄 유지
    try:
        logger.info("⏰ 스케줄러 대기 중... (Ctrl+C로 종료)")
        while True:
            schedule.run_pending()
            time.sleep(60)  # 1분마다 스케줄 체크
            
    except KeyboardInterrupt:
        logger.info("🔔 사용자에 의해 중단됨")
    except Exception as e:
        logger.error(f"❌ 스케줄러 실행 실패: {e}")
    finally:
        executor.shutdown()

if __name__ == "__main__":
    main()