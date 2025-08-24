# services/__init__.py - 중복 초기화 방지 개선 버전
"""
서비스 모듈 초기화 및 전역 인스턴스 관리
중복 초기화 방지 및 싱글톤 패턴 적용
"""

import threading

# 개별 서비스 import
try:
    from .database_service import DatabaseService, get_database_service
    print("✅ DatabaseService import 성공")
except ImportError as e:
    print(f"⚠️ DatabaseService import 실패: {e}")
    DatabaseService = None

try:
    from .rag_service import RAGService
    print("✅ RAGService import 성공")
except ImportError as e:
    print(f"⚠️ RAGService import 실패: {e}")
    RAGService = None

try:
    from .crawling_service import CrawlingService
    print("✅ CrawlingService import 성공")
except ImportError as e:
    print(f"⚠️ CrawlingService import 실패: {e}")
    CrawlingService = None

try:
    from .pipeline_service import PipelineService
    print("✅ PipelineService import 성공")
except ImportError as e:
    print(f"⚠️ PipelineService import 실패: {e}")
    PipelineService = None

# 전역 서비스 인스턴스들 및 초기화 상태
_database_service = None
_pipeline_service = None
_rag_service = None
_initialization_lock = threading.Lock()
_initialized = False

def initialize_all_services():
    """모든 서비스를 초기화 (중복 실행 방지)"""
    global _database_service, _pipeline_service, _rag_service, _initialized
    
    # 이미 초기화되었다면 건너뛰기
    with _initialization_lock:
        if _initialized:
            print("✅ 서비스들이 이미 초기화되었습니다.")
            return True
        
        print("🔄 서비스 초기화 시작...")
        success_count = 0
        total_services = 0
        
        # 데이터베이스 서비스 초기화
        if DatabaseService and _database_service is None:
            try:
                _database_service = get_database_service()
                _database_service.initialize()
                print("✅ Database Service 초기화 완료")
                success_count += 1
            except Exception as e:
                print(f"❌ Database Service 초기화 실패: {e}")
            total_services += 1
        
        # RAG 서비스 초기화 (중복 방지)
        if RAGService and _rag_service is None:
            try:
                _rag_service = RAGService()
                print("✅ RAG Service 초기화 완료")
                success_count += 1
            except Exception as e:
                print(f"❌ RAG Service 초기화 실패: {e}")
            total_services += 1
        
        # 파이프라인 서비스 초기화 (중복 방지)
        if PipelineService and _pipeline_service is None:
            try:
                _pipeline_service = PipelineService()
                print("✅ Pipeline Service 초기화 완료")
                success_count += 1
            except Exception as e:
                print(f"❌ Pipeline Service 초기화 실패: {e}")
            total_services += 1
        
        _initialized = True
        print(f"📊 서비스 초기화 완료: {success_count}/{total_services}")
        return success_count == total_services

def get_pipeline_service():
    """파이프라인 서비스 인스턴스 반환 (싱글톤)"""
    global _pipeline_service
    with _initialization_lock:
        if _pipeline_service is None and PipelineService:
            _pipeline_service = PipelineService()
            print("✅ Pipeline Service 지연 초기화 완료")
        return _pipeline_service

def get_rag_service():
    """RAG 서비스 인스턴스 반환 (싱글톤)"""
    global _rag_service
    with _initialization_lock:
        if _rag_service is None and RAGService:
            _rag_service = RAGService()
            print("✅ RAG Service 지연 초기화 완료")
        return _rag_service

# 서비스 상태 확인
def check_services_health():
    """모든 서비스의 상태 확인"""
    status = {
        "database": _database_service.is_initialized() if _database_service else False,
        "pipeline": _pipeline_service is not None,
        "rag": _rag_service is not None,
        "initialized": _initialized
    }
    return status

# 편의를 위한 개별 서비스 접근 함수들
def is_database_initialized():
    """데이터베이스 서비스 초기화 상태 확인"""
    return _database_service and _database_service.is_initialized()

def is_rag_initialized():
    """RAG 서비스 초기화 상태 확인"""
    return _rag_service is not None

def is_pipeline_initialized():
    """파이프라인 서비스 초기화 상태 확인"""
    return _pipeline_service is not None

def reset_services():
    """테스트용: 모든 서비스 초기화 상태 리셋"""
    global _database_service, _pipeline_service, _rag_service, _initialized
    with _initialization_lock:
        _database_service = None
        _pipeline_service = None
        _rag_service = None
        _initialized = False
        print("🔄 서비스 초기화 상태 리셋 완료")