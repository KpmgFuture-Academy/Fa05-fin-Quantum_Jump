"""
MySQL 데이터베이스 서비스 - 간단한 버전
백그라운드 파이프라인 결과 저장 + API 조회용
"""

import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Optional
import json
from datetime import datetime
from config import DATABASE_CONFIG

class DatabaseService:
    """MySQL 기반 데이터베이스 서비스"""
    
    def __init__(self):
        self.connection = None
        self._initialized = False
    
    def initialize(self):
        """MySQL 연결 초기화"""
        try:
            self.connection = mysql.connector.connect(**DATABASE_CONFIG)
            if self.connection.is_connected():
                self._initialized = True
                print(f"✅ MySQL 연결 성공 (포트: {DATABASE_CONFIG['port']})")
                self._create_tables()
        except Error as e:
            print(f"❌ MySQL 연결 실패: {e}")
            self._initialized = False
    
    def is_initialized(self) -> bool:
        """연결 상태 확인"""
        try:
            return (self._initialized and 
                   self.connection and 
                   self.connection.is_connected())
        except:
            return False
    
    async def test_connection(self):
        """연결 테스트"""
        if not self.is_initialized():
            raise Exception("데이터베이스가 연결되지 않았습니다.")
        
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True
        finally:
            cursor.close()
    
    def _create_tables(self):
        """필요한 테이블 생성"""
        cursor = self.connection.cursor()
        
        try:
            # 뉴스 이슈 테이블
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS news_issues (
                id INT PRIMARY KEY AUTO_INCREMENT,
                issue_number INT,
                title VARCHAR(500) NOT NULL,
                content TEXT,
                category VARCHAR(100),
                extracted_at DATETIME,
                stock_relevance_score DECIMAL(4,1),
                ranking INT,
                rag_confidence DECIMAL(4,1),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB CHARSET=utf8mb4
            """)
            
            # 관련 산업 테이블
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS related_industries (
                id INT PRIMARY KEY AUTO_INCREMENT,
                news_issue_id INT NOT NULL,
                industry_name VARCHAR(200),
                final_score DECIMAL(4,1),
                ai_reason TEXT,
                FOREIGN KEY (news_issue_id) REFERENCES news_issues(id) ON DELETE CASCADE
            ) ENGINE=InnoDB CHARSET=utf8mb4
            """)
            
            # 관련 과거 이슈 테이블
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS related_past_issues (
                id INT PRIMARY KEY AUTO_INCREMENT,
                news_issue_id INT NOT NULL,
                issue_name VARCHAR(200),
                final_score DECIMAL(4,1),
                period VARCHAR(100),
                ai_reason TEXT,
                FOREIGN KEY (news_issue_id) REFERENCES news_issues(id) ON DELETE CASCADE
            ) ENGINE=InnoDB CHARSET=utf8mb4
            """)
            
            # 파이프라인 로그 테이블
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_logs (
                id INT PRIMARY KEY AUTO_INCREMENT,
                pipeline_id VARCHAR(50),
                started_at DATETIME,
                completed_at DATETIME,
                final_status VARCHAR(20),
                total_crawled INT,
                selected_count INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB CHARSET=utf8mb4
            """)
            
            self.connection.commit()
            print("✅ MySQL 테이블 생성 완료")
            
        except Error as e:
            print(f"❌ 테이블 생성 실패: {e}")
            raise
        finally:
            cursor.close()
    
    # ========================================================================
    # 파이프라인 결과 저장
    # ========================================================================
    
    async def save_pipeline_result(self, result: Dict):
        """파이프라인 결과를 MySQL에 저장"""
        if not self.is_initialized():
            raise Exception("데이터베이스가 연결되지 않았습니다.")
        
        cursor = self.connection.cursor()
        
        try:
            print("💾 MySQL에 파이프라인 결과 저장 중...")
            
            # 기존 데이터 삭제 (최신 상태 유지)
            cursor.execute("DELETE FROM related_past_issues")
            cursor.execute("DELETE FROM related_industries")
            cursor.execute("DELETE FROM news_issues")
            
            # API 데이터 추출
            api_data = result.get("api_ready_data", {})
            selected_issues = api_data.get("data", {}).get("selected_issues", [])
            
            # 새 뉴스 이슈들 저장
            for issue_data in selected_issues:
                # 1. 뉴스 이슈 저장
                issue_id = self._save_news_issue(cursor, issue_data)
                
                # 2. 관련 산업 저장
                for industry in issue_data.get("관련산업", []):
                    self._save_related_industry(cursor, issue_id, industry)
                
                # 3. 관련 과거 이슈 저장
                for past_issue in issue_data.get("관련과거이슈", []):
                    self._save_related_past_issue(cursor, issue_id, past_issue)
            
            # 파이프라인 로그 저장
            self._save_pipeline_log(cursor, result, api_data)
            
            self.connection.commit()
            print(f"✅ MySQL 저장 완료: {len(selected_issues)}개 이슈")
            
        except Error as e:
            self.connection.rollback()
            print(f"❌ MySQL 저장 실패: {e}")
            raise
        finally:
            cursor.close()
    
    def _save_news_issue(self, cursor, issue_data: Dict) -> int:
        """뉴스 이슈 저장"""
        query = """
        INSERT INTO news_issues 
        (issue_number, title, content, category, extracted_at, 
         stock_relevance_score, ranking, rag_confidence)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # 날짜 처리
        extracted_at = issue_data.get("추출시간")
        if isinstance(extracted_at, str):
            try:
                extracted_at = datetime.fromisoformat(extracted_at.replace('Z', '+00:00'))
            except:
                extracted_at = datetime.now()
        
        values = (
            issue_data.get("이슈번호", 0),
            issue_data.get("제목", "")[:500],  # 길이 제한
            issue_data.get("내용", ""),
            issue_data.get("카테고리", ""),
            extracted_at,
            float(issue_data.get("주식시장_관련성_점수", 0)),
            issue_data.get("순위", 0),
            float(issue_data.get("RAG분석신뢰도", 0))
        )
        
        cursor.execute(query, values)
        return cursor.lastrowid
    
    def _save_related_industry(self, cursor, news_issue_id: int, industry: Dict):
        """관련 산업 저장"""
        query = """
        INSERT INTO related_industries 
        (news_issue_id, industry_name, final_score, ai_reason)
        VALUES (%s, %s, %s, %s)
        """
        
        values = (
            news_issue_id,
            industry.get("name", "")[:200],
            float(industry.get("final_score", 0)),
            industry.get("ai_reason", "")
        )
        
        cursor.execute(query, values)
    
    def _save_related_past_issue(self, cursor, news_issue_id: int, past_issue: Dict):
        """관련 과거 이슈 저장"""
        query = """
        INSERT INTO related_past_issues 
        (news_issue_id, issue_name, final_score, period, ai_reason)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        values = (
            news_issue_id,
            past_issue.get("name", "")[:200],
            float(past_issue.get("final_score", 0)),
            past_issue.get("period", ""),
            past_issue.get("ai_reason", "")
        )
        
        cursor.execute(query, values)
    
    def _save_pipeline_log(self, cursor, result: Dict, api_data: Dict):
        """파이프라인 로그 저장"""
        query = """
        INSERT INTO pipeline_logs 
        (pipeline_id, started_at, completed_at, final_status, total_crawled, selected_count)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        def parse_datetime(date_str):
            if not date_str:
                return None
            try:
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except:
                return None
        
        values = (
            result.get("pipeline_id", ""),
            parse_datetime(result.get("started_at")),
            parse_datetime(result.get("completed_at")),
            result.get("final_status", ""),
            api_data.get("data", {}).get("total_crawled", 0),
            api_data.get("data", {}).get("selected_count", 0)
        )
        
        cursor.execute(query, values)
    
    # ========================================================================
    # 데이터 조회 (API용)
    # ========================================================================
    
    async def get_latest_news_issues(self) -> List[Dict]:
        """최신 뉴스 이슈들 조회"""
        if not self.is_initialized():
            return []
        
        cursor = self.connection.cursor(dictionary=True)
        
        try:
            # 뉴스 이슈 조회
            cursor.execute("""
            SELECT * FROM news_issues 
            ORDER BY ranking ASC
            """)
            news_issues = cursor.fetchall()
            
            # 각 이슈에 관련 정보 추가
            for issue in news_issues:
                issue_id = issue['id']
                
                # 관련 산업 조회
                cursor.execute("""
                SELECT industry_name, final_score, ai_reason
                FROM related_industries 
                WHERE news_issue_id = %s 
                ORDER BY final_score DESC
                """, (issue_id,))
                issue['related_industries'] = cursor.fetchall()
                
                # 관련 과거 이슈 조회
                cursor.execute("""
                SELECT issue_name, final_score, period, ai_reason
                FROM related_past_issues 
                WHERE news_issue_id = %s 
                ORDER BY final_score DESC
                """, (issue_id,))
                issue['related_past_issues'] = cursor.fetchall()
                
                # 날짜 형식 변환
                if issue.get('extracted_at'):
                    issue['extracted_at'] = issue['extracted_at'].isoformat()
                if issue.get('updated_at'):
                    issue['updated_at'] = issue['updated_at'].isoformat()
            
            return news_issues
            
        except Error as e:
            print(f"❌ 뉴스 조회 실패: {e}")
            return []
        finally:
            cursor.close()
    
    async def get_issue_with_relations(self, issue_id: int) -> Optional[Dict]:
        """특정 이슈 상세 조회"""
        if not self.is_initialized():
            return None
        
        cursor = self.connection.cursor(dictionary=True)
        
        try:
            # 뉴스 이슈 기본 정보
            cursor.execute("SELECT * FROM news_issues WHERE id = %s", (issue_id,))
            issue = cursor.fetchone()
            
            if not issue:
                return None
            
            # 관련 산업
            cursor.execute("""
            SELECT industry_name, final_score, ai_reason
            FROM related_industries WHERE news_issue_id = %s
            """, (issue_id,))
            issue['related_industries'] = cursor.fetchall()
            
            # 관련 과거 이슈
            cursor.execute("""
            SELECT issue_name, final_score, period, ai_reason
            FROM related_past_issues WHERE news_issue_id = %s
            """, (issue_id,))
            issue['related_past_issues'] = cursor.fetchall()
            
            return issue
            
        except Error as e:
            print(f"❌ 이슈 상세 조회 실패: {e}")
            return None
        finally:
            cursor.close()
    
    async def get_latest_pipeline_log(self) -> Optional[Dict]:
        """최근 파이프라인 로그 조회"""
        if not self.is_initialized():
            return None
        
        cursor = self.connection.cursor(dictionary=True)
        
        try:
            cursor.execute("""
            SELECT * FROM pipeline_logs 
            ORDER BY created_at DESC 
            LIMIT 1
            """)
            result = cursor.fetchone()
            
            if result:
                # 날짜 형식 변환
                for date_field in ['started_at', 'completed_at', 'created_at']:
                    if result.get(date_field):
                        result[date_field] = result[date_field].isoformat()
            
            return result
            
        except Error as e:
            print(f"❌ 파이프라인 로그 조회 실패: {e}")
            return None
        finally:
            cursor.close()

# 전역 인스턴스
_database_service = None

def get_database_service() -> DatabaseService:
    global _database_service
    if _database_service is None:
        _database_service = DatabaseService()
    return _database_service