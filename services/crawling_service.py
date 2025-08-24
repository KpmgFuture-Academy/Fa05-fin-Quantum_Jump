# services/crawling_service.py (수정된 버전 - 원본 코드 그대로 사용)
"""
크롤링 및 필터링 통합 서비스
원본 BigKindsCrawler를 그대로 사용하고 필터링만 추가
"""

import os
import json
import time
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser

from .crawling_bigkinds import BigKindsCrawler

class CrawlingService:
    """크롤링 및 필터링 통합 서비스 - 원본 BigKindsCrawler 사용"""
    
    def __init__(self, data_dir: str = "data2", headless: bool = True):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.headless = headless
        
        load_dotenv(override=True)
        
        # AI 필터링용 LLM 초기화
        self.llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
        
        print("✅ 크롤링 서비스 초기화 완료")
    
    def crawl_and_filter_news(self, 
                                issues_per_category: int = 10,
                                target_filtered_count: int = 5) -> Dict:
        """원본 BigKindsCrawler 사용 + 필터링"""
        
        print(f"🕷️ BigKinds 크롤링 시작: 카테고리별 {issues_per_category}개씩")
        
        # Step 1: 원본 BigKindsCrawler로 크롤링
        crawler = BigKindsCrawler(
            data_dir=str(self.data_dir),
            headless=self.headless,
            issues_per_category=issues_per_category
        )
        
        # 원본 메서드 그대로 호출
        crawling_result = crawler.crawl_all_categories()
        
        print(f"✅ 크롤링 완료: {crawling_result.get('total_issues', 0)}개 이슈")
        
        # Step 2: 필터링
        all_issues = crawling_result.get("all_issues", [])
        if all_issues:
            filtering_result = self._filter_by_stock_relevance(all_issues, target_filtered_count)
        else:
            filtering_result = {
                "selected_issues": [],
                "filter_metadata": {
                    "filtering_method": "no_issues_to_filter",
                    "original_count": 0,
                    "selected_count": 0,
                    "filtered_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }
        
        return {
            **crawling_result,
            "filtered_issues": filtering_result["selected_issues"],
            "filter_metadata": filtering_result["filter_metadata"]
        }
    
    def _filter_by_stock_relevance(self, all_issues: List[Dict], target_count: int) -> Dict:
        """주식시장 관련성 기반 필터링"""
        
        print(f"🤖 AI 필터링 시작: {len(all_issues)}개 → {target_count}개 선별")
        
        # 각 이슈별로 주식시장 관련성 점수 계산
        scored_issues = []
        
        for i, issue in enumerate(all_issues, 1):
            print(f"🔄 이슈 {i}/{len(all_issues)} 분석 중: {issue.get('제목', 'N/A')[:30]}...")
            
            # AI로 주식시장 관련성 분석
            relevance_score = self._analyze_stock_market_relevance(issue)
            
            scored_issue = issue.copy()
            scored_issue.update({
                "주식시장_관련성_점수": relevance_score["종합점수"],
                "관련성_분석": relevance_score
            })
            
            scored_issues.append(scored_issue)
        
        # 점수순 정렬 및 상위 선별
        scored_issues.sort(key=lambda x: x["주식시장_관련성_점수"], reverse=True)
        selected_issues = scored_issues[:target_count]
        
        # 순위 부여
        for rank, issue in enumerate(selected_issues, 1):
            issue["rank"] = rank
        
        result = {
            "selected_issues": selected_issues,
            "filter_metadata": {
                "filtering_method": "gpt-4o-mini_stock_relevance",
                "original_count": len(all_issues),
                "selected_count": len(selected_issues),
                "average_score": sum(issue["주식시장_관련성_점수"] for issue in selected_issues) / len(selected_issues) if selected_issues else 0,
                "filtered_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        }
        
        # 필터링 결과 저장
        self._save_filtering_result(result)
        
        print(f"✅ AI 필터링 완료: 상위 {len(selected_issues)}개 선별")
        return result
    
    def _analyze_stock_market_relevance(self, issue: Dict) -> Dict:
        """AI를 사용한 주식시장 관련성 분석 (근거 포함)"""
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", """너는 한국 주식시장 전문 애널리스트야. 
    주어진 뉴스 이슈들을 분석하여 주식시장에 가장 큰 영향을 미칠 것으로 예상되는 이슈들을 선별해야 해.

    📊 평가 기준 (각 1-10점):
    1. **직접적 기업 영향**: 특정 기업이나 산업의 실적에 직접적인 영향을 미치는가?
    2. **정책적 영향**: 금리, 세금, 규제 변화 등 시장 전반에 영향을 미치는 정책인가?
    3. **시장 심리**: 투자자 신뢰도, 리스크 인식, 투자 심리에 미치는 영향은?
    4. **거시경제**: GDP, 인플레이션, 환율 등 거시경제 지표에 미치는 영향은?
    5. **산업 트렌드**: 새로운 기술이나 소비 패턴 변화로 인한 산업 영향은?

    💡 우선순위:
    - 단기적 주가 변동을 일으킬 가능성이 높은 이슈
    - 특정 업종이나 테마주에 영향을 미치는 이슈
    - 외국인 투자나 기관 투자에 영향을 미치는 이슈
    - 정부 정책이나 규제 변화 관련 이슈

    ⚠️ 중요: 각 점수에 대해 반드시 구체적인 근거를 제시해야 합니다."""),
            ("human", """
    [뉴스 제목]
    {title}

    [뉴스 내용]  
    {content}

    위 뉴스의 주식시장 관련성을 분석해주세요.
    각 항목별로 점수와 함께 구체적인 근거를 제시해주세요.

    출력 형식 (JSON):
    {{
        "직접적_기업영향": 점수,
        "직접적_기업영향_근거": "구체적인 분석 근거 (어떤 기업에게 어떤 영향을 미치는지)",
        "정책적_영향": 점수,
        "정책적_영향_근거": "구체적인 분석 근거 (어떤 정책 변화가 예상되는지)",
        "시장_심리_영향": 점수,
        "시장_심리_영향_근거": "구체적인 분석 근거 (투자자 심리에 어떤 영향을 미치는지)",
        "거시경제_영향": 점수,
        "거시경제_영향_근거": "구체적인 분석 근거 (거시경제 지표에 어떤 영향을 미치는지)",
        "산업_트렌드_영향": 점수,
        "산업_트렌드_영향_근거": "구체적인 분석 근거 (어떤 산업 트렌드 변화가 예상되는지)",
        "종합점수": 점수,
        "종합점수_계산방식": "합계/평균/가중평균 중 어떤 방식으로 계산했는지",
        "주된영향분야": ["섹터1", "섹터2"],
        "예상영향방향": "긍정적/부정적/중립적",
        "영향시기": "즉시/단기/중기",
        "분석근거": "상세 분석 내용",
        "예상시장반응": "예상되는 시장 반응 설명"
    }}""")
        ])
        
        parser = JsonOutputParser()
        chain = prompt | self.llm | parser
        
        try:
            result = chain.invoke({
                "title": issue.get("제목", ""),
                "content": issue.get("내용", "")
            })
            
            # 🔥 수정된 반환 데이터 - 근거 포함
            return {
                "직접적_기업영향": result.get("직접적_기업영향", 5),
                "직접적_기업영향_근거": result.get("직접적_기업영향_근거", "분석 근거 미제공"),
                "정책적_영향": result.get("정책적_영향", 5),
                "정책적_영향_근거": result.get("정책적_영향_근거", "분석 근거 미제공"),
                "시장_심리_영향": result.get("시장_심리_영향", 5),
                "시장_심리_영향_근거": result.get("시장_심리_영향_근거", "분석 근거 미제공"),
                "거시경제_영향": result.get("거시경제_영향", 5),
                "거시경제_영향_근거": result.get("거시경제_영향_근거", "분석 근거 미제공"),
                "산업_트렌드_영향": result.get("산업_트렌드_영향", 5),
                "산업_트렌드_영향_근거": result.get("산업_트렌드_영향_근거", "분석 근거 미제공"),
                "종합점수": result.get("종합점수", 5),
                "종합점수_계산방식": result.get("종합점수_계산방식", "AI 자체 계산"),
                "주된영향분야": result.get("주된영향분야", []),
                "예상영향방향": result.get("예상영향방향", "중립적"),
                "영향시기": result.get("영향시기", "단기"),
                "분석근거": result.get("분석근거", "AI 분석 완료"),
                "예상시장반응": result.get("예상시장반응", "")
            }
            
        except Exception as e:
            print(f"❌ AI 분석 실패: {e}")
            return {
                "직접적_기업영향": 5, 
                "직접적_기업영향_근거": f"AI 분석 실패: {e}",
                "정책적_영향": 5, 
                "정책적_영향_근거": f"AI 분석 실패: {e}",
                "시장_심리_영향": 5,
                "시장_심리_영향_근거": f"AI 분석 실패: {e}",
                "거시경제_영향": 5, 
                "거시경제_영향_근거": f"AI 분석 실패: {e}",
                "산업_트렌드_영향": 5, 
                "산업_트렌드_영향_근거": f"AI 분석 실패: {e}",
                "종합점수": 5,
                "종합점수_계산방식": "오류로 인한 기본값",
                "주된영향분야": [], 
                "예상영향방향": "중립적", 
                "영향시기": "단기",
                "분석근거": f"AI 분석 실패: {e}", 
                "예상시장반응": ""
            }

    def _save_filtering_result(self, result: Dict):
        """필터링 결과 저장"""
        timestamp = datetime.now().strftime("%Y.%m.%d_%H.%M.%S")
        filename = f"{timestamp}_StockFiltered_{len(result['selected_issues'])}issues.json"
        filepath = self.data_dir / filename
        
        save_data = {
            **result,
            "file_info": {
                "filename": filename,
                "created_at": datetime.now().isoformat(),
                "filter_version": "StockRelevanceFilter_v2.0_WithReasons"  # 🔥 버전 업데이트
            }
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, ensure_ascii=False, indent=2)
        
        print(f"💾 필터링 결과 저장 (근거 포함): {filepath}")