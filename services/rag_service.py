"""
RAG 분석 서비스 - 최종 개선 버전
- 검증 레이어(Verification Layer) 추가
- 다차원 신뢰도(일관성+최고 연관도) 도입
- 후보군 생성 로직 개선 (벡터 검색 결과 -> AI 분석 -> 검증)
- 오류 처리 강화
"""

import os
import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pinecone import Pinecone

# .env 파일에서 환경 변수 로드
load_dotenv(override=True)

class RAGService:
    """RAG 분석 서비스 (검증 및 오류 처리 강화 버전)"""
    
    def __init__(self):
        # 환경 설정
        self.EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
        self.INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "ordaproject")
        self.PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
        
        # LLM 초기화 (분석용, 검증용)
        self.analyzer_llm = ChatOpenAI(model="gpt-4o", temperature=0)
        self.verifier_llm = ChatOpenAI(model="gpt-4o-mini", temperature=0) # 검증은 더 빠르고 저렴한 모델 사용
        self.embedding = OpenAIEmbeddings(model=self.EMBEDDING_MODEL)
        
        # Pinecone 클라이언트 초기화
        self.pc = Pinecone(api_key=self.PINECONE_API_KEY)
        self.index = self.pc.Index(self.INDEX_NAME)
        
        # 데이터베이스 로딩
        self._load_databases()
        
        print("✅ RAG 분석 서비스 초기화 완료 (검증 레이어 및 오류 처리 강화)")
    
    def _load_databases(self):
        """산업 DB 및 과거 이슈 DB 로딩"""
        try:
            # 산업 DB 로딩
            self.industry_df = pd.read_csv("data/산업DB.v.0.3.csv")
            self.industry_dict = dict(zip(self.industry_df["KRX 업종명"], self.industry_df["상세내용"]))
            print(f"✅ 산업 DB 로드: {len(self.industry_dict)}개 업종")
            
            # 과거 이슈 DB 로딩
            self.past_df = pd.read_csv("data/Past_news.csv")
            self.issue_dict = dict(zip(
                self.past_df["Issue_name"], 
                self.past_df["Contents"] + "\n\n상세: " + self.past_df["Contentes(Spec)"]
            ))
            print(f"✅ 과거 이슈 DB 로드: {len(self.issue_dict)}개 이슈")
            
        except Exception as e:
            print(f"⚠️ DB 로드 실패: {e}")
            self.industry_dict = {}
            self.issue_dict = {}

    def analyze_issues_with_rag(self, filtered_issues: List[Dict]) -> List[Dict]:
        """필터링된 이슈들에 대해 RAG 분석 수행 (오류 방지 강화)"""
        print(f"🔍 RAG 분석 시작: {len(filtered_issues)}개 이슈")
        enriched_issues = []
        
        for i, issue in enumerate(filtered_issues, 1):
            print(f"🔄 이슈 {i}/{len(filtered_issues)} RAG 분석 중: {issue.get('제목', 'N/A')[:50]}...")
            
            try:
                # 관련 산업 분석
                related_industries = self._analyze_industry_for_issue(issue)
                
                # 관련 과거 이슈 분석
                related_past_issues = self._analyze_past_issues_for_issue(issue)
                
                # 안전한 RAG 다차원 신뢰도 계산
                rag_confidence = self._calculate_rag_confidence(related_industries, related_past_issues)
                
                # 기본 이슈에 RAG 결과 추가
                enriched_issue = issue.copy()
                enriched_issue.update({
                    "관련산업": related_industries,
                    "관련과거이슈": related_past_issues,
                    "RAG분석신뢰도": rag_confidence
                })
                enriched_issues.append(enriched_issue)
                
                confidence_display = self._format_confidence_for_display(rag_confidence)
                print(f"  ✅ 이슈 {i} RAG 완료: 신뢰도 {confidence_display}")
                
            except Exception as e:
                print(f"  ❌ 이슈 {i} RAG 분석 실패: {e}")
                # 실패한 경우에도 기본 구조 유지하며 다음 이슈로 진행
                enriched_issue = issue.copy()
                enriched_issue.update({
                    "관련산업": [],
                    "관련과거이슈": [],
                    "RAG분석신뢰도": {"consistency_score": 0.0, "peak_relevance_score": 0.0},
                    "error": str(e)
                })
                enriched_issues.append(enriched_issue)
        
        avg_confidence = self._calculate_average_confidence(enriched_issues)
        print(f"✅ RAG 분석 완료: 전체 평균 일관성 점수 {avg_confidence}")
        
        return enriched_issues

    def _analyze_industry_for_issue(self, issue: Dict) -> List[Dict]:
        """특정 이슈에 대한 관련 산업 분석 (검증 레이어 포함)"""
        query = f"{issue.get('제목', '')}\n{issue.get('원본내용', issue.get('내용', ''))}"
        
        # Step 1: 벡터 검색으로 1차 후보군 추출
        vector_candidates = self._vector_search(query, namespace="industry")
        
        # Step 2: AI Agent가 1차 후보군을 재평가(Rerank)
        ai_candidates = self._ai_rerank_candidates(query, vector_candidates, "industry")
        
        # Step 3: 결과 통합 및 정렬
        combined_candidates = self._combine_results(vector_candidates, ai_candidates, "industry")
        
        # Step 4: 검증 레이어 (상위 3개 후보에 대해 수행)
        verified_candidates = self._apply_verification_layer(query, combined_candidates)
        
        # Step 5: 최종 정렬 후 반환
        return sorted(verified_candidates, key=lambda x: x["final_score"], reverse=True)

    def _analyze_past_issues_for_issue(self, issue: Dict) -> List[Dict]:
        """특정 이슈에 대한 관련 과거 이슈 분석 (검증 레이어 포함)"""
        query = f"{issue.get('제목', '')}\n{issue.get('원본내용', issue.get('내용', ''))}"
        
        # Step 1: 벡터 검색으로 1차 후보군 추출
        vector_candidates = self._vector_search(query, namespace="past_issue")

        # Step 2: AI Agent가 1차 후보군을 재평가(Rerank)
        ai_candidates = self._ai_rerank_candidates(query, vector_candidates, "past_issue")
        
        # Step 3: 결과 통합 및 정렬
        combined_candidates = self._combine_results(vector_candidates, ai_candidates, "past_issue")
        
        # Step 4: 검증 레이어 (상위 3개 후보에 대해 수행)
        verified_candidates = self._apply_verification_layer(query, combined_candidates)

        # Step 5: 최종 정렬 후 반환
        return sorted(verified_candidates, key=lambda x: x["final_score"], reverse=True)
    
    def _vector_search(self, query: str, namespace: str, top_k: int = 10) -> List[Dict]:
        """Pinecone 벡터 검색 수행"""
        try:
            query_embedding = self.embedding.embed_query(query)
            search_results = self.index.query(
                vector=query_embedding, top_k=top_k, include_metadata=True, namespace=namespace
            )
            
            candidates = []
            for match in search_results.matches:
                meta = match.metadata or {}
                name = meta.get("name")
                if name and not any(c["name"] == name for c in candidates):
                    candidates.append({
                        "name": name,
                        "similarity": round(match.score * 100, 1),
                        "description": meta.get("description", ""),
                        "period": f"{meta.get('start_date', '')} ~ {meta.get('end_date', '')}" if namespace == 'past_issue' else None
                    })
            print(f"  📊 {namespace} 벡터 검색: {len(candidates)}개 후보 발견")
            return candidates
        except Exception as e:
            print(f"❌ {namespace} 벡터 검색 실패: {e}")
            return []

    def _ai_rerank_candidates(self, news_content: str, vector_candidates: List[Dict], mode: str) -> List[Dict]:
        """AI Agent가 벡터 검색 후보군을 재평가하여 순위, 점수, 이유 부여"""
        if not vector_candidates: return []
        
        task_description = {
            "industry": "뉴스와 가장 관련성이 높은 순서대로 순위를 매기고 점수와 이유를 부여",
            "past_issue": "현재 뉴스와 가장 유사한 패턴을 보이는 순서대로 순위를 매기고 점수와 이유를 부여"
        }.get(mode)
        
        field_name = "industry" if mode == "industry" else "issue"

        prompt = ChatPromptTemplate.from_messages([
            ("system", f"너는 뉴스와의 관련성을 판단하는 전문 애널리스트다. 주어진 뉴스 내용과 후보 리스트를 분석하여, {task_description}해야 한다. 출력은 JSON 형식이어야 한다."),
            ("human", """
[뉴스 내용]
{news}

[후보 리스트] 
{candidate_list}

위 뉴스 내용과 관련성이 높은 순서대로 후보들의 순위를 다시 정렬하고, 각 후보에 대해 관련성 점수(1-10점)와 간단한 이유를 제시해주세요.

출력 형식 (JSON):
{{
  "candidates": [
    {{"{field}": "후보명", "score": 점수, "reason": "관련성 이유"}}, ...
  ]
}}""")
        ])
        
        parser = JsonOutputParser()
        chain = prompt | self.analyzer_llm | parser
        
        candidate_names = [c['name'] for c in vector_candidates]
        
        try:
            result = chain.invoke({
                "news": news_content, 
                "candidate_list": ", ".join(candidate_names),
                "field": field_name
            })
            candidates = result.get("candidates", [])
            print(f"  🤖 AI {mode} 재평가: {len(candidates)}개 후보 생성")
            return candidates
        except Exception as e:
            print(f"❌ AI {mode} 후보 추출 실패: {e}")
            return []

    def _combine_results(self, vector_candidates: List[Dict], ai_candidates: List[Dict], mode: str) -> List[Dict]:
        """벡터 검색 결과와 AI 재평가 결과를 결합하여 최종 점수 계산"""
        all_candidates = {}
        field_name = "industry" if mode == "industry" else "issue"
        
        for candidate in vector_candidates:
            name = candidate["name"]
            all_candidates[name] = {
                "name": name,
                "vector_score": min(candidate["similarity"] / 10, 10),
                "ai_score": 0, "ai_reason": "",
                "description": candidate["description"],
                "period": candidate.get("period")
            }
        
        for candidate in ai_candidates:
            name = candidate.get(field_name)
            if name in all_candidates:
                all_candidates[name]["ai_score"] = candidate.get("score", 0)
                all_candidates[name]["ai_reason"] = candidate.get("reason", "")
        
        for candidate in all_candidates.values():
            vector_score = candidate.get("vector_score", 0)
            ai_score = candidate.get("ai_score", 0)
            candidate["final_score"] = round((vector_score * 0.3 + ai_score * 0.7), 1)
        
        return sorted(all_candidates.values(), key=lambda x: x.get("final_score", 0), reverse=True)

    def _apply_verification_layer(self, news_content: str, candidates: List[Dict], top_k: int = 3) -> List[Dict]:
        """상위 후보군에 대해 검증 레이어 적용"""
        verified_candidates = []
        for candidate in candidates[:top_k]:
            print(f"  🔍 검증 시작: {candidate['name']}")
            verification_result = self._verify_reasoning(
                news_content=news_content,
                item_name=candidate['name'],
                reason=candidate.get('ai_reason', '')
            )
            candidate['verification'] = verification_result
            if not verification_result.get('is_grounded'):
                candidate['final_score'] = round(candidate['final_score'] * 0.5, 1) # 검증 실패 시 50% 페널티
                print(f"    ❌ 검증 실패: {candidate['name']}")
            else:
                print(f"    ✅ 검증 성공: {candidate['name']}")
            verified_candidates.append(candidate)
        
        return verified_candidates
        
    def _verify_reasoning(self, news_content: str, item_name: str, reason: str) -> dict:
        """AI가 생성한 분석 근거가 원본 뉴스에 기반하는지 검증 (실패 이유 포함)"""
        if not reason:
            return {"is_grounded": False, "supporting_quote": "", "unverified_reason": "AI가 분석 근거를 생성하지 않음"}

        # 🔥 [수정] 프롬프트에 'unverified_reason' 필드와 실패 이유 작성 요청 추가
        prompt = ChatPromptTemplate.from_template("""
system: 너는 매우 꼼꼼한 팩트체커(Fact-Checker)다. '분석 근거'가 '원본 뉴스' 내용에 기반하는지 확인하고, 결과는 반드시 JSON으로만 응답해야 한다.
human:
[원본 뉴스]
{news}

[분석 대상]: {item}
[분석 근거]: {reason}

'분석 근거'가 '원본 뉴스'에 기반하면 `is_grounded`를 `true`로, 근거 문장을 `supporting_quote`에 넣고 `unverified_reason`은 빈 문자열("")로 설정해.
만약 '원본 뉴스'에서 근거를 찾을 수 없거나 과도한 추론이라면 `is_grounded`를 `false`로, `supporting_quote`는 비워두고, 실패 이유를 `unverified_reason`에 간결하게 작성해줘. (예: "뉴스에 언급되지 않은 내용", "과도한 추론", "내용 불일치")

JSON 출력 형식:
{{
    "is_grounded": boolean,
    "supporting_quote": "인용문",
    "unverified_reason": "실패 이유"
}}
""")
        parser = JsonOutputParser()
        chain = prompt | self.verifier_llm | parser

        try:
            return chain.invoke({ "news": news_content, "item": item_name, "reason": reason })
        except Exception as e:
            print(f"⚠️ 검증 레이어 실패: {e}")
            # 🔥 [수정] 예외 발생 시 반환값에 unverified_reason 추가
            return {"is_grounded": False, "supporting_quote": "", "unverified_reason": "검증 중 오류 발생"}

    def _calculate_rag_confidence(self, industries: List[Dict], past_issues: List[Dict]) -> dict:
        """RAG 분석 신뢰도를 다각적으로 계산 (일관성 점수 + 최고 연관도 평균)"""
        if not industries and not past_issues:
            return {"consistency_score": 0.0, "peak_relevance_score": 0.0}
        
        # --- 1. 일관성 점수 (평균 방식) ---
        total_avg_score = 0
        count = 0
        if industries:
            # 유효한 점수만 필터링하여 평균 계산
            valid_scores = [ind.get("final_score", 0) for ind in industries if isinstance(ind.get("final_score"), (int, float))]
            if valid_scores:
                industry_avg = sum(valid_scores) / len(valid_scores)
                total_avg_score += industry_avg
                count += 1
        if past_issues:
            # 유효한 점수만 필터링하여 평균 계산
            valid_scores = [issue.get("final_score", 0) for issue in past_issues if isinstance(issue.get("final_score"), (int, float))]
            if valid_scores:
                past_avg = sum(valid_scores) / len(valid_scores)
                total_avg_score += past_avg
                count += 1
        consistency_score = round(total_avg_score / count if count > 0 else 0, 1)

        # --- 🔥 [수정] 2. 최고 연관도 (최고점들의 '평균' 방식) ---
        peak_scores = []
        if industries:
            valid_scores = [ind.get("final_score", 0) for ind in industries if isinstance(ind.get("final_score"), (int, float))]
            if valid_scores:
                # '관련 산업'의 최고 점수를 리스트에 추가
                peak_scores.append(max(valid_scores))
        
        if past_issues:
            valid_scores = [issue.get("final_score", 0) for issue in past_issues if isinstance(issue.get("final_score"), (int, float))]
            if valid_scores:
                # '과거 이슈'의 최고 점수를 리스트에 추가
                peak_scores.append(max(valid_scores))
        
        # 수집된 최고 점수들의 평균을 계산
        if peak_scores:
            peak_relevance_score = round(sum(peak_scores) / len(peak_scores), 1)
        else:
            peak_relevance_score = 0.0
        
        return { 
            "consistency_score": consistency_score, 
            "peak_relevance_score": peak_relevance_score 
        }
    
    def _calculate_average_confidence(self, enriched_issues: List[Dict]) -> float:
        """안전한 전체 이슈들의 평균 RAG 일관성 점수 계산"""
        if not enriched_issues: return 0.0
        try:
            confidences = []
            for issue in enriched_issues:
                rag_confidence = issue.get("RAG분석신뢰도")
                if isinstance(rag_confidence, dict):
                    consistency = rag_confidence.get("consistency_score")
                    if consistency is not None:
                        confidences.append(float(consistency))
            
            return round(sum(confidences) / len(confidences), 2) if confidences else 0.0
        except Exception as e:
            print(f"⚠️ 평균 신뢰도 계산 오류: {e}")
            return 0.0

    def _format_confidence_for_display(self, rag_confidence) -> str:
        """신뢰도를 로깅용으로 포맷팅"""
        if isinstance(rag_confidence, dict):
            consistency = rag_confidence.get("consistency_score", "N/A")
            peak = rag_confidence.get("peak_relevance_score", "N/A")
            return f"일관성:{consistency}, 최고연관도:{peak}"
        return str(rag_confidence)