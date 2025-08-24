# models/schemas.py (누락된 모델들 추가)
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Literal

# --- Health Check Schemas ---
class ComponentHealth(BaseModel):
    name: str
    status: Literal["ok", "degraded", "error", "disabled"]
    detail: Optional[Dict | str] = None

class HealthResponse(BaseModel):
    status: Literal["ok", "degraded", "error"]
    timestamp: str
    components: Dict[str, ComponentHealth]

# --- News Schemas (추가) ---
class NewsIssue(BaseModel):
    """뉴스 이슈 모델"""
    id: Optional[int] = None
    issue_number: Optional[int] = None
    title: str
    content: Optional[str] = None
    category: Optional[str] = None
    extracted_at: Optional[str] = None
    stock_relevance_score: Optional[float] = None
    ranking: Optional[int] = None
    rag_confidence: Optional[float] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    
    # RAG 분석 결과
    related_industries: Optional[List[Dict]] = None
    related_past_issues: Optional[List[Dict]] = None

class NewsListResponse(BaseModel):
    """뉴스 목록 응답 모델"""
    success: bool
    data: Dict[str, Any]
    message: Optional[str] = None

# --- Analysis Schemas ---
class AnalysisRequest(BaseModel):
    content: str = Field(..., description="분석할 뉴스 기사 본문")

class PastIssueInfo(BaseModel):
    issue_name: str
    contents: str
    similarity_score: float

class IndustryInfo(BaseModel):
    industry_name: str
    description: str
    similarity_score: float

class FullAnalysisResponse(BaseModel):
    explanation: str
    confidence: float
    past_issues: List[PastIssueInfo]
    industries: List[IndustryInfo]

class CurrentIssue(BaseModel):
    이슈번호: int
    카테고리: str
    제목: str
    내용: str
    # RAG 분석으로 추가된 필드들
    explanation: Optional[str] = None
    confidence: Optional[float] = None
    related_industries: Optional[List[IndustryInfo]] = None

# --- Database Schemas ---
class PastIssue(BaseModel):
    id: str
    issue_name: str
    contents: Optional[str] = None
    related_industries: Optional[str] = None
    start_date: Optional[str] = None

class Industry(BaseModel):
    krx_name: str
    description: Optional[str] = None

class DatabaseStats(BaseModel):
    industries: int
    past_issues: int
    current_issues: int
    simulation_results: int
    db_size_mb: float

# --- Simulation Schemas ---
class StockSelection(BaseModel):
    code: str
    name: str
    allocation: float = Field(..., gt=0, le=100)

class SimulationRequest(BaseModel):
    scenario_id: str
    investment_amount: int = Field(..., gt=0)
    investment_period: int = Field(..., ge=1, le=24, description="투자 기간(개월)")
    selected_stocks: List[StockSelection]

class SimulationResult(BaseModel):
    initial_amount: int
    final_amount: int
    total_return_pct: float

class SimulationResponse(BaseModel):
    scenario_info: Dict
    simulation_results: SimulationResult
    market_comparison: Dict
    stock_analysis: List[Dict]
    learning_points: List[str]

class Scenario(BaseModel):
    id: str
    name: str
    description: str
    period: str
    related_industries: List[str]

class RecommendedStockInfo(BaseModel):
    scenario_id: str
    recommended_stocks: Dict[str, List[Dict[str, str]]]

class ValidationResponse(BaseModel):
    valid: bool
    errors: List[str]
    warnings: List[str]

# --- 상세 분석을 위한 새로운 스키마들 (추가) ---
class DetailedSectorAnalysis(BaseModel):
    섹터명: str
    영향도: str  # "높음", "중간", "낮음"
    방향: str   # "긍정적", "부정적", "중립적"

class DetailedIssueAnalysis(BaseModel):
    rank: int
    제목: str
    핵심영향요인: List[str]
    영향섹터: List[DetailedSectorAnalysis]
    관련종목예시: List[str]
    과거유사사례: str
    투자전략: str
    리스크요인: List[str]
    신뢰도: float

class MarketOutlook(BaseModel):
    overall_sentiment: str
    key_themes: List[str]
    attention_sectors: List[str]
    risk_factors: List[str]

class EnhancedAnalysisResponse(BaseModel):
    selected_issues: List[Dict]
    detailed_analysis: List[DetailedIssueAnalysis]
    market_outlook: MarketOutlook
    filter_metadata: Dict