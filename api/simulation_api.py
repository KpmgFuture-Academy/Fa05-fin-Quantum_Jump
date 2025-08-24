from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any

# 1. 서비스 로직을 임포트합니다.
from services.simulation_service import simulation_service

# 2. 이 파일의 모든 API 경로를 관리할 APIRouter를 생성합니다.
router = APIRouter()

# 3. API가 받을 요청/응답 데이터의 형식을 정의합니다.
class IndustryRequest(BaseModel):
    issue_name: str
    issue_description: str

class StockRequest(BaseModel):
    issue_name: str
    issue_date: str
    industry_name: str

class CalculationRequest(BaseModel):
    issue_name: str
    issue_date: str
    tickers: Dict[str, str]
    predictions: Dict[str, str]
    investments: Dict[str, float]

# --- API 엔드포인트 정의 ---

@router.post("/analyze_industries")
async def analyze_industries_for_issue(request: IndustryRequest):
    """[1단계] 과거 이슈를 받아 AI가 관련 산업 3개를 분석하여 반환"""
    ai_analysis = simulation_service.analyze_issue_for_industries(request.issue_name, request.issue_description)
    if not ai_analysis or not ai_analysis.get("industries"):
        raise HTTPException(status_code=500, detail="AI 산업 분석에 실패했습니다.")
    return {"success": True, "data": ai_analysis}

@router.post("/analyze_stocks")
async def analyze_stocks_for_industry(request: StockRequest):
    """[2단계] 선택된 산업을 받아 AI가 관련 종목 4개를 분석하고 차트 데이터 반환"""
    ai_analysis = simulation_service.analyze_industry_for_stocks(request.issue_name, request.industry_name)
    if not ai_analysis or not ai_analysis.get("related_stocks"):
        raise HTTPException(status_code=500, detail="AI 종목 분석에 실패했습니다.")

    tickers = {stock['ticker']: stock['name'] for stock in ai_analysis.get('related_stocks', [])}
    if not tickers:
        raise HTTPException(status_code=404, detail="AI가 관련 종목을 찾지 못했습니다.")

    initial_chart = simulation_service.create_stock_chart(request.issue_date, tickers, show_future=False)

    return {"success": True, "data": {"ai_analysis": ai_analysis, "tickers": tickers, "chart_image": initial_chart}}

@router.post("/calculate_result")
async def calculate_simulation_result(request: CalculationRequest):
    """[3단계] 사용자의 투자를 받아 실제 결과와 AI 코멘터리를 함께 반환"""
    investment_results = simulation_service.get_investment_results(request.issue_date, request.tickers, request.investments)
    full_chart = simulation_service.create_stock_chart(request.issue_date, request.tickers, show_future=True)
    ai_commentary = simulation_service.generate_investment_commentary(request.issue_name, investment_results, request.predictions)

    total_investment = sum(request.investments.values())
    total_final_value = sum(res.get('final_value', 0) for res in investment_results.values())
    total_profit_loss = total_final_value - total_investment

    correct_predictions = 0
    total_predictions = len(request.predictions)
    if total_predictions > 0:
        for ticker, pred in request.predictions.items():
            if pred == investment_results.get(ticker, {}).get('status'):
                correct_predictions += 1
        prediction_accuracy = (correct_predictions / total_predictions * 100)
    else:
        prediction_accuracy = 0

    return {
        "success": True,
        "data": {
            "issue_name": request.issue_name, "tickers": request.tickers,
            "investment_results": investment_results, "predictions": request.predictions,
            "chart_image": full_chart, "total_investment": total_investment,
            "total_final_value": total_final_value, "total_profit_loss": total_profit_loss,
            "prediction_accuracy": prediction_accuracy, "ai_commentary": ai_commentary
        }
    }