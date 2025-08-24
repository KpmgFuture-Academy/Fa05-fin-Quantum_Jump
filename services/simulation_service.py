# services/simulation_service.py

import os
import json
import io
import base64
from datetime import datetime, timedelta
from typing import Dict, Any, List

import openai
import yfinance as yf
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

# --- 설정 ---
matplotlib.use('Agg')
openai.api_key = os.getenv("OPENAI_API_KEY")

try:
    font_path = 'c:/Windows/Fonts/malgun.ttf'
    font_prop = fm.FontProperties(fname=font_path).get_name()
    plt.rc('font', family=font_prop)
except FileNotFoundError:
    print("경고: 맑은 고딕 폰트를 찾을 수 없습니다.")

class SimulationService:
    def __init__(self):
        self.client = openai.OpenAI()

    def analyze_issue_for_industries(self, issue_name: str, issue_description: str) -> Dict[str, Any]:
        """[AI Agent 1] 과거 이슈로부터 가장 영향받은 3개 산업을 분석"""
        prompt = f"""
        당신은 과거 한국 주식 시장 데이터에 정통한 전문 퀀트 애널리스트입니다.
        주어진 과거 경제 이벤트 정보를 바탕으로, 당시 가장 큰 영향을 받았을 **핵심 산업 3개**를 선정하고 그 이유를 분석해주세요.

        [과거 경제 이벤트 정보]
        - 이벤트명: {issue_name}
        - 상세 내용: {issue_description}

        [출력 형식]
        반드시 아래와 같은 JSON 형식으로만 답변해주세요.
        {{
          "industries": [
            {{"industry_name": "산업명 1", "reason": "이 산업이 왜 이벤트의 영향을 받았는지에 대한 분석"}},
            {{"industry_name": "산업명 2", "reason": "이 산업이 왜 이벤트의 영향을 받았는지에 대한 분석"}},
            {{"industry_name": "산업명 3", "reason": "이 산업이 왜 이벤트의 영향을 받았는지에 대한 분석"}}
          ]
        }}
        """
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "system", "content": "당신은 한국 경제사와 주식시장 역사에 정통한 전문가입니다."}, {"role": "user", "content": prompt}],
                response_format={"type": "json_object"}, temperature=0.1
            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            print(f"AI 산업 분석 오류: {e}")
            return None

    def analyze_industry_for_stocks(self, issue_name: str, industry_name: str) -> Dict[str, Any]:
        """[AI Agent 2] 특정 산업 내에서 대표주 2개와 변동성주 2개를 분석"""
        prompt = f"""
        당신은 특정 산업과 이벤트에 대한 종목 분석 전문가입니다.
        주어진 과거 이벤트와 산업 분야를 바탕으로, 당시 가장 주목할 만한 종목 4개를 선정해주세요.

        [과거 경제 이벤트 정보]
        - 이벤트명: {issue_name}
        
        [분석 대상 산업]
        - 산업명: {industry_name}

        [분석 요청]
        1. 위 산업 분야에서, 당시 이벤트와 관련하여 가장 대표적인 **대형주 2개**를 선정해주세요.
        2. 위 산업 분야 또는 관련 테마에서, 당시 이벤트로 인해 **주가 변동성이 컸던 중소형주 2개**를 선정해주세요.
        3. 각 기업을 선정한 이유를 간략하고 명확하게 설명해주세요.

        [출력 형식]
        반드시 아래와 같은 JSON 형식으로만 답변해주세요. 종목 코드는 '.KS'(코스피) 또는 '.KQ'(코스닥)를 포함해야 합니다.
        {{
          "related_stocks": [
            {{"name": "대표 대형주 A", "ticker": "005930.KS", "reason": "이 기업을 선정한 이유 (대형주 관점)"}},
            {{"name": "대표 대형주 B", "ticker": "000660.KS", "reason": "이 기업을 선정한 이유 (대형주 관점)"}},
            {{"name": "고변동성 중소형주 C", "ticker": "036930.KQ", "reason": "이 기업을 선정한 이유 (중소형 테마주 관점)"}},
            {{"name": "고변동성 중소형주 D", "ticker": "053300.KQ", "reason": "이 기업을 선정한 이유 (중소형 테마주 관점)"}}
          ]
        }}
        """
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "system", "content": "당신은 특정 산업과 이벤트에 대한 종목 분석 전문가입니다."}, {"role": "user", "content": prompt}],
                response_format={"type": "json_object"}, temperature=0.3
            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            print(f"AI 종목 분석 오류: {e}")
            return None

    def generate_investment_commentary(self, issue_name: str, results: Dict, predictions: Dict) -> str:
        """[AI Agent 3] 사용자의 투자 결과에 대한 AI 코멘터리를 생성"""
        prompt = f"""
        당신은 투자 경험이 풍부한 멘토입니다. 사용자의 모의 투자 결과를 보고 맞춤형 피드백을 제공해주세요. 단 하락했다고 예측했을 때 숏 포지션이나 풋 옵션을 사용해야 했다는 점은 고려하지 말아주세요.

        [과거 사례]
        {issue_name}

        [사용자의 예측 및 실제 결과]
        {json.dumps(results, indent=2, ensure_ascii=False)}

        [피드백 요청]
        위 결과를 바탕으로, 다음 항목을 포함하여 사용자에게 유익한 분석 코멘트를 마크다운 형식으로 작성해주세요.
        1.  **총평**: 전체적인 투자 결과(수익률, 예측 정확도)에 대한 간단한 총평.
        2.  **잘한 점과 아쉬운 점**: 사용자의 예측 중 맞고 틀린 것을 짚어주고, 왜 그런 결과가 나왔는지 당시 시장 상황과 연관지어 설명.
        3.  **핵심 교훈 (Key Takeaway)**: 이 과거 사례를 통해 투자자가 배울 수 있는 가장 중요한 교훈 한 가지를 제시.
        """
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "system", "content": "당신은 투자 결과를 분석하고 조언하는 친절한 AI 멘토입니다."}, {"role": "user", "content": prompt}],
                temperature=0.5
            )
            return response.choices[0].message.content
        except Exception as e:
            print(f"AI 코멘터리 생성 오류: {e}")
            return "결과 분석 코멘트를 생성하는 데 실패했습니다."

    def create_stock_chart(self, event_date_str: str, tickers: Dict, show_future: bool = False) -> str:
        """주가 차트를 생성하고 base64 이미지 문자열로 반환"""
        try:
            event_date = datetime.strptime(event_date_str, '%Y-%m-%d')
            start_date = event_date - timedelta(days=60)
            end_date = event_date + timedelta(days=14) if show_future else event_date + timedelta(days=1)

            fig, ax = plt.subplots(figsize=(12, 6))
            ax.axvline(x=event_date, color='red', linestyle='--', linewidth=1.5, label=f'이벤트 시점 ({event_date_str})')

            for ticker, name in tickers.items():
                data = yf.download(ticker, start=start_date, end=end_date, progress=False)
                if not data.empty:
                    ax.plot(data.index, data['Close'], label=f'{name} ({ticker})', linewidth=2, alpha=0.8)

            ax.set_title("과거 사례 주가 변동 추이", fontsize=16, weight='bold')
            ax.legend()
            ax.grid(True, axis='y', linestyle=':', alpha=0.6)
            
            img = io.BytesIO()
            fig.savefig(img, format='png', bbox_inches='tight')
            plt.close(fig)
            img.seek(0)
            return base64.b64encode(img.getvalue()).decode('utf8')
        except Exception as e:
            print(f"차트 생성 오류: {e}")
            return ""

    def get_investment_results(self, event_date_str: str, tickers: Dict, investments: Dict) -> Dict:
        """사용자의 투자를 기반으로 실제 수익률 및 손익을 계산"""
        try:
            event_date = datetime.strptime(event_date_str, '%Y-%m-%d')
        except ValueError:
            print(f"오류: 잘못된 날짜 형식입니다 - {event_date_str}")
            return {ticker: {'status': 'error', 'message': '잘못된 날짜 형식'} for ticker in tickers}

        # 데이터 다운로드 기간을 충분히 확보
        start_date_for_download = event_date - timedelta(days=30)
        end_date_for_download = event_date + timedelta(days=30)
        results = {}
        
        for ticker in tickers.keys():
            try:
                print(f"처리 중: {ticker}")
                
                # yfinance에서 데이터 다운로드
                data = yf.download(ticker, start=start_date_for_download, end=end_date_for_download, progress=False)
                
                if data.empty:
                    raise ValueError(f"{ticker}: yfinance에서 데이터를 가져오지 못했습니다.")
                
                # 데이터가 있는지 확인
                print(f"{ticker} 데이터 수: {len(data)}")
                print(f"{ticker} 날짜 범위: {data.index[0]} ~ {data.index[-1]}")
                
                # timezone 처리를 단순화
                # yfinance 데이터는 보통 timezone이 없거나 UTC
                data.index = pd.to_datetime(data.index).tz_localize(None)
                event_date_normalized = pd.Timestamp(event_date).tz_localize(None)
                
                # 이벤트 날짜 또는 그 이전의 가장 가까운 거래일 찾기
                before_event = data[data.index <= event_date_normalized]
                if before_event.empty:
                    raise ValueError(f"{ticker}: 이벤트 날짜 이전 데이터가 없습니다.")
                
                # 시작 가격 (이벤트 날짜 또는 그 직전 거래일의 종가)
                start_idx = -1  # 마지막 거래일
                start_price = float(before_event['Close'].iloc[start_idx])
                start_date_actual = before_event.index[start_idx]
                
                print(f"{ticker} 시작 가격: {start_price} (날짜: {start_date_actual})")
                
                # 이벤트 이후 14일 이내의 데이터 찾기
                after_event = data[data.index > event_date_normalized]
                if after_event.empty:
                    raise ValueError(f"{ticker}: 이벤트 이후 데이터가 없습니다.")
                
                # 14일 후 또는 가장 가까운 거래일 찾기
                target_end_date = event_date_normalized + timedelta(days=14)
                after_event_in_range = after_event[after_event.index <= target_end_date]
                
                if after_event_in_range.empty:
                    # 14일 이내에 거래일이 없으면 그 이후 첫 거래일 사용
                    print(f"{ticker}: 14일 이내 거래일이 없어 직후 거래일 사용")
                    end_idx = 0  # 이벤트 이후 첫 거래일
                    end_price = float(after_event['Close'].iloc[end_idx])
                    end_date_actual = after_event.index[end_idx]
                else:
                    # 14일 이내 마지막 거래일 사용
                    end_idx = -1
                    end_price = float(after_event_in_range['Close'].iloc[end_idx])
                    end_date_actual = after_event_in_range.index[end_idx]
                
                print(f"{ticker} 종료 가격: {end_price} (날짜: {end_date_actual})")
                
                # 수익률 계산
                return_rate = ((end_price - start_price) / start_price) * 100
                investment = investments.get(ticker, 0)
                
                # 투자금이 0인 경우 처리
                if investment == 0:
                    final_value = 0
                    profit_loss = 0
                else:
                    final_value = investment * (1 + return_rate / 100)
                    profit_loss = final_value - investment
                
                # 상태 결정
                if return_rate > 0.1:  # 0.1% 이상 상승
                    status = 'up'
                elif return_rate < -0.1:  # 0.1% 이상 하락
                    status = 'down'
                else:
                    status = 'flat'  # 거의 변동 없음
                
                results[ticker] = {
                    'status': status,
                    'return_rate': round(return_rate, 2),
                    'investment': investment,
                    'final_value': round(final_value, 2),
                    'profit_loss': round(profit_loss, 2),
                    'start_price': round(start_price, 2),
                    'end_price': round(end_price, 2),
                    'start_date': start_date_actual.strftime('%Y-%m-%d'),
                    'end_date': end_date_actual.strftime('%Y-%m-%d')
                }
                
                print(f"{ticker} 결과: 수익률 {return_rate:.2f}%, 상태: {status}")
                
            except Exception as e:
                print(f"❌ 결과 계산 오류 {ticker}: {str(e)}")
                results[ticker] = {
                    'status': 'error',
                    'return_rate': 0,
                    'investment': investments.get(ticker, 0),
                    'profit_loss': 0,
                    'final_value': investments.get(ticker, 0),
                    'message': str(e),
                    'start_price': 0,
                    'end_price': 0,
                    'start_date': '',
                    'end_date': ''
                }
        
        print(f"최종 결과: {results}")
        return results

simulation_service = SimulationService()