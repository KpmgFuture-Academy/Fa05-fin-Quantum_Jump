# test_rag_correct.py
import json
from services.rag_service import RAGService

try:
    print("📊 올바른 구조로 RAG 분석 테스트...")
    
    # 필터링 결과 로드
    filename = 'data2/2025.08.05_13.30.30_StockFiltered_5issues.json'
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 🔥 올바른 데이터 추출
    filtered_issues = data['selected_issues']  # 이게 핵심!
    
    print(f"📋 실제 이슈 수: {len(filtered_issues)}")
    print(f"📋 첫 번째 이슈 제목: {filtered_issues[0].get('제목', 'N/A')}")
    print(f"📋 첫 번째 이슈 키들: {filtered_issues[0].keys()}")
    
    # RAG 분석 실행
    rag = RAGService()
    print("🔍 RAG 분석 시작...")
    enriched_issues = rag.analyze_issues_with_rag(filtered_issues)
    print(f"✅ RAG 분석 성공! 결과: {len(enriched_issues)}개")
    
    # 결과 저장 (Pipeline_Results 형태로)
    from datetime import datetime
    
    result = {
        "timestamp": datetime.now().isoformat(),
        "total_issues": len(enriched_issues),
        "selected_issues": enriched_issues,
        "average_confidence": rag._calculate_average_confidence(enriched_issues),
        "processing_time": 0,
        "note": "올바른 구조로 RAG 분석 완료"
    }
    
    # Pipeline Results 파일 생성
    timestamp = datetime.now().strftime("%Y.%m.%d_%H.%M.%S")
    result_file = f"data2/{timestamp}_Pipeline_Results.json"
    
    with open(result_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Pipeline Results 파일 생성 성공: {result_file}")
    
    # 결과 상세 출력
    print("\n📊 RAG 분석 결과:")
    for i, issue in enumerate(enriched_issues):
        title = issue.get('제목', 'N/A')[:40]
        industries = len(issue.get('관련산업', []))
        past_issues = len(issue.get('관련과거이슈', []))
        confidence = issue.get('RAG분석신뢰도', {})
        
        print(f"  {i+1}. {title}")
        print(f"     관련산업: {industries}개, 과거이슈: {past_issues}개")
        print(f"     신뢰도: {confidence}")

except Exception as e:
    print(f"❌ RAG 분석 실패: {e}")
    import traceback
    traceback.print_exc()