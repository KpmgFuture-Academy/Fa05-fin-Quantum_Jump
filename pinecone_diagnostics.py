#!/usr/bin/env python3
"""
Pinecone 인덱스의 실제 데이터 구조를 확인하고 LangChain 호환성 문제를 해결하는 스크립트
"""

import os
from dotenv import load_dotenv
from pinecone import Pinecone
from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore

def inspect_pinecone_structure():
    """Pinecone 인덱스의 실제 데이터 구조 확인"""
    load_dotenv(override=True)
    
    api_key = os.getenv("PINECONE_API_KEY")
    index_name = os.getenv("PINECONE_INDEX_NAME", "ordaproject")
    
    # Pinecone 클라이언트 직접 사용
    pc = Pinecone(api_key=api_key)
    index = pc.Index(index_name)
    
    print("🔍 Pinecone 데이터 구조 분석...")
    
    # 각 네임스페이스별로 샘플 데이터 확인
    for namespace in ["industry", "past_issue"]:
        print(f"\n📂 '{namespace}' 네임스페이스 분석:")
        
        try:
            # 첫 번째 벡터 조회 (query 사용)
            dummy_vector = [0.0] * 1536  # OpenAI embedding 차원
            
            # 유사도 검색으로 실제 데이터 가져오기
            query_result = index.query(
                vector=dummy_vector,
                top_k=3,
                include_metadata=True,
                namespace=namespace
            )
            
            if query_result.matches:
                print(f"✅ {len(query_result.matches)}개 벡터 발견")
                
                for i, match in enumerate(query_result.matches[:2]):  # 처음 2개만 확인
                    print(f"\n🔸 벡터 {i+1}:")
                    print(f"   ID: {match.id}")
                    print(f"   Score: {match.score:.4f}")
                    
                    if match.metadata:
                        print(f"   메타데이터 키들: {list(match.metadata.keys())}")
                        
                        # 중요한 키들 확인
                        important_keys = ['text', 'page_content', 'content', 'krx_name', 'issue_name']
                        for key in important_keys:
                            if key in match.metadata:
                                value = match.metadata[key]
                                if isinstance(value, str) and len(value) > 100:
                                    print(f"   {key}: {value[:100]}...")
                                else:
                                    print(f"   {key}: {value}")
                        
                        # 전체 메타데이터 구조 출력 (크기 제한)
                        print("   전체 메타데이터:")
                        for key, value in match.metadata.items():
                            if isinstance(value, str) and len(value) > 50:
                                print(f"     {key}: {value[:50]}...")
                            else:
                                print(f"     {key}: {value}")
                    else:
                        print("   ❌ 메타데이터 없음")
            else:
                print("❌ 벡터 없음")
                
        except Exception as e:
            print(f"❌ 네임스페이스 '{namespace}' 분석 실패: {e}")

def test_langchain_compatibility():
    """LangChain 호환성 테스트 및 수정"""
    print("\n🔄 LangChain 호환성 테스트...")
    
    load_dotenv(override=True)
    index_name = os.getenv("PINECONE_INDEX_NAME", "ordaproject")
    
    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
    
    for namespace in ["industry", "past_issue"]:
        print(f"\n📂 '{namespace}' 네임스페이스 LangChain 테스트:")
        
        try:
            # 기본 text_field로 시도
            print("   🔸 기본 'text' 필드로 시도...")
            vector_store = PineconeVectorStore(
                index_name=index_name,
                embedding=embeddings,
                namespace=namespace,
                text_field="text"  # 기본값
            )
            
            results = vector_store.similarity_search("테스트", k=1)
            if results and results[0].page_content.strip():
                print(f"   ✅ 성공: {len(results)}개 결과, 내용: {results[0].page_content[:50]}...")
                continue
            else:
                print("   ❌ 결과 없음 또는 빈 내용")
            
        except Exception as e:
            print(f"   ❌ 기본 필드 실패: {e}")
        
        # 다른 가능한 text_field들 시도
        possible_fields = ["page_content", "content", "source", "krx_name", "issue_name"]
        
        for field in possible_fields:
            try:
                print(f"   🔸 '{field}' 필드로 시도...")
                vector_store = PineconeVectorStore(
                    index_name=index_name,
                    embedding=embeddings,
                    namespace=namespace,
                    text_field=field
                )
                
                results = vector_store.similarity_search("테스트", k=1)
                if results and results[0].page_content.strip():
                    print(f"   ✅ '{field}' 필드 성공!")
                    print(f"      내용: {results[0].page_content[:100]}...")
                    print(f"      메타데이터: {results[0].metadata}")
                    break
                else:
                    print(f"   ⚠️ '{field}' 필드: 결과 있지만 내용 없음")
                    
            except Exception as e:
                print(f"   ❌ '{field}' 필드 실패: {e}")

def suggest_rag_service_fix():
    """RAG 서비스 수정 제안"""
    print("\n💡 RAG 서비스 수정 제안:")
    print("="*50)
    
    print("""
1. PineconeVectorStore 초기화 시 text_field 명시적 지정:
   
   # services/rag_service.py에서
   self.industry_store = PineconeVectorStore(
       index_name=self.INDEX_NAME,
       embedding=self.embedding,
       namespace="industry",
       text_field="page_content"  # 또는 실제 작동하는 필드명
   )

2. 또는 from_existing_index 메서드 사용:
   
   self.industry_store = PineconeVectorStore.from_existing_index(
       index_name=self.INDEX_NAME,
       embedding=self.embedding,
       namespace="industry",
       text_key="page_content"  # 텍스트가 저장된 메타데이터 키
   )

3. 사용자 정의 document 변환 함수 사용
""")

def main():
    """메인 실행 함수"""
    print("🚀 Pinecone 데이터 구조 진단 시작")
    print("="*60)
    
    try:
        # 1. 실제 데이터 구조 확인
        inspect_pinecone_structure()
        
        # 2. LangChain 호환성 테스트
        test_langchain_compatibility()
        
        # 3. 수정 제안
        suggest_rag_service_fix()
        
    except Exception as e:
        print(f"\n❌ 진단 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()