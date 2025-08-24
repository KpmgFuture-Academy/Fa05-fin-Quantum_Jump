# populate_pinecone.py

import pandas as pd
from tqdm import tqdm
from pinecone import Pinecone, ServerlessSpec
from langchain_openai import OpenAIEmbeddings

import config

def initialize_pinecone():
    """Pinecone 클라이언트와 인덱스를 초기화하고 연결합니다."""
    print("🌲 Pinecone 초기화 중...")
    pc = Pinecone(api_key=config.PINECONE_API_KEY)
    
    # [수정] config 파일의 인덱스 이름을 사용
    index_name = config.PINECONE_INDEX_NAME
    if index_name not in pc.list_indexes().names():
        print(f"인덱스 '{index_name}'가 존재하지 않아 새로 생성합니다.")
        pc.create_index(
            name=index_name,
            dimension=1536,
            metric="cosine",
            spec=ServerlessSpec(cloud="aws", region="us-east-1")
        )
    else:
        print(f"✅ 기존 인덱스 '{index_name}'에 연결합니다.")
        
    return pc.Index(index_name)

def prepare_data_for_pinecone(df: pd.DataFrame, type: str):
    """DataFrame을 Pinecone에 업로드할 형식으로 준비합니다."""
    records = []
    
    for index, row in df.iterrows():
        if type == 'industry':
            text_to_embed = f"KRX 업종명: {row['KRX 업종명']}\n상세내용: {row['상세내용']}"
            metadata = {
                "name": str(row['KRX 업종명']),
                "description": str(row['상세내용'])
            }
            # [수정] ID에 한글이 들어가지 않도록 'industry-' 접두어와 행 번호(index)만 사용
            record_id = f"industry-{index}"
            
        elif type == 'past_issue':
            text_to_embed = f"Issue_name: {row['Issue_name']}\nContents: {row['Contents']}"
            metadata = {
                "name": str(row['Issue_name']),
                "description": str(row['Contents']),
                "related_industries": str(row.get('관련 산업', '')),
                "start_date": str(row.get('Start_date', '')),
                "end_date": str(row.get('Fin_date', ''))
            }
            # CSV의 ID 컬럼은 이미 ASCII이므로 그대로 사용
            record_id = str(row['ID'])
            
        records.append({
            "id": record_id,
            "text": text_to_embed,
            "metadata": metadata
        })
    return records

def embed_and_upsert(index, records: list, namespace: str, batch_size: int = 100):
    """데이터를 배치 단위로 임베딩하고 Pinecone에 업로드합니다."""
    embedding_model = OpenAIEmbeddings(
        model=config.OPENAI_EMBEDDING_MODEL,
        api_key=config.OPENAI_API_KEY
    )
    print(f"총 {len(records)}개의 레코드를 {namespace} 네임스페이스에 업로드합니다.")
    
    for i in tqdm(range(0, len(records), batch_size), desc=f"Uploading to {namespace}"):
        batch = records[i : i + batch_size]
        
        texts_to_embed = [item['text'] for item in batch]
        ids_to_upsert = [item['id'] for item in batch]
        metadata_to_upsert = [item['metadata'] for item in batch]
        
        embeddings = embedding_model.embed_documents(texts_to_embed)
        vectors_to_upsert = zip(ids_to_upsert, embeddings, metadata_to_upsert)
        index.upsert(vectors=vectors_to_upsert, namespace=namespace)

def safe_delete_namespace(index, namespace: str):
    """네임스페이스가 존재하는 경우에만 삭제합니다."""
    try:
        # 네임스페이스의 통계를 조회하여 존재 여부 확인
        stats = index.describe_index_stats()
        if namespace in stats.get('namespaces', {}):
            print(f"🧹 기존 네임스페이스 '{namespace}'를 초기화합니다...")
            index.delete(delete_all=True, namespace=namespace)
            print(f"✅ 네임스페이스 '{namespace}' 초기화 완료.")
        else:
            print(f"ℹ️ 네임스페이스 '{namespace}'가 존재하지 않아 초기화를 건너뜁니다.")
    except Exception as e:
        print(f"⚠️ 네임스페이스 '{namespace}' 초기화 중 오류 발생: {e}")
        print("새로운 데이터로 진행합니다...")

def main():
    """메인 실행 함수: CSV 로드 -> 데이터 준비 -> Pinecone 업로드"""
    index = initialize_pinecone()
    
    # [수정] 안전한 네임스페이스 초기화
    print("\n🧹 기존 네임스페이스를 확인하고 초기화합니다...")
    safe_delete_namespace(index, 'industry')
    safe_delete_namespace(index, 'past_issue')

    print("\n--- 🏭 산업 DB 처리 시작 ---")
    df_industry = pd.read_csv(config.INDUSTRY_CSV_PATH).dropna(subset=['KRX 업종명']).fillna('')
    industry_records = prepare_data_for_pinecone(df_industry, 'industry')
    embed_and_upsert(index, industry_records, namespace='industry')
    print("✅ 산업 DB 처리 완료.")
    
    print("\n--- 📰 과거 이슈 DB 처리 시작 ---")
    df_past_issue = pd.read_csv(config.PAST_NEWS_CSV_PATH).dropna(subset=['ID']).fillna('')
    past_issue_records = prepare_data_for_pinecone(df_past_issue, 'past_issue')
    embed_and_upsert(index, past_issue_records, namespace='past_issue')
    print("✅ 과거 이슈 DB 처리 완료.")
    
    print("\n--- 📊 최종 결과 확인 ---")
    stats = index.describe_index_stats()
    print(stats)
    print("\n🎉 Pinecone 데이터베이스 초기화가 성공적으로 완료되었습니다!")

if __name__ == "__main__":
    main()