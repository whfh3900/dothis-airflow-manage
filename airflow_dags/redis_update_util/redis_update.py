import pandas as pd
import sys
sys.path.append("./airflow_dags")
sys.path.append('/data_store/airflow_dir/airflow_dags')
sys.path.append('./')

from util.dothis_nlp import decode_and_convert
from util.db_util import get_mysql_connector
from util.redis_method import REDIS
from tqdm import tqdm
tqdm.pandas()

def update_redis_data(now_date, stopwords_path="/data_store/airflow_dir/airflow_dags/ai_tools/stopwords_for_redis.txt"):

    with open(stopwords_path, 'r', encoding="utf-8-sig") as file:
        stopwords = file.read().splitlines()

    print("#"*50, " Data collection ", "#"*50)
    date = "".join(now_date.split("-"))
    conn = get_mysql_connector()
    cursor = conn.cursor(buffered=True)
    # DESC 쿼리를 사용하여 테이블의 구조를 가져옵니다.
    # 컬럼명을 추출하여 리스트에 저장합니다.
    cursor.execute(f"DESC dothis_pre.video_data_{date}")
    columns = [column[0] for column in cursor.fetchall()] 
    
    cursor.execute(f"""select * from dothis_pre.video_data_{date};""")
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    df['use_text'] = df['use_text'].progress_apply(decode_and_convert)
    
    ######### 여기서 임계값 0.3 이상으로 볼 지 안볼지
    # df['use_text'] = df['use_text'].apply(lambda x: " ".join([i[0] for i in x if i[2] >= 0.3]))
    # 첫 번째 요소가 문자열인 것만 추출하여 공백으로 결합
    df['use_text'] = df['use_text'].apply(lambda x: " ".join([str(i[0]) for i in x if isinstance(i[0], str) and i[0] not in stopwords]))
    df = df[~df.video_cluster.isin([9999, "None"])].copy()

    conn.close()
    
    # print("*"*50, f" Post Processing for Token Table... ", "*"*50)
    df["video_cluster"] = df["video_cluster"].astype(int).astype(str).str.zfill(2)
    df["video_published"] = pd.to_datetime(df["video_published"], format="%Y-%m-%d")
    df["video_published"] = df["video_published"].astype("str").str.replace("-","", regex=False)
    df["redis_value"] = df["video_published"]+":"+df["channel_id"]+":"+df["video_id"]+":"+df["video_cluster"] 
    # 데이터프레임을 dict 형식으로 변환
    # 'use_text'가 null 값이거나 빈 값인 항목을 제외한 dict 생성
    result_dict = {k: v for k, v in zip(df.redis_value, df.use_text) if pd.notna(v) and v != ''}
    for key, values in result_dict.items():
        result_dict[key] = list(set(values.split()))

    print("*"*50, f" Make to Token Table for Redis... ", "*"*50)
    tokens = {value for values in result_dict.values() for value in values}

    _redis_data = dict()
    for token in tqdm(list(tokens)):
        _redis_data[token] = [key for key, values in result_dict.items() if token in values]
    del result_dict

    print("*"*50, f" Save to Redis... ", "*"*50)
    r = REDIS(db=0)
    for i, (key, values) in enumerate(tqdm(_redis_data.items())):
        for value in values:
            r.cursor.sadd(key, value)
    del _redis_data
    print("*"*50, f" End ", "*"*50)
    
    
if __name__ == "__main__":
    update_redis_data(now_date="2024-09-01",
                      stopwords_path="./airflow_dags/ai_tools/stopwords_for_redis.txt")
    
