import os
from tqdm import tqdm
import pandas as pd
from datetime import datetime, timedelta
import sys
sys.path.append('./')
sys.path.append("./airflow_dags")
sys.path.append("/data_store/airflow_dir/airflow_dags")

# .env 파일 경로 지정
from dotenv import load_dotenv
dotenv_path = os.path.join("/data_store/airflow_dir/airflow_dags", '.env')
if not os.path.exists(dotenv_path):
    dotenv_path = os.path.join("./", '.env')
load_dotenv(dotenv_path)

from util.dothis_nlp import decode_and_convert
from util.dothis_keyword import TfidfExtract
from util.db_util import get_mysql_connector
# from util.os_client import opensearch_client
# from util.dothis_nlp import clean_and_parse_list, hashtag_extraction


# def channel_keywords(josa_path, stopwords_path, 
#                      ntop=10, data_size=30, 
#                      channel_range_day=180, data_range_day=365):
#     db = "dothis_svc"
#     conn = get_mysql_connector(db=db)
#     cursor = conn.cursor(buffered=True)
#     client = opensearch_client()

#     tfidf = TfidfExtract(josa_path=josa_path,
#                     stopwords_path=stopwords_path)

#     query = "select channel_id from channel_data;"
#     cursor.execute(query)
#     channel_id = set([i[0] for i in cursor.fetchall()])

#     end_limit = 60
#     now_limit = 0
#     end_total = channel_range_day
#     now_total = 0
#     time_date = 1

#     use_channel_id = set()
#     print("#"*50, " Channel ID Collect ", "#"*50)
#     while True:
#         if end_total == now_total:
#             break
        
#         ### time_date까지 데이터가 채워지지 않았을 경우를 대비.
#         if end_limit == now_limit:
#             print(f"Data is not sufficiently populated. Collects only total {end_total} match data by {now_limit}.")
#             break
#         end_date = datetime.now() - timedelta(days=time_date)
#         end_date_str = end_date.strftime("%Y%m%d")
        
#         table_name = f"video_data_{end_date_str}"
#         # 테이블 존재 여부 확인 쿼리
#         check_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{db}' AND table_name = '{table_name}';"
#         cursor.execute(check_query)
#         result = cursor.fetchone()
#         if not result:
#             now_limit += 1
#             time_date += 1
#             continue
        
#         # print(f"Use {table_name}")
#         query = f"""
#                 select channel_id from dothis_pre.{table_name};
#                 """
#         cursor.execute(query)
#         use_channel_id.update([i[0] for i in cursor.fetchall()])
#         time_date += 1
#         now_total += 1

#     year_ago = datetime.now() - timedelta(days=data_range_day)
#     year_ago_str = year_ago.strftime("%Y-%m-%d")
#     use_channel_id = use_channel_id & channel_id
#     for id in tqdm(list(use_channel_id)):

#         query = {
#             "size": data_size, 
#             "_source": ["video_id", "use_text", "video_published", "video_description", "video_tags"],  # 반환할 필드 지정
#             "query": {
#                 "bool": {
#                 "filter": [
#                     {
#                     "terms": {
#                         "channel_id": [id]
#                     }
#                     },
#                     {
#                     "range": {
#                         "video_published": {
#                         "gte": year_ago_str,  # 시작 날짜 (포함)
#                         }
#                     }
#                     }
#                 ]
#                 }
#             },
#             "sort": [
#                 {
#                 "video_published": {
#                     "order": "desc"  # 최신 항목부터 내림차순으로 정렬
#                 }
#                 }
#             ]
#         }
#         # 검색 쿼리 실행
#         response = client.search(
#             index="video_data",  # 검색할 인덱스 이름
#             body=query
#         )

#         df = pd.DataFrame([hit['_source'] for hit in response['hits']['hits']])
#         if len(df) == 0:
#             continue
        
#         df["hashtag"] = df.video_description.apply(lambda x: [i.strip() for i in hashtag_extraction(x).split("#") if i.strip() != ""])
#         df["video_tags"] = df.video_tags.apply(lambda x: [i.replace("'","").replace("[","").replace("]","").strip() for i in clean_and_parse_list(x)])
#         df["tags"] = df["hashtag"] + df["video_tags"]

#         df["use_text"] = df["use_text"].apply(lambda x: " ".join(x) if isinstance(x, list) else str(x))
#         df["tags"] = df["tags"].apply(lambda x: " ".join(x) if isinstance(x, list) else str(x))
#         MAINLY_USED_KEYWORDS = ", ".join(tfidf.tfidf_extract(df.use_text, ntop=ntop))
#         MAINLY_USED_TAGS = ", ".join(tfidf.tfidf_extract(df.tags, ntop=ntop))

#         today_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#         query = f"UPDATE dothis_svc.channel_data SET MAINLY_USED_KEYWORDS = '{MAINLY_USED_KEYWORDS}', \
#             MAINLY_USED_TAGS = '{MAINLY_USED_TAGS}', CRAWLED_DATE = '{today_date}' \
#             WHERE CHANNEL_ID = '{id}';"
#         cursor.execute(query)
#         conn.commit()

def channel_keywords(josa_path, stopwords_path, 
                     ntop=10, data_size=30, 
                     channel_range_day=180):
    db = "dothis_pre"
    conn = get_mysql_connector(db=db)
    cursor = conn.cursor(buffered=True)
    tfidf = TfidfExtract(josa_path=josa_path,
                    stopwords_path=stopwords_path)
    
    end_limit = 60
    now_limit = 0
    end_total = channel_range_day
    now_total = 0
    time_date = 1

    df = pd.DataFrame()

    print("#"*50, " Channel Data Collect ", "#"*50)

    while True:
        if end_total == now_total:
            break
        
        ### time_date까지 데이터가 채워지지 않았을 경우를 대비.
        if end_limit == now_limit:
            print(f"Data is not sufficiently populated. Collects only total {end_total} match data by {now_limit}.")
            break
        end_date = datetime.now() - timedelta(days=time_date)
        end_date_str = end_date.strftime("%Y%m%d")
        
        table_name = f"video_data_{end_date_str}"
        # 테이블 존재 여부 확인 쿼리
        check_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{db}' AND table_name = '{table_name}';"
        cursor.execute(check_query)
        result = cursor.fetchone()
        if not result:
            now_limit += 1
            time_date += 1
            continue
        
        # print(f"Use {table_name}")
        query = f"""
                select channel_id, use_text, video_published from {db}.{table_name};
                """
        cursor.execute(query)
        etc = pd.DataFrame(cursor.fetchall(), columns=["channel_id", "use_text", "video_published"])
        df = pd.concat([df, etc], axis=0)

        time_date += 1
        now_total += 1
        print(f"({now_total}/{end_total})")
        
    query = "select channel_id, channel_name from dothis_svc.channel_data;"
    cursor.execute(query)
    channel_names = pd.DataFrame(cursor.fetchall(), columns=["channel_id", "channel_name"])
    df = pd.merge(df, channel_names, on="channel_id", how="inner")

    # 날짜 형식으로 변환
    df['video_published'] = pd.to_datetime(df['video_published'])
    # 최신순으로 내림차순 정렬
    df = df.sort_values(by='video_published', ascending=False)
    df.reset_index(drop=True, inplace=True)

    print("#"*50, " Group by id ", "#"*50)
    # channel_id별로 groupby, video_published로 정렬 후 최대 size 만큼의 use_text 추출
    df = df.sort_values(by=['channel_id', 'video_published'], ascending=[True, False]) \
        .groupby(['channel_id', 'channel_name']) \
        .head(data_size) \
        .groupby(['channel_id', 'channel_name'])['use_text'] \
        .apply(list) \
        .reset_index()
    df.reset_index(drop=True, inplace=True)

    print("#"*50, " Update Channel Keywords ", "#"*50)
    for row in tqdm(df.iterrows(), total=len(df)):
        channel_id = row[1].channel_id
        channel_name = row[1].channel_name
        use_text = [decode_and_convert(i) for i in row[1].use_text]

        title = [
            " ".join([i[0] for i in text if (i[1] not in ["tag", "hashtag"]) or (i[0] != channel_name)])
            for text in use_text
            if any(i[1] not in ["tag", "hashtag"] for i in text)
        ]
        tag = [
            " ".join([i[0] for i in text if i[1] in ["tag", "hashtag"] or (i[0] != channel_name)])
            for text in use_text
            if any(i[1] in ["tag", "hashtag"] for i in text)
        ]
        MAINLY_USED_KEYWORDS = ", ".join(tfidf.tfidf_extract(title, ntop=ntop))
        MAINLY_USED_TAGS = ", ".join(tfidf.tfidf_extract(tag, ntop=ntop))
        today_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query = f"UPDATE dothis_svc.channel_data SET MAINLY_USED_KEYWORDS = '{MAINLY_USED_KEYWORDS}', \
            MAINLY_USED_TAGS = '{MAINLY_USED_TAGS}', CRAWLED_DATE = '{today_date}' \
            WHERE CHANNEL_ID = '{channel_id}';"
        cursor.execute(query)
        conn.commit()
    print("#"*50, " End ", "#"*50)


if __name__ == "__main__":
    josa_path="./airflow_dags/ai_tools/kor_josa.txt"
    stopwords_path="./airflow_dags/ai_tools/stopwords_for_keyword.txt"
    channel_keywords(josa_path=josa_path,
                     stopwords_path=stopwords_path)