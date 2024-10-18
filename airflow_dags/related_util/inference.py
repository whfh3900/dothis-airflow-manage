
import os
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

from datetime import datetime, timedelta
import pandas as pd

from util.dothis_keyword import VBR, GensimRelated, PostProcessing
from util.auto_folder_make import local_folder_make
from util.dothis_nlp import decode_and_convert
from util.db_util import get_mysql_connector
import pymysql
from tqdm import tqdm
import json

#### to temp
def vbr_data_collect(josa_path="/data_store/airflow_dir/airflow_dags/ai_tools/kor_josa.txt",
            stopwords_path="/data_store/airflow_dir/airflow_dags/ai_tools/stopwords_for_related.txt"):

    def get_dates_between(start_date, end_date):
        """
        두 날짜 사이의 모든 날짜를 리스트로 반환하되, 현재 날짜 이후의 날짜는 제외합니다.
        
        :param start_date: 시작 날짜 (YYYY-MM-DD 형식의 문자열)
        :param end_date: 종료 날짜 (YYYY-MM-DD 형식의 문자열)
        :return: 두 날짜 사이의 모든 날짜를 포함하는 리스트 (현재 날짜 이후 제외)
        """
        # 현재 날짜
        current_date = datetime.now().strftime('%Y%m%d')
        
        # 날짜 범위 생성
        dates = pd.date_range(start=start_date, end=end_date)
        
        # 날짜를 문자열 형식으로 변환하여 리스트로 반환, 현재 날짜 이후는 제외
        return [date.strftime('%Y%m%d') for date in dates if date.strftime('%Y%m%d') <= current_date]

    p = PostProcessing(josa_path=josa_path,
                       stopwords_path=stopwords_path)

    ##################################################################################
    db_name = "dothis_pre"
    conn = get_mysql_connector(db=db_name)
    cursor = conn.cursor(buffered=True)

    df = pd.DataFrame()
    end_limit = 30
    now_limit = 0
    end_total = 7
    now_total = 0
    time_date = 0

    while True:
        if end_total == now_total:
            break

        ### time_date까지 데이터가 채워지지 않았을 경우를 대비.
        if end_limit == now_limit:
            print(f"Data is not sufficiently populated. Collects only total {end_total} match data by {end_limit}.")
            break

        start_date = datetime.now() - timedelta(days=time_date)
        start_date_str = start_date.strftime("%Y-%m-%d")
        _start_date_str = start_date.strftime("%Y%m%d")
        
        end_date = start_date + timedelta(days=7)
        _end_date_str = end_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        dates = get_dates_between(start_date_str, end_date_str)
        # print("start_date_str, end_date_str", start_date_str, end_date_str)
        # print("dates", dates)
        ############################ pre 데이터 가져오기
        # 테이블 존재 여부 확인 쿼리
        video_data_table = f"video_data_{_start_date_str}"
        check_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{db_name}' AND table_name = '{video_data_table}';"
        cursor.execute(check_query)
        result = cursor.fetchone()
        if not result:
            now_limit += 1
            time_date += 1
            continue
        
        query = f"""
                desc {db_name}.{video_data_table};
                """
        cursor.execute(query)
        columns = [row[0] for row in cursor.fetchall()]
        
        query = f"""
                select * from {db_name}.{video_data_table};
                """
        cursor.execute(query)

        v_etc = pd.DataFrame(cursor.fetchall(), columns=columns)
        if len(v_etc) == 0:
            now_limit += 1
            time_date += 1
            continue
        ########################################################

        # 1번 13번 클러스터 제외
        v_etc = v_etc[~v_etc.video_cluster.isin([1, 13, "None"])]
        v_etc['video_published'] = pd.to_datetime(v_etc['video_published'])  # video_published 컬럼을 datetime 타입으로 변환
        v_etc.dropna(subset=['use_text'], inplace=True)
        
        # print(start_date_str,"~",end_date_str, " 시작")
        columns = ["video_id", "video_views", "video_performance", "YEAR", "MONTH", "DAY"]
        columns_to_str = ", ".join(columns)

        ########################### 히스토리 가져오기 to temp
        etc = pd.DataFrame()
        h_etc = pd.DataFrame()
        for date in dates:
            history_data_table = f"video_history_{date}"
            query = f"""
                        select {columns_to_str} from dothis_temp.{history_data_table};
                    """
            cursor.execute(query)
            _h_etc = pd.DataFrame(cursor.fetchall(), columns=columns)
            h_etc = pd.concat([h_etc, _h_etc], axis=0)
            
        etc = pd.merge(v_etc, h_etc, on='video_id', how='inner')            
        etc = etc.drop_duplicates()
        # print("len(etc)", len(etc)) 

        df = pd.concat([df, etc], axis=0)
        time_date += 1
        now_total  += 1
        print(f"Use {start_date_str} ({now_total}/{end_total})")

        ########################################################

    ############################ 조회수 계산하기
    # crawled_date를 datetime 형식으로 변환
    # null 값을 0으로 대체 및 소수점 이하 제거 및 정수형 변환
    df[["YEAR", "MONTH", "DAY"]] = df[["YEAR", "MONTH", "DAY"]].fillna(0).astype(int)
    df["crawled_date"] = pd.to_datetime(df['YEAR'].astype(str)+"-"+df['MONTH'].astype(str)+"-"+df['DAY'].astype(str), format="%Y-%m-%d", errors='coerce')
    df.sort_values(['crawled_date'], ascending=True, inplace=True)            
    df.reset_index(drop=True, inplace=True)   
    
    # 각 id에 대해 최신 날짜를 가진 행의 인덱스를 찾기
    idx = df.groupby('video_id')['crawled_date'].idxmax()

    # 인덱스를 이용해 최신 날짜 데이터를 필터링
    df = df.loc[idx]
    df = df.drop_duplicates()
    df.use_text = df.use_text.progress_apply(decode_and_convert)
    # 후처리 한번더 9월달부터 후처리 부분 삭제
    df['use_text'] = df['use_text'].progress_apply(lambda x: " ".join(p.post_processing([str(i[0]) for i in x if isinstance(i[0], str)])))
    df.sort_values(['crawled_date'], ascending=True, inplace=True)            
    df.reset_index(drop=True, inplace=True)          

    return df


def vbr_predict(stopwords_path="./stopwords.txt", 
                josa_path="./josa_list.txt", 
                cache_path="path/to/cache", 
                size=1000,
                ntop=10):
    
    local_folder_make(cache_path)

    db_name = "new_dothis"
    conn = get_mysql_connector(db=db_name, host="RDS")
    cursor = conn.cursor(buffered=True)

    query = f"select keyword, ranking, year, month, day from weekly_views where ranking <= 30000"
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall(), columns=["keyword", "ranking", "year", "month", "day"])
    df[['year', 'month', 'day']] = df[['year', 'month', 'day']].fillna(0).astype(int)
    df["date"] = pd.to_datetime(df['year'].astype(str)+"-"+df['month'].astype(str)+"-"+df['day'].astype(str), format="%Y-%m-%d", errors='coerce')
    search_words = df[df['date'] == df['date'].max()]
    del df
    ##################################################################################

    filepath = os.path.join(cache_path, "vbr_related.json")
    # df = vbr_data_collect(stopwords_path=stopwords_path, josa_path=josa_path)
    # cr = VBR(df=df, stopwords_path=stopwords_path, josa_path=josa_path)
    cr = VBR(stopwords_path=stopwords_path, josa_path=josa_path)

    print("#"*50, " Data-driven related words ", "#"*50)
    _filepath = os.path.join(cache_path, "_vbr_related.json")
    if not os.path.exists(_filepath):
        vbr_related = dict()
    else:
        with open(_filepath, 'r', encoding="utf-8-sig") as json_file:
            vbr_related = json.load(json_file)
        have_keywords = list(vbr_related.keys())
        # search_words = search_words[search_words.keyword not in have_keywords].copy()
        search_words = search_words[~search_words.keyword.isin(have_keywords)].copy()


    # for row in tqdm(search_words.iterrows(), total=len(search_words)):
    for i, row in enumerate(tqdm(search_words.iterrows(), total=len(search_words))):
        #### 너무 오래걸려서 5000개 할 때마다 저장하자
        if i % 1000 == 0:
            with open(_filepath, 'w', encoding="utf-8-sig") as json_file:
                json.dump(vbr_related, json_file, indent=4)

        keyword = row[1].keyword
        # ranking = row[1].ranking
        # print(f"({row[0]+1}/{len(search_words)}) {keyword}")
        # result = cr.vbr_related(keyword, ranking, find_col="use_text",
        #                                 video_published_col="video_published",
        #                                 video_views_col="video_views", 
        #                                 video_performance_col="video_performance",
        #                                 video_duration="video_duration",
        #                                 ntop=ntop, df_len=df_len)
        result = cr.related(keyword, ntop=ntop, size=size)

        # 검색결과 없을 경우 삭제
        if len(result) == 0:
            continue
        else:
            vbr_related[keyword] = dict()
            vbr_related[keyword] = result
        
    # 연결 종료
    conn.close()
    
    filepath = os.path.join(cache_path, "vbr_related.json")
    with open(filepath, 'w', encoding="utf-8-sig") as json_file:
        json.dump(vbr_related, json_file, indent=4)
        
    os.remove(_filepath)

    print("#"*50, " Process Completion ", "#"*50)
    return filepath
    
    
def gensim_predict(model="word2vec",
                    model_path="path/to/model", 
                    cache_path="path/to/cache", 
                    josa_path="./josa_list.txt", 
                    stopwords_path="./stopwords_for_related.txt",
                    ntop=10):
    
    local_folder_make(cache_path)

    db_name = "new_dothis"
    conn = get_mysql_connector(db=db_name, host="RDS")
    cursor = conn.cursor(buffered=True)
    
    query = f"select keyword, ranking, year, month, day from weekly_views where ranking <= 30000"
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall(), columns=["keyword", "ranking", "year", "month", "day"])
    df[['year', 'month', 'day']] = df[['year', 'month', 'day']].fillna(0).astype(int)
    df["date"] = pd.to_datetime(df['year'].astype(str)+"-"+df['month'].astype(str)+"-"+df['day'].astype(str), format="%Y-%m-%d", errors='coerce')
    search_words = df[df['date'] == df['date'].max()]
    del df
    ##################################################################################
    
    filepath = os.path.join(cache_path, "related_inference.json")
    gr = GensimRelated(path=model_path, 
                        model=model,
                        josa_path=josa_path,
                        stopwords_path=stopwords_path)
    related_inference = dict()

    print("#"*50, " Prediction using model ", "#"*50)
    for row in tqdm(search_words.iterrows(), total=len(search_words)):
        keyword = row[1].keyword
        related_inference[keyword] = dict()            
        related_inference[keyword] = gr.gensim_related(keyword, ntop=ntop, split_word_check=True)

    ### 저장
    with open(filepath, 'w', encoding="utf-8-sig") as json_file:
        json.dump(related_inference, json_file, indent=4)
    print("#"*50, " Process Completion ", "#"*50)
    return filepath

if __name__ == "__main__":
    # now_date="2024-08-05"
    # now_date = datetime.today().strftime('%Y-%m-%d')
    model_name="word2vec"
    model_path="/home/suchoi/dothis-ai/models/related/related_model.bin"
    cache_path="/home/suchoi/dothis-ai/data/related"
    josa_path="./airflow_dags/ai_tools/kor_josa.txt"
    stopwords_path="./airflow_dags/ai_tools/stopwords_for_related.txt"
    gensim_predict(model=model_name,
                    model_path=model_path, 
                    cache_path=cache_path, 
                    josa_path=josa_path, 
                    stopwords_path=stopwords_path,
                    ntop=10)
    # vbr_predict(stopwords_path=stopwords_path, 
    #             josa_path=josa_path, 
    #             cache_path=cache_path, 
    #             df_len=10000,
    #             ntop=10)