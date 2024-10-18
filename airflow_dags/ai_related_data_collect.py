from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import os
import sys
sys.path.append('./')
from dotenv import load_dotenv
# .env 파일 경로 지정
dotenv_path = os.path.join("/data_store/airflow_dir/airflow_dags", '.env')
print(dotenv_path)
# .env 파일 로드
load_dotenv(dotenv_path)
from datetime import datetime, timedelta
import pandas as pd
from util.dothis_nlp import PreProcessing, calculate_korean_ratio, hashtag_extraction, remove_special_characters, PostProcessing
from util.auto_folder_make import local_folder_make
from tqdm import tqdm
tqdm.pandas()
import requests
import pymysql
from collections import defaultdict

# JSON 파일로부터 default_args 로드
with open('/app/airflow/dags/config.json', 'r') as f:
    default_args = json.load(f)['related_data_collect']
# default_args['start_date'] = datetime.now()
default_args['start_date'] = datetime(2024, 7, 5)

# DAG 정의
dag = DAG(
    'ai_related_data_collect_dag',
    default_args=default_args,
    schedule_interval = default_args["schedule_interval"]
)

# Python 함수 정의
def schedule_start(**context):
    execution_date = context['execution_date']
    execution_date = datetime.today() - timedelta(days=1)
    # 사용자 정의 로직에서 execution_date 사용
    execution_date_str = execution_date.strftime("%Y-%m-%d")  # execution_date를 문자열로 변환
    print("Airflow execution date:", execution_date_str)
    return execution_date_str

def get_date_range_dict(start_date_str, end_date_str):
    # 날짜 문자열을 datetime 객체로 변환
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    # 현재 날짜 가져오기
    current_date = datetime.now() - timedelta(days=1)
    
    # 결과를 저장할 defaultdict 생성
    date_range_dict = defaultdict(list)
    
    # 시작 날짜부터 종료 날짜까지 반복
    temp_date = start_date
    while temp_date <= end_date:
        # 현재 날짜 이후는 포함하지 않음
        if temp_date > current_date:
            break
        # 년월 키 생성
        year_month_key = temp_date.strftime('%Y%m')
        # 일 추가
        date_range_dict[year_month_key].append(temp_date.strftime('%d'))
        # 다음 날로 이동
        temp_date += timedelta(days=1)
    
    # defaultdict을 dict로 변환하여 반환
    return dict(date_range_dict)

# 실행될 함수 정의
def video_data_collect_function(now_date, cache_path="path/to/folder"):
    
    print("#"*50, " Data collection ", "#"*50)

    # end_date = datetime.now() - timedelta(days=1)
    # end_date = datetime.strptime(now_date, "%Y-%m-%d") - timedelta(days=1)
    end_date = datetime.strptime(now_date, "%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")    
    filename = "video_"+"".join(end_date_str.split("-"))+".csv"
    filepath = os.path.join(cache_path, filename)
    
    if os.path.exists(filepath):
        raise Exception("이미 수집된 데이터 입니다. -> %s"%filepath)
    else:
        print(end_date_str, " 시작")

    # RDS 데이터베이스 연결 정보
    host = os.environ.get('PROXMOX_MYSQL_HOST') # RDS 엔드포인트 URL 또는 IP 주소
    port = int(os.environ.get('PROXMOX_MYSQL_PORT'))
    user = os.environ.get('PROXMOX_MYSQL_USER') # MySQL 계정 아이디
    password = os.environ.get('PROXMOX_MYSQL_PW') # MySQL 계정 비밀번호
    db_name = "dothis_svc"
    
    # RDS MySQL 연결하기
    conn = pymysql.connect(host=host, 
                        port=port,
                        user=user, 
                        password=password, 
                        db=db_name, 
                        charset='utf8mb4')

    # 데이터베이스 커서(Cursor) 객체 생성
    cursor = conn.cursor()

    # # 비디오 데이터 테이블 목록
    # query = f"""
    #             SELECT table_name
    #             FROM information_schema.tables
    #             WHERE table_schema = '{db_name}'
    #             AND table_name LIKE 'video_data_%'
    #             AND table_name REGEXP '^video_data_[0-9]+$'
    #         """
    # cursor.execute(query)
    # video_data_tables = [row[0] for row in cursor.fetchall()]

    ### 연관어 전용 테이블 가져오기
    df = pd.DataFrame()
    columns = ["video_id", "channel_id", "video_title", "video_description", "video_tags", "video_published", "video_cluster"]
    columns_to_str = " ,".join(columns)

    for cluster in tqdm(range(0, 94)):
        cluster = str(cluster).zfill(2)
        query = f"""
                    select {columns_to_str}
                    from {db_name}.video_data_{cluster} as vd
                    where vd.video_published = '{end_date_str}'
                """
        try:
            cursor.execute(query)
        except pymysql.err.ProgrammingError as e:
            print(e)
            continue
        etc = pd.DataFrame(cursor.fetchall(), columns=columns)
        df = pd.concat([df, etc], axis=0)

    # 중복제거
    df = df.drop_duplicates()
    df.reset_index(drop=True, inplace=True)
    
    df.to_csv(filepath, encoding="utf-8-sig")
    print("#"*50, " Process Completion ", "#"*50)
    return filepath


# def bigkinds_news_collect_function(now_date, save_path="path/to/folder", time_date=1, data_num=5):
    
#     # end_date = datetime.now() - timedelta(days=1)
#     end_date = datetime.strptime(now_date, "%Y-%m-%d")
#     end_date_str = end_date.strftime("%Y-%m-%d")  
#     start_date = end_date - timedelta(days=time_date)
#     start_date_str = start_date.strftime("%Y-%m-%d")

#     # # 주말엔 뉴스가 없음
#     # if end_date.weekday() in [5, 6]:
#     #     raise Exception("주말에는 뉴스가 없습니다.")

#     # start_date 날짜 기준으로 뽑힘
#     local_folder_make(save_path)
#     filename = "news_"+"".join(start_date_str.split("-"))+".txt"
#     filepath = os.path.join(save_path, filename)
    
#     if os.path.exists(filepath):
#         raise Exception("이미 수집된 데이터 입니다. -> %s"%filepath)
#     else:
#         print(start_date_str, " 시작")


#     ################### 키워드 가져오기
#     # RDS 데이터베이스 연결 정보
#     host = os.environ.get('RDS_MYSQL_HOST') # RDS 엔드포인트 URL 또는 IP 주소
#     port = int(os.environ.get('RDS_MYSQL_PORT')) # RDS 데이터베이스 포트 (기본값: 3306)
#     user = os.environ.get('RDS_MYSQL_USER') # MySQL 계정 아이디
#     password = os.environ.get('RDS_MYSQL_PW') # MySQL 계정 비밀번호
#     db_name = "new_dothis" # 데이터베이스 이름

#     # RDS MySQL 연결하기
#     conn = pymysql.connect(host=host, 
#                         port=port, 
#                         user=user, 
#                         password=password, 
#                         db=db_name, 
#                         charset='utf8mb4')

#     # 데이터베이스 커서(Cursor) 객체 생성
#     cursor = conn.cursor()
#     query = "select keyword from related_words"
#     cursor.execute(query)
#     keywords = [row[0] for row in cursor.fetchall()]
#     # 연결 종료
#     conn.close()
#     ###################

#     ################### 뉴스 
#     # 요청할 API의 엔드포인트 URL
#     url = "http://tools.kinds.or.kr:8888/search/news"
#     access_key = os.environ.get('BIGKINDS_API_KEY')

#     news_data = set()
#     print("#"*50, " Data collection ", "#"*50)
#     for keyword in tqdm(keywords):
#         data = {
#             "access_key": access_key,
#             "argument": {
#                 "query": keyword,
#                 "published_at": {
#                     "from": start_date_str,
#                     "until": end_date_str
#                 },
#                 "provider": [
#                 ],
#                 "category": [
#                     "문화",
#                     "IT_과학",
#                     "스포츠",
#                     # "사회",
#                     # "정치",
#                     # "경제",
#                     # "국제",
#                 ],
#                 "category_incident": [
#                 ],
#                 "byline": "",
#                 "provider_subject": [
#                 ],

#                 "sort": {"date": "desc"},
#                 "hilight": 200,
#                 "return_from": 0,
#                 "return_size": 15000,
#                 "fields": [
#                     "content",
#                     "byline",
#                     "category",
#                     "category_incident",
#                     "provider_news_id"
#                 ]
#             }
#         }

#         # JSON 데이터를 문자열로 변환
#         payload = json.dumps(data)
#         try:
#             # POST 요청 보내기
#             response = requests.post(url, 
#                                     data=payload, 
#                                     headers={'Content-Type': 'application/json'}).json()
#         except requests.exceptions.ConnectionError as e:
#             print(e, keyword)
#             continue
            
#         try:
#             # 응답 결과 확인
#             news_data.update([news_remove_words(i['hilight']) for i in response['return_object']['documents']][:data_num]) 
#         except KeyError as e:
#             pass
    
#     with open(filepath, "w", encoding="utf-8-sig") as f:
#         for text in list(news_data):
#             f.write(text+"\n")
#     ###################
#     print("#"*50, " Process Completion ", "#"*50)
#     return filepath


def video_preprocessing_function(load_path,
                                 cache_path="path/to/cache", 
                                 pre_model="taeminlee/gliner_ko",
                                 josa_path="path/to/kor_josa.txt", 
                                 cache_dir="path/to/huggingface",
                                 stopwords_path="path/to/stopwords.txt",
                                 use_cuda=False):
    local_folder_make(cache_path)
    print()
    _date_str = os.path.basename(load_path).split("_")[1].split(".")[0]
    tta_labels = ["artifacts", "person", "animal", "CIVILIZATION", "organization", \
            "phone number", "address", "passport number", "email", "credit card number", \
            "social security number", "health insurance id number", 'Business/organization', \
            "mobile phone number", "bank account number", "medication", "cpf", "driver's license number", \
            "tax identification number", "medical condition", "identity card number", "national id number", \
            "ip address", "email address", "iban", "credit card expiration date", "username", \
            "health insurance number", "student id number", "insurance number", \
            "flight number", "landline phone number", "blood type", "cvv", \
            "digital signature", "social media handle", "license plate number", "cnpj", "postal code", \
            "passport_number", "vehicle registration number", "credit card brand", \
            "fax number", "visa number", "insurance company", "identity document number", \
            "national health insurance number", "cvc", "birth certificate number", "train ticket number", \
            "passport expiration date", "social_security_number", "EVENT", "STUDY_FIELD", "LOCATION", \
            "MATERIAL", "PLANT", "TERM", "THEORY", 'Analysis Requirement']

    prep = PreProcessing(model=pre_model, 
                        tta_labels=tta_labels,
                        cache_dir=cache_dir,
                        stopwords_path=stopwords_path,
                        use_cuda=use_cuda)
    posp = PostProcessing(josa_path=josa_path, 
                          stopwords_path=stopwords_path)
    
    print("#"*50, " Data preprocessing ", "#"*50)
    df = pd.read_csv(load_path, encoding="utf-8-sig", usecols=["channel_id", "video_id", "video_title", "video_description", "video_tags", "video_cluster"])
    ### video_title에 null 값이 있는듯
    df = df[~df.video_title.isna()]
    ### 외국어로만 이루어진 데이터는 제외
    df = df[df.video_title.apply(calculate_korean_ratio)]
    if len(df) > 0:
        ### 전처리
        ### 전처리시에는 불용어 제거 안함.
        df['use_text'] = df.progress_apply(lambda x: " ".join([i[0] for i in prep.use_norns(x.video_title.strip() + " " + 
                                                    " ".join(remove_special_characters(x.video_tags, use_hashtag=False).split()).strip() + " " + 
                                                    " ".join(hashtag_extraction(x.video_description).split('#')).strip(), 
                                                    use_upper=True, use_stopword=False, threshold=0.3)]), axis=1)
        df["use_text"] = df.use_text.apply(lambda x: " ".join(posp.post_processing(x.split(), use_stopword=True)))
        df.dropna(subset=['use_text'], inplace=True)
        ### 특정 원소 수를 넘기는 데이터만 사용
        # df = df[df.use_text.apply(lambda x: len(x.split())) >= use_token_len]

        df = df[["channel_id", "video_id", "use_text", "video_cluster"]]
        # 특정 컬럼이 null이거나 공백인 행을 삭제
        # 'use_text' 컬럼을 기준으로 삭제
        df = df[df['use_text'].notna() & df['use_text'].str.strip().astype(bool)]
        df.reset_index(drop=True, inplace=True)
        
        # 저장
        filepath = os.path.join(cache_path, "video_%s_pro.csv"%_date_str)
        df.to_csv(filepath, encoding="utf-8-sig")
    else:
        print(f"There is no data to preprocess in the current data frame. {load_path}")
    
    print("#"*50, " Process Completion ", "#"*50)
    return filepath



def vcp_video_data_function(now_date,
                            cache_path="path/to/cache",
                            chunk_size=1000):
    # PROXMOX MYSQL 데이터베이스 연결 정보
    host = os.environ.get('PROXMOX_MYSQL_HOST') # RDS 엔드포인트 URL 또는 IP 주소
    port = int(os.environ.get('PROXMOX_MYSQL_PORT')) # RDS 데이터베이스 포트 (기본값: 3306)
    user = os.environ.get('PROXMOX_MYSQL_USER') # MySQL 계정 아이디
    password = os.environ.get('PROXMOX_MYSQL_PW') # MySQL 계정 비밀번호
    db_name = "dothis_svc" # 데이터베이스 이름

    # PROXMOX MYSQL 연결하기
    conn = pymysql.connect(host=host, 
                        port=port,
                        user=user, 
                        password=password, 
                        db=db_name, 
                        charset='utf8mb4')

    # 데이터베이스 커서(Cursor) 객체 생성
    cursor = conn.cursor()

    ### 오늘 날짜로 부터 일주일 전의 영상데이터를 사용함.
    ### 일주일 전의 영상으로 부터 일주일 후 즉, 현재 날짜 기준의 조회수와 퍼포먼스 가져오기.
    ### 일주일 전에 올라온 영상에서는 그 날 히스토리가 없으므로 현재날짜의 히스토리만 가져오면 됨.

    # end_date = datetime.strptime(now_date, "%Y-%m-%d") - timedelta(days=1)
    end_date = datetime.strptime(now_date, "%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date = end_date - timedelta(days=7)
    start_date_str = start_date.strftime("%Y-%m-%d")    
    
    dates = get_date_range_dict(start_date_str, end_date_str)
    
    filename = "video_"+"".join(start_date_str.split("-"))+"_pro.csv"
    filepath = os.path.join(cache_path, filename)

    ### 일주일 전처리된 비디오 파일이 없으면 수행하지 않음.
    if not os.path.exists(filepath):
        print(f"This file does not exist {filepath}")
        
    else:
        yearmonth = end_date.strftime("%Y%m")
        columns = ["video_id", "video_views", "video_performance", "year", "month", "day"]
        columns_to_str = ", ".join(columns)
        
        df = pd.read_csv(filepath, encoding="utf-8-sig", usecols=["channel_id", "video_id", "use_text", "video_cluster"])

        # 'video_cluster' 컬럼을 기준으로 그룹화
        grouped = df.groupby('video_cluster')
        # 그룹별 데이터프레임으로 나누기
        grouped = {cluster: group.reset_index(drop=True).copy() for cluster, group in grouped}
            
        df = pd.DataFrame()
        for cluster, v_etc in tqdm(grouped.items()):

            cluster = str(int(cluster)).zfill(2)
            
            if len(v_etc) > chunk_size:
                # 각 서브 데이터프레임의 크기 계산
                split_size = len(v_etc) // chunk_size
                # 서브 데이터프레임 리스트 생성
                sub_v_etcs = [v_etc.iloc[i*split_size:(i+1)*split_size] for i in range(chunk_size)]   
            else:
                sub_v_etcs = [v_etc]
            
            h_etc = pd.DataFrame()  
            for sub_etc in sub_v_etcs:
                video_ids = list(sub_etc.video_id.unique())
                # 각 요소를 작은 따옴표로 감싸고 쉼표로 구분
                video_ids_to_string = ', '.join(f"'{vid}'" for vid in video_ids)
                
                for yearmonth, days in dates.items():
                    history_data_table = "video_history_%s_%s"%(cluster, yearmonth)
                    days_to_str = ', '.join(f"{day}" for day in days)
                    
                    ### 년, 월은 이미 그에 맞춰서 split된 테이블이므로 day로만 필터
                    query = f"""
                                select {columns_to_str} from {history_data_table} 
                                where video_id in ({video_ids_to_string})
                                and day in ({days_to_str});
                            """
                    try:
                        cursor.execute(query)
                    except pymysql.err.ProgrammingError as e:
                        print(e)
                        continue
                    sub_h_etc = pd.DataFrame(cursor.fetchall(), columns=columns)
                    h_etc = pd.concat([h_etc, sub_h_etc], axis=0)
                
            etc = pd.merge(v_etc, h_etc, on='video_id', how='inner')
            df = pd.concat([df, etc], axis=0)
        df = df.drop_duplicates()
        
        # crawled_date를 datetime 형식으로 변환
        # null 값을 0으로 대체 및 소수점 이하 제거 및 정수형 변환
        df[['year', 'month', 'day']] = df[['year', 'month', 'day']].fillna(0).astype(int)
        df["crawled_date"] = pd.to_datetime(df['year'].astype(str)+"-"+df['month'].astype(str)+"-"+df['day'].astype(str), format="%Y-%m-%d", errors='coerce')
        # 각 id에 대해 최신 날짜를 가진 행의 인덱스를 찾기
        idx = df.groupby('video_id')['crawled_date'].idxmax()

        # 인덱스를 이용해 최신 날짜 데이터를 필터링
        df = df.loc[idx]
        df = df.drop_duplicates()
        df.sort_values(['crawled_date'], ascending=True, inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        filename = "video_%s_vcp.csv"%("".join(start_date_str.split("-")))
        filepath = os.path.join(cache_path, filename)
        df.to_csv(filepath, encoding="utf-8-sig")   

        print("#"*50, " Process Completion ", "#"*50)
        return filepath
    

# def news_preprocessing_function(load_path, 
#                                 cache_path="path/to/cache", 
#                                 pre_model="taeminlee/gliner_ko", 
#                                 cache_dir="path/to/huggingface",
#                                 stopwords_path="path/to/stopwords.txt",
#                                 use_token_len=10,
#                                 use_cuda=False):
#     local_folder_make(cache_path)
#     _date = os.path.basename(load_path).split("_")[1].split(".")[0]
    
#     pp = PreProcessing(model=pre_model, 
#                         cache_dir=cache_dir,
#                         stopwords_path=stopwords_path,
#                         use_cuda=use_cuda)
#     print("#"*50, " Data preprocessing ", "#"*50)
#     news = list()

#     with open(load_path, "r", encoding="utf-8-sig") as f:
#         # 파일의 전체 라인 수를 가져옵니다.
#         total_lines = sum(1 for _ in f)
#         # 파일을 다시 처음부터 읽기 위해 커서를 처음으로 이동합니다.
#         f.seek(0)
        
#         # tqdm을 사용하여 진행 상황을 모니터링합니다.
#         for line in tqdm(f, total=total_lines, desc="Processing"):
#             text = line.strip()
            
#             ### 전처리
#             text = " ".join([i[0] for i in pp.use_norns(text, use_upper=True, use_stopword=True)])
#             ### 특정 원소 수를 넘기는 데이터만 사용
#             if len(text.split()) >= use_token_len:
#                 news.append(text)
                
#     profilepath = os.path.join(cache_path, "news_%s_pro.txt"%_date)
#     with open(profilepath, "w", encoding="utf-8-sig") as f:
#         for text in news:
#             f.write(text+"\n")
#     print("#"*50, " Process Completion ", "#"*50)

    
# PythonOperator를 사용하여 Python 함수 실행
schedule_start_task = PythonOperator(
    task_id='schedule_start_task',
    python_callable=schedule_start,
    provide_context=True,
    dag=dag,
)

# PythonOperator를 사용하여 DAG에 함수 추가
video_data_collect_task = PythonOperator(
    task_id='video_data_collect_task',
    python_callable=video_data_collect_function,  # 실행될 함수
    provide_context=True,
    queue="ai",
    op_kwargs={"now_date": "{{ task_instance.xcom_pull(task_ids='schedule_start_task') }}",
               "cache_path":"/data_store/airflow_dir/airflow_dags/cache/data/origin"},  # 키워드 인수
    dag=dag,
)

# bigkinds_news_collect_task = PythonOperator(
#     task_id='bigkinds_news_collect_task',
#     python_callable=bigkinds_news_collect_function,  # 실행될 함수
#     provide_context=True,
#     queue="ai",
#     op_kwargs={"now_date": "{{ task_instance.xcom_pull(task_ids='schedule_start_task') }}",
#                "save_path":"/data_store/airflow_dir/airflow_dags/cache/data/origin", 
#                "time_date":1,
#                "data_num":5},  # 키워드 인수
#     dag=dag,
# )



# PythonOperator를 사용하여 DAG에 함수 추가
video_preprocessing_task = PythonOperator(
    task_id='video_preprocessing_task',
    python_callable=video_preprocessing_function, # 실행될 함수
    provide_context=True,
    queue="ai",
    op_kwargs={"load_path":"{{ task_instance.xcom_pull(task_ids='video_data_collect_task') }}", 
               "cache_path":"/data_store/airflow_dir/airflow_dags/cache/data/pre/related", 
               "pre_model":"taeminlee/gliner_ko", 
               "josa_path":"/data_store/airflow_dir/airflow_dags/ai_tools/kor_josa.txt",
               "cache_dir":"/data_store/airflow_dir/airflow_dags/cache/model/huggingface",
               "stopwords_path": "/data_store/airflow_dir/airflow_dags/ai_tools/stopwords_for_redis.txt",
               "use_cuda":False},  # 키워드 인수
    dag=dag,
)

# news_preprocessing_task = PythonOperator(
#     task_id='news_preprocessing_task',
#     python_callable=news_preprocessing_function,  # 실행될 함수
#     provide_context=True,
#     queue="ai",
#     op_kwargs={"load_path":"{{ task_instance.xcom_pull(task_ids='bigkinds_news_collect_task') }}",  
#                "cache_path":"/data_store/airflow_dir/airflow_dags/cache/data/pre", 
#                "pre_model":"taeminlee/gliner_ko", 
#                "cache_dir":"/data_store/airflow_dir/airflow_dags/cache/model/huggingface",
#                "stopwords_path": "/data_store/airflow_dir/airflow_dags/stopwords_for_related.txt",
#                "use_cuda":False},  # 키워드 인수
#     dag=dag,
# )

vcp_video_data_task = PythonOperator(
    task_id='vcp_video_data_task',
    python_callable=vcp_video_data_function,  # 실행될 함수
    provide_context=True,
    queue="ai",
    op_kwargs={"now_date":"{{ task_instance.xcom_pull(task_ids='schedule_start_task') }}", 
               "cache_path":"/data_store/airflow_dir/airflow_dags/cache/data/pre/related", 
               "chunk_size":1000},  # 키워드 인수
    dag=dag,
)
schedule_start_task >> [video_data_collect_task, vcp_video_data_task] >> video_preprocessing_task
