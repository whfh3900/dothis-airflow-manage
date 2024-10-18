from sys import platform
import pandas as pd
from tqdm import tqdm
tqdm.pandas()
import sys
sys.path.append("./")
sys.path.append("./airflow_dags")
sys.path.append("/data_store/airflow_dir/airflow_dags")
from util.db_util import get_mysql_connector
import json
from datetime import datetime, timedelta
from util.dothis_nlp import PreProcessing, hashtag_extraction, ZED, PostProcessing, remove_special_characters

def video_cluster(date, load_db="dothis_ld", save_db="dothis_temp"):

    # 오늘 날짜 가져오기
    today = datetime.today()
    
    # 날짜 형식 변환
    _date = datetime.strptime(date, '%Y%m%d')
    
    # 오늘 날짜와 비교
    if _date >= today:
        # 입력날짜의 전날로 설정
        _date = _date - timedelta(days=1)
        date = datetime.strftime(_date, '%Y%m%d')
        
            
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
    
    use_cuda = True
    airflow_dag_abs_path = "/data_store/airflow_dir/airflow_dags"
    # airflow_dag_abs_path = "/Users/dothis/suchoi/airflow-manage/airflow_dags"
    # airflow_dag_abs_path = "/home/suchoi/airflow-manage/airflow_dags"
    cache_dir = f'{airflow_dag_abs_path}/ai/models/huggingface'
    stopwords_path = f"{airflow_dag_abs_path}/ai_tools/stopwords_for_redis.txt"
    category_df_path = f"{airflow_dag_abs_path}/ai_tools/zed_category.csv"
    josa_path = f"{airflow_dag_abs_path}/ai_tools/kor_josa.txt"
    model_name = "taeminlee/gliner_ko"
    
    ## 전처리 및 분류 클래스
    prep = PreProcessing(model=model_name, tta_labels=tta_labels, cache_dir=cache_dir, stopwords_path=stopwords_path, use_cuda=use_cuda) ## 전처리
    posp = PostProcessing(josa_path=josa_path, stopwords_path=stopwords_path) ## 후처리
    zed = ZED(cache_dir=cache_dir, category_df=category_df_path, use_cuda=use_cuda)
    
    conn = get_mysql_connector()
    cursor = conn.cursor(buffered=True)
    # # 채널 데이터를 가져옵니다
    # cursor.execute(f"""select CHANNEL_ID, MAINLY_USED_TAGS from dothis_svc.channel_data where MAINLY_USED_TAGS != '';""")
    # channel_dict = {row[0]: [i.strip() for i in row[1].split(",")] for row in cursor.fetchall()}    
    
    # DESC 쿼리를 사용하여 테이블의 구조를 가져옵니다.
    # 컬럼명을 추출하여 리스트에 저장합니다.
    cursor.execute(f"DESC {load_db}.video_data_{date}")
    columns = [column[0] for column in cursor.fetchall()] 
    
    cursor.execute(f"""select * from {load_db}.video_data_{date};""")
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    # df.dropna(subset=["video_title", "video_tags"], inplace=True)
    df.dropna(subset=["video_title"], inplace=True)
    df = df.drop_duplicates(subset=['video_id'])
    
    print("*"*50, " PreProcessing... ", "*"*50)
    df["_video_title"] = df.video_title.progress_apply(lambda x: [(i[0], i[1], i[2]) for i in prep.use_norns(x.strip(), use_upper=True, use_stopword=False, threshold=1.0e-5) if i[1] != "Error"])
    df["_video_tags"] = df.video_tags.apply(lambda x: [(i, "tag", 1) for i in remove_special_characters(x).split() if len(i) <= 6])
    df["_video_description"] = df.video_description.apply(lambda x: [(i, "hashtag", 1) for i in hashtag_extraction(x).split() if len(i) <= 6])

    # Combine the lists from the three columns into one column
    df["use_text"] = df.apply(lambda row: row["_video_title"] + row["_video_tags"] + row["_video_description"], axis=1)

    df["use_text"] = df.use_text.apply(lambda x: [(posp.post_processing(i[0], use_stopword=True), i[1], i[2]) for i in x])
    df["use_text"] = df.use_text.apply(lambda x: [(i[0], i[1], i[2]) for i in x if i[0] != ""])
    
    print("*"*50, " Clustering... ", "*"*50)
    # def exclude_used_tags(channel_id, use_text):
    #     try:
    #         ex_tags = set(channel_dict[channel_id])
    #         tags = set([i[0] for i in use_text if (i[1] in ["tag", "hashtag"]) and (i[0] not in ex_tags)])
            
    #         if len(tags - ex_tags) < 10:
    #             use_text = [(i[0], i[1], i[2]) for i in use_text if (i[1] in ["tag", "hashtag"]) and (i[0] not in ex_tags)]
    #         return use_text
    #     except KeyError:
    #         return use_text
        
    # df["_use_text"] = df.apply(lambda x: exclude_used_tags(x.channel_id, x.use_text), axis=1)
    df["_use_text"] = df["use_text"].apply(lambda x: " ".join([i[0] for i in x]).strip())
    # df['video_cluster'] = df.progress_apply(lambda x: zed.classification(" ".join([i[0] for i in x['_use_text']]).strip() + " [SEP] " + x.video_category)[1], axis=1)
    df['video_cluster'] = df.progress_apply(lambda x: zed.classification(x['_use_text'].strip() + " [SEP] " + x.video_category)[1], axis=1)

    del df['_use_text']
    
    df = df[df["video_cluster"]!=9999]
    df.dropna(subset=["video_cluster"], inplace=True)
    df = _process_df(df)
    
    print("*"*50, " Create dothis pre table ", "*"*50)
    create_dothis_pre_table(date)

    print("*"*50, f" Insert Data for dothis_pre... ", "*"*50)
    try:
        conn.close()
    except:
        pass
    conn = get_mysql_connector()
    cursor = conn.cursor(buffered=True)
    
    df["model_name"] = model_name
    df["_use_text"] = df["use_text"].apply(lambda x: json.dumps(x))
    _pre_columns = ["video_id", "channel_id", "_use_text", "video_duration", "video_published", \
                    "video_category", "video_cluster", "crawled_date", "year", "month", "day", \
                    "model_name"]
    pre_columns = ["video_id", "channel_id", "use_text", "video_duration", "video_published", \
                    "video_category", "video_cluster", "crawled_date", "year", "month", "day", \
                    "model_name"]
    columns_to_str = ", ".join(pre_columns)
    values_placeholder = ', '.join(['%s'] * len(pre_columns))
    
    # SQL 쿼리 생성
    query = f"INSERT IGNORE INTO dothis_pre.video_data_{date} ({columns_to_str}) VALUES ({values_placeholder})"
    # 데이터프레임의 값을 튜플로 변환
    values = [tuple(x) for x in df[_pre_columns].to_numpy()]
    # 여러 개의 데이터를 한번에 삽입
    cursor.executemany(query, values)
    conn.commit()
    
    
    print("*"*50, " Insert Data... ", "*"*50)
    
    #### temp와 svc에 use_text 컬럼 추가
    df["use_text"] = df["use_text"].apply(lambda x: ", ".join([i[0] for i in x]))
    columns.append("use_text")
    
    columns_to_str = ", ".join(columns)
    values_placeholder = ', '.join(['%s'] * len(columns))
    try:
        conn.close()
    except:
        pass
    conn = get_mysql_connector()
    cursor = conn.cursor(buffered=True)
    query = f"INSERT IGNORE INTO {save_db}.video_data_{date} ({columns_to_str}) VALUES ({values_placeholder});"
    values = [tuple(x) for x in df[columns].to_numpy()]
    cursor.executemany(query, values)
    conn.commit()
    conn.close()
    
    print("*"*50, " End ", "*"*50)



def _process_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df[~df["channel_id"].isna()]
    df = df[~df["video_cluster"].isna()]
    df = df.replace(pd.NA, None)
    df = df[df["video_cluster"]!= 9999]
    df["video_published"] = pd.to_datetime(df["video_published"], errors='coerce')
    # df["video_published"] = df["video_published"].dt.strftime('%Y-%m-%d %H:%M:%S')
    df["video_published"] = df["video_published"].dt.strftime('%Y-%m-%d')

    df["crawled_date"] = df["crawled_date"].dt.strftime('%Y-%m-%d %H:%M:%S')
    df["video_duration"] = df["video_duration"].astype(int, errors="ignore")
    df["video_info_card"] = df["video_info_card"].astype(int, errors="ignore")
    df["video_end_screen"] = df["video_end_screen"].astype(int, errors="ignore")
    df["video_cluster"] = df["video_cluster"].astype(int, errors="ignore")
    df["year"] = df["year"].astype(int, errors="ignore")
    df["month"] = df["month"].astype(int, errors="ignore")
    df["day"] = df["day"].astype(int, errors="ignore")
    return df


def create_dothis_pre_table(date):
    conn = get_mysql_connector(db="dothis_pre")
    cursor = conn.cursor(buffered=True)
    
    # 테이블 이름 설정
    table_name = f"video_data_{date}"

    # dothis_pre 사용 설정
    cursor.execute("USE dothis_pre")
    
    # 테이블 존재 여부 확인 쿼리
    check_query = f"SHOW TABLES LIKE '{table_name}';"
    cursor.execute(check_query)
    result = cursor.fetchone()
    
    if result:
        print(f"Table {table_name} already exists. Cancelling operation.")
        return table_name, False
    
    # 테이블 생성 쿼리
    create_query = f"""
            CREATE TABLE `{table_name}` (
            `video_id` varchar(11) NOT NULL,
            `channel_id` varchar(52) NOT NULL,
            `use_text` JSON,
            `video_duration` int(6) DEFAULT '0',
            `video_published` date DEFAULT NULL,
            `video_category` varchar(50) DEFAULT '',
            `video_cluster` int(6) DEFAULT NULL,
            `crawled_date` timestamp NULL DEFAULT NULL,
            `year` int(4) DEFAULT NULL,
            `month` int(2) DEFAULT NULL,
            `day` int(2) DEFAULT NULL,
            `model_name` varchar(50) DEFAULT '',
            PRIMARY KEY (`video_id`)
            ) ENGINE=InnoDB;
            """
    # 테이블 생성
    cursor.execute(create_query)
    conn.commit()
    
    print(f"Table {table_name} created successfully.")
    return table_name, True

if __name__ == "__main__":
    for i in ["20240821", "20240823", "20240824", "20240825"]:
        print(f"Start {i}")
        video_cluster(date=i)
        # create_dothis_pre_table("20240723")
        