import os
import sys
sys.path.append("./airflow_dags")
sys.path.append('/data_store/airflow_dir/airflow_dags')
sys.path.append('./')
from util.db_util import get_mysql_connector
from util.dothis_keyword import WeeklyKeywords
from util.dothis_nlp import decode_and_convert, is_two_char_with_english
from tqdm import tqdm
tqdm.pandas()
import pandas as pd
from collections import Counter, defaultdict
from datetime import datetime, timedelta
import numpy as np

# .env 파일 경로 지정
from dotenv import load_dotenv
dotenv_path = os.path.join("/data_store/airflow_dir/airflow_dags", '.env')
if not os.path.exists(dotenv_path):
    dotenv_path = os.path.join("./", '.env')
load_dotenv(dotenv_path)

def update_weekly_keywords(now_date, 
                            day_range=7,
                            subscribers=1000000,
                            stopwords_path="/home/suchoi/airflow-manage/airflow_dags/ai_tools/stopwords_for_keyword.txt",
                            size=1000):

    update_table = "weekly_views"
    sucess = False
    with open(stopwords_path, 'r', encoding="utf-8-sig") as file:
        stopwords = file.read().splitlines()
        
    conn = get_mysql_connector(db="dothis_pre")
    cursor = conn.cursor(buffered=True)

    print()
    
    ####################################################
    # 매주 화요일에 실행되므로 찾는 범위는 전주 화요일 부터 이번주 월요일(어제)까지
    now_date = datetime.strptime(now_date, "%Y-%m-%d") 
    wk = WeeklyKeywords(now_date, subscribers=subscribers)    
    end_limit = 30
    now_limit = 0
    end_total = 7
    now_total = 0
    time_date = 0

    print("#"*50, " Data collection ", "#"*50)
    df = pd.DataFrame()
    while True:
        if end_total == now_total:
            break
        
        ### time_date까지 데이터가 채워지지 않았을 경우를 대비.
        if end_limit == now_limit:
            print(f"Data is not sufficiently populated. Collects only total {now_total} match data by {end_limit}.")
            break
        # date = fake_end_date - timedelta(days=time_date)
        date = wk.end_date - timedelta(days=time_date)
        date_str = datetime.strftime(date, "%Y%m%d")
        
        # 테이블 존재 여부 확인 쿼리
        check_query = f"SHOW TABLES LIKE 'video_data_{date_str}';"
        cursor.execute(check_query)
        result = cursor.fetchone()
        
        if not result:
            time_date += 1
            now_limit += 1
            continue
        
        print(f"Use {date_str}")
        query = f"desc video_data_{date_str};"
        cursor.execute(query)
        columns = [row[0] for row in cursor.fetchall()]
        query = f"select * from video_data_{date_str};"
        cursor.execute(query)
        etc = pd.DataFrame(cursor.fetchall(), columns=columns)
        df = pd.concat([df, etc], axis=0)
        now_total += 1
        time_date += 1

    conn.close()

    df.dropna(subset=["video_cluster"], inplace=True)
    df = df[~df.video_cluster.isin([1, 13, "None"])].copy()
    df["video_cluster"] = df["video_cluster"].astype(int)

    df['use_text'] = df['use_text'].progress_apply(decode_and_convert)
    df['use_text'] = df['use_text'].apply(lambda x: " ".join([str(i[0]).upper() for i in x 
                                                            if (isinstance(i[0], str)) and 
                                                            (str(i[0]).upper() not in stopwords) and 
                                                            (not is_two_char_with_english(i[0]))]))
    df.reset_index(drop=True, inplace=True)

    print("#"*50, " Select keywords ", "#"*50)
    data = dict()
    for cluster in tqdm(df.video_cluster.unique()):
        etc = df[df.video_cluster == cluster]
        etc = Counter([token for text in etc.use_text.tolist() for token in text.split()])
        etc = {word: count for word, count in etc.items() if count >= 5}
        data[str(cluster).zfill(2)] = dict(sorted(etc.items(), key=lambda item: item[1], reverse=True))

    tokens = list()
    for cluster, token_counts in data.items():
        tokens.extend(sorted(token_counts.items(), key=lambda item: item[1], reverse=True))

    # defaultdict를 사용하여 중복 키의 값을 합침
    result = defaultdict(int)
    for key, value in tokens:
        result[key] += value

    # defaultdict를 일반 dict로 변환
    search_words = dict(sorted(result.items(), key=lambda item: item[1], reverse=True))
    search_words = list([key for key in search_words.keys() if len(key) > 1])
    del df
    ####################################################
    
    print("#"*50, " Start Calculating weekly views ", "#"*50)

    df = pd.DataFrame()
    for i, keyword in enumerate(search_words):
        print(f"({i+1}/{len(search_words)}) {keyword}")
        df = pd.concat([df, wk.calc_weekly_views(keyword, day_range=day_range, size=size)], axis=0)

    print("1. df길이: ", len(df))
    df = df.fillna(0)
    df = df[df.weekly_views > 0].copy()
    # weekly_views 기준으로 내림차순 정렬 후 인덱스 리셋, inplace=True로 원본 DataFrame 수정
    df = df.sort_values(by="weekly_views", ascending=False)

    df.reset_index(drop=True, inplace=True)
    df['ranking'] = df.index + 1  # 인덱스는 0부터 시작하므로 +1을 더해서 1부터 시작하도록 설정
    print("2. df길이: ", len(df))
    
    conn = get_mysql_connector(db="new_dothis", host="RDS")
    cursor = conn.cursor(buffered=True)
    
    ## 마지막 주차만
    query = f"select keyword, ranking, YEAR, MONTH, DAY from {update_table};"
    cursor.execute(query)
    last_week = pd.DataFrame(cursor.fetchall(), columns=["keyword", "ranking", "year", "month", "day"])
    last_week[['year', 'month', 'day']] = last_week[['year', 'month', 'day']].fillna(0).astype(int)
    last_week["date"] = pd.to_datetime(last_week['year'].astype(str)+"-"+last_week['month'].astype(str)+"-"+last_week['day'].astype(str), format="%Y-%m-%d", errors='coerce')

    search_date = wk.end_date - timedelta(days=7)
    _last_week = last_week[last_week['date'] == search_date]
    
    ## 만약 전주 주간키워드가 없을 경우 업데이트 전 가장 최근꺼 불러오기
    if len(_last_week) == 0:
        search_date_str = search_date.strftime("%Y-%m-%d")
        _last_week = last_week[last_week['date'] < wk.end_date_str]
        _last_week_max_str = _last_week['date'].max().strftime("%Y-%m-%d")

        print(f"The date data does not exist in the weekly keyword table {search_date_str}")
        print(f"Gets the most recent date for that table {_last_week_max_str}")
        _last_week = _last_week[_last_week['date'] == _last_week['date'].max()]

    # 저번주 순위
    print("#"*50, " Start last rank ", "#"*50)
    # last_week 데이터프레임에서 keyword와 ranking을 사전으로 변환
    last_week_dict = _last_week.set_index('keyword')['ranking'].to_dict()
    # 랭킹을 리스트로 저장
    last_ranking = list()
    for keyword in tqdm(df['keyword'], total=len(df)):
        last_ranking.append(last_week_dict.get(keyword, 0))  # 사전에서 키워드를 검색, 없으면 0 반환
        
    df['last_ranking'] = last_ranking    
    YEAR = int(wk.end_date_str.split("-")[0])
    MONTH = int(wk.end_date_str.split("-")[1])
    DAY = int(wk.end_date_str.split("-")[2])

    df['YEAR'] = YEAR
    df['MONTH'] = MONTH
    df['DAY'] = DAY

    df.reset_index(drop=True, inplace=True)
    print(df.head().to_string())
    if len(df) > 0:
        conn.ping(reconnect=True)
        ### 이번주것이 이미 있을 수 있으므로 삭제하고 넣자.
        query = f"delete from {update_table} where YEAR={YEAR} and MONTH={MONTH} and DAY={DAY};"
        cursor.execute(query)
        conn.commit()
        
        print("#"*50, " Start Update ", "#"*50)
        columns_to_str = ", ".join(df.columns)
        values_placeholder = ', '.join(['%s'] * len(df.columns))
        # ON DUPLICATE KEY UPDATE 부분 생성
        on_duplicate_key_update = ", ".join([f"{col} = VALUES({col})" for col in df.columns])
        query = f"""
                    INSERT INTO {update_table} ({columns_to_str}) 
                    VALUES ({values_placeholder}) 
                    ON DUPLICATE KEY UPDATE 
                    {on_duplicate_key_update};
                """
        values = [tuple(value) for value in df.values]
        cursor.executemany(query, values)
        conn.commit()
    else:
        raise Exception("Error: No data to update")
    
    print(f"{wk.start_date_str} ~ {wk.end_date_str}")
    print("#"*50, " Process Completion ", "#"*50)
    sucess = True
    return sucess


def table_exists(cursor, schema, table):
    # 테이블 존재 여부 확인 쿼리 (특정 스키마에서 검사)
    check_query = f"""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{table}';
    """
    cursor.execute(check_query)
    result = cursor.fetchone()
    # result[0]이 1이면 테이블 존재, 0이면 테이블 없음
    if result and result[0] > 0:
        return table, True
    else:
        return table, False

    
def update_weekly_keywords_sub(now_date=None, 
                    day_range=7,
                    subscribers=1000000,
                    stopwords_path="/home/suchoi/airflow-manage/airflow_dags/ai_tools/stopwords_for_keyword.txt",
                    db_name="dothis_temp",
                    condition=False):
    if condition:
        print("Already run on previous task")
        return
        
    update_table = "weekly_views"

    with open(stopwords_path, 'r', encoding="utf-8-sig") as file:
        stopwords = file.read().splitlines()

    print()
    if now_date is None:
        now_date = datetime.strftime(datetime.now(), "%Y-%m-%d")
    ####################################################
    # 매주 화요일에 실행되므로 찾는 범위는 전주 화요일 부터 이번주 월요일(어제)까지
    end_date = datetime.strptime(now_date, "%Y-%m-%d") 
    end_date_str = datetime.strftime(end_date, "%Y-%m-%d")
    wk = WeeklyKeywords(end_date, subscribers=subscribers)    
    end_limit = 30
    now_limit = 0
    end_total = 7
    now_total = 0
    time_date = 0

    print("#"*50, " Data collection ", "#"*50)
    df = pd.DataFrame()
    while True:
        if end_total == now_total:
            break
        
        ### time_date까지 데이터가 채워지지 않았을 경우를 대비.
        if end_limit == now_limit:
            print(f"Data is not sufficiently populated. Collects only total {now_total} match data by {end_limit}.")
            break
        # date = fake_end_date - timedelta(days=time_date)
        date = wk.end_date - timedelta(days=time_date)
        date_str = datetime.strftime(date, "%Y%m%d")
        
        # 테이블 존재 여부 확인 쿼리 (특정 스키마에서 검사)
        table_name = f"video_data_{date_str}"
        table_name, result = table_exists(wk.cursor, "dothis_pre", table_name)
        
        if not result:
            time_date += 1
            now_limit += 1
            continue
        
        print(f"Use {table_name}")
        query = f"desc dothis_pre.{table_name};"
        wk.cursor.execute(query)
        columns = [row[0] for row in wk.cursor.fetchall()]
        query = f"select * from dothis_pre.{table_name};"
        wk.cursor.execute(query)
        etc = pd.DataFrame(wk.cursor.fetchall(), columns=columns)
        df = pd.concat([df, etc], axis=0)
        now_total += 1
        time_date += 1

    df.dropna(subset=["video_cluster"], inplace=True)
    df = df[~df.video_cluster.isin([1, 13, "None", None])].copy()
    df["video_cluster"] = df["video_cluster"].astype(int)

    df['use_text'] = df['use_text'].progress_apply(decode_and_convert)
    df['use_text'] = df['use_text'].apply(lambda x: " ".join([str(i[0]).upper() for i in x 
                                                              if (isinstance(i[0], str)) and 
                                                              (str(i[0]).upper() not in stopwords) and 
                                                              (not is_two_char_with_english(i[0]))]))
    df.drop(['year', 'month', 'day'], axis=1, inplace=True)
    df.drop_duplicates(subset=['video_id'], inplace=True)
    df.reset_index(drop=True, inplace=True)        

    print("history join")
    result = pd.DataFrame()
    history_columns = ["video_id", "video_views", "video_performance", "year", "month", "day"]

    history_columns_to_str = ", ".join(history_columns)

    for day in tqdm(wk.get_week_dates(day_range=day_range)):
        day_str = "".join(day.split("-"))
        table_name = "video_history_%s"%(day_str)
        # 테이블 존재 여부 확인 쿼리 (특정 스키마에서 검사)
        table_name, result = table_exists(wk.cursor, db_name, table_name)
        if not result:
            # dropable 테이블확인
            table_name = "dropable_video_history_%s"%(day_str)
            # 테이블 존재 여부 확인 쿼리 (특정 스키마에서 검사)
            table_name, result = table_exists(wk.cursor, db_name, table_name)
            if not result:
                print(f"Not table {table_name}")
                continue
        query = f"""
                    select {history_columns_to_str} from {db_name}.{table_name};
                """.strip()
        wk.cursor.execute(query)

        _etc = pd.DataFrame(wk.cursor.fetchall(), columns=history_columns)
        etc = pd.merge(df, _etc, on="video_id", how="inner")
        if len(etc) > 0:
            result = pd.concat([result, etc], axis=0)
            # etc에 있는 video_id와 일치하는 행들을 제거한 df 생성
            df = df[~df["video_id"].isin(_etc["video_id"])].copy()
    
    result[['year', 'month', 'day']] = result[['year', 'month', 'day']].fillna(0).astype(int)
    result["date"] = pd.to_datetime(result['year'].astype(str)+"-"+result['month'].astype(str)+"-"+result['day'].astype(str), format="%Y-%m-%d", errors='coerce')
    result.drop(['year', 'month', 'day'], axis=1, inplace=True)

    recent_views = result.loc[result.groupby('video_id')['date'].idxmax(), ['video_id', 'video_views']].rename(columns={'video_views': 'video_views_recent'})
    oldest_views = result.loc[result.groupby('video_id')['date'].idxmin(), ['video_id', 'video_views']].rename(columns={'video_views': 'video_views_oldest'})

    # 최근 날짜와 오래된 날짜의 video_views의 차이 계산
    views_diff = recent_views.merge(oldest_views, on='video_id', suffixes=('_recent', '_oldest'))
    views_diff.columns = ["video_id", "video_views_recent", "video_views_oldest"]
    views_diff['weekly_views'] = views_diff['video_views_recent'] - views_diff['video_views_oldest']

    # weekly_views가 0인 경우, video_views_recent 값으로 변경
    views_diff['weekly_views'] = np.where(views_diff['weekly_views'] == 0, views_diff['video_views_recent'], views_diff['weekly_views'])
    views_diff.reset_index(drop=True, inplace=True)
    df = pd.merge(result, views_diff, on="video_id", how="inner")
    del views_diff

    if len(df) < 1:
        raise Exception("Not Found history data")
    
    print("#"*50, " Select keywords ", "#"*50)
    data = dict()
    for cluster in tqdm(df.video_cluster.unique()):
        etc = df[df.video_cluster == cluster]
        # etc = Counter([token.strip() for text in etc.use_text.tolist() for token in text.split()])
        etc = Counter([token.strip() for text in etc.use_text.tolist() for token in text])
        etc = {word: count for word, count in etc.items() if count >= 5}
        data[str(cluster).zfill(2)] = dict(sorted(etc.items(), key=lambda item: item[1], reverse=True))

    tokens = list()
    for cluster, token_counts in data.items():
        tokens.extend(sorted(token_counts.items(), key=lambda item: item[1], reverse=True))

    # defaultdict를 사용하여 중복 키의 값을 합침
    result = defaultdict(int)
    for key, value in tokens:
        result[key] += value

    # defaultdict를 일반 dict로 변환
    search_words = dict(sorted(result.items(), key=lambda item: item[1], reverse=True))
    search_words = list([key for key in search_words.keys() if len(key) > 1])

    print("#"*50, " Calc Keyword Ranking ", "#"*50)

    new_df = pd.DataFrame()
    df['use_text'] = df['use_text'].str.split().apply(lambda x: [word.strip() for word in x])
    for keyword in tqdm(search_words):
        # etc = df[df.use_text.str.contains(keyword)]
        etc = df[df['use_text'].apply(lambda x: keyword in x)]
        weekly_views = etc.video_views.sum()
        category = str(etc['video_cluster'].value_counts().idxmax()).zfill(2)
        video_count = len(etc)
        competitive = round(weekly_views / video_count / 1000, 5)
        mega_channel = int(len(set(etc.channel_id) & set(wk.mega_channel_ids)))
        etc = pd.DataFrame({"keyword": keyword, 
                            "category": category, 
                            "weekly_views":weekly_views, 
                            "video_count":video_count,
                            "competitive":competitive,
                            "mega_channel":mega_channel
                            }, index = [0])
        new_df = pd.concat([new_df, etc], axis=0)    
    
    print("1. df길이: ", len(new_df))
    new_df = new_df.fillna(0)
    new_df = new_df[new_df.weekly_views > 0].copy()
    # weekly_views 기준으로 내림차순 정렬 후 인덱스 리셋, inplace=True로 원본 DataFrame 수정
    new_df = new_df.sort_values(by="weekly_views", ascending=False)

    new_df.reset_index(drop=True, inplace=True)
    new_df['ranking'] = new_df.index + 1  # 인덱스는 0부터 시작하므로 +1을 더해서 1부터 시작하도록 설정
    print("2. df길이: ", len(new_df))
    
    # RDS 데이터베이스 연결 정보
    conn = get_mysql_connector(host="RDS")
    cursor = conn.cursor(buffered=True)
    
    ## 마지막 주차만
    query = f"select keyword, ranking, YEAR, MONTH, DAY from {update_table};"
    cursor.execute(query)
    last_week = pd.DataFrame(cursor.fetchall(), columns=["keyword", "ranking", "year", "month", "day"])
    last_week[['year', 'month', 'day']] = last_week[['year', 'month', 'day']].fillna(0).astype(int)
    last_week["date"] = pd.to_datetime(last_week['year'].astype(str)+"-"+last_week['month'].astype(str)+"-"+last_week['day'].astype(str), format="%Y-%m-%d", errors='coerce')

    search_date = wk.end_date - timedelta(days=7)
    _last_week = last_week[last_week['date'] == search_date]
    
    ## 만약 전주 주간키워드가 없을 경우 업데이트 전 가장 최근꺼 불러오기
    if len(_last_week) == 0:
        search_date_str = search_date.strftime("%Y-%m-%d")
        _last_week = last_week[last_week['date'] < wk.end_date_str]
        _last_week_max_str = _last_week['date'].max().strftime("%Y-%m-%d")

        print(f"The date data does not exist in the weekly keyword table {search_date_str}")
        print(f"Gets the most recent date for that table {_last_week_max_str}")
        _last_week = _last_week[_last_week['date'] == _last_week['date'].max()]

    # 저번주 순위
    print("#"*50, " Start last rank ", "#"*50)
    # last_week 데이터프레임에서 keyword와 ranking을 사전으로 변환
    last_week_dict = _last_week.set_index('keyword')['ranking'].to_dict()
    # 랭킹을 리스트로 저장
    last_ranking = list()
    for keyword in tqdm(new_df['keyword'], total=len(new_df)):
        last_ranking.append(last_week_dict.get(keyword, 0))  # 사전에서 키워드를 검색, 없으면 0 반환
        
    new_df['last_ranking'] = last_ranking    
    YEAR = int(wk.end_date_str.split("-")[0])
    MONTH = int(wk.end_date_str.split("-")[1])
    DAY = int(wk.end_date_str.split("-")[2])

    new_df['YEAR'] = YEAR
    new_df['MONTH'] = MONTH
    new_df['DAY'] = DAY

    new_df.reset_index(drop=True, inplace=True)
    print(new_df.head().to_string())
    if len(new_df) > 0:
        conn.ping(reconnect=True)
        ### 이번주것이 이미 있을 수 있으므로 삭제하고 넣자.
        query = f"delete from {update_table} where YEAR={YEAR} and MONTH={MONTH} and DAY={DAY};"
        cursor.execute(query)
        conn.commit()
        
        print("#"*50, " Start Update ", "#"*50)
        columns_to_str = ", ".join(new_df.columns)
        values_placeholder = ', '.join(['%s'] * len(new_df.columns))
        # ON DUPLICATE KEY UPDATE 부분 생성
        on_duplicate_key_update = ", ".join([f"{col} = VALUES({col})" for col in new_df.columns])
        query = f"""
                    INSERT INTO {update_table} ({columns_to_str}) 
                    VALUES ({values_placeholder}) 
                    ON DUPLICATE KEY UPDATE 
                    {on_duplicate_key_update};
                """
        values = [tuple(value) for value in new_df.values]
        cursor.executemany(query, values)
        conn.commit()
    else:
        print("Error: No data to update")
    
    print(f"{wk.start_date_str} ~ {wk.end_date_str}")
    print("#"*50, " Process Completion ", "#"*50)


if __name__ == "__main__":
    
    # now_date = datetime.today() - timedelta(days=7)
    now_date = datetime.today()
    now_date_str = datetime.strftime(now_date, "%Y-%m-%d")
    update_weekly_keywords_sub(now_date=now_date_str,
                           stopwords_path="./airflow_dags/ai_tools/stopwords_for_keyword.txt",
                           db_name="dothis_temp")