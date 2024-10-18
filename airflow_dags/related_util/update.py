
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

from datetime import datetime
import json
import pymysql
from tqdm import tqdm
import math
from collections import Counter
# from redis.sentinel import Sentinel
from util.db_util import get_mysql_connector
from util.redis_method import REDIS


def merge(inference_path, vbr_path, 
                    inference_ratio=0.3, 
                    vbr_ratio=0.7,
                    cache_path="../../data/related"):

    filepath = os.path.join(cache_path, "all_related_words.json")
    
    print("#"*50, " Start merging ", "#"*50)
    with open(vbr_path, 'r', encoding="utf-8-sig") as json_file:
        vbr_related = json.load(json_file)
    with open(inference_path, 'r', encoding="utf-8-sig") as json_file:
        related_inference = json.load(json_file)
    
    combined_results = dict()
    # 키워드에 대한 공통 키만 사용하도록 추출
    common_keys = list(set(vbr_related.keys()) & set(related_inference.keys()))
    
    # 공통 키워드에 대한 연관어 스코어 스케일링
    for keyword in tqdm(common_keys):

        vbr_related_scores = list(vbr_related[keyword].values())
        related_inference_scores = list(related_inference[keyword].values())
        
        vbr_related_score_max = max(vbr_related_scores) if vbr_related_scores else 0
        related_inference_score_max = max(related_inference_scores) if related_inference_scores else 0
        
        # 스케일 조정
        if vbr_related_score_max != 0:
            vbr_score_scales = [(x / vbr_related_score_max) * vbr_ratio for x in vbr_related_scores]
        else:
            vbr_score_scales = [0 for _ in vbr_related_scores]  # max_score가 0일 때 처리 방법

        if related_inference_score_max != 0:
            inference_score_scales = [(x / related_inference_score_max) * inference_ratio for x in related_inference_scores]
        else:
            inference_score_scales = [0 for _ in related_inference_scores]  # max_score가 0일 때 처리 방법
        
        # 업데이트
        vbr_related[keyword] = dict(zip(vbr_related[keyword].keys(), vbr_score_scales))
        related_inference[keyword] = dict(zip(related_inference[keyword].keys(), inference_score_scales))

        # 두 딕셔너리의 모든 연관어 모음
        all_related_word = list(set(vbr_related[keyword].keys()) | set(related_inference[keyword].keys()))
        
        ### dict.get(key, default)로 해당 연관어가 없으면 0이 반환되도록 함.
        combined_dict = dict()
        for related_word in all_related_word:
            vcp_score_value = vbr_related[keyword].get(related_word, 0)
            inference_score_value = related_inference[keyword].get(related_word, 0)
            
            combined_score = vcp_score_value + inference_score_value
            if not math.isnan(combined_score):
                combined_dict[related_word] = combined_score
        
        # 값을 기준으로 내림차순 정렬
        combined_dict = dict(sorted(combined_dict.items(), key=lambda item: item[1], reverse=True))
        combined_results[keyword] = combined_dict
    

    with open(filepath, 'w', encoding="utf-8-sig") as json_file:
        json.dump(combined_results, json_file, indent=4)
        
    # # fasttext, count 기반 json 삭제
    # os.remove(inference_path)
    # os.remove(vcp_path)
    print("#"*50, " Process Completion ", "#"*50)
        
    return filepath
    


def update(load_path):
    
    with open(load_path, 'r', encoding="utf-8-sig") as json_file:
        related_words = json.load(json_file)

    db_name = "new_dothis"
    conn = get_mysql_connector(db=db_name, host="RDS")
    cursor = conn.cursor(buffered=True)

    #### REDIS 연결 정보

    r = REDIS(db=0)

    #### 키워드 클러스터링
    def keyword_clustering(keyword):
        clusters = list()
        values = list(r.cursor.smembers(keyword))
        if len(values) > 0:
            for value in values:
                if isinstance(value, bytes):  # 바이트 객체인지 확인
                    value = value.decode('utf-8-sig')
                if value not in ["None", "nan"]:
                    clusters.append(value.split(":")[-1])

            cluster_dict = dict(Counter(clusters))
            if len(cluster_dict) != 0:
                # 딕셔너리의 값을 기준으로 내림차순으로 정렬하여 키와 값을 튜플로 묶어 리스트에 저장
                sorted_dict = sorted(cluster_dict.items(), key=lambda x: x[1], reverse=True)
            return sorted_dict[0][0]
        else:
            return "None"

    print("#"*50, " Start Update ", "#"*50)
    for keyword, values in tqdm(related_words.items()): 
        rel_words = ", ".join(list(values.keys()))
        scores = ", ".join([str(i) for i in values.values()])
        cluster = keyword_clustering(keyword)
        today_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # query = "UPDATE related_words SET rel_words = '%s', scores = '%s', update_date = '%s' WHERE keyword = '%s';"%(rel_words, scores, today_date, keyword)
        query = f"""
                INSERT INTO related_words (keyword, rel_words, scores, cluster, update_date)
                VALUES ('{keyword}', '{rel_words}', '{scores}', '{cluster}', '{today_date}')
                ON DUPLICATE KEY UPDATE
                rel_words = VALUES(rel_words), 
                scores = VALUES(scores), 
                cluster = VALUES(cluster), 
                update_date = VALUES(update_date);
                """
        try:
            cursor.execute(query)
            conn.commit()
        except pymysql.err.ProgrammingError as e:
            print(e, "keyword: %s, rel_words: %s"%(keyword, rel_words))

    ### 마지막에 연관어가 비어있는 키워드는 삭제
    query = "DELETE FROM related_words WHERE rel_words = '';"
    cursor.execute(query)
    conn.commit()

    # 연결 종료
    conn.close()

    # # 연관어 파일 삭제
    # os.remove(load_path)
    print("#"*50, " Process Completion ", "#"*50)
    
if __name__ == "__main__":
    # now_date="2024-08-05"
    cache_path="/home/suchoi/dothis-ai/data/related"
    inference_path = os.path.join(cache_path, "related_inference.json")
    vbr_path = os.path.join(cache_path, "vbr_related.json")
    load_path = merge(inference_path, vbr_path, 
                        inference_ratio=0.45, 
                        vcp_ratio=0.55,
                        cache_path=cache_path)
    update(load_path)

