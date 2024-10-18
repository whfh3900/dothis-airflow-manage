from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd
from gensim import corpora
from gensim.models.ldamodel import LdaModel
from gensim.models.fasttext import FastText
from gensim.models import Word2Vec
from collections import OrderedDict
from collections import Counter
import sys
sys.path.append("./")
sys.path.append("./airflow_dags")
sys.path.append("/data_store/airflow_dir/airflow_dags")

from util.dothis_nlp import PostProcessing, remove_special_characters
from util.db_util import get_mysql_connector
from util.redis_method import REDIS
from util.os_client import opensearch_client

import math
from datetime import datetime, timedelta
import json
import requests
import os
import re
from googleapiclient.discovery import build

# .env 파일 경로 지정
from dotenv import load_dotenv
dotenv_path = os.path.join("/data_store/airflow_dir/airflow_dags", '.env')
if not os.path.exists(dotenv_path):
    dotenv_path = os.path.join("./", '.env')
load_dotenv(dotenv_path)

# import pymysql
# from redis.sentinel import Sentinel
# from tqdm import tqdm
# from konlpy.tag import Mecab
# import redis
# from opensearchpy import OpenSearch

from collections import defaultdict
import numpy as np
from itertools import islice

class TfidfExtract(PostProcessing):

    def __init__(self, josa_path='./kor_josa.txt', 
                 stopwords_path="./stopwords_fin.txt"):
        super().__init__(josa_path=josa_path,
                         stopwords_path=stopwords_path)
                        
        ### 불용어
        self.stopwords = set()
        with open(stopwords_path, "rt", encoding="utf-8-sig") as f:
            while True:
                text = f.readline()
                self.stopwords.add(text.strip())
                if not text:
                    break
        self.stopwords = list(self.stopwords)

    # tf-idf 키워드(탐색어) 추출
    def tfidf_extract(self, docs, threshold_1=0.8, threshold_2=3, ntop=30, keyword_max=None, use_upper=True):
        
        # threshold_1 = 키워드등장수/영상수 의 임계값
        # threshold_2 = 키워드등장수 임계값
        vector = TfidfVectorizer()
        try:
            tfidf = vector.fit_transform(docs).toarray()
        except Exception as e:
            return list()
        columns = vector.get_feature_names_out()
        tfidf = pd.DataFrame(tfidf, columns=columns)
        tfidf = tfidf.astype(bool).sum(axis = 0)
        tfidf = tfidf[~tfidf.index.str.isdigit()] # 숫자로만 이루어진 키워드는 제거
        sum_score = sum(tfidf)
        tfidf = (tfidf/sum_score)
        
        # 정렬
        tfidf = tfidf.sort_values(ascending=False)
        # 불용어 처리
        tfidf_result = [word for word in tfidf.index if word not in self.stopwords]
        result = list()
        
        # 키워드등장수/영상수가 threshold_1 보다 높으면 무분별한 키워드
        # 키워드가 threshold_2 밑이면 키워드가 아니므로 삭제하도록
        for keyword in tfidf_result:
            if len(result) >= ntop:
                break
            
            keyword = keyword.strip()
            # 6글자이하인 키워드만 추출
            if keyword_max:
                if len(keyword) > keyword_max:
                    continue
            count = 0
            for text in docs:
                # 대소문자를 구분하지 않고 단어를 찾습니다.
                count += text.lower().split().count(keyword.lower())
            if ((count/len(docs)) <= threshold_1) & (count >= threshold_2):
                # 후처리해서 넣자(조사제거)
                keyword = self.post_processing(keyword)
                if (keyword != "") & (len(keyword) > 1):
                    if use_upper:
                        keyword = keyword.upper()
                    result.append(keyword)
                    
        return list(OrderedDict.fromkeys(result))

# lda 토픽추출
def lda_extract(docs, top=10):
    # 단어 사전 생성
    lda_word = [text.split() for text in docs]
    dictionary = corpora.Dictionary(lda_word)
    # 문서-단어 매트릭스 생성
    corpus = [dictionary.doc2bow(text) for text in lda_word]
    # LDA 모델 학습
    lda_model = LdaModel(corpus=corpus, id2word=dictionary, num_topics=1, passes=1)
    lda_topic = lda_model.print_topics()[0][1]
    lda_topic = re.findall(r'"([^"]*)"', lda_topic)[:top]
    return lda_topic


# 많은걸로
def counter_extract(docs, top=10):
    counter_word = [text for texts in docs for text in texts.split()]
    counter_sort = [i[0] for i in Counter(counter_word).most_common()][:top]
    return counter_sort

def bigkinds_related(keyword, time_date, topn=10):
    
    # 오늘로부터 n일 전의 날짜 계산
    ## 기간이 길면 길수록 응답확률이 올라감
    end_date = datetime.now()
    start_date = end_date - timedelta(days=time_date)

    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    # 요청할 API의 엔드포인트 URL
    url = "https://tools.kinds.or.kr/word_cloud"
    access_key = os.environ.get('NEWS_API_ACCESSKEY')
    
    data = {
        "access_key": access_key,
        "argument": {
            "query": keyword,
            "published_at": {
                "from": start_date,
                "until": end_date
            },
            "provider": [
                # "경향신문",
            ],
            "category": [
                # "정치>정치일반",
                # "스포츠일반",
            ],
            "category_incident": [
                # "범죄",
                # "교통사고",
            ],
        "byline": "",
        "provider_subject": [ ]
        }
    }
    # JSON 데이터를 문자열로 변환
    payload = json.dumps(data)
    
    # POST 요청 보내기
    response = requests.post(url, data=payload, headers={'Content-Type': 'application/json'})
    
    try:
        # 응답 결과 확인
        result = [i['name'].replace(keyword, "").strip() for i in response.json()['return_object']['nodes']][:topn]
        return result
    except Exception as e:
        return list()

def tfidf_related(df, keyword, find_col="USE_TEXT", recent_video_views_col="recent_video_views", ntop=10):
    tfidf_data = df[find_col].tolist()
    vector = TfidfVectorizer()
    tfidf = vector.fit_transform(tfidf_data).toarray()
    columns = vector.get_feature_names_out()
    tfidf = pd.DataFrame(tfidf, columns=columns)
    tfidf = tfidf.astype(bool).sum(axis = 0)

    word_score_list = list()
    for word in tfidf.keys():
        word_score = tfidf[word]
        video_views_sum = df[df[find_col].str.contains(word)][recent_video_views_col].sum()
        try:
            word_score_list.append(word_score*math.log(video_views_sum))
        except ValueError as e:
            word_score_list.append(0)

    tfidf_t = pd.DataFrame(tfidf.T)
    tfidf_t['word_score'] = word_score_list
    tfidf_t.sort_values(['word_score'], ascending=False, inplace=True)
    tfidf_t.reset_index(inplace=True)
    tfidf_t['index'] = tfidf_t['index'].apply(lambda x: x.replace(keyword, "").strip())
    tfidf_t = tfidf_t[tfidf_t["index"]!=""]
    result = tfidf_t.drop_duplicates(['index'])[:ntop]['index'].tolist()

    return result

class GensimRelated(PostProcessing):
    def __init__(self,
                 path="../../models/related/fasttext/fine_tuned_fasttext_model.bin",
                 josa_path="../../data/josa/kor_josa.txt",
                 stopwords_path="../../data/stopwords/stopwords_for_related.txt",
                 model="fasttext"):
        if model == "fasttext":
            self.model = FastText.load(path)
        elif model == "word2vec":
            self.model = Word2Vec.load(path)
        else:
            raise ValueError("The variables that can fit in the model are fasttext or word2vec.")
        super().__init__(josa_path=josa_path,
                         stopwords_path=stopwords_path)

    def gensim_related(self, keyword, ntop=None, split_word_check=False):
        try:
            # ### 연관어에 탐색어가 포함되어 있으면 삭제 ex) 가제트 -> 가제트팔 이면 삭제
            # ### 연관어 모델은 10개로 개수 맞추기
            
            rel_words_tuple = self.model.wv.similar_by_word(keyword)
            rel_words_list = self.post_processing([key[0] for key in rel_words_tuple])
            # rel_words_list = [key[0] for key in rel_words_tuple]
            rel_words_list = [i for i in rel_words_list if (i not in ["", None]) & (keyword not in i) & (len(i) < 10)]

            if split_word_check:
                ### 키워드에서 잘려진 단어
                split_word = [keyword[i:i+2] for i in range(len(keyword) - 1)]
                # 잘려진 단어가 포함된 단어를 rel_words_list에서 제거
                rel_words_list = [word for word in rel_words_list if not any(sub in word for sub in split_word)]
                
            rel_words_dict = {key[0]:key[1] for key in rel_words_tuple if key[0] in rel_words_list}
            
            result = dict(sorted(rel_words_dict.items(), key=lambda x: x[1], reverse=True))
            if ntop:
                return dict(list(result.items())[:ntop])
            else:
                return dict(list(result.items()))
            
        except Exception as e:
            return dict()
        

class VBR(PostProcessing):
    def __init__(self,
                 stopwords_path="../../data/stopwords/stopwords_for_related.txt", 
                 josa_path="../../data/josa/kor_josa.txt"):
        
        self.client = opensearch_client()
        self.end_date = datetime.now()
        self.end_date_str = self.end_date.strftime("%Y-%m-%d")
        self.start_date = self.end_date - timedelta(days=7, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0)
        self.start_date_str = self.start_date.strftime("%Y-%m-%d")

        super().__init__(josa_path=josa_path, 
                         stopwords_path=stopwords_path)

    def related(self, keyword, size=100000, threadholds=5, ntop=None):
        query = {
            "size": size,  # 최대 10000개의 결과 반환
            "_source": ["@timestamp", "video_views", "video_id", "use_text"],  # 반환할 필드 지정
            "query": {
                "bool": {
                "filter": [
                    {
                    "terms": {
                        "use_text": [keyword]
                    }
                    },
                    {
                    "range": {
                        "@timestamp": {
                            "gte": self.start_date_str,  # 시작 날짜 (포함)
                            "lte": self.end_date_str   # 종료 날짜 (포함)
                        }
                    }
                    }
                ]
                }
            }
        }

        # 검색 쿼리 실행
        response = self.client.search(
            index="video_history",  # 검색할 인덱스 이름
            body=query
        )
        
        df = pd.DataFrame([hit['_source'] for hit in response['hits']['hits']])
        if len(df) > 0:
            
            df['@timestamp'] = pd.to_datetime(df['@timestamp'])  # 날짜 형식으로 변환
            # df["use_text"] = df["use_text"].apply(lambda x: " ".join(x))
            df["use_text"] = df["use_text"].apply(lambda x: " ".join([i.upper() for i in x]))
            df = df.sort_values('@timestamp', ascending=False).drop_duplicates(subset='video_id', keep='first')
            
            # Counter를 사용하여 각 토큰의 등장 횟수를 세고 딕셔너리로 변환합니다.
            token_counts = dict(Counter(" ".join(df["use_text"].tolist()).split()))
            token_counts = {self.post_processing(remove_special_characters(k).replace(keyword, "").strip().lower()): v for k, v in token_counts.items()}
            # 토큰 길이가 1을 넘겨야 함
            # 토큰 카운트 개수가 10개 이상이어야 함
            token_counts = {k: v for k, v in token_counts.items() if len(k) > 1 and v >= threadholds}
            token_counts = dict(sorted(token_counts.items(), key=lambda x: x[1], reverse=True))
        
            result = dict()
            for i, (key, value) in enumerate(token_counts.items()):
                _df = df[df["use_text"].str.split().apply(lambda x: key in x)]
                video_views_sum = _df["video_views"].sum()
                score = int(video_views_sum)
                if not np.isnan(score):
                    result[key] = score
            result = dict(sorted(result.items(), key=lambda x: x[1], reverse=True))
            if ntop:
                return dict(list(result.items())[:ntop])
            else:
                return dict(list(result.items()))
        else:
            return dict()


class WeeklyKeywords():
    def __init__(self, now_date, subscribers=1000000) -> None:
        if isinstance(now_date, str):
            self.now_date_str = now_date
            self.now_date = datetime.strptime(now_date, "%Y-%m-%d")
        elif isinstance(now_date, datetime):
            self.now_date = now_date
            self.now_date_str = datetime.strftime(now_date, "%Y-%m-%d")
        else:
            raise ValueError("now_date only supports datetime or string format %Y-%m-%d format.")
        
        # 요일 (0=월, 1=화, ..., 6=일)을 구합니다.
        day_of_week = self.now_date.weekday()

        # 전주의 월요일을 구합니다.
        days_to_previous_monday = day_of_week + 7
        self.start_date = self.now_date - timedelta(days=days_to_previous_monday)
        
        # 전주의 일요일을 구합니다.
        days_to_previous_sunday = day_of_week + 1
        self.end_date = self.now_date - timedelta(days=days_to_previous_sunday)

        self.start_date_str = datetime.strftime(self.start_date, "%Y-%m-%d")
        self.end_date_str = datetime.strftime(self.end_date, "%Y-%m-%d")
        print(f"Data range {self.start_date_str} ~ {self.end_date_str}")
        
        self.conn = get_mysql_connector(db = "dothis_svc")
        self.cursor = self.conn.cursor(buffered=True)
        
        _now_date_str = "".join(self.now_date_str.split("-")[:2])

        query = f"""
                    SELECT channel_id 
                    FROM dothis_svc.channel_history_{_now_date_str} 
                    WHERE channel_subscribers >= {subscribers};
                """
        self.cursor.execute(query)
        self.mega_channel_ids = [row[0] for row in self.cursor.fetchall()]
        
        self.client = opensearch_client()
        self.r = REDIS(db=0)

    #### 키워드 클러스터링
    def keyword_clustering(self, keyword):
        clusters = list()
        values = list(self.r.smembers(keyword))
        if len(values) > 0:
            for value in values:
                if isinstance(value, bytes):  # 바이트 객체인지 확인
                    value = value.decode('utf-8-sig')
                if value not in ["9999", "None", "nan"]:
                    clusters.append(value.split(":")[-1])

            cluster_dict = dict(Counter(clusters))
            # top_10_keys = ""
            if len(cluster_dict) != 0:
                # 딕셔너리의 값을 기준으로 내림차순으로 정렬하여 키와 값을 튜플로 묶어 리스트에 저장
                sorted_dict = sorted(cluster_dict.items(), key=lambda x: x[1], reverse=True)

            return sorted_dict[0][0]
        else:
            return "None"

        
    def filter_published(self, day=7):
        year_ago = self.end_date - timedelta(days=day)
        year_ago_str = year_ago.strftime("%Y-%m-%d")
        return year_ago_str


    def get_date_range_dict(self, day_range=7):
        # 결과를 저장할 defaultdict 생성
        date_range_dict = defaultdict(list)
        # 시작 날짜부터 종료 날짜까지 반복
        start_date = self.end_date - timedelta(days=day_range)
        # current_date = self.start_date
        current_date = start_date
        while current_date <= self.end_date:
            # 년월 키 생성
            year_month_key = current_date.strftime('%Y%m')
            # 일 추가
            date_range_dict[year_month_key].append(current_date.strftime('%d'))
            # 다음 날로 이동
            current_date += timedelta(days=1)
        # defaultdict을 dict로 변환하여 반환
        return dict(date_range_dict)


    def get_week_dates(self, day_range=7):
        # 일주일 전까지의 날짜 리스트 생성
        date_list = [(self.end_date - timedelta(days=x)).strftime('%Y-%m-%d') for x in range(day_range)]
        return date_list


    def video_view_count(self, video_id):
        # YouTube API 키를 입력하세요
        api_key = os.getenv('GOOGLE_YOUTUBE_API_KEY')
        # YouTube API 클라이언트 빌드
        youtube = build('youtube', 'v3', developerKey=api_key)
        try:
            # API 호출을 통해 동영상 정보 가져오기
            request = youtube.videos().list(
                part='statistics',
                id=video_id
            )
            response = request.execute()
            # 조회수 출력
            view_count = response['items'][0]['statistics']['viewCount']
            return int(view_count)
        except Exception as e:
            print(e)
            return 0
        
        
    def calc_weekly_views(self, keyword, day_range=7, size=10000):
        df = pd.DataFrame()
        for date in self.get_week_dates(day_range=day_range):
            gte_date = date+"T00:00:00.000Z"
            lte_date = date+"T23:59:59.999Z"
            query = {
                "size": size,  # 최대 10000개의 결과 반환
                "_source": ["@timestamp", "video_views", "video_id", "channel_id", "video_cluster", "video_published"],  # 반환할 필드 지정
                "query": {
                    "bool": {
                    "filter": [
                        {
                        "terms": {
                            "use_text": [keyword]
                        }
                        },
                        {
                        "range": {
                            "video_published": {
                            "gte": gte_date,  # 시작 날짜 (포함)
                            "lte": lte_date   # 종료 날짜 (포함)
                            }
                        }
                        }
                    ]
                    }
                },
                # "sort": [
                #     {
                #     "video_views": {
                #         "order": "desc"  # 최신 항목부터 내림차순으로 정렬
                #     }
                #     }
                # ]
            }

            # 검색 쿼리 실행
            response = self.client.search(
                index="video_history",  # 검색할 인덱스 이름
                body=query
            )
            etc = pd.DataFrame([hit['_source'] for hit in response['hits']['hits']])
            df = pd.concat([df, etc], axis=0)

        if len(df) == 0:
            df = pd.DataFrame()
            for date in self.get_week_dates(day_range=day_range):
                gte_date = date+"T00:00:00.000Z"
                lte_date = date+"T23:59:59.999Z"
                query = {
                    "size": size,  # 최대 10000개의 결과 반환
                    "_source": ["@timestamp", "video_id", "channel_id", "video_cluster", "video_published"],  # 반환할 필드 지정
                    "query": {
                        "bool": {
                        "filter": [
                            {
                            "terms": {
                                "use_text": [keyword]
                            }
                            },
                            {
                            "range": {
                                "video_published": {
                                "gte": gte_date,  # 시작 날짜 (포함)
                                "lte": lte_date   # 종료 날짜 (포함)
                                }
                            }
                            }
                        ]
                        }
                    },
                    # "sort": [
                    #     {
                    #     "video_views": {
                    #         "order": "desc"  # 최신 항목부터 내림차순으로 정렬
                    #     }
                    #     }
                    # ]
                }

                # 검색 쿼리 실행
                response = self.client.search(
                    index="video_data",  # 검색할 인덱스 이름
                    body=query
                )
                etc = pd.DataFrame([hit['_source'] for hit in response['hits']['hits']])
                df = pd.concat([df, etc], axis=0)
                
                if len(df) == 0:
                    return pd.DataFrame({"keyword": keyword, 
                                        "category": None, 
                                        "weekly_views":0, 
                                        "video_count":0,
                                        "competitive":0,
                                        "mega_channel":0
                                        }, index = [0])
                    
                df['video_views'] = df.video_id.apply(lambda x: self.video_view_count(x))
        
        df.reset_index(drop=True, inplace=True)
        df['@timestamp'] = pd.to_datetime(df['@timestamp'])
        df['video_views'] = df['video_views'].astype(int)
        category = df.video_cluster.value_counts().index[0]
        channel_id = df.channel_id.unique().tolist()
        recent_views = df.loc[df.groupby('video_id')['@timestamp'].idxmax(), ['video_id', 'video_views']].rename(columns={'video_views': 'video_views_recent'})
        oldest_views = df.loc[df.groupby('video_id')['@timestamp'].idxmin(), ['video_id', 'video_views']].rename(columns={'video_views': 'video_views_oldest'})

        # 최근 날짜와 오래된 날짜의 video_views의 차이 계산
        views_diff = recent_views.merge(oldest_views, on='video_id', suffixes=('_recent', '_oldest'))
        views_diff.columns = ["video_id", "video_views_recent", "video_views_oldest"]
        views_diff['weekly_views'] = views_diff['video_views_recent'] - views_diff['video_views_oldest']

        # weekly_views가 0인 경우, video_views_recent 값으로 변경
        views_diff['weekly_views'] = np.where(views_diff['weekly_views'] == 0, views_diff['video_views_recent'], views_diff['weekly_views'])

        weekly_views = views_diff['weekly_views'].sum()
        video_count = len(views_diff.video_id.unique())

        # VIDEO_ID를 기준으로 중복 제거
        new_df = pd.DataFrame({"keyword": keyword, 
                            "category": category, 
                            "weekly_views":weekly_views, 
                            "video_count":video_count}, index = [0])

        new_df['competitive'] = round(weekly_views / video_count / 1000, 5)
        new_df['mega_channel'] = int(len(set(channel_id) & set(self.mega_channel_ids)))
        return new_df


class Predicate():
    def __init__(self, stopwords_path="../usedata/stopwords_for_predicate.txt",
                 mecab_dic_path=None, match_poss=None, not_match_poss=None):

        # Mecab 형태소 분석기 초기화
        if mecab_dic_path:
            self.mecab = Mecab(dicpath=mecab_dic_path)
        else:
            self.mecab = Mecab()

        ### 불용어
        self.stopwords = set()
        with open(stopwords_path, "rt", encoding="utf-8-sig") as f:
            while True:
                text = f.readline()
                self.stopwords.add(text.strip())
                if not text:
                    break
        self.stopwords = list(self.stopwords)
        
        if not match_poss:
            self.match_poss = [["XR", "XSA+EP"], ["VV+EP", "EF"], ["VV+EP", "ETM"],
                            ["NNG", "XSV+EF"], ["MAG", "XSA+ETM"], ["NNG", "XSA+EC"], 
                            ["VV+EC", "VX+EP"], ["VV+EC", "VX+EF"], ["VV+ETM", "NNB", "VA"],
                            ["NNG", "XSV+ETM"], ["NNG", "XSA+EP"], ["VA", "EP"],
                            ["NNG", "XSV+EP"],  ["MAG", "VV+EC"], ["VV+ETM", "NNB", "VV"], 
                            ["VV", "EP+EP"], ["NNG", "XSA+ETM"], ["VV+EC", "VX"], 
                            ["VV+EP", "EC"], ['NNG', 'XSV+EC']]
        elif isinstance(match_poss, list):
            raise ValueError("Invalid type of match_poss. List format only supported")

        if not not_match_poss:
            self.not_match_poss = [["NNG", "XSA+ETM", "XPN"], ["NNG", "XSV+ETM", "NNG"], 
                                ["NNG", "XSV", "SY"], ["NNG", "VV+EC", "NNG"], 
                                ["NNG", "VCP+EP", "EF"], ["NNG", "XSV+ETM", "XPN"]]
        elif isinstance(not_match_poss, list):
            raise ValueError("Invalid type of not_match_poss. List format only supported")
    
        # host = os.environ.get('OPENSEARCH_HOST') # RDS 엔드포인트 URL 또는 IP 주소
        # port = int(os.environ.get('OPENSEARCH_PORT')) # RDS 데이터베이스 포트 (기본값: 3306)
        # user = os.environ.get('OPENSEARCH_USER') # MySQL 계정 아이디
        # password = os.environ.get('OPENSEARCH_PW') # MySQL 계정 비밀번호

        # # OpenSearch 클러스터에 연결
        # self.client = OpenSearch(
        #     hosts=[{'host': host, 'port': port}],
        #     http_auth=(user, password),
        #     use_ssl=True,
        #     verify_certs=False,  # SSL 인증서 검증 비활성화
        #     time_out=360
        # )



    def get_data(self, keyword, related, days=90, size=100000):
        ago = datetime.now() - timedelta(days=days)
        ago_str = datetime.strftime(ago, format="%Y-%m-%d")
        # 검색 쿼리 작성
        query = {
                "size": size,
                "_source": ["video_title"],  # 반환할 필드 지정
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "bool": {
                                    "must": [
                                        {
                                            "match": {
                                                "use_text": keyword
                                            }
                                        },
                                        {
                                            "match": {
                                                "use_text": related
                                            }
                                        }
                                    ]
                                }
                            },
                            {
                                "range": {
                                    "video_published": {  
                                        "gte": ago_str
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        # 검색 쿼리 실행
        response = self.client.search(
            index="video_history",  # 검색할 인덱스 이름
            body=query)
        texts = [hit['_source']['video_title'] for hit in response['hits']['hits']]
        if texts:
            return texts
        else:
            return None
    
    def predict(self, keyword, related, days=31, size=20000, top = 10):
        
        datas = self.get_data(keyword, related, days=days, size=size)
        if datas is None:
            return dict()

        verbs = dict()
        for data in datas:
            results = self.convert_to_da_form(data)
            for result in results:
                if result[0] not in verbs:
                    verbs[result[0]] = 1
                else:
                    verbs[result[0]] += 1
        # 1보다 큰 항목만 필터링하고, 내림차순으로 정렬
        filtered_verbs = {k: v for k, v in islice(sorted(verbs.items(), key=lambda item: item[1], reverse=True), top) if v > 1}
        return filtered_verbs
                        
    def convert_to_da_form(self, text):
        
        # 입력값이 None인지 확인
        if text is None:
            return list()  # 빈 리스트나 기본 값을 반환
        
        # 해시태그 패턴
        hashtag_pattern = r'#\w+'
        
        # URL 패턴 (http, https, www로 시작하는 URL)
        url_pattern = r'https?://\S+|www\.\S+'
        
        # 해시태그와 URL 제거
        text_without_hashtags = re.sub(hashtag_pattern, ' ', text)
        clean_text = re.sub(url_pattern, ' ', text_without_hashtags)
        
        # | 특수문자 제거
        clean_text = clean_text.replace("|", " ").strip()
        clean_text = clean_text.replace("ㅣ", " ").strip()
        # 양쪽 공백 제거
        text = clean_text.replace(" ", "[MASK]").strip()

        # 품사 태깅
        pos = self.mecab.pos(text)
        
        # 형태소 태그들만 추출
        # tags = [tag for token, tag in pos if token not in self.stopwords]
        tags = [tag for _, tag in pos]

        # 필터링 결과
        filtered_pos = []
        i = 0
        while i < len(pos):
            matched = False
            for pattern in self.not_match_poss:
                pattern_length = len(pattern)
                if tags[i:i + pattern_length] == pattern:
                    i += pattern_length  # 패턴 길이만큼 건너뜀
                    filtered_pos.append(("삭제됨", "DEL"))  # 삭제된 부분에 '삭제됨' 추가
                    matched = True
                    break
            if not matched:
                filtered_pos.append(pos[i])
                i += 1
        verbs = list()
        
        for match_pos in self.match_poss:
            # 슬라이딩 윈도우 방식으로 연속으로 존재하는 VV, EC, VX 찾기
            for i in range(len(filtered_pos) - len(match_pos) + 1):
                window = filtered_pos[i:i + len(match_pos)]
                window_tags = [tag for _, tag in window]
                
                if window_tags == match_pos:
                    text = "".join([i[0] for i in window])
                    
                    if text in self.stopwords:
                        continue
                    
                    last_pos = window[-1][-1].split("+")[-1]
                    if last_pos in ["EP", "VV", "VX", "VA", "XSV", "JX", "ETM"]:
                        if text.endswith(tuple(["을", "는"])):
                            text = text[:-1]
                        text += "다"
                    if text.endswith(tuple(["합니다", "하겠다", "하였다", "했었다", "할까요", "해야다", 
                                            "한다면", "해서다", "해봤다", "합시다", "해라다", "하잖아", 
                                            "해도다", "한지다", "할까다", "합니까", "할런다", "합네다", 
                                            "할리다", "할려다", "할런지", "할려나", "할려고"])):
                        text = text[:-3] + "하다"
                    elif text.endswith(tuple(["하고있다", "해버렸다", "해드려요", "한다면다", "할까요다"])):
                        text = text[:-4] + "하다"
                    elif text.endswith(tuple(["올라갈까"])):
                        text = text[:-4] + "오르다"
                    elif text.endswith(tuple(["겨났는지다"])):
                        text = text[:-5] + "기다"
                    elif text.endswith(tuple(["져버렸다"])):
                        text = text[:-4] + "졌다"
                    elif text.endswith(tuple(["켰으니까"])):
                        text = text[:-4] + "키다"
                    elif text.endswith(tuple(["한다", "한가", "했다", "해요", "해서", "할까", "해라", 
                                              "해다", "한데", "해줘", "할다", "항다", "햇다", "할지",
                                              "할려"])):
                        text = text[:-2] + "하다"
                    elif text.endswith(tuple(["려다", "려줘"])):
                        text = text[:-2] + "리다"
                    elif text.endswith(tuple(["와다"])):
                        text = text[:-2] + "왔다"
                    elif text.endswith(tuple(["싶어"])):
                        text = text[:-2] + "싶다"
                    elif text.endswith(tuple(["깼다"])):
                        text = text[:-2] + "깨다"
                    elif text.endswith(tuple(["있어"])):
                        text = text[:-2] + "있다"
                    elif text.endswith(tuple(["춰다", "췄다"])):
                        text = text[:-2] + "추다"
                    elif text.endswith(tuple(["웠다", "워다"])):
                        text = text[:-2] + "우다"
                    elif text.endswith(tuple(["녀다"])):
                        text = text[:-2] + "녔다"
                    elif text.endswith(tuple(["갔다"])):
                        text = text[:-2] + "가다"
                    elif text.endswith(tuple(["켜다"])):
                        text = text[:-2] + "켰다"
                    elif text.endswith(tuple(["셨", "셧"])):
                        text = text[:-1] + "시다"
                    elif text.endswith(tuple(["셨다"])):
                        text = text[:-2] + "시다"
                    elif text.endswith(tuple(["셨습니까"])):
                        text = text[:-4] + "시다"
                    elif text.endswith(tuple(["져다"])):
                        text = text[:-2] + "졌다"
                    elif text.endswith(tuple(["탔다"])):
                        text = text[:-2] + "타다"
                    elif text.endswith(tuple(["겨다"])):
                        text = text[:-2] + "기다"
                    elif text.endswith(tuple(["겨났는다"])):
                        text = text[:-4] + "기다"
                    elif text.endswith(tuple(["킨다", "킬까"])):
                        text = text[:-2] + "키다"
                    elif text.endswith(tuple(["섰다"])):
                        text = text[:-2] + "서다"
                    elif text.endswith(tuple(["줬다", "준다", "줄게"])):
                        text = text[:-2] + "주다"
                    elif text.endswith(tuple(["냈다"])):
                        text = text[:-2] + "내다"
                    elif text.endswith(tuple(["꼈다"])):
                        text = text[:-2] + "끼다"
                    elif text.endswith(tuple(["꼈다다", "끼시다"])):
                        text = text[:-3] + "끼다"
                    elif text.endswith(tuple(["음"])):
                        text = text[:-1] + "으다"
                    elif text.endswith(tuple(["혀다", "혔다"])):
                        text = text[:-2] + "히다"
                    elif text.endswith(tuple(["졌다", "진다"])):
                        text = text[:-2] + "지다"
                    elif text.endswith(tuple(["려"])):
                        text = text[:-1] + "리다"
                    elif text.endswith(tuple(["드다"])):
                        text = text[:-2] + "들다"
                    elif text.endswith(tuple(["써다", "썼다"])):
                        text = text[:-2] + "쓰다"
                    elif text.endswith(tuple(["러워", "런다"])):
                        text = text[:-2] + "럽다"
                    elif text.endswith(tuple(["됨"])):
                        text = text[:-1] + "되다"
                    elif text.endswith(tuple(["일다", "여다", "려다", "였다", "였어"])):
                        text = text[:-2] + "이다"
                    elif text.endswith(tuple(["떠다"])):
                        text = text[:-2] + "뜨다"
                    elif text.endswith(tuple(["띈다"])):
                        text = text[:-2] + "띄다"
                    elif text.endswith(tuple(["둬라"])):
                        text = text[:-2] + "두다"
                    elif text.endswith(tuple(["되고있다", "되어있다"])):
                        text = text[:-4] + "되다"
                    elif text.endswith(tuple(["나버렸다"])):
                        text = text[:-4] + "났다"
                    elif text.endswith(tuple(["뎌야한다"])):
                        text = text[:-4] + "디다"
                    elif text.endswith(tuple(["려드리다", "릴게요다"])):
                        text = text[:-4] + "리다"
                    elif text.endswith(tuple(["들으며다"])):
                        text = text[:-4] + "듣다"
                    elif text.endswith(tuple(["됩니다", "된다면", "될지다", "된다던"])):
                        text = text[:-3] + "되다"
                    elif text.endswith(tuple(["이었다", "여줄까", "여봐요"])):
                        text = text[:-3] + "이다"
                    elif text.endswith(tuple(["폈는데다", "펴봤더니"])):
                        text = text[:-4] + "피다"
                    elif text.endswith(tuple(["려드릴께요", "려드릴게요"])):
                        text = text[:-5] + "리다"
                    elif text.endswith(tuple(["여드릴께요", "여드릴게요"])):
                        text = text[:-5] + "이다"
                    elif text.endswith(tuple(["라갈까"])):
                        text = text[:-3] + "르다"
                    elif text.endswith(tuple(["봅니다", "볼게요", "볼까요"])):
                        text = text[:-3] + "보다"
                    elif text.endswith(tuple(["타봤다"])):
                        text = text[:-3] + "타다"
                    elif text.endswith(tuple(["로운다"])):
                        text = text[:-3] + "롭다"
                    elif text.endswith(tuple(["진다면"])):
                        text = text[:-3] + "지다"
                    elif text.endswith(tuple(["개놓다"])):
                        text = text[:-3] + "개다"
                    elif text.endswith(tuple(["사볼까"])):
                        text = text[:-3] + "사다"
                    elif text.endswith(tuple(["다운다"])):
                        text = text[:-3] + "답다"
                    elif text.endswith(tuple(["췄다"])):
                        text = text[:-2] + "추다"
                    elif text.endswith(tuple(["춰줄게", "춰줄께"])):
                        text = text[:-3] + "추다"
                    elif text.endswith(tuple(["립니다", "릴까다", "려서다", "려볼까"])):
                        text = text[:-3] + "리다"
                    elif text.endswith(tuple(["겨봤다", "겼으니"])):
                        text = text[:-3] + "기다"
                    elif text.endswith(tuple(["져지다"])):
                        text = text[:-3] + "졌다"
                    elif text.endswith(tuple(["워봤다", "웠더니"])):
                        text = text[:-3] + "우다"
                    elif text.endswith(tuple(["나봤다"])):
                        text = text[:-3] + "났다"
                    elif text.endswith(tuple(["뎌야다"])):
                        text = text[:-3] + "디다"
                    elif text.endswith(tuple(["웁시다"])):
                        text = text[:-3] + "울다"
                    elif text.endswith(tuple(["않아요"])):
                        text = text[:-3] + "않다"
                    elif text.endswith(tuple(["켜야다"])):
                        text = text[:-3] + "키다"
                    elif text.endswith(tuple(["났다"])):
                        text = text[:-2] + "나다"
                    elif text.endswith(tuple(["켰다", "킨다", "킬다"])):
                        text = text[:-2] + "키다"
                    elif text.endswith(tuple(["샀다"])):
                        text = text[:-2] + "사다"
                    elif text.endswith(tuple(["켜"])):
                        text = text[:-1] + "키다"
                    elif text.endswith(tuple(["해"])):
                        text = text[:-1] + "하다"
                    elif text.endswith(tuple(["봐"])):
                        text = text[:-1] + "보다"
                    elif text.endswith(tuple(["기", "냐", "네", "죠", "어", "지", "나", "고", "니"])):
                        text = text[:-1] + "다"
                    elif text.endswith(tuple(["으니까다", "잖아요다"])):
                        text = text[:-4] + "다"
                    elif text.endswith(tuple(["러운다", "러우다"])):
                        text = text[:-3] + "럽다"
                    elif text.endswith(tuple(["네요", "셨다", "슴다", "어요", "다구", "라다", "렸나", "을까", \
                        "다면", "는데", "잖아", "다고", "었다", "어다", "쥬다", "나다", "고다", "았다", "겠다", \
                        "은다" "다다", "던다", "나요", "줘요", "다가", "지만", "더니", "였니", "는다", "습니", \
                        "으면", "습다", "시다", "으다"])):
                        text = text[:-2] + "다"
                    elif text.endswith(tuple(["습니다", "다고다", "읍니다", "더니다", "는데요", \
                        "냐구요", "아겠네", "지만다", "는데다", "구나다", "다가다", "리면다", "을까다", \
                        "냐고요", "으시다"])):
                        text = text[:-3] + "다"
                    elif text.endswith(tuple(["됐다", "된다", "될다", "될까", "돼다"])):
                        text = text[:-2] + "되다"
                    elif text.endswith(tuple(["돼"])):
                        text = text[:-2] + "되다"
                    
                    ### 2번째 처리(과거시제)
                    if text.endswith(tuple(["합니다", "한데다", "할게요"])):
                        text = text[:-3] + "하다"
                    elif text.endswith(tuple(["줬다"])):
                        text = text[:-2] + "주다"
                    elif text.endswith(tuple(["다다", "는다", "더다"])):
                        text = text[:-2] + "다"
                    elif text.endswith(tuple(["려드리다", "려야하다", "렸잖아요"])):
                        text = text[:-4] + "리다"
                    elif text.endswith(tuple(["춰다", "췄다"])):
                        text = text[:-2] + "추다"
                    elif text.endswith(tuple(["립니다", "렸더니", "릴게요"])):
                        text = text[:-3] + "리다"
                    elif text.startswith(tuple(["지으"])):
                        text = "짓" + text[2:]
                    elif text.endswith(tuple(["섰다"])):
                        text = text[:-2] + "서다"
                    elif text.startswith(tuple(["아무리"])):
                        text = text[3:]
                    elif text.endswith(tuple(["렸다", "랐다", "린다"])):
                        text = text[:-2] + "리다"
                    elif text.endswith(tuple(["였다"])):
                        text = text[:-2] + "이다"
                    elif text.endswith(tuple(["웠다"])):
                        text = text[:-2] + "우다"
                    elif text.endswith(tuple(["떴다"])):
                        text = text[:-2] + "뜨다"
                    elif text.endswith(tuple(["갔다"])):
                        text = text[:-2] + "가다"
                    elif text.endswith(tuple(["겼다"])):
                        text = text[:-2] + "기다"
                    elif text.endswith(tuple(["켰다", "켜야"])):
                        text = text[:-2] + "키다"
                    elif text.endswith(tuple(["쳤다", "친다"])):
                        text = text[:-2] + "치다"
                    elif text.endswith(tuple(["혀다", "혔다"])):
                        text = text[:-2] + "히다"
                    elif text.endswith(tuple(["쳤지만"])):
                        text = text[:-3] + "치다"
                    elif text.endswith(tuple(["끼시다"])):
                        text = text[:-3] + "끼다"
                    elif text.endswith(tuple(["났다"])):
                        text = text[:-2] + "나다"
                    elif text.endswith(tuple(["탔다"])):
                        text = text[:-2] + "타다"
                    elif text.endswith(tuple(["봤다", "봐다"])):
                        text = text[:-2] + "보다"
                    elif text.endswith(tuple(["졌다", "졌나"])):
                        text = text[:-2] + "지다"
                    elif text.endswith(tuple(["냈다"])):
                        text = text[:-2] + "내다"
                    elif text.endswith(tuple(["왔다", "왔나", "와라"])):
                        text = text[:-2] + "오다"
                    elif text.endswith(tuple(["꿨다"])):
                        text = text[:-2] + "꾸다"
                    elif text.endswith(tuple(["뤘다"])):
                        text = text[:-2] + "루다"
                    elif text.endswith(tuple(["해"])):
                        text = text[:-1] + "하다"
                    elif text.endswith(tuple(["갔었다", "갔다다", "갑니다"])):
                        text = text[:-3] + "가다"
                    elif text.endswith(tuple(["났었다", "났구나", "났지만"])):
                        text = text[:-3] + "나다"
                    elif text.endswith(tuple(["오시다"])):
                        text = text[:-3] + "오다"
                    elif text.endswith(tuple(["올랐다"])):
                        text = text[:-3] + "오르다"
                    elif text.endswith(tuple(["몰랐다"])):
                        text = text[:-3] + "모르다"
                    elif text.endswith(tuple(["늘렀다"])):
                        text = text[:-3] + "누르다"
                    elif text.endswith(tuple(["질렀다"])):
                        text = text[:-3] + "지르다"
                    elif text.endswith(tuple(["질렀길래"])):
                        text = text[:-4] + "지르다"
                    elif text.endswith(tuple(["질러야지다"])):
                        text = text[:-5] + "지르다"
                    elif text.endswith(tuple(["불렀다"])):
                        text = text[:-3] + "부르다"
                    elif text.endswith(tuple(["샀다"])):
                        text = text[:-2] + "사다"
                    elif text.endswith(tuple(["찼다"])):
                        text = text[:-2] + "차다"
                    elif text.endswith(tuple(["뗐다"])):
                        text = text[:-2] + "떼다"
                    elif text.endswith(tuple(["됐다", "된다", "될다", "될까", "돼다", "돼야"])):
                        text = text[:-2] + "되다"
                    elif text.endswith(tuple(["한다", "했다", "해도", "해야", "할다"])):
                        text = text[:-2] + "하다"

                    if text not in ["하다", "보다", "받다", "되다", "봤다", "나다", "났다", \
                        "가다", "있다", "없다", "많다", "그렇다", "그랬다", "저랬다"]:
                        if text.strip().endswith("다"):
                            verbs.append((text, match_pos))

        return verbs
    
    
    
if __name__ == "__main__":
    path="../../models/related/related_model.bin"
    josa_path="../../data/josa/kor_josa.txt"
    stopword_path = "../../data/stopwords/stopwords_for_related.txt"
    df_path = "../../data/video/_video_20240513_pro.csv"
    model="fasttext"
    wk = WeeklyKeywords(datetime.now()-timedelta(days=7))
    
    keyword = "코인전망"
    print(wk.calc_weekly_views(keyword=keyword))

