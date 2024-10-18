
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
import gensim
from gensim.models.fasttext import FastText
from gensim.models import Word2Vec
from util.auto_folder_make import local_folder_make
from util.dothis_nlp import decode_and_convert, PostProcessing
from util.db_util import get_mysql_connector
import numpy as np
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


def train(now_date,
            model_name="word2vec",
            epochs=500,
            vector_size=200,
            window=3,
            min_count=5,
            workers=18,
            cache_path="../../models/related",
            epoch_increment=100,
            patience=5,
            end_limit = 30,
            end_total = 7,
            josa_path="/data_store/airflow_dir/airflow_dags/ai_tools/kor_josa.txt",
            stopwords_path="/data_store/airflow_dir/airflow_dags/ai_tools/stopwords_for_related.txt"):

    p = PostProcessing(josa_path=josa_path,
                       stopwords_path=stopwords_path)
    
    def upload_directory_to_s3(local_directory, bucket_name, s3_prefix=''):
        """
        로컬 디렉토리의 모든 파일을 S3 버킷에 업로드합니다.
        
        :param local_directory: 로컬 디렉토리 경로
        :param bucket_name: S3 버킷 이름
        :param s3_prefix: S3에서 파일을 저장할 경로의 접두사
        """
        # 환경 변수에서 AWS 자격 증명 읽기
        aws_access_key_id = os.environ.get('AWS_IAM_ACCESS_KEY')
        aws_secret_access_key = os.environ.get('AWS_IAM_SECRET_KEY')
        aws_region = 'ap-northeast-2'  # 한국 리전

        # AWS 자격 증명을 사용하여 S3 클라이언트 생성
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        
        # 로컬 디렉토리 내의 파일을 재귀적으로 탐색
        for root, dirs, files in os.walk(local_directory):
            for filename in files:
                local_path = os.path.join(root, filename)
                relative_path = os.path.relpath(local_path, local_directory)
                s3_key = os.path.join(s3_prefix, relative_path)
                
                try:
                    # 파일을 S3에 업로드
                    print(f"Uploading {local_path} to s3://{bucket_name}/{s3_key}")
                    s3.upload_file(local_path, bucket_name, s3_key)
                except FileNotFoundError:
                    print(f"The file was not found: {local_path}")
                except NoCredentialsError:
                    print("Credentials not available.")
                except PartialCredentialsError:
                    print("Incomplete credentials provided.")
                except Exception as e:
                    print(f"An error occurred: {str(e)}")

    db_name = "dothis_pre"
    conn = get_mysql_connector(db=db_name)
    cursor = conn.cursor(buffered=True)
    
    local_folder_make(cache_path)
    
    now_limit = 0
    now_total = 0
    time_date = 1
    
    df = pd.DataFrame()
    now_date = datetime.strptime(now_date, "%Y-%m-%d")
    
    print("#"*50, " Data Collect ", "#"*50)
    while True:
        if end_total == now_total:
            break
        
        ### time_date까지 데이터가 채워지지 않았을 경우를 대비.
        if end_limit == now_limit:
            print(f"Data is not sufficiently populated. Collects only total {end_total} match data by {now_limit}.")
            break
        end_date = now_date - timedelta(days=time_date)
        end_date_str = end_date.strftime("%Y%m%d")
        
        table_name = f"video_data_{end_date_str}"
        # 테이블 존재 여부 확인 쿼리
        check_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{db_name}' AND table_name = '{table_name}';"
        cursor.execute(check_query)
        result = cursor.fetchone()
        if not result:
            now_limit += 1
            time_date += 1
            continue
        
        query = f"""
                select use_text from {db_name}.{table_name};
                """
        cursor.execute(query)
        etc = pd.DataFrame(cursor.fetchall(), columns=["use_text"])
        if len(etc) == 0:
            now_limit += 1
            time_date += 1
            continue
        print(f"Use {end_date_str}")
        df = pd.concat([df, etc], axis=0)
        time_date += 1
        now_total  += 1

    df.reset_index(drop=True, inplace=True)
    df.use_text = df.use_text.progress_apply(decode_and_convert)
    # 후처리 한번더 9월달부터 후처리 부분 삭제
    df['use_text'] = df['use_text'].progress_apply(lambda x: p.post_processing([str(i[0]) for i in x if isinstance(i[0], str)]))
    texts = df.use_text.to_list()
    del df
    ###########################################################################
    
    print("#"*50, " Start finetune ", "#"*50)
    # 학습 관련 변수 초기화
    previous_losses = []
    patience_counter = 0
    best_loss = np.inf
    
    if model_name == "fasttext":
        model = FastText(texts, vector_size=vector_size, 
                        window=window, min_count=min_count, 
                        workers=workers, sg=0, compute_loss=True)
    elif model_name == "word2vec":
        # Word2Vec 모델 학습
        # Gensim 버전 확인
        gensim_version = gensim.__version__

        # 모델 학습
        if gensim_version >= '4.0.0':
            model = Word2Vec(texts, vector_size=vector_size, 
                            window=window, min_count=min_count, 
                            workers=workers, sg=0, compute_loss=True)
        else:
            model = Word2Vec(texts, size=vector_size, 
                            window=window, min_count=min_count, 
                            workers=workers, sg=0, compute_loss=True)
    else:
        raise Exception("model_name must be selected between 'fasttext' and 'word2vec'.")
    
    # 점진적 학습 루프
    for epoch in range(0, epochs, epoch_increment):
        model.train(texts, total_examples=model.corpus_count, epochs=epoch_increment)
        current_loss = model.get_latest_training_loss()

        # 손실값 평가
        if current_loss < best_loss:
            best_loss = current_loss
            patience_counter = 0  # 손실값이 줄어들면 카운터 초기화
        else:
            patience_counter += 1  # 손실값이 줄어들지 않으면 카운터 증가

        # 손실값을 저장하고 확인
        previous_losses.append(current_loss)
        print(f"Epoch: {epoch + epoch_increment}, Loss: {current_loss}")

        # 연속 5번 손실값이 줄어들지 않으면 학습 중단
        if patience_counter >= patience:
            print("Early stopping: No improvement in loss for 5 consecutive epochs.")
            break
        
    filepath = os.path.join(cache_path, "related_model.bin")
    model.save(filepath)
    ###########################################################################
    
    print("#"*50, " Save to S3 ", "#"*50)
    upload_directory_to_s3(local_directory=cache_path, 
                           bucket_name="dothis-ai", 
                           s3_prefix='models/related')
    
    print("#"*50, " Process Completion ", "#"*50)
    
if __name__ == "__main__":
    # now_date="2024-08-05"
    now_date = datetime.today().strftime('%Y-%m-%d')
    model_name="word2vec"
    epochs=500
    vector_size=200
    window=3
    min_count=5
    workers=18
    cache_path="/home/suchoi/dothis-ai/models/related"
    epoch_increment=100
    patience=5
    end_limit = 30
    end_total = 7
    josa_path="./airflow_dags/ai_tools/kor_josa.txt"
    stopwords_path="./airflow_dags/ai_tools/stopwords_for_related.txt"
    train(now_date=now_date,
          model_name=model_name,
          epochs=epochs,
          vector_size=vector_size,
          window=window,
          min_count=min_count,
          workers=workers,
          cache_path=cache_path,
          epoch_increment=epoch_increment,
          patience=patience,
          end_limit=end_limit,
          end_total=end_total,
          josa_path=josa_path,
          stopwords_path=stopwords_path)
    