from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import os
import sys
sys.path.append('./')
sys.path.append('./airflow_dags')
sys.path.append('/data_store/airflow_dir/airflow_dags')
from dotenv import load_dotenv
# .env 파일 경로 지정
dotenv_path = os.path.join("/data_store/airflow_dir/airflow_dags", '.env')
# .env 파일 로드
load_dotenv(dotenv_path)
from datetime import datetime, timedelta
from channel_update_util.channel_keywords import channel_keywords


# JSON 파일로부터 default_args 로드
with open('/app/airflow/dags/config.json', 'r') as f:
    default_args = json.load(f)['keyword']
# default_args['start_date'] = datetime.now()
default_args['start_date'] = datetime(2024, 7, 5)


# DAG 정의
dag = DAG(
    'ai_keyword_dag',
    default_args=default_args,
    schedule_interval = default_args["schedule_interval"],
    tags=["channel", "keyword", "tf-idf", "update"]
)

# Python 함수 정의
def schedule_start(**context):
    execution_date = context['execution_date']
    execution_date = datetime.today() - timedelta(days=1)
    # 사용자 정의 로직에서 execution_date 사용
    execution_date_str = execution_date.strftime("%Y-%m-%d")  # execution_date를 문자열로 변환
    print("Airflow execution date:", execution_date_str)
    return execution_date_str


# PythonOperator를 사용하여 Python 함수 실행
schedule_start_task = PythonOperator(
    task_id='schedule_start_task',
    python_callable=schedule_start,
    provide_context=True,
    dag=dag,
)

# PythonOperator를 사용하여 DAG에 함수 추가
channel_keyword_update_task = PythonOperator(
    task_id='channel_keyword_update_task',
    python_callable=channel_keywords,  # 실행될 함수
    provide_context=True,
    queue="ai",
    op_kwargs={
                "josa_path":'/data_store/airflow_dir/airflow_dags/ai_tools/kor_josa.txt', 
                "stopwords_path":"/data_store/airflow_dir/airflow_dags/ai_tools/stopwords_for_keyword.txt",
                "ntop":5,
                "data_size":100,
                "channel_range_day":90,
                "data_range_day":365},  # 키워드 인수
    dag=dag,
)

schedule_start_task >> channel_keyword_update_task