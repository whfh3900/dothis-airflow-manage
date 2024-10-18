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
from related_util.inference import vbr_predict, gensim_predict
from related_util.update import merge, update
from util.backup_table import backup_table

# JSON 파일로부터 default_args 로드
with open('/app/airflow/dags/config.json', 'r') as f:
    default_args = json.load(f)['update_db']
# default_args['start_date'] = datetime.now()
default_args['start_date'] = datetime(2024, 7, 5)

# DAG 정의
dag = DAG(
    'ai_related_update_dag',
    default_args=default_args,
    schedule_interval = default_args["schedule_interval"],
    tags=["related", "vbr", "inference", "merge", "update"]
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
    dag=dag,
)


vbr_related_task = PythonOperator(
    task_id='vbr_related_task',
    python_callable=vbr_predict,  # 실행될 함수
    provide_context=True,
    queue="ai",
    op_kwargs={'load_path': "/data_store/airflow_dir/airflow_dags/cache/data/pre/related",
               'stopwords_path': "/data_store/airflow_dir/airflow_dags/ai_tools/stopwords_for_related.txt",
               'josa_path' : "/data_store/airflow_dir/airflow_dags/ai_tools/kor_josa.txt",
               'cache_path': "/data_store/airflow_dir/airflow_dags/cache/data/result/related",
               'size':1000,
               'topn': 10},
    dag=dag,
)

related_inference_task = PythonOperator(
    task_id='related_inference_task',
    python_callable=gensim_predict,  # 실행될 함수
    provide_context=True,
    queue="ai",
    op_kwargs={'model': 'word2vec',
               'model_path': "/data_store/airflow_dir/airflow_dags/cache/model/related/related_model.bin",
               'cache_path': "/data_store/airflow_dir/airflow_dags/cache/data/result/related",
               'josa_path':"/data_store/airflow_dir/airflow_dags/ai_tools/kor_josa.txt",
               'stopwords_path':"/data_store/airflow_dir/airflow_dags/ai_tools/stopwords_for_related.txt",
               'ntop': 10},
    dag=dag,
)


merge_task = PythonOperator(
    task_id='merge_task',
    python_callable=merge,  # 실행될 함수
    provide_context=True,
    queue="ai",
    op_kwargs={'inference_path': "{{ task_instance.xcom_pull(task_ids='related_inference_task') }}",
               'vbr_path': "{{ task_instance.xcom_pull(task_ids='vbr_related_task') }}",
               "inference_ratio": 0.45,
               "vbr_ratio" : 0.55,
               'cache_path': "/data_store/airflow_dir/airflow_dags/cache/data/result/related"},
    dag=dag,
)

update_db_task = PythonOperator(
    task_id='update_db_task',
    python_callable=update,  # 실행될 함수
    provide_context=True,
    queue="ai",
    op_kwargs={'load_path': "{{ task_instance.xcom_pull(task_ids='merge_task') }}"},
    dag=dag,
)


backup_task = PythonOperator(
    task_id='backup_task',
    python_callable=backup_table,  # 실행될 함수
    provide_context=True,
    queue="ai",
    op_kwargs={'table': "related_words",
               'db': "new_dothis",
               'host': "RDS"},
    dag=dag,
)

schedule_start_task >> [related_inference_task, vbr_related_task] >> merge_task >> update_db_task >> backup_task