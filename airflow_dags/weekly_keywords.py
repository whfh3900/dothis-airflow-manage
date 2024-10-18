from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import os
from datetime import datetime, timedelta
import sys
sys.path.append('./')
sys.path.append('./airflow_dags')
sys.path.append('/data_store/airflow_dir/airflow_dags')
from weekly_keywords_util.weekly_keywords import update_weekly_keywords, update_weekly_keywords_sub
from util.backup_table import cold_table, backup_table
from dotenv import load_dotenv
# .env 파일 경로 지정
dotenv_path = os.path.join("/data_store/airflow_dir/airflow_dags", '.env')
# .env 파일 로드
load_dotenv(dotenv_path)

# JSON 파일로부터 default_args 로드
with open('/app/airflow/dags/config.json', 'r') as f:
    default_args = json.load(f)['weekly_keywords']
# default_args['start_date'] = datetime.now()
default_args['start_date'] = datetime(2024, 7, 5)

# DAG 정의
dag = DAG(
    'weekly_keywords_dag',
    default_args=default_args,
    schedule_interval = default_args["schedule_interval"],
    tags=["weekly", "keyword", "update"]
)

# Python 함수 정의
def schedule_start(**context):
    execution_date = context['execution_date']
    execution_date = datetime.today()
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


update_weekly_keywords_task = PythonOperator(
    task_id='update_weekly_keywords_task',
    python_callable=update_weekly_keywords,  # 실행될 함수
    provide_context=True,
    queue="ai",
    op_kwargs={'now_date': "{{ task_instance.xcom_pull(task_ids='schedule_start_task') }}",
               'day_range': 7,
               'subscribers': 1000000,
               'stopwords_path': "/data_store/airflow_dir/airflow_dags/ai_tools/stopwords_for_keyword.txt",
               'size':100000},
    dag=dag,
)

update_weekly_keywords_sub_task = PythonOperator(
    task_id='update_weekly_keywords_sub_task',
    python_callable=update_weekly_keywords_sub,  # 실행될 함수
    provide_context=True,
    queue="ai",
    op_kwargs={'now_date': "{{ task_instance.xcom_pull(task_ids='schedule_start_task') }}",
               'day_range': 7,
               'subscribers': 1000000,
               'stopwords_path': "/data_store/airflow_dir/airflow_dags/ai_tools/stopwords_for_keyword.txt",
               'db_name': "dothis_ld",
               'condition': "{{ task_instance.xcom_pull(task_ids='update_weekly_keywords_task') }}"},
    dag=dag,
)

cold_task = PythonOperator(
    task_id='cold_task',
    python_callable=cold_table,  # 실행될 함수
    provide_context=True,
    queue="ai",
    op_kwargs={'table': "weekly_views",
               'db': "new_dothis",
               'host': "RDS",
               'days':30},
    dag=dag,
)

backup_task = PythonOperator(
    task_id='backup_task',
    python_callable=backup_table,  # 실행될 함수
    provide_context=True,
    queue="ai",
    op_kwargs={'table': "weekly_views",
               'db': "new_dothis",
               'host': "RDS"},
    dag=dag,
)

backup_cold_task = PythonOperator(
    task_id='backup_cold_task',
    python_callable=backup_table,  # 실행될 함수
    provide_context=True,
    queue="ai",
    op_kwargs={'table': "weekly_views_cold",
               'db': "new_dothis",
               'host': "RDS"},
    dag=dag,
)

schedule_start_task >> update_weekly_keywords_task >> update_weekly_keywords_sub_task >> cold_task >> backup_task >> backup_cold_task