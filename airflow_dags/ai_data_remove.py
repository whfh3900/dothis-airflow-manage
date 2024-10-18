from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import os
import sys
sys.path.append('./')
sys.path.append("./airflow_dags")
sys.path.append("/data_store/airflow_dir/airflow_dags")
from datetime import datetime, timedelta
from util.db_util import db_table_remove

# JSON 파일로부터 default_args 로드
with open('/app/airflow/dags/config.json', 'r') as f:
    default_args = json.load(f)['data_remove']
# default_args['start_date'] = datetime.now()
default_args['start_date'] = datetime(2024, 7, 5)

# DAG 정의
dag = DAG(
    'ai_data_remove_dag',
    default_args=default_args,
    schedule_interval = default_args["schedule_interval"],
    tags=["mysql", "remove", "update"]
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

# PythonOperator를 사용하여 DAG에 함수 추가
data_remove_task = PythonOperator(
    task_id='data_remove_task',
    python_callable=db_table_remove,  # 실행될 함수
    provide_context=True,
    op_kwargs={"now_date": "{{ task_instance.xcom_pull(task_ids='schedule_start_task') }}",
               "db": "dothis_pre",
                "remaining_dates": 365},  # 키워드 인수
    dag=dag,
)

schedule_start_task >> data_remove_task

