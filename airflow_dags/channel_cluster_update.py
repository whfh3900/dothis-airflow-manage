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
from channel_update_util.channel_cluster import calculate_channel_clusters, channel_data_cluster_update


# JSON 파일로부터 default_args 로드
with open('/app/airflow/dags/config.json', 'r') as f:
    default_args = json.load(f)['channel_cluster']
default_args['start_date'] = datetime(2024, 7, 5)

# DAG 정의
dag = DAG(
    'channel_cluster_update_dag',
    default_args=default_args,
    schedule_interval = default_args["schedule_interval"],
    tags=["channel", "cluster", "update"]

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

calculate_channel_clusters_task = PythonOperator(
    task_id='calculate_channel_clusters_task',
    python_callable=calculate_channel_clusters,  # 실행될 함수
    provide_context=True,
    queue="ai",
    dag=dag,
)

channel_data_cluster_update_task = PythonOperator(
    task_id='channel_data_cluster_update_task',
    python_callable=channel_data_cluster_update,  # 실행될 함수
    provide_context=True,
    queue="ai",
    dag=dag,
)

schedule_start_task >> calculate_channel_clusters_task >> channel_data_cluster_update_task