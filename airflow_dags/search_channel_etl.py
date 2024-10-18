import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from util.callback_util import on_success_callback, on_failure_callback

from channel_search_util.channel_data import (
    extract_rds_keywords,
    search_keyword,
    mysql_create_channel_list_tbl,
    process_search_result,
    get_new_channel_list,
    create_channel_data_tbl,
    get_channel_data,
    transfer_raw_to_ld,
    transfer_ld_to_temp,
    svc_load,
    get_channel_list,
    get_new_video_ids,
    get_new_video_data
)

sys.path.append('./')
dotenv_path = os.path.join("/data_store/airflow_dir/airflow_dags", '.env')
args = {
    "owner": "ybw",
    "depends_on_past": False,
    "email": ["lms46784678@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "on_success_callback": on_success_callback,
    "on_failure_callback": on_failure_callback
    }                                                                
                                                                     
dag = DAG(                                                           
    "channel_search_etl",                        
    default_args = args,
    description="channel_search_etl",
    schedule_interval='0 0 1 * *',  # 매달 1일에 시작
    start_date=datetime(2023, 11, 28),
    catchup=False,
    tags=["Ignite", "kafka", "mysql" , "ETL", "channel-data", "lambda", "crawling", "YouTube"]
)

# 각 작업을 PythonOperator로 정의
extract_rds_keywords_task = PythonOperator(
    task_id='extract_rds_keywords',
    python_callable=extract_rds_keywords,
    dag=dag
)

search_keyword_task = PythonOperator(
    task_id='search_keyword',
    python_callable=search_keyword,
    dag=dag
)

mysql_create_channel_list_tbl_task = PythonOperator(
    task_id='mysql_create_channel_list_tbl',
    python_callable=mysql_create_channel_list_tbl,
    dag=dag
)

process_search_result_task = PythonOperator(
    task_id='process_search_result',
    python_callable=process_search_result,
    dag=dag
)

get_new_channel_list_task = PythonOperator(
    task_id='get_new_channel_list',
    python_callable=get_new_channel_list,
    dag=dag
)

create_channel_data_tbl_task = PythonOperator(
    task_id='create_channel_data_tbl',
    python_callable=create_channel_data_tbl,
    dag=dag
)

get_channel_data_task = PythonOperator(
    task_id='get_channel_data',
    python_callable=get_channel_data,
    dag=dag
)

transfer_raw_to_ld_task = PythonOperator(
    task_id='transfer_raw_to_ld',
    python_callable=transfer_raw_to_ld,
    dag=dag
)

transfer_ld_to_temp_task = PythonOperator(
    task_id='transfer_ld_to_temp',
    python_callable=transfer_ld_to_temp,
    dag=dag
)

svc_load_task = PythonOperator(
    task_id='svc_load',
    python_callable=svc_load,
    dag=dag
)

get_channel_list_task = PythonOperator(
    task_id='get_channel_list',
    python_callable=get_channel_list,
    dag=dag
)

get_new_video_ids_task = PythonOperator(
    task_id='get_new_video_ids',
    python_callable=get_new_video_ids,
    dag=dag
)

get_new_video_data_task = PythonOperator(
    task_id='get_new_video_data_youtube_search',
    python_callable=get_new_video_data,
    dag=dag
)   

# 작업 간의 순서 정의
extract_rds_keywords_task >> search_keyword_task >> mysql_create_channel_list_tbl_task >> process_search_result_task >> get_new_channel_list_task >> create_channel_data_tbl_task >> get_channel_data_task >> transfer_raw_to_ld_task >> transfer_ld_to_temp_task >> svc_load_task >> get_channel_list_task >> get_new_video_ids_task >> get_new_video_data_task