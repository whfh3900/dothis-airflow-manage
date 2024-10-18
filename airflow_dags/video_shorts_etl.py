import sys
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from util.callback_util import on_success_callback, on_failure_callback
from video_shorts_util import (
    extract, 
    process_data_shorts,
    process_history_shorts,
    create_etl_tbl,
    transfer_data_etl,
    rds_update_bad_video,
    rds_update_data,
    data_raw_load,
    history_raw_load,
    data_svc_load,
    history_svc_load
    )

sys.path.append('./')
dotenv_path = os.path.join("/data_store/airflow_dir/airflow_dags", '.env')
load_dotenv(dotenv_path)

args = {
    "owner": "ybw",
    "depends_on_past": False,
    "email": ["lms46784678@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": on_failure_callback
    }                                                                
                                                                     
dag = DAG(                                                           
    "video_shorts_etl",                        
    default_args = args,
    description="video_shorts_etl",
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 7, 19),
    catchup=False,
    tags=["kafka", "video-shorts", "lambda", "youtube"]
)

video_extract_task = PythonOperator(
    task_id='video_shorts_extract',
    python_callable=extract.video_shorts_extract,
    dag=dag
)

video_process_data_task = PythonOperator(
    task_id='video_shorts_process_data',
    python_callable=process_data_shorts.video_shorts_data_process,
    dag=dag
)

video_process_history_task = PythonOperator(
    task_id='video_shorts_process_history',
    python_callable=process_history_shorts.video_shorts_history_process,
    dag=dag
)

create_etl_tbl_data_task = PythonOperator(
    task_id='video_shorts_create_etl_tbl_data',
    python_callable=create_etl_tbl.create_video_data_shorts_etl_tbl,
    dag=dag
)

create_etl_tbl_history_task = PythonOperator(
    task_id='video_shorts_create_etl_tbl_history',
    python_callable=create_etl_tbl.create_video_history_shorts_etl_tbl,
    dag=dag
)

load_raw_data_task = PythonOperator(
    task_id='video_shorts_raw_data_load',
    python_callable=data_raw_load.video_data_raw_load,
    dag=dag
)

load_raw_history_task = PythonOperator(
    task_id='video_shorts_raw_history_load',
    python_callable=history_raw_load.video_history_raw_load,
    dag=dag
)

transfer_data_etl_task = PythonOperator(
    task_id='video_shorts_transfer_data_etl',
    python_callable=transfer_data_etl.video_data_shorts_transfer,
    dag=dag
)

transfer_history_etl_task = PythonOperator(
    task_id='video_shorts_transfer_history_etl',
    python_callable=transfer_data_etl.video_history_shorts_transfer,
    dag=dag
)

load_svc_data_task = PythonOperator(
    task_id='video_shorts_svc_data_load',
    python_callable=data_svc_load.video_data_shorts_svc_load,
    on_success_callback=on_success_callback,
    dag=dag
)

load_svc_history_task = PythonOperator(
    task_id='video_shorts_svc_history_load',
    python_callable=history_svc_load.video_history_shorts_svc_load,
    on_success_callback=on_success_callback,
    dag=dag
)

rds_update_bad_video_task = PythonOperator(
    task_id='video_shorts_rds_update_bad_video',
    python_callable=rds_update_bad_video.rds_update_bad_video,
    dag=dag
)


rds_update_need_crawling_data_task = PythonOperator(
    task_id='video_shorts_rds_update_data',
    python_callable=rds_update_data.rds_update_need_crawling,
    dag=dag
)

trigger_http_request_dag = TriggerDagRunOperator(
    task_id="trigger_http_request_dag",
    trigger_dag_id="http_request_dag",
    dag=dag,
)


####### TASK DEPENDENCIES ########

video_extract_task >> video_process_data_task
video_extract_task >> video_process_history_task
video_process_data_task >> create_etl_tbl_data_task
video_process_history_task >> create_etl_tbl_history_task
create_etl_tbl_data_task >> load_raw_data_task
create_etl_tbl_history_task >> load_raw_history_task
load_raw_data_task >> transfer_data_etl_task
load_raw_history_task >> transfer_history_etl_task
transfer_data_etl_task >> load_svc_data_task
transfer_history_etl_task >> load_svc_history_task
load_svc_data_task >> rds_update_need_crawling_data_task
load_svc_history_task >> rds_update_bad_video_task
rds_update_bad_video_task >> trigger_http_request_dag
rds_update_need_crawling_data_task >> trigger_http_request_dag