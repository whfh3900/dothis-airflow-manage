import sys
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from util.callback_util import on_success_callback, on_failure_callback
from video_history_util.process_video_history import video_history_crawling
from video_history_util.extract import video_history_extract

sys.path.append('./')
load_dotenv(".")

py_script_dir = "/home/etluser/ray_job/crawler/youtube/aws/video_history"
ssh_user_1 = "etluser@10.0.0.31"
ssh_user_2 = "etluser@10.0.0.32"
python_loc = '/usr/local/bin/python3.10'
num_partition = 9
consume_topic = "video-history-url"
crawled_topic = "video-history-crawled"
group_id = "video-history-consumer"
git_secret = os.environ.get("GIT_SECRET")
etl_ami = os.environ.get("ETL_AMI")

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
    "video_history_etl",                        
    default_args = args,
    description="video_history_etl",
    schedule_interval='0 0 * * *',
    start_date=datetime(2023, 11, 28),
    catchup=False,
    tags=["video_history", "lambda", "youtube"],
)

extract = PythonOperator(
    task_id='get_video_ids_to_kafka',
    python_callable=video_history_extract,
    queue="de",
    dag=dag,
)


create_etl_table = BashOperator(
    task_id='create_daily_table',
    bash_command=f'ssh {ssh_user_2} {python_loc} {py_script_dir}/create_etl_tbl.py',
    dag=dag,
    )

load_raw = BashOperator(
    task_id='history_load_from_kafka_to_mysql_raw',
    bash_command=f'ssh {ssh_user_2} {python_loc} {py_script_dir}/load.py mysql',
    dag=dag,
)

transfer = BashOperator(
    task_id='video_history_transfer',
    bash_command=f'ssh {ssh_user_2} {python_loc} {py_script_dir}/transfer.py',
    dag=dag,
    )

load_svc = BashOperator(
    task_id='history_load_to_svc',
    bash_command=f'ssh {ssh_user_2} {python_loc} {py_script_dir}/svc_load.py',
    dag=dag,
)


next_dag_etl_to_ignite_video_data = TriggerDagRunOperator(
    task_id="next_dag_etl_to_ignite_video_data",
    trigger_dag_id="etl_to_ig_video_data",  # The ID of the downstream DAG to trigger
    on_success_callback=on_success_callback,
    dag=dag,
)

crawling_task_list = []
for i in range(num_partition):
    task_id = f'video_history_crawling_{i}'
    crawling_task = PythonOperator(
        task_id=task_id,
        python_callable=video_history_crawling,
        op_kwargs={"partition_no": i},
        retries=5,
        dag=dag
    )
    crawling_task_list.append(crawling_task)
crawling_task_list.append(extract)
crawling_task_list.append(load_raw)

next_dag_etl_to_ignite_video_history_shorts = TriggerDagRunOperator(
    task_id="next_dag_etl_to_ignite_video_history_shorts",
    trigger_dag_id="etl_to_ig_video_history_shorts",  # The ID of the downstream DAG to trigger
    dag=dag,
)

next_dag_etl_to_ignite_video_data_shorts = TriggerDagRunOperator(
    task_id="next_dag_etl_to_ignite_video_data_shorts",
    trigger_dag_id="etl_to_ig_video_data_shorts",  # The ID of the downstream DAG to trigger
    dag=dag,
)



create_etl_table >> crawling_task_list >> transfer >> load_svc 
# >> [next_dag_etl_to_ignite_video_data,next_dag_etl_to_ignite_video_history_shorts,next_dag_etl_to_ignite_video_data_shorts]
