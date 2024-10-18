import sys
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from video_update_util.extract_video_list import video_list_crawling
from video_update_util.process_video_data import new_video_data_crawling
from video_cluster_util.video_data_cluster import video_cluster
from util.callback_util import on_success_callback, on_failure_callback

sys.path.append('./')
load_dotenv(".")

args = {
    "owner": "ybw",
    "depends_on_past": False,
    "email": ["lms46784678@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_success_callback": on_success_callback,
    "on_failure_callback": on_failure_callback
    }                                                                
                                                                     
dag = DAG(                                                           
    "video_update_etl",                        
    default_args = args,
    description="video_update_etl",
    schedule_interval='0 17 * * *',
    start_date=datetime(2023, 11, 28),
    catchup=False,
    tags=["video-update", "Ignite", "kafka", "youtube", "lambda"],
)


py_script_dir = "/home/etluser/ray_job/crawler/youtube/aws/video_update"
ssh_user_1 = "etluser@10.0.0.31"
ssh_user_2 = "etluser@10.0.0.32"
python_loc = '/usr/local/bin/python3.10'
num_partition = 3
git_secret = os.environ.get("GIT_SECRET")
etl_ami = os.environ.get("ETL_AMI")
etl_table_date = datetime.today().strftime("%Y%m%d")

extract_channel_list = BashOperator(
    task_id='ext_ch_lst_mysql_to_kafka',
    bash_command=f'ssh {ssh_user_1} {python_loc} {py_script_dir}/channel_list_extract.py mysql',
    dag=dag,
)

crawling_video_list_task = []
for i in range(num_partition):
    crawling_video_list_task.append(
        PythonOperator(
            task_id=f'crawling_new_video_list_from_kafka_to_kafka_{i}',
            python_callable=video_list_crawling,
            op_args=[i],
            dag=dag
        )
    )
    
crawling_new_video_task = []
for i in range(num_partition):
    crawling_new_video_task.append(
        PythonOperator(
            task_id=f'crawling_new_video_info_partition_no_{i}',
            python_callable=new_video_data_crawling,
            op_args=[i],
            dag=dag
        )
    )

channel_update_mysql = BashOperator(
    task_id='channel_crawled_date_update',
    bash_command=f'ssh {ssh_user_2} {python_loc} {py_script_dir}/channel_list_update.py mysql',
    dag=dag,
    )

create_etl_table = BashOperator(
    task_id='create_video_data_table',
    bash_command=f'ssh {ssh_user_2} {python_loc} {py_script_dir}/video_data_tbl.py',
    dag=dag,
    )

transfer_raw_to_ld = BashOperator(
    task_id='video_data_transfer',
    bash_command=f'ssh {ssh_user_2} {python_loc} {py_script_dir}/video_data_transfer.py',
    dag=dag,
    )

video_cluster_task = PythonOperator(
    task_id=f'video_data_cluster_to_temp_db',
    queue="de",
    python_callable=video_cluster,
    op_kwargs={
        "date": etl_table_date,
        "load_db": "dothis_ld",
        "save_db": "dothis_temp"
        },
    dag=dag
    )

load_raw = BashOperator(
    task_id='data_load_from_kafka_to_mysql',
    bash_command=f'ssh {ssh_user_2} {python_loc} {py_script_dir}/new_video_data_load.py mysql',
    dag=dag,
    )

load_svc = BashOperator(
    task_id='data_load_from_temp_to_svc',
    bash_command=f'ssh {ssh_user_2} {python_loc} {py_script_dir}/video_data_svc_load.py',
    dag=dag,
    )

next_dag_etl_to_ignite = TriggerDagRunOperator(
    task_id="next_dag_etl_to_ignite",
    trigger_dag_id="etl_to_ig_video_history",  # The ID of the downstream DAG to trigger
    dag=dag,
)

next_dag_etl_to_ignite_id_mapper = TriggerDagRunOperator(
    task_id="next_dag_etl_to_ignite_id_mapper",
    trigger_dag_id="etl_to_ig_id_mapper",  # The ID of the downstream DAG to trigger
    dag=dag,
)


next_dag_etl_to_ignite_invalid_video = TriggerDagRunOperator(
    task_id="next_dag_etl_to_ignite_invalid_video",
    trigger_dag_id="etl_to_ig_invalid_video",  # The ID of the downstream DAG to trigger
    dag=dag,
)


next_dag_etl_to_ignite_validate = TriggerDagRunOperator(
    task_id="next_dag_etl_to_ig_validate",
    trigger_dag_id="etl_to_ig_validate",  # The ID of the downstream DAG to trigger
    dag=dag,
)

next_dag_etl_to_opensearch = TriggerDagRunOperator(
    task_id="video_data_next_dag_etl_to_opensearch",
    trigger_dag_id="etl_to_os_video_data",
    dag=dag,
)
    

extract_channel_list >> crawling_video_list_task[0]
extract_channel_list >> crawling_video_list_task[1]
extract_channel_list >> crawling_video_list_task[2]
crawling_video_list_task[0] >> crawling_new_video_task[0]
crawling_video_list_task[1] >> crawling_new_video_task[1]
crawling_video_list_task[2] >> crawling_new_video_task[2]
crawling_new_video_task[0] >> channel_update_mysql
crawling_new_video_task[1] >> channel_update_mysql
crawling_new_video_task[2] >> channel_update_mysql
crawling_new_video_task[0] >> create_etl_table
crawling_new_video_task[1] >> create_etl_table
crawling_new_video_task[2] >> create_etl_table
create_etl_table >> load_raw >> transfer_raw_to_ld >> video_cluster_task >> load_svc >> next_dag_etl_to_opensearch
# load_svc >> next_dag_etl_to_ignite
# load_svc >> next_dag_etl_to_ignite_id_mapper
# load_svc >> next_dag_etl_to_ignite_invalid_video
# next_dag_etl_to_ignite >> next_dag_etl_to_ignite_validate
# next_dag_etl_to_ignite_id_mapper >> next_dag_etl_to_ignite_validate
# next_dag_etl_to_ignite_invalid_video >> next_dag_etl_to_ignite_validate

