import sys
import os
import asyncio
import aiohttp
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from util.kafka_util import kafka_topic_end_producer, SimpleKafkaConsumer, SimpleKafkaProducer
from util.callback_util import on_success_callback, on_failure_callback
from channel_history_util.extract import extract_channel_list


sys.path.append('./')
load_dotenv(".")

py_script_dir = "/home/etluser/ray_job/crawler/youtube/aws/channel_history"
ssh_user_1 = "etluser@10.0.0.31"
ssh_user_2 = "etluser@10.0.0.32"
python_loc = '/usr/local/bin/python3.10'
num_partition = 3
crawled_topic = "channel-history-result"
group_id = "channel-history-consumer"
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
                                         
def channel_history_process():
    load_dotenv()
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    lambda_base_url = os.getenv("FASTAPI_LAMBDA_URL")
    path = "channel_history"
    url = f"{lambda_base_url}/{path}"
    consumer = SimpleKafkaConsumer(topic="channel-history-avg-view", consume_size=300, group_id="channel-history-etl-group")
    producer = SimpleKafkaProducer(topic="channel-history-result", acks=-1)
    re_producer = SimpleKafkaProducer(topic="channel-history-avg-view", acks=-1)
    while True:
        channel_list = consumer.consume()
        if channel_list is None:
            break
        try:
            datas = []
            for channel in channel_list:
                if channel.get("channel_id") is None:
                    continue
                datas.append(
                    {
                        "channel_id": channel.get("channel_id"),
                        "channel_average_views": channel.get("channel_average_views")
                        }
                    )
            result = asyncio.run(gather_fetch(url, headers, datas))
            good_result = []
            for res in result:
                if res.get("code") == 200:
                    good_result.append(res.get("data"))
                elif res.get("code") == 429:
                    re_producer.produce([res.get("data")])
                else:
                    print(res)
            producer.produce(good_result)
        except Exception as e:
            print(e)
            continue
    consumer.close()

async def gather_fetch(url: str, headers: dict, datas: list):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for data in datas:
            tasks.append(fetch_request(url, session, headers, data))
        return await asyncio.gather(*tasks)
    
async def fetch_request(url, session: aiohttp.ClientSession, headers, data):
    async with session.post(url, headers=headers, json=data) as response:
        try:
            return await response.json()
        except:
            return {}
                                                                
dag = DAG(                                                           
    "channel_history_etl",                        
    default_args = args,
    description="channel_history_etl",
    schedule_interval='0 11 * * *',
    start_date=datetime(2023, 11, 28),
    catchup=False,
    tags=["mysql", "kafka", "channel-history", "lambda"],
)

extract = PythonOperator(
    task_id="ch_extract_to_kafka",
    python_callable=extract_channel_list,
    queue="de",
    dag=dag
)

process = PythonOperator(
    task_id=f'ch_crawling',
    python_callable=channel_history_process,
    dag=dag
)

process_done = PythonOperator(
    task_id='ch_end_msg',
    python_callable=kafka_topic_end_producer,
    op_kwargs={'topic': crawled_topic, 'num_partitions': num_partition},
    dag=dag
)

create_etl_table = BashOperator(
    task_id='ch_create_daily_tbl',
    bash_command=f'ssh {ssh_user_1} {python_loc} {py_script_dir}/create_etl_tbl.py',
    dag=dag,    
)

load_raw = BashOperator(
    task_id='ch_raw_load_from_kafka_to_mysql',
    bash_command=f'ssh {ssh_user_1} {python_loc} {py_script_dir}/load.py',
    dag=dag,    
)

data_transfer = BashOperator(
    task_id='ch_data_transfer',
    bash_command=f'ssh {ssh_user_1} {python_loc} {py_script_dir}/data_transfer.py',
    dag=dag,    
)

svc_load = BashOperator(
    task_id='ch_svc_load_mysql',
    bash_command=f'ssh {ssh_user_1} {python_loc} {py_script_dir}/svc_load.py',
    dag=dag,    
)


next_dag_etl_to_ignite = TriggerDagRunOperator(
    task_id="next_dag_etl_to_ignite",
    trigger_dag_id="etl_to_ig_channel_history",
    dag=dag,
)

next_dag_etl_to_opensearch = TriggerDagRunOperator(
    task_id="channel_history_next_dag_etl_to_opensearch",
    trigger_dag_id="etl_to_os_channel_history",
    dag=dag,
)



extract >> create_etl_table >> process >> process_done >> load_raw >> data_transfer >> svc_load >> next_dag_etl_to_opensearch
#>> next_dag_etl_to_ignite
