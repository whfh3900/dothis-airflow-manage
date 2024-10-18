import json
import asyncio
from dotenv import load_dotenv
from aiokafka import AIOKafkaConsumer
import sys
from datetime import datetime, timedelta
from util.db_util import get_mysql_connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from util.callback_util import on_success_callback, on_failure_callback

sys.path.append('./')
load_dotenv()
CONSUME_TOPIC = "video-history-bad-topic"
GROUP_ID = "bad-video-consumer"
NUM_PARTITION = 5
BROKER_LIST = ["dothis2.iptime.org:19092", "dothis2.iptime.org:29092", "dothis2.iptime.org:39092"]
BATCH_SIZE = 5000
MAX_RETRIES = 1
TIMEOUT = 60



async def invalid_video_process():
    """
    매일 10시에 실행되는 DAG
    video_history_etl 로직에서 생성되는 bad-video topic을 소비하여 invalid_video 테이블에 데이터를 적재
    """
    conn = get_mysql_connector("dothis_svc")
    cursor = conn.cursor()
    check_ymd = datetime.now().strftime("%Y-%m-%d")
    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=','.join(BROKER_LIST),
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        max_poll_interval_ms=100000
    )
    
    await consumer.start()
    async def process_msg_batch(msg_list):
        record_list = []
        for _, records in msg_list.items():
            if records:
                for record in records:
                    record_list.append(record)
        print(f"Processing batch of size: {len(record_list)}")
        value_list = [[msg.value.get("channel_id"), msg.value.get("video_id"), check_ymd]for msg in record_list]
        query = """
        INSERT INTO dothis_svc.invalid_video (channel_id, video_id, check_ymd) 
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
        channel_id = VALUES(channel_id),
        video_id = VALUES(video_id),
        check_ymd = VALUES(check_ymd);
        """
        cursor.executemany(query, value_list)
        conn.commit()
        await consumer.commit()
        return len(record_list)
    try:
        while True:
            msg_cnt = 0
            msg_list: dict = await consumer.getmany(timeout_ms=100000, max_records=BATCH_SIZE)
            msg_cnt += await process_msg_batch(msg_list)
            if msg_list == {}:
                break
        cursor.execute(f"SELECT COUNT(*) FROM dothis_svc.invalid_video WHERE check_ymd = '{check_ymd}'")
        insert_cnt = cursor.fetchone()[0]
    finally:
        await consumer.stop()
        conn.close()
        return {"insert_row_cnt": insert_cnt, "consume_msg_cnt": msg_cnt}


def do_async_job():
    result = asyncio.run(invalid_video_process())
    return result

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
    "invalid_video_load",                        
    default_args = args,
    description="video_history_etl 로직에서 생성되는 bad-video topic을 소비하여 invalid_video 테이블에 데이터를 적재",
    schedule_interval='0 10 * * *',
    start_date=datetime(2023, 11, 28),
    catchup=False,
    tags=["video-history", "kafka", "youtube", "bad-video"],
)

invalid_video_load = PythonOperator(
    task_id="invalid_video_load",
    python_callable=do_async_job,
    dag=dag
)


invalid_video_load