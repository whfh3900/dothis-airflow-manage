
import json
import asyncio
from dotenv import load_dotenv
from aiokafka import AIOKafkaProducer
import sys
sys.path.append("/Users/byungwoyoon/Desktop/Projects/dothis-airflow/airflow-manage/airflow_dags")

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from util.db_util import get_mysql_connector

PRODUCE_TOPIC = "video-shorts-url"
BROKER_LIST = ["dothis2.iptime.org:19092", "dothis2.iptime.org:29092", "dothis2.iptime.org:39092"]
NUM_PARTITION = 5
BATCH_SIZE = 5000

async def extract_mysql() -> int:
    conn = get_mysql_connector("new_dothis", "RDS")
    cursor = conn.cursor()
    cursor.execute("DESC request_video")
    fetch_columns = cursor.fetchall()
    columns = [data[0] for data in fetch_columns]
    cursor.execute("SELECT * FROM request_video")
    fetch_data = cursor.fetchall()
    video_id_list = []
    for data in fetch_data:
        row = dict(zip(columns, data))
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.strftime("%Y-%m-%d %H:%M:%S")  # datetime 객체를 문자열로 변환
        if row["is_keep_crawling"] == 1:
            video_id_list.append(row)
    conn.close()
    await producer(video_id_list)
    return len(video_id_list)

async def producer(data_list: dict):
    producer = AIOKafkaProducer(
        bootstrap_servers=','.join(BROKER_LIST)
        )
    await producer.start()
    try:
        for data in data_list:
            await produce_message(producer, data)
        await asyncio.gather(*[produce_final_message(producer, i)for i in range(NUM_PARTITION)])
    finally:
        await producer.stop()

async def produce_message(producer: AIOKafkaProducer, message: dict):
    await producer.send_and_wait(PRODUCE_TOPIC, json.dumps(message).encode('utf-8'))

async def produce_final_message(producer: AIOKafkaProducer, partiton_no: int):
    final_message = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "msg": "done",
            "partition_no": int(partiton_no),
            "content": "this topic is end"
        }
    await producer.send_and_wait(PRODUCE_TOPIC, json.dumps(final_message).encode('utf-8'), partition=partiton_no)   

        
def video_shorts_extract():
    asyncio.run(extract_mysql())