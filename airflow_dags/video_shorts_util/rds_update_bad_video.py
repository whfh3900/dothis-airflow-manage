import sys
import json
import asyncio
from dotenv import load_dotenv
from aiokafka import AIOKafkaConsumer

sys.path.append("/Users/byungwoyoon/Desktop/Projects/dothis-airflow/airflow-manage/airflow_dags")

from util.db_util import get_mysql_connector

sys.path.append('./')
load_dotenv()
CONSUME_TOPIC = "video-history-shorts-bad-url"
GROUP_ID = "video-history-shorts-consumer"
NUM_PARTITION = 5
BROKER_LIST = ["dothis2.iptime.org:19092", "dothis2.iptime.org:29092", "dothis2.iptime.org:39092"]
BATCH_SIZE = 200

async def update_bad_video():
    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=','.join(BROKER_LIST),
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        max_poll_interval_ms=100000
    )
    video_count = 0
    conn = get_mysql_connector("new_dothis", "RDS")
    cursor = conn.cursor()
    await consumer.start()
    partition_eof = {partition: False for partition in range(NUM_PARTITION)}
    async def process_msg_batch(msg_list):
        record_list = []
        for _, records in msg_list.items():
            if records:
                for record in records:
                    record_list.append(record)
        query_list = []
        for msg in record_list:
            if msg.value.get("msg"):
                partition_eof[msg.partition] = True
            else:
                value = msg.value
                query_list.append(f"UPDATE request_video SET is_bad_video = 1 WHERE video_id = '{value['video_id']}'")  
        if query_list != []:
            for query in query_list:
                print(f"Update query: {query}")
                cursor.execute(query)
            conn.commit()
        await consumer.commit()
        return len(query_list)
    try:
        while True:
            msg_list: dict = await consumer.getmany(timeout_ms=10000, max_records=100)
            video_count += await process_msg_batch(msg_list)
            if all(partition_eof.values()) or msg_list == {}:
                break
        print(f"bad video count : {video_count}")
    finally:
        await consumer.stop()
        conn.close()


def rds_update_bad_video():
    asyncio.run(update_bad_video())