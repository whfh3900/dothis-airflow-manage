import sys
import json
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from aiokafka import AIOKafkaConsumer
from util.db_util import get_mysql_connector

sys.path.append('./')
load_dotenv()
CONSUME_TOPIC = "video-data-shorts-crawled"
GROUP_ID = "video-data-shorts-mysql-consumer"
NUM_PARTITION = 5
BROKER_LIST = ["dothis2.iptime.org:19092", "dothis2.iptime.org:29092", "dothis2.iptime.org:39092"]
BATCH_SIZE = 200

async def data_raw_load():
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
    conn = get_mysql_connector("dothis_raw")
    cursor = conn.cursor()
    cols = [
        "video_id", "channel_id", "video_title", 
        "video_description", "video_tags", "video_duration", 
        "video_published", "video_category", "video_info_card", 
        "video_with_ads", "video_end_screen", "video_cluster" , 
        "crawled_date", "year", "month", "day"
        ]
    insert_query = f"""
        INSERT IGNORE INTO dothis_raw.video_data_shorts_{datetime.today().strftime("%Y%m%d")}
        ({", ".join(cols)})
        VALUES ({", ".join(['%s'] * len(cols))})
        """
    await consumer.start()
    partition_eof = {partition: False for partition in range(NUM_PARTITION)}
    async def process_msg_batch(msg_list):
        record_list = []
        for _, records in msg_list.items():
            if records:
                for record in records:
                    record_list.append(record)
        data_list = []
        for msg in record_list:
            if msg.value.get("msg"):
                partition_eof[msg.partition] = True
            else:
                value: dict= msg.value
                value["year"], value["month"], value["day"] = value['video_published'].split("-")
                data_list.append(list(value.values()))
        if data_list != []:
            cursor.executemany(insert_query, data_list)
            conn.commit()
        await consumer.commit()
        return len(data_list)
    try:
        while True:
            msg_list: dict = await consumer.getmany(timeout_ms=10000, max_records=100)
            video_count += await process_msg_batch(msg_list)
            if all(partition_eof.values()) or msg_list == {}:
                break
        print(f"insert video_data_shorts row count : {video_count}")
    finally:
        await consumer.stop()
        conn.close()


def video_data_raw_load():
    asyncio.run(data_raw_load())