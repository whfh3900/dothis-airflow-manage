import sys
import json
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from aiokafka import AIOKafkaConsumer
from util.db_util import get_mysql_connector

sys.path.append('./')
load_dotenv()
CONSUME_TOPIC = "video-history-crawled"
GROUP_ID = "video-history-consumer"
NUM_PARTITION = 9
BROKER_LIST = ["dothis2.iptime.org:19092", "dothis2.iptime.org:29092", "dothis2.iptime.org:39092"]
BATCH_SIZE = 20000

async def history_raw_load():
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
    today_str = datetime.today().strftime("%Y-%m-%d")
    conn = get_mysql_connector("dothis_raw")
    cursor = conn.cursor()
    cols = [
        "video_id", "video_views", "video_likes",
        "video_comments", "video_performance", "year", 
        "month", "day", "video_cluster"
        ]
    insert_query = f"""
        INSERT IGNORE INTO dothis_raw.video_history_{today_str.replace("-", "")}
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
                value = msg.value
                value["year"], value["month"], value["day"] = today_str.split("-")
                row: dict = {}
                for col in cols:
                    row[col] = value.get(col)
                data_list.append(list(row.values()))
        if data_list != []:
            cursor.executemany(insert_query, data_list)
            conn.commit()
        await consumer.commit()
        return len(data_list)
    try:
        while True:
            msg_list: dict = await consumer.getmany(timeout_ms=10000, max_records=20000)
            video_count += await process_msg_batch(msg_list)
            if all(partition_eof.values()):
                break
        print(f"insert video_history_shorts row count : {video_count}")
    finally:
        await consumer.stop()
        conn.close()
        return video_count


def video_history_raw_load():
    asyncio.run(history_raw_load())