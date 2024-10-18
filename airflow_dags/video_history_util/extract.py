import json
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from aiokafka import AIOKafkaProducer
from contextlib import closing  # context manager를 위한 임포트
from util.db_util import get_mysql_connector


PRODUCE_TOPIC = "video-history-url"
BROKER_LIST = ["dothis2.iptime.org:19092", "dothis2.iptime.org:29092", "dothis2.iptime.org:39092"]
NUM_PARTITION = 9
BATCH_SIZE = 20000

async def get_video_data_table_list() -> list:
    video_data_tables = [f"video_data_{str(i).zfill(2)}" for i in range(101)]
    video_data_tables.insert(0, "video_data_NULL")
    return video_data_tables

async def get_invalid_video_data(cursor):
    cursor.execute("SELECT * FROM dothis_svc.invalid_video")
    invalid_video_list = [dict(zip(cursor.column_names, data)) for data in cursor.fetchall()]
    return invalid_video_list

async def extract_mysql() -> int:
    producer = AIOKafkaProducer(bootstrap_servers=','.join(BROKER_LIST), request_timeout_ms=80000) # 10MB
    await producer.start()
    try:
        produced_video_cnt = 0
        with closing(get_mysql_connector("dothis_raw")) as conn:  # context manager로 MySQL 연결 관리
            with closing(conn.cursor()) as cursor:
                invalid_video_list = await get_invalid_video_data(cursor)
                df_invalid_video = pd.DataFrame(invalid_video_list)
                invalid_video_list = list(df_invalid_video["video_id"].values)
                
                table_list = await get_video_data_table_list()
                for table_name in table_list:
                    cursor.execute(f"SELECT video_id, channel_id, video_cluster FROM dothis_svc.{table_name} where video_published >= '{(datetime.now() - timedelta(days=365)).date()}'")
                    df_video_data = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
                    df_video_data = df_video_data[~df_video_data["video_id"].isin(invalid_video_list)]
                    produced_video_cnt += len(df_video_data)
                    await produce_message(producer, df_video_data.to_dict(orient="records"), BATCH_SIZE)
                    
    finally:
        if table_name == "video_data_100":
            await asyncio.gather(*[produce_final_message(producer, i) for i in range(NUM_PARTITION)])
        await producer.stop()
        return produced_video_cnt

async def produce_message(producer: AIOKafkaProducer, messages: list, batch_size: int = BATCH_SIZE):
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i + batch_size]
        try:
            await asyncio.gather(*[producer.send(PRODUCE_TOPIC, json.dumps(message).encode('utf-8')) for message in batch])
        except Exception as e:
            print(f"Failed to send batch: {e}")

async def produce_final_message(producer: AIOKafkaProducer, partition_no: int):
    final_message = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "msg": "done",
        "partition_no": partition_no,
        "content": "this topic is end"
    }
    await producer.send(PRODUCE_TOPIC, json.dumps(final_message).encode('utf-8'), partition=partition_no)

def video_history_extract():
    produced_video_cnt = asyncio.run(extract_mysql())
    return produced_video_cnt