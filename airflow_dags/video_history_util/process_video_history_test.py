import sys
import os
import json
import asyncio
import aiohttp
from dotenv import load_dotenv
from datetime import datetime
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

sys.path.append('./')
load_dotenv()

CONSUME_TOPIC = "video-history-url"
GOOD_TOPIC = "video-history-crawled"
GROUP_ID = "video-history-consumer"
BAD_TOPIC = "video-history-bad-topic"
NUM_PARTITION = 9
BASE_URL = os.getenv("FASTAPI_LAMBDA_URL")
API_ENDPOINT = "video_history"
URL = f"{BASE_URL}/{API_ENDPOINT}"
MAX_RETRIES = 2
BROKER_LIST = ["dothis2.iptime.org:19092", "dothis2.iptime.org:29092", "dothis2.iptime.org:39092"]
HEADERS = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
}
BATCH_SIZE = 350

async def consume_and_process_video_history(partition_no: int=0):
    consumer = AIOKafkaConsumer(
        bootstrap_servers=','.join(BROKER_LIST),
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        max_poll_interval_ms=60000
    )
    
    producer = AIOKafkaProducer(
        bootstrap_servers=','.join(BROKER_LIST)
    )
    topic_partition = TopicPartition(CONSUME_TOPIC, partition_no)
    consumer.assign([topic_partition])
    await consumer.start()
    await producer.start()
    session = aiohttp.ClientSession()
    partition_eof_dict = {"partition_eof": False}
    async def process_msg_batch(msg_list: dict, partition_no):
        record_list = []
        for _, records in msg_list.items():
            if records:
                for record in records:
                    record_list.append(record)
        task_list = []
        for msg in record_list:
            if msg.value.get("msg"):
                partition_eof_dict["partition_eof"] = True
            else:
                task_list.append(fetch_data_from_api(msg.value, session))
        print(f"Processing batch of size: {len(record_list)}")
        fetch_list = await asyncio.gather(*task_list)
        for data, status_code in fetch_list:
            await handle_response(producer, data.get("data", {}), status_code, partition_no)
        await consumer.commit()
        return len(record_list)
    try:
        msg_cnt = 0
        while True:
            msg_list: dict = await consumer.getmany(topic_partition, timeout_ms=60000, max_records=BATCH_SIZE)
            msg_cnt += await process_msg_batch(msg_list, partition_no)
            if partition_eof_dict["partition_eof"]:
                final_message = {
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "msg": "done",
                    "partition_no": int(partition_no),
                    "content": "this topic is end"
                }
                await produce_message(producer, GOOD_TOPIC, final_message, partition_no)
                break
    finally:
        await consumer.stop()
        await producer.stop()
        await session.close()
        return msg_cnt

async def fetch_data_from_api(data, session, retries=0):
    try:
        async with session.post(URL, headers=HEADERS, json=data) as response:
            if response.status == 500 and retries < MAX_RETRIES:
                print(f"500 error encountered. Retrying... ({retries+1}/{MAX_RETRIES})")
                return await fetch_data_from_api(data, session, retries+1)
            response_data = await response.json()
            return response_data, response_data.get("code", response.status)
    except aiohttp.ClientError as e:
        print(f"HTTP request failed: {e}")
        return {"error": str(e)}, 500

async def handle_response(producer, response_data, status_code, partition_no):
    if status_code == 200:
        await produce_message(producer, GOOD_TOPIC, response_data, partition_no)
    elif status_code == 404:
        await produce_message(producer, BAD_TOPIC, response_data)
    else:
        print(f"Unhandled status code: {status_code}")

async def produce_message(producer: AIOKafkaProducer, topic, message: dict, partition_no=None):
    await producer.send_and_wait(topic, json.dumps(message).encode('utf-8'), partition=partition_no)

def video_history_crawling(partition_no: int=0):
    msg_cnt = asyncio.run(consume_and_process_video_history(partition_no))
    return msg_cnt

if __name__ == "__main__":
    video_history_crawling()