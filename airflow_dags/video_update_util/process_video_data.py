import sys
import os
import json
import asyncio
import aiohttp
import traceback
from dotenv import load_dotenv
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

sys.path.append('./')
load_dotenv()

CONSUME_TOPIC = "video-update-video-list"
GOOD_TOPIC = "video-update-new-video-data"
GROUP_ID = "new-video-data-crawling-consumer"
NUM_PARTITION = 3
BASE_URL = os.getenv("FASTAPI_LAMBDA_URL")
API_ENDPOINT = "video_info"
URL = f"{BASE_URL}/{API_ENDPOINT}"
MAX_RETRIES = 2
BROKER_LIST = ["dothis2.iptime.org:19092", "dothis2.iptime.org:29092", "dothis2.iptime.org:39092"]
HEADERS = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
}
BATCH_SIZE = 100

async def consume_and_process_new_video_data(partition_no: int=0):
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
    async def process_msg_batch(msg_list: dict, partition_no):
        record_list = []
        for _, records in msg_list.items():
            if records:
                for record in records:
                    record_list.append(record)
        task_list = []
        for msg in record_list:
            if msg.value.get('video_list') != []:
                for video_id in msg.value.get('video_list'):
                    task_list.append(fetch_data_from_api(url=f"{URL}/{video_id}", session=session))
        print(f"Processing batch of size: {len(record_list)}")
        if task_list != []: 
            fetch_list = await asyncio.gather(*task_list)
            for data, status_code in fetch_list:
                await handle_response(producer, data, status_code, partition_no)
        await consumer.commit()
        return len(record_list)
    try:
        msg_cnt = 0
        while True:
            msg_list: dict = await consumer.getmany(topic_partition, timeout_ms=60000, max_records=BATCH_SIZE)
            msg_cnt += await process_msg_batch(msg_list, partition_no)
            if consumer.highwater(topic_partition) == await consumer.position(topic_partition) and msg_list == {}:
                print(f"Partition {partition_no} reached EOF")
                break
    except:
        print(traceback.format_exc())
    finally:
        await consumer.stop()
        await producer.stop()
        await session.close()
        return msg_cnt

async def fetch_data_from_api(url, session, retries=0):
    try:
        async with session.get(url, headers=HEADERS) as response:
            if (response.status == 500 or response.status == 502) and retries < MAX_RETRIES:
                print(f"url: {url}, status_code: {response.status} error encountered. Retrying... ({retries+1}/{MAX_RETRIES})")
                return await fetch_data_from_api(url, session, retries+1)
            response_data = await response.json()
            if response_data.get("code") == 200:
                res_data = response_data.get("data")[0]
                return res_data, response_data.get("code", response.status)
            else:
                raise aiohttp.ClientError(f"API request failed: {response_data}")
    except aiohttp.ClientError as e:
        print(f"HTTP request failed: {e}")
        return {"error": str(e)}, 500

async def handle_response(producer, response_data, status_code, partition_no):
    if status_code == 200:
        await produce_message(producer, GOOD_TOPIC, response_data, partition_no)
    elif status_code == 500:
        pass
    else:
        print(f"Unhandled status code: {status_code}")

async def produce_message(producer: AIOKafkaProducer, topic, message: dict, partition_no=None):
    await producer.send_and_wait(topic, json.dumps(message).encode('utf-8'), partition=partition_no)

def new_video_data_crawling(partition_no: int=0):
    msg_cnt = asyncio.run(consume_and_process_new_video_data(partition_no))
    return msg_cnt