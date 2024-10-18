import json
import asyncio
import aiohttp
from dotenv import load_dotenv
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import sys
from datetime import datetime

sys.path.append('./')
load_dotenv()
CONSUME_TOPIC = "video-shorts-url"
GOOD_TOPIC = "video-history-shorts-crawled"
BAD_TOPIC = "video-history-shorts-bad-url"
GROUP_ID = "video-history-shorts-consumer"
NUM_PARTITION = 5
BASE_URL = "https://vc2nqcxgphxrixcizq2jfqdsei0mytqt.lambda-url.ap-northeast-2.on.aws"
API_ENDPOINT = "video_history"
URL = f"{BASE_URL}/{API_ENDPOINT}"
BROKER_LIST = ["dothis2.iptime.org:19092", "dothis2.iptime.org:29092", "dothis2.iptime.org:39092"]
BATCH_SIZE = 200
MAX_RETRIES = 1
HEADERS = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
}
TIMEOUT = 60


async def consume_and_process_video_history():
    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=','.join(BROKER_LIST),
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        max_poll_interval_ms=100000
    )
    
    producer = AIOKafkaProducer(
        bootstrap_servers=','.join(BROKER_LIST)
    )
    await consumer.start()
    await producer.start()
    session = aiohttp.ClientSession()
    partition_eof = {partition: False for partition in range(NUM_PARTITION)}
    async def process_msg_batch(msg_list):
        record_list = []
        for _, records in msg_list.items():
            if records:
                for record in records:
                    record_list.append(record)
        print(f"Processing batch of size: {len(record_list)}")
        body_list = []
        for msg in record_list:
            if msg.value.get("msg"):
                partition_eof[msg.partition] = True
            else:
                value = msg.value
                is_keep_crawling = value.get("is_keep_crawling", 1)
                is_bad_video = value.get("is_bad_video", 0)
                if is_keep_crawling == 1 and is_bad_video == 0:
                    body_list.append({"video_id": value["video_id"], "channel_id": "", "video_cluster ": -1})
        task_list = [fetch_data_from_api(body, session) for body in body_list]
        fetch_list = await asyncio.gather(*task_list)
        for data, status_code in fetch_list:
            await handle_response(producer, data.get("data", {}), status_code)
        await consumer.commit()
    try:
        while True:
            msg_list: dict = await consumer.getmany(timeout_ms=10000, max_records=100)
            await process_msg_batch(msg_list)
            if all(partition_eof.values()) or msg_list == {}:
                break
        await asyncio.gather(*[produce_final_message(producer, i)for i in range(NUM_PARTITION)])
    finally:
        await consumer.stop()
        await producer.stop()
        await session.close()

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

async def handle_response(producer, response_data, status_code):
    if status_code == 200:
        await produce_message(producer, GOOD_TOPIC, response_data)
    elif status_code == 404:
        await produce_message(producer, BAD_TOPIC, response_data)
    else:
        print(f"Unhandled status code: {status_code}")

async def produce_message(producer: AIOKafkaProducer, topic, message: dict):
    await producer.send_and_wait(topic, json.dumps(message).encode('utf-8'))
    
async def produce_final_message(producer: AIOKafkaProducer, partiton_no: int):
    final_message = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "msg": "done",
            "partition_no": int(partiton_no),
            "content": "this topic is end"
        }
    await producer.send_and_wait(GOOD_TOPIC, json.dumps(final_message).encode('utf-8'), partition=partiton_no)   

def video_shorts_history_process():
    asyncio.run(consume_and_process_video_history())
    return "done"