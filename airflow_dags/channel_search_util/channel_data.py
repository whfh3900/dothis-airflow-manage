import os
import asyncio
import aiohttp
import functools
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from util.db_util import get_mysql_connector
from util.kafka_util import kafka_topic_end_producer, SimpleKafkaProducer, SimpleKafkaConsumer, KafkaEndCheck
from channel_search_util.channel_data_ddl import create_channel_data_raw_table, create_channel_data_ld_table, create_channel_data_temp_table

def rate_limited(max_concurrent, duration):
    def decorator(func):
        semaphore = asyncio.Semaphore(max_concurrent)

        async def dequeue():
            try:
                await asyncio.sleep(duration)
            finally:
                semaphore.release()

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            await semaphore.acquire()
            asyncio.create_task(dequeue())
            return await func(*args, **kwargs)

        return wrapper
    return decorator


def extract_rds_keywords():
    try:
        producer = SimpleKafkaProducer(topic="youtube-search-keyword", acks=-1)
        mysql_conn = get_mysql_connector(db="new_dothis", host="RDS")
        cursor = mysql_conn.cursor(dictionary=True)
        cursor.execute("SELECT id, keyword FROM related_words")
        result = cursor.fetchall()
        keywords_list = []
        for row in result:
            keywords_list.append(
                {
                    'id': row['id'],
                    'keyword': row['keyword']
                }
            )
        cursor.close()
        mysql_conn.close()
        producer.produce(keywords_list)
    except Exception as e:
        print(e)
        return
    
def search_keyword():
    load_dotenv()
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    lambda_base_url = os.getenv("FASTAPI_LAMBDA_URL")
    path = "channel_search"
    url = f"{lambda_base_url}/{path}"
    consumer = SimpleKafkaConsumer(topic="youtube-search-keyword", consume_size=50, group_id="youtube-search-etl-group")
    producer = SimpleKafkaProducer(topic="youtube-search-result", acks=-1)
    while True:
        keywords = consumer.consume()
        if keywords is None:
            break
        try:
            datas = []
            for keyword in keywords:
                datas.append({"search_query": keyword.get("keyword"), "scroll_count": 10})
            result = asyncio.run(gather_fetch(url, headers, datas))
            producer.produce(result)
        except Exception as e:
            print(e)
            continue
        
def mysql_create_channel_list_tbl(date=datetime.today().strftime("%Y%m")):
    dothis_mysql_conn = get_mysql_connector(db="dothis_ld")
    cursor = dothis_mysql_conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS dothis_raw.channel_list_{date} (
            `channel_id` VARCHAR(255) UNIQUE,
            `channel_url` VARCHAR(255) DEFAULT "", 
            `channel_name` text,
            `crawled_date` timestamp NULL DEFAULT NULL,
            `is_new` tinyint(1) DEFAULT 0
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED;
        """)
    dothis_mysql_conn.commit()
    cursor.execute(f"""
        INSERT INTO dothis_raw.channel_list_{date} (`channel_id`, `channel_name`, `crawled_date`)
        SELECT `channel_id`, `channel_name`, `crawled_date`
        FROM dothis_svc.channel_data;
        """)
    dothis_mysql_conn.commit()
    cursor.close()
    dothis_mysql_conn.close()

def process_search_result(date=datetime.today().strftime("%Y%m")):
    dothis_mysql_conn = get_mysql_connector(db="dothis_raw")
    cursor = dothis_mysql_conn.cursor()
    consumer = SimpleKafkaConsumer(topic="youtube-search-result", consume_size=200, group_id="youtube-search-etl-group")
    query = f"INSERT IGNORE INTO dothis_raw.channel_list_{date} VALUES (%s, %s, %s, %s, %s)"
    while True:
        search_result_list = consumer.consume()
        if search_result_list is None:
            break
        try:
            for search_result in search_result_list:
                if search_result.get("data") != []:
                    df = pd.DataFrame(search_result.get("data"), columns=["channel_id", "channel_url", "title"])
                    df = df.drop_duplicates(subset=["channel_id"])
                    df = df.rename(columns={"title": "channel_name"})
                    df["crawled_date"] = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
                    df["is_new"] = 1
                    rows = [tuple(row) for row in df.to_numpy()]
                    cursor.executemany(query, rows)
                    dothis_mysql_conn.commit()
        except Exception as e:
            print(e)
            continue
    cursor.close()
    dothis_mysql_conn.close()
    
def get_new_channel_list(date=datetime.today().strftime("%Y%m")):
    producer = SimpleKafkaProducer(topic="youtube-search-new-channel-id", acks=-1)
    dothis_mysql_conn = get_mysql_connector(db="dothis_raw")
    cursor = dothis_mysql_conn.cursor(dictionary=True)
    cursor.execute(f"SELECT channel_id, channel_name, channel_url FROM dothis_raw.channel_list_{date} WHERE is_new = 1")
    batch_size = 5000
    while True:
        rows = cursor.fetchmany(size=batch_size)
        if not rows:
            break
        producer.produce(rows)
    cursor.close()
    dothis_mysql_conn.close()


def create_channel_data_tbl(date=datetime.today().strftime("%Y%m")):
    dothis_mysql_conn = get_mysql_connector(db="dothis_raw")
    cursor = dothis_mysql_conn.cursor()
    cursor.execute(create_channel_data_raw_table(date))
    cursor.execute(create_channel_data_ld_table(date))
    cursor.execute(create_channel_data_temp_table(date))
    dothis_mysql_conn.commit()
    cursor.close()
    dothis_mysql_conn.close()
    
def get_channel_data(date=datetime.today().strftime("%Y%m")):
    load_dotenv()
    dothis_mysql_conn = get_mysql_connector(db="dothis_raw")
    cursor = dothis_mysql_conn.cursor()
    consumer = SimpleKafkaConsumer(topic="youtube-search-new-channel-id", consume_size=50, group_id="youtube-search-etl-group")
    # re_producer = SimpleKafkaProducer(topic="youtube-search-new-channel-id", acks=-1)
    query = f"INSERT IGNORE INTO dothis_raw.channel_data_{date} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    lambda_base_url = os.getenv("FASTAPI_LAMBDA_URL")
    path = "new_channel_info"
    url = f"{lambda_base_url}/{path}"
    while True:
        channel_list = consumer.consume()
        if channel_list is None:
            break
        try:
            datas = []
            for channel in channel_list:
                if "https://www.youtube.com/channel/" in channel.get("channel_url") or "https://www.youtube.com/c/" in channel.get("channel_url"):
                    datas.append({"url": channel.get("channel_name"), "user_id": "", "need_detail": True})
                else:
                    datas.append({"url": channel.get("channel_url"), "user_id": "", "need_detail": True})
            channel_info_result_list = asyncio.run(gather_fetch(url, headers, datas))
            good_result = []
            for channel_info_result in channel_info_result_list:
                if channel_info_result.get("code") == 200 and channel_info_result.get("is_valid_channel") == True:
                    channel_data_dict = channel_info_result.get("data")
                    channel_data_dict["channel_id_part"] = channel_data_dict.get("channel_id")[3]
                    good_result.append(channel_data_dict)
            for dict in good_result:
                dict.pop('channel_url', None)
            good_result = list(map(lambda x: tuple(x.values()), good_result))
            cursor.executemany(query, good_result)
            dothis_mysql_conn.commit()
        except Exception as e:
            print(e)
            continue
    cursor.close()
    dothis_mysql_conn.close()

def transfer_raw_to_ld(date=datetime.today().strftime("%Y%m")):
    dothis_mysql_conn = get_mysql_connector(db="dothis_raw")
    cursor = dothis_mysql_conn.cursor()
    cursor.execute(f"""
        INSERT IGNORE INTO dothis_ld.channel_data_{date}
        SELECT * FROM dothis_raw.channel_data_{date}
        """)
    dothis_mysql_conn.commit()
    cursor.close()
    dothis_mysql_conn.close()

def transfer_ld_to_temp(date=datetime.today().strftime("%Y%m")):
    dothis_mysql_conn = get_mysql_connector(db="dothis_raw")
    cursor = dothis_mysql_conn.cursor()
    cursor.execute(f"""
        INSERT IGNORE INTO dothis_temp.channel_data_{date}
        SELECT * FROM dothis_ld.channel_data_{date}
        """)
    dothis_mysql_conn.commit()
    cursor.close()
    dothis_mysql_conn.close()
    
def svc_load(date=datetime.today().strftime("%Y%m")):
    dothis_mysql_conn = get_mysql_connector(db="dothis_svc")
    cursor = dothis_mysql_conn.cursor()
    cursor.execute(f"""
        INSERT IGNORE INTO dothis_svc.channel_data
        SELECT * FROM dothis_temp.channel_data_{date}
        """)
    dothis_mysql_conn.commit()
    cursor.close()
    dothis_mysql_conn.close()

def get_channel_list(date=datetime.today().strftime("%Y%m")):
    producer = SimpleKafkaProducer(topic="youtube-search-processed-channel-id", acks=-1)
    dothis_mysql_conn = get_mysql_connector(db="dothis_temp")
    cursor = dothis_mysql_conn.cursor(dictionary=True)
    cursor.execute(f"SELECT channel_id FROM dothis_temp.channel_data_{date}")
    producer.produce(cursor.fetchall())
    cursor.close()
    dothis_mysql_conn.close()

def get_new_video_ids(date=datetime.today().strftime("%Y%m")):
    load_dotenv()
    consumer = SimpleKafkaConsumer(topic="youtube-search-processed-channel-id", consume_size=50, group_id="youtube-search-etl-group")
    producer = SimpleKafkaProducer(topic="youtube-search-video-update-video-list", acks=-1, num_partitions=40)
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    lambda_base_url = os.getenv("FASTAPI_LAMBDA_URL")
    path = "new_channel_video_list"
    url = f"{lambda_base_url}/{path}"
    while True:
        channel_list = consumer.consume()
        if channel_list is None:
            break
        datas = [{"channel_id": channel.get("channel_id"), "extract_period": "WHOLE"} for channel in channel_list]
        result_list = asyncio.run(gather_fetch(url, headers, datas))
        kafka_data = []
        for result in result_list:
            try:
                if result.get("code") == 200:
                    kafka_data.append({
                        "crawled_date": result.get("crawled_date"),
                        "channel_id": result.get("data").get("channel_id"),
                        "video_list": result.get("data").get("video_list")  
                    })
            except Exception as e:
                print(e)
                continue
        producer.produce(kafka_data)

def get_new_video_data():
    load_dotenv()
    consumer = SimpleKafkaConsumer(topic="youtube-search-video-update-video-list", consume_size=10, group_id="youtube-search-etl-group", num_partitions=40)
    producer = SimpleKafkaProducer(topic="youtube-search-new-video-data", acks=-1, num_partitions=40)
    failed_topic_producer = SimpleKafkaProducer(topic="youtube-search-failed-videos", acks=-1, num_partitions=40)
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    lambda_base_url = os.getenv("FASTAPI_LAMBDA_URL")
    path = "video_info"
    base_url = f"{lambda_base_url}/{path}/"
    loop = asyncio.get_event_loop()
    while True:
        video_list = consumer.consume()
        if video_list is None:
            break
        video_id_list = [video_id for video in video_list for video_id in video.get("video_list", [])]
        urls = [f"{base_url}{video_id}" for video_id in video_id_list]
        result_list = loop.run_until_complete(gather_fetch_get(urls, headers))
        kafka_data = []
        for idx, result in enumerate(result_list):
            try:
                if isinstance(result, dict) and result.get("code") == 200:
                    kafka_data.append(result.get("data")[0])
                else:
                    failed_topic_producer.produce([{"video_id": video_id_list[idx]}])
            except Exception as e:
                print(e)
                continue
        producer.produce(kafka_data)
    loop.close()

async def fetch_request(url, session: aiohttp.ClientSession, headers, data):
    async with session.post(url, headers=headers, json=data) as response:
        try:
            return await response.json()
        except:
            return {}

@rate_limited(200, 1)
async def fetch_request_get(url, session: aiohttp.ClientSession, headers):
    async with session.get(url, headers=headers) as response:
        try:
            return await response.json()
        except:
            return {}

async def gather_fetch_get(urls: list, headers: dict):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(fetch_request_get(url, session, headers))
        return await asyncio.gather(*tasks)

async def gather_fetch(url: str, headers: dict, datas: list):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for data in datas:
            tasks.append(fetch_request(url, session, headers, data))
        return await asyncio.gather(*tasks)
