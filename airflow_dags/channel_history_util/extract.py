import json
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from aiokafka import AIOKafkaProducer
from util.db_util import get_mysql_connector


PRODUCE_TOPIC = "channel-history-avg-view"
BROKER_LIST = ["dothis2.iptime.org:19092", "dothis2.iptime.org:29092", "dothis2.iptime.org:39092"]
NUM_PARTITION = 3
BATCH_SIZE = 20000

async def get_video_history_table_list(date) -> list:
    video_history_tables = [{"tbl": f"video_history_{str(i).zfill(2)}_{date[:-2]}", "video_cluster": i} for i in range(101)]
    video_history_tables.insert(0, {"tbl": f"video_history_NULL_{date[:-2]}", "video_cluster": "null"})
    return video_history_tables

async def get_video_data_table_list() -> list:
    video_data_tables = [f"video_data_{str(i).zfill(2)}" for i in range(101)]
    video_data_tables.insert(0, "video_data_NULL")
    return video_data_tables

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

async def extract(today: str=datetime.today().strftime("%Y-%m-%d")):
    try:
        conn = get_mysql_connector()
        cursor = conn.cursor()
        year, month, day= today.split("-")
        date = today.replace("-", "")
        yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
        video_history_tbls = await get_video_history_table_list(date)
        video_data_tbls = await get_video_data_table_list()
        data = []
        row_cnt = 0
        for tbl in video_history_tbls:
            cursor.execute(f"""SELECT video_id, video_views FROM dothis_svc.{tbl.get("tbl")}
                                WHERE `year` = {year} AND `month` = {month} AND `day` = {day}""")
            while True:
                rows = cursor.fetchmany(BATCH_SIZE)
                row_cnt += len(rows)
                if rows == []:
                    break
                data.extend(rows)
        df_video_history= pd.DataFrame(data, columns=["video_id", "video_views"])
        print(f"video_history select done. {row_cnt=}")

        data = []
        row_cnt = 0
        for tbl in video_data_tbls:
            cursor.execute(f"""SELECT video_id, channel_id FROM dothis_svc.{tbl}""")
            while True:
                rows = cursor.fetchmany(BATCH_SIZE)
                row_cnt += len(rows)
                if rows == []:
                    break
                data.extend(rows)
        df_video_data = pd.DataFrame(data, columns=["video_id", "channel_id"])
        print(f"video_data select done. {row_cnt=}")
        merged_df = pd.merge(df_video_history, df_video_data, on="video_id")
        merged_df = merged_df.groupby('channel_id'). \
            mean("video_views"). \
            reset_index(). \
                rename(columns={"video_views": "channel_average_views"}). \
                    round(3)
        cursor.execute(f"""SELECT channel_id FROM dothis_svc.channel_data""")
        df_channel_data = pd.DataFrame(cursor.fetchall(), columns=["channel_id"])
        print(f"video_data select done. row_cnt={len(df_channel_data)}")
        merged_df = pd.merge(df_channel_data, merged_df, on="channel_id", how="left").fillna(0)
        merged_df = merged_df.drop_duplicates(subset=["channel_id"])
        merged_df["count_date"] = today
        # merged_df 에 channel_average_views 라는 컬럼이 없으면 추가 값은 0 으로 채움
        cursor.execute(f"""SELECT channel_id FROM dothis_raw.channel_history_{yesterday}""")
        channel_data = cursor.fetchall()
        channel_data = [channel[0] for channel in channel_data]
        for channel in channel_data:
            if channel not in merged_df["channel_id"].values:
                merged_df = pd.concat([merged_df, pd.DataFrame([{"channel_id": channel, "channel_average_views": 0, "count_date": today}])], ignore_index=True)
        conn.close()
        print("calc channel avg process done.")
        
        producer = AIOKafkaProducer(bootstrap_servers=','.join(BROKER_LIST), request_timeout_ms=80000) # 10MB
        await producer.start()
        await produce_message(producer, merged_df.to_dict(orient="records"), BATCH_SIZE)
        print("data produce done.")
    except:
        print("channel_avg_views extract failed.")
    
    finally:
        for i in range(NUM_PARTITION):
            await produce_final_message(producer=producer, partition_no=i)
        await producer.stop()
        
        

def extract_channel_list():
    asyncio.run(extract())