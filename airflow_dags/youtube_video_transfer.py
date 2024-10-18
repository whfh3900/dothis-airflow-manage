


from datetime import datetime, timedelta
from typing import Optional, Dict, Union, List
from airflow import DAG
from airflow.operators.python import PythonOperator
from util.kafka_util import SimpleKafkaConsumer, SimpleKafkaProducer
from util.db_util import get_ignite_client, get_mysql_connector

# task_1: video_data_from_ignite_to_kafka
def video_data_from_ignite_to_kafka(produce_topic: str):
    year_ago = datetime.today() - timedelta(days=365)
    client = get_ignite_client()
    cols = [
        "VIDEO_ID", "CHANNEL_ID", "VIDEO_TITLE", 
        "VIDEO_DESCRIPTION", "VIDEO_TAGS", "VIDEO_DURATION", 
        "VIDEO_PUBLISHED", "VIDEO_CATEGORY", "VIDEO_INFO_CARD", 
        "VIDEO_WITH_ADS", "VIDEO_END_SCREEN", "VIDEO_CLUSTER", 
        "CRAWLED_DATE", "YEAR", "MONTH", "DAY"
        ]
    cols = list(map(lambda e: e.lower(), cols))
    query_str = f'''
    SELECT VIDEO_ID, CHANNEL_ID, VIDEO_TITLE, VIDEO_DESCRIPTION, VIDEO_TAGS, VIDEO_DURATION, TO_CHAR(VIDEO_PUBLISHED, 'YYYY-MM-DD') as video_published, VIDEO_CATEGORY, VIDEO_INFO_CARD, VIDEO_WITH_ADS, VIDEO_END_SCREEN, VIDEO_CLUSTER, TO_CHAR(CRAWLED_DATE, 'YYYY-MM-DD HH:MM:SS') as crawled_date, YEAR, MONTH, DAY
    FROM DOTHIS.VIDEO_DATA
    WHERE VIDEO_PUBLISHED = CAST('{year_ago.strftime("%Y-%m-%d")}' AS DATE)
    '''
    video_data = []
    with client.sql(query_str) as cursor:
        for row in cursor:
            video_data.append(dict(zip(cols,row)))
    producer = SimpleKafkaProducer(topic=produce_topic)

    producer.produce(video_data)

# task_2: video_history_from_ignite_to_kafka
def video_history_from_ignite_to_kafka(consume_topic: str, produce_topic: str):
    
    cols = ["VIDEO_ID", "VIDEO_VIEWS", "VIDEO_LIKES", "VIDEO_COMMENTS", "VIDEO_PERFORMANCE", "YEAR", "MONTH", "DAY"]
    cols = list(map(lambda e: e.lower(), cols))
    consumer = SimpleKafkaConsumer(consume_topic, consume_size=100, group_id="video-history-select-consumer")
    producer = SimpleKafkaProducer(produce_topic)
    client = get_ignite_client()
    while True:
        video_data = consumer.consume()
        if video_data is None:
            consumer.close()
            break

        video_ids = list(map(lambda e: e.get("video_id"), video_data))
        video_history = []
        for video_id in video_ids:
            query_str = f'''
            SELECT *
            FROM DOTHIS.VIDEO_HISTORY
            WHERE VIDEO_ID = '{video_id}'
            '''
            try:
                with client.sql(query_str) as cursor:
                    for row in cursor:
                        video_history.append(dict(zip(cols,row)))
            except Exception as e:
                print(e)
                continue

        producer = SimpleKafkaProducer(topic=produce_topic)
        producer.produce(video_history)



# task_3: task_2 병행: video_data_from_kafka_to_mysql
def video_data_from_kafka_to_mysql(batch_size: int, consume_topic: str):
    conn = get_mysql_connector()
    cursor = conn.cursor()

    consumer = SimpleKafkaConsumer(consume_topic, consume_size=100, group_id="video-data-mysql-load-consumer")
    while True:
        video_data = consumer.consume()
        if video_data is None:
            consumer.close()
            break
    video_data = list(map(lambda e: list(e.values()), video_data))
    
    insert_query = """
        INSERT INTO video_data 
        (video_id, channel_id, video_title, video_description, video_tags, video_duration, video_published, video_category, video_info_card, video_with_ads, video_end_screen, video_cluster, crawled_date, year, month, day) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    # for data in video_data:
    for i in range(0, len(video_data), batch_size):
        batch = video_data[i:i+batch_size]
        cursor.executemany(insert_query, batch)
        conn.commit()

    # 연결 종료
    cursor.close()
    conn.close()


# task6 :
def video_history_from_kafka_to_mysql(batch_size: int, consume_topic: str):
    conn = get_mysql_connector()
    cursor = conn.cursor()

    consumer = SimpleKafkaConsumer(consume_topic, consume_size=100, group_id="video-history-mysql-load-consumer")
    while True:
        video_history = consumer.consume()
        if video_history is None:
            consumer.close()
            break
    video_history = list(map(lambda e: list(e.values()), video_history))
    
    insert_query = """
        INSERT INTO video_history 
        (video_id, video_views, video_likes, video_comments, video_performance, year, month, day) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
    # for data in video_data:
    for i in range(0, len(video_history), batch_size):
        batch = video_history[i:i+batch_size]
        cursor.executemany(insert_query, batch)
        conn.commit()

    # 연결 종료
    cursor.close()
    conn.close()


# task_4: task_2, task3 병행: video_data_delete_from_ignite

# def video_data_delete_from_ignite(consume_topic: str):
#     consumer = SimpleKafkaConsumer(consume_topic, consume_size=100, group_id="video-data-delete-consumer")
#     client = get_ignite_client()
#     while True:
#         video_data = consumer.consume()
#         if video_data is None:
#             consumer.close()
#             break

#         video_ids = list(map(lambda e: e.get("video_id"), video_data))
#         for video_id in video_ids:
#             video_data_query_str = f'''
#                 DELETE FROM DOTIHS.VIDEO_DATA WHERE video_id = '{video_id}'
#                 '''
#             # client.sql(video_data_query_str)

# # task 5

# def video_history_delete_from_ignite(consume_topic: str):

#     consumer = SimpleKafkaConsumer(consume_topic, consume_size=10000, group_id="video-history-delete-consumer")
#     client = get_ignite_client()
#     while True:
#         video_data = consumer.consume()
#         if video_data is None:
#             consumer.close()
#             break

#         video_ids = list(map(lambda e: e.get("video_id"), video_data))
#         for video_id in video_ids:
#             video_history_query_str = f'''
#                 DELETE FROM DOTIHS.VIDEO_HISTORY WHERE video_id = '{video_id}'
#                 '''
            # client.sql(video_data_query_str)


args = {
    "owner": "ybw",
    "depends_on_past": False,
    "email": ["lms46784678@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    }                                                                
                                                                     
dag = DAG(                                                           
    "video_data_transfer",                        
    default_args = args,
    description="video_data_transfer",
    schedule_interval='0 1 * * *',
    start_date=datetime(2024, 2, 26),
    catchup=False,
    tags=["video_data", "video_history", "kafka", "youtube"],
)


t_video_data_from_ignite_to_kafka = PythonOperator(
    task_id='video_data_from_ignite_to_kafka',
    python_callable=video_data_from_ignite_to_kafka,
    op_kwargs={'produce_topic': "data-transfer-video-data"},
    dag=dag
    )
t_video_history_from_ignite_to_kafka = PythonOperator(
    task_id='video_history_from_ignite_to_kafka',
    python_callable=video_history_from_ignite_to_kafka,
    op_kwargs={"consume_topic": "data-transfer-video-data", 'produce_topic': "data-transfer-video-history"},
    dag=dag
    )

t_video_data_load_to_mysql = PythonOperator(
    task_id='video_data_from_kafka_to_mysql',
    python_callable=video_data_from_kafka_to_mysql,
    op_kwargs={'batch_size': 100, "consume_topic": "data-transfer-video-data" },
    dag=dag
    )
t_video_history_load_to_mysql = PythonOperator(
    task_id='video_history_from_kafka_to_mysql',
    python_callable=video_history_from_kafka_to_mysql,
    op_kwargs={'batch_size': 100, "consume_topic": "data-transfer-video-history" },
    dag=dag
    )

# t_video_data_delete_from_ignite = PythonOperator(
#     task_id='video_data_delete_from_ignite',
#     python_callable=video_data_delete_from_ignite,
#     op_kwargs={'consume_topic': 100, "consume_topic": "data-transfer-video-data" },
#     dag=dag
#     )

# t_video_history_delete_from_ignite = PythonOperator(
#     task_id='video_history_delete_from_ignite',
#     python_callable=video_history_delete_from_ignite,
#     op_kwargs={'consume_topic': 100, "consume_topic": "data-transfer-video-history" },
#     dag=dag
#     )


# t_video_data_from_ignite_to_kafka >> [t_video_history_from_ignite_to_kafka, t_video_data_load_to_mysql, t_video_data_delete_from_ignite]
# t_video_history_from_ignite_to_kafka >> [t_video_history_load_to_mysql, t_video_history_delete_from_ignite]

t_video_data_from_ignite_to_kafka >> [t_video_history_from_ignite_to_kafka, t_video_data_load_to_mysql]
t_video_history_from_ignite_to_kafka >> [t_video_history_load_to_mysql]