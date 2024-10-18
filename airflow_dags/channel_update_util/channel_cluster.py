import os
import sys
sys.path.append('./')
sys.path.append("./airflow_dags")
sys.path.append("/data_store/airflow_dir/airflow_dags")

# .env 파일 경로 지정
from dotenv import load_dotenv
dotenv_path = os.path.join("/data_store/airflow_dir/airflow_dags", '.env')
if not os.path.exists(dotenv_path):
    dotenv_path = os.path.join("./", '.env')
load_dotenv(dotenv_path)

import pandas as pd
from tqdm import tqdm
from datetime import datetime
from util.db_util import get_mysql_connector

def calculate_channel_clusters():
    db_name = "dothis_svc"
    conn = get_mysql_connector(db=db_name)
    cursor = conn.cursor(buffered=True)
    query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{db_name}'  
        AND table_name REGEXP 'video_data_[0-9]+$';
        """
    cursor.execute(query)
    data_table_names = [row[0] for row in cursor.fetchall() if row[0] != "video_data_9999"]

    ### 앞에서 2개의 테이블 먼저 
    table = data_table_names[0]
    query = f"SELECT channel_id, COUNT(*) AS count FROM {table} GROUP BY channel_id;"
    cursor.execute(query)
    a = pd.DataFrame(cursor.fetchall(), columns=["channel_id", "count_%s"%(table.split("_")[-1])])

    table = data_table_names[1]
    query = f"SELECT channel_id, COUNT(*) AS count FROM {table} GROUP BY channel_id;"
    cursor.execute(query)
    b = pd.DataFrame(cursor.fetchall(), columns=["channel_id", "count_%s"%(table.split("_")[-1])])

    df = pd.merge(a, b, on="channel_id", how="outer")
    ###
    print("#"*50, " Calculate channel cluster ", "#"*50)

    for table in tqdm(data_table_names[2:]):
        query = f"SELECT channel_id, COUNT(*) AS count FROM {table} GROUP BY channel_id;"
        cursor.execute(query)
        etc = pd.DataFrame(cursor.fetchall(), columns=["channel_id", "count_%s"%(table.split("_")[-1])])
        df = pd.merge(df, etc, on="channel_id", how="outer")
        
    print("#"*50, " Update channel cluster map ", "#"*50)
    df_T = df.set_index(["channel_id"]).T
    for channel_id in tqdm(df_T.columns):
        clusters = [cluster.split("_")[1] for cluster in df_T[channel_id].dropna().sort_values(ascending=False).head(10).index]
        query = f"delete from channel_cluster_map where CHANNEL_ID = '{channel_id}'"
        cursor.execute(query)
        conn.commit()
        for i, cluster in enumerate(clusters):
            query = """
                insert ignore into channel_cluster_map (CHANNEL_ID, CHANNEL_CLUSTER, PRIME_SEQ, CRAWLED_DATE) values ('%s', '%s', %s, '%s')
                """%(channel_id, cluster, i+1, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            cursor.execute(query)
            conn.commit()
            
def channel_data_cluster_update():
    db_name = "dothis_svc"
    conn = get_mysql_connector(db=db_name)
    cursor = conn.cursor(buffered=True)
    print("#"*50, " Update channel data ", "#"*50)
    query = f"select CHANNEL_ID, CHANNEL_CLUSTER from channel_cluster_map where PRIME_SEQ = 1"
    cursor.execute(query)
    data = [(row[0], row[1]) for row in cursor.fetchall()]
    for channel_id, cluster in tqdm(data):
        query = "UPDATE channel_data SET CHANNEL_CLUSTER = %s, CRAWLED_DATE = '%s' WHERE CHANNEL_ID = '%s';"%(cluster, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), channel_id)
        cursor.execute(query)
        conn.commit()
        
if __name__ == "__main__":
    calculate_channel_clusters()
    channel_data_cluster_update()