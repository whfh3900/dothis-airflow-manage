import pandas as pd
import traceback
from datetime import datetime
from util.db_util import get_mysql_connector

def video_data_shorts_svc_load(date: str=datetime.today().strftime("%Y%m%d")):
    conn = get_mysql_connector(db="dothis_temp")
    cursor = conn.cursor()
    svc_db = "dothis_svc"
    date_str = datetime.today().strftime("%Y-%m-%d")
    print("video_data_shorts svc_load begin")
    try:
        table = f"video_data_shorts"
        query = f"INSERT IGNORE INTO {svc_db}.video_data_shorts select * from dothis_temp.video_data_shorts_{date}"
        cursor.execute(query)
        conn.commit()
        cursor.execute(f"""
                       select count(*) from dothis_temp.video_data_shorts_{date} union all
                       select count(*) from dothis_svc.video_data_shorts where crawled_date between '{date_str} 00:00:00' and '{date_str} 23:59:59'
                       """)
        row_counts = [row for row in cursor.fetchall()]
        temp_row_count = row_counts[0][0]
        svc_row_count = row_counts[1][0]
        print(f"insert into {svc_db}.{table} done")
    except Exception:
        print(str(traceback.format_exc()))
        print(f"insert into dothis_svc.video_data_shorts failed")
    finally:
        conn.close()
    print(f"video_data_shorts_{date} temp: {temp_row_count}")
    print(f"video_data_shorts_{date} svc: {svc_row_count}")
    print("video_data_shorts svc_load done")
