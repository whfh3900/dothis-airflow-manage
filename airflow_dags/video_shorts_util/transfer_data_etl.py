import sys
import traceback
from datetime import datetime
from dotenv import load_dotenv
from util.db_util import get_mysql_connector
sys.path.append('./')

load_dotenv()
def video_history_shorts_transfer(date: str=datetime.today().strftime("%Y%m%d")):
    conn = get_mysql_connector(db="dothis_raw")
    cursor = conn.cursor()
    print("video_history_shorts_transfer begin")
    try:
        cursor.execute(f"""insert ignore into dothis_ld.video_history_shorts_{date} select * from dothis_raw.video_history_shorts_{date};""")
        conn.commit()
        print(f"insert ignore into dothis_ld.video_history_shorts_{date} select * from dothis_raw.video_history_shorts_{date} done")
    
        cursor.execute(f"""insert ignore into dothis_temp.video_history_shorts_{date} select * from dothis_ld.video_history_shorts_{date}""")
        conn.commit()
        print(f"insert into dothis_temp.video_history_shorts_{date} select * from dothis_ld.video_history_shorts_{date} done")
    
    except Exception:
        print(str(traceback.format_exc()))
        print(f"insert into dothis_temp.video_history_shorts_{date} select * from dothis_ld.video_history_shorts_{date} failed")
    
    cursor.execute(f"""
                   select count(*) from dothis_raw.video_history_shorts_{date} union all
                   select count(*) from dothis_ld.video_history_shorts_{date} union all
                   select count(*) from dothis_temp.video_history_shorts_{date};""")
    
    row_counts = [row for row in cursor.fetchall()]
    print(f"video_history_shorts_{date} raw: {row_counts[0][0]}")
    print(f"video_history_shorts_{date} ld: {row_counts[1][0]}")
    print(f"video_history_shorts_{date} temp: {row_counts[2][0]}")
    
    print("video_history_shorts_transfer done")
    conn.close()


def video_data_shorts_transfer(date: str=datetime.today().strftime("%Y%m%d")):
    conn = get_mysql_connector(db="dothis_raw")
    cursor = conn.cursor()
    print("video_data_shorts transfer begin")
    try:
        cursor.execute(f"""insert into dothis_ld.video_data_shorts_{date} select * from dothis_raw.video_data_shorts_{date};""")
        conn.commit()
        print(f"insert into dothis_ld.video_data_shorts_{date} select * from dothis_raw.video_data_shorts_{date} done")

        cursor.execute(f"""insert ignore into dothis_temp.video_data_shorts_{date} select * from dothis_ld.video_data_shorts_{date}""")
        conn.commit()
        print(f"insert into dothis_temp.video_data_shorts_{date} select * from dothis_ld.video_data_shorts_{date} done")
        
        cursor.execute(f"""
                    select count(*) from dothis_raw.video_data_shorts_{date} union all
                    select count(*) from dothis_ld.video_data_shorts_{date} union all
                    select count(*) from dothis_temp.video_data_shorts_{date};
                    """)
        row_counts = [row for row in cursor.fetchall()]
        print(f"video_data_shorts_{date} raw: {row_counts[0][0]}")
        print(f"video_data_shorts_{date} ld: {row_counts[1][0]}")
        print(f"video_data_shorts_{date} temp: {row_counts[2][0]}")
        print("video_data_shorts transfer done")
        conn.close()
    
    except Exception:
        print(traceback.format_exc())
    