
import traceback
from datetime import datetime

from util.db_util import get_mysql_connector


def video_history_shorts_svc_load(date: str=datetime.today().strftime("%Y%m%d")):
    conn = get_mysql_connector(db="dothis_temp")
    cursor = conn.cursor()
    year_month = date[:6]
    print("video_history_shorts_svc_load begin")
    qry = f"""insert ignore into dothis_svc.video_history_shorts_{year_month} 
            select * from dothis_temp.video_history_shorts_{date}"""
    count_qry = f"""select count(*) from dothis_temp.video_history_shorts_{date}"""
    try:
        cursor.execute(qry)
        conn.commit()
        cursor.execute(count_qry)
        row_cnt = cursor.fetchone()[0]
    except Exception:
        print(traceback.format_exc())
        print(f"{qry} failed")
    finally:
        conn.close()
    print(f"video_history_shorts_{date} row count: {row_cnt}")
    print("video_history_shorts svc_load done")