import pandas as pd
import sys
sys.path.append("./airflow_dags")
sys.path.append('/data_store/airflow_dir/airflow_dags')
sys.path.append('./')

from util.redis_method import REDIS
from tqdm import tqdm
from datetime import datetime, timedelta
import redis
tqdm.pandas()

def remove_old_data(now_date, days=365):

    r = REDIS(db=0)
    now_date_str = now_date
    now_date = datetime.strptime(now_date_str, "%Y-%m-%d")
    ago_day = now_date - timedelta(days=days)
    
    all_keys = [key.decode('utf-8') if isinstance(key, bytes) else key for key in r.cursor.keys('*')]
    all_keys = sorted(all_keys, reverse=False)
    print("*"*50, f" Start old redis data... ", "*"*50)
    for key in tqdm(all_keys):  
        try:
            result = list(r.cursor.smembers(key))
        except redis.exceptions.ResponseError:
            continue
        df = pd.DataFrame([data.split(":") for data in result], columns=["video_published", "channel_id", "video_id", "video_cluster"])
        df["result"] = result
        df["video_published"] = pd.to_datetime(df["video_published"], format="%Y%m%d")
        df.sort_values(by="video_published", inplace=True)

        values_to_remove = df[df.video_published < ago_day].result.tolist()
        if len(values_to_remove) > 0:
            r.cursor.srem(key, *values_to_remove)

    print("*"*50, f" End ", "*"*50)
    
if __name__ == "__main__":
    remove_old_data(now_date="2024-08-27")
    
