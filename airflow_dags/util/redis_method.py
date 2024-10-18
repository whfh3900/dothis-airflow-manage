import redis
import os
from dotenv import load_dotenv
from redis.sentinel import Sentinel
import redis

class REDIS():
    def __init__(self, db=1):
        load_dotenv()
        self.db = db
        try:
            host = os.environ.get("REDIS_HOST")
            port = int(os.environ.get("REDIS_PORT"))
            pw = os.environ.get("REDIS_PW")

            # self.r = redis.Redis(host=host, port=port, password=pw, db=db, decode_responses=True)
            # Sentinel 서버 목록
            sentinel_hosts = [
                ('192.168.0.120', 3400),  # 첫 번째 Sentinel 포트
                ('192.168.0.121', 3400),  # 두 번째 Sentinel 포트
                ('192.168.0.122', 3400)   # 세 번째 Sentinel 포트
            ]
            # Sentinel 연결
            sentinel = Sentinel(sentinel_hosts, socket_timeout=5)
            # 마스터 Redis 서버에 연결
            self.cursor = sentinel.master_for('redismaster', socket_timeout=5, decode_responses=True, password=pw)

        except Exception as e:
            print(f"Raise error ==> {e}")
