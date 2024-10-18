import os
from pyignite import Client
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import re
from datetime import datetime, timedelta

def get_ignite_client():
    client = Client(username='ignite', password='data123!') # 최대 권한 계정
    client.connect('dothis2.iptime.org', 10800) # ubuntu 내 컨테이너에 접근하는 connection string
    return client

def get_mysql_connector(db: str="dothis_svc", host: str=None) -> mysql.connector:
    load_dotenv()
    # MySQL 서버 연결 설정
    if host is None:
        mysql_db_config = {
            'host': os.getenv("MYSQL_HOST"),
            'user': os.getenv("MYSQL_USER"),
            'password': os.getenv("MYSQL_PW"),
            'port': os.getenv("MYSQL_PORT"),
            'database': db
        }

    elif host == "RDS":
        mysql_db_config = {
            'host': os.getenv("RDS_MYSQL_HOST"),
            'user': os.getenv("RDS_MYSQL_USER"),
            'password': os.getenv("RDS_MYSQL_PW"),
            'port': os.getenv("RDS_MYSQL_PORT"),
            'database': db
        }
    try:
        connector = mysql.connector.connect(**mysql_db_config) 
        return connector
    except Error as e:
        print(e)
        return   
    

def db_table_remove(now_date, db="dothis_pre", remaining_dates=365):
    conn = get_mysql_connector(db=db)
    
    try:
        with conn.cursor() as cursor:
            # 1. 테이블 이름 목록 가져오기
            cursor.execute("SHOW TABLES LIKE 'video_data_________';")
            tables = [row[0] for row in cursor.fetchall()]
            # 2. 날짜 추출 및 정렬
            table_dates = []
            for table in tables:
                match = re.search(r'_(\d{8})$', table)
                if match:
                    date_str = match.group(1)
                    date_obj = datetime.strptime(date_str, "%Y%m%d")
                    table_dates.append((table, date_obj))
            
            # 날짜를 기준으로 정렬
            table_dates.sort(key=lambda x: x[1], reverse=True)
            
            # 3. 오늘 날짜로부터 remaining_dates일 이전 날짜 계산
            now_date = datetime.strptime(now_date, "%Y-%m-%d")
            cutoff_date = now_date - timedelta(days=remaining_dates)
            
            # 4. 삭제할 테이블 결정
            tables_to_delete = []
            for table_name, table_date in table_dates:
                if table_date < cutoff_date:
                    tables_to_delete.append(table_name)

            # 5. 테이블 삭제
            for table in tables_to_delete:
                delete_query = f"DROP TABLE IF EXISTS {table};"
                cursor.execute(delete_query)
                print(f"Deleted table: {table}")
            conn.commit()

    finally:
        conn.close()
        
if __name__ == "__main__":
    now_date = datetime.today().strftime("%Y-%m-%d")
    db_table_remove(now_date, db="dothis_pre", remaining_dates=365)

