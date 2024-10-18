import os
import sys
sys.path.append("./airflow_dags")
sys.path.append('/data_store/airflow_dir/airflow_dags')
sys.path.append('./')
from util.db_util import get_mysql_connector
from dotenv import load_dotenv
# .env 파일 경로 지정
dotenv_path = os.path.join("/data_store/airflow_dir/airflow_dags", '.env')
# dotenv_path = os.path.join("./", '.env')
# .env 파일 로드
load_dotenv(dotenv_path)

def cold_table(table, db="new_dothis", host="RDS", days=30):
    _cold_table = table+"_cold"
    # MySQL 데이터베이스에 연결
    conn = get_mysql_connector(db=db, host=host)
    cursor = conn.cursor(buffered=True)
    try:
        # 테이블 존재 여부 확인 쿼리
        check_table_query = f"SHOW TABLES LIKE '{_cold_table}';"
        cursor.execute(check_table_query)
        result = cursor.fetchone()
        # 테이블이 없으면 생성
        if not result:
            create_table_query = f"CREATE TABLE {_cold_table} LIKE {table};"
            cursor.execute(create_table_query)
            conn.commit()   
            print(f"Create {_cold_table} table.")

        # cold 테이블에 days만큼 지난 데이터 삽입
        query = f'''
                INSERT INTO {_cold_table}
                SELECT *
                FROM {table}
                WHERE DATE(CONCAT_WS('-', `YEAR`, `MONTH`, `DAY`)) < CURDATE() - INTERVAL {days} DAY;
                '''
        cursor.execute(query)
        conn.commit()   
        print(f"Insert data into {_cold_table}")

        # 원본 테이블에 days만큼 지난 데이터 삭제
        query = f'''
                DELETE FROM {table}
                WHERE DATE(CONCAT_WS('-', `YEAR`, `MONTH`, `DAY`)) < CURDATE() - INTERVAL {days} DAY;
                '''
        cursor.execute(query)
        conn.commit()  
        print(f"Delete source {table} data")

    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        # 연결 닫기
        cursor.close()
        conn.close()

# 백업 로직
def backup_table(table, db="new_dothis", host="RDS"):
    _backup_table = table+"_backup"
    # MySQL 데이터베이스에 연결
    conn = get_mysql_connector(db=db, host=host)
    cursor = conn.cursor(buffered=True)
    try:
        # 테이블 존재 여부 확인 쿼리
        check_table_query = f"SHOW TABLES LIKE '{_backup_table}';"
        cursor.execute(check_table_query)
        result = cursor.fetchone()
        
        if result:
            # 테이블이 존재하면 삭제
            drop_table_query = f"DROP TABLE IF EXISTS {_backup_table};"
            cursor.execute(drop_table_query)
            print(f"Table {_backup_table} exists. Dropping the table.")
            create_table_query = f"CREATE TABLE {_backup_table} LIKE {table};"
            cursor.execute(create_table_query)
            conn.commit()
        
        # 테이블 복사 쿼리
        copy_table_query = f"INSERT INTO {_backup_table} SELECT * FROM {table};"
        cursor.execute(copy_table_query)
        conn.commit()
        print(f"Table {_backup_table} created by copying from {table}.")
    
    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        # 연결 닫기
        cursor.close()
        conn.close()