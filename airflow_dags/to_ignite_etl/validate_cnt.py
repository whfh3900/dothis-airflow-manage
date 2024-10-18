# -*- coding: utf-8 -*-

import pymysql
from pyignite import Client
from datetime import datetime
import pandas as pd

mysql_config = {
    'host': '192.168.0.132',
    'user': 'etluser',
    'port': 2300,
    'password': 'data123!',
    'database': 'dothis_svc'
}

mysql_config_master = {
    'host': '192.168.0.131',
    'user': 'etluser',
    'port': 2300,
    'password': 'data123!',
    'database': 'dothis_svc'
}



ignite_config = {
    'host': '192.168.0.31',
    'user': 'ignite',
    'password': 'data123!',
    'port': 10800
}

# MySQL에서 테이블 목록 조회
def get_mysql_tables():
    try:
        connection = pymysql.connect(**mysql_config_master)
        cursor = connection.cursor()
        cursor.execute("truncate table dothis_mng.validate_etl_cnt")
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema ='dothis_exp' order by 1")
        tables = cursor.fetchall()
        cursor.close()
        connection.close()
        return [table[0] for table in tables]
    except Exception as e:
        print("Err on MySQL: ",e)
#        print(f"MySQL에서 테이블 목록을 조회하는 중 오류 발생: {e}")
        return []

# 데이터베이스에서 테이블의 count 수행
def get_table_count(db, table_name):
    try:
        if db == 'mysql':
            connection = pymysql.connect(**mysql_config)
            query = f"SELECT COUNT(*) FROM dothis_exp.{table_name}"
            cursor = connection.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()
        elif db == 'ignite':
            connection = Client(username=ignite_config['user'], password=ignite_config['password'])
            connection.connect(ignite_config['host'], ignite_config['port'])
            query = f"SELECT COUNT(*) FROM dothis.{table_name}"
            target_count_result = connection.sql(query)
            target_count_result = pd.DataFrame(target_count_result)
            count = target_count_result[0][0]    
            connection.close()                   
        return count, None
    except Exception as e:
        print(f"{db}에서 {table_name} 테이블의 데이터 수를 조회하는 중 오류 발생: {e}")
        return None, str(e)

# 결과를 MySQL에 저장
def save_result(table_name, mysql_count, ignite_count, error_msg=None):
    try:
        connection = pymysql.connect(**mysql_config_master)
        cursor = connection.cursor()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        is_equal = mysql_count == ignite_count if mysql_count is not None and ignite_count is not None else False
        query = "INSERT INTO dothis_mng.validate_etl_cnt (table_name, mysql_count, ignite_count, is_equal, timestamp, error_msg) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.execute(query, (table_name, mysql_count, ignite_count, is_equal, timestamp, error_msg))
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"MySQL에 결과를 저장하는 중 오류 발생: {e}")

# 메인 실행 부
if __name__ == "__main__":
    tables = get_mysql_tables()
    index = 0
    total_tables = len(tables)
    for table_name in tables:
        index = index + 1
        mysql_count, mysql_error = get_table_count('mysql', table_name)
        ignite_count, ignite_error = get_table_count('ignite', table_name)
        if mysql_error or ignite_error:
            error_msg = f"MySQL 오류: {mysql_error} / Ignite 오류: {ignite_error}"
            print(f"{index + 1}/{total_tables}) 테이블: {table_name} 처리 중 오류 발생. 오류 메시지: {error_msg}")
            save_result(table_name, mysql_count, ignite_count, error_msg)
        else:
            print(f"{index + 1}/{total_tables}) 테이블: {table_name}, MySQL 데이터 수: {mysql_count}, Ignite 데이터 수: {ignite_count}")
            save_result(table_name, mysql_count, ignite_count)

