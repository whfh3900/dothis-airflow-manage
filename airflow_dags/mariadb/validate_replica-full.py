# -*- coding: utf-8 -*-


# for validation
import pymysql
from datetime import datetime
#import pandas as pd
import os
from dotenv import load_dotenv
import sys
sys.path.append('./')
load_dotenv(".")



# for msg
import requests
import sys
msg_txt = "작업 완료"

if len(sys.argv) > 1:
    msg_txt = '[' + str(sys.argv[1]) + '] ' +msg_txt


# Slack 설정
chnl = "#dothis알림"
myToken = os.environ.get("MYTOKEN")

def post_message(token, channel, text):
    response = requests.post("https://slack.com/api/chat.postMessage", headers={"Authorization": "Bearer " + token}, data={"channel": channel, "text": text})
    print(response)











mysql_config_slave = {
    'host': '192.168.0.132',
    'user': 'root',
    'port': 2300,
    'password': 'data123!',
    'database': 'dothis_svc'
}

mysql_config_master = {
    'host': '192.168.0.131',
    'user': 'root',
    'port': 2300,
    'password': 'data123!',
    'database': 'dothis_svc'
}




# MySQL에서 테이블 목록 조회
def get_tab_list():
    try:
        connection = pymysql.connect(**mysql_config_master)
        cursor = connection.cursor()
        cursor.execute("truncate table dothis_mng.validate_replication_cnt")
        cursor.execute("SELECT concat(table_schema,'.',table_name) as table_name FROM information_schema.tables WHERE table_schema like 'dothis_%' and table_schema not in ('dothis_exp') order by 1")
        tables = cursor.fetchall()
        cursor.close()
        connection.close()
        return [table[0] for table in tables]
    except Exception as e:
        print("Err on information_schema: ",e)
#        print(f"master에서 테이블 목록을 조회하는 중 오류 발생: {e}")
        return []

# 데이터베이스에서 테이블의 count 수행
def get_table_count(db, table_name):
    try:
        if db == 'master':
            connection = pymysql.connect(**mysql_config_master)
            query = f"SELECT COUNT(*) FROM {table_name}"
            cursor = connection.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()
        elif db == 'slave':
            connection = pymysql.connect(**mysql_config_slave)
            query = f"SELECT COUNT(*) FROM {table_name}"
            cursor = connection.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            cursor.close()
        return count, None
    except Exception as e:
        print(f"{db}에서 {table_name} 테이블의 데이터 수를 조회하는 중 오류 발생: {e}")
        return None, str(e)

# 결과를 Master에 저장
def save_result(table_name, master_count, slave_count, error_msg=None):
    try:
        connection = pymysql.connect(**mysql_config_master)
        cursor = connection.cursor()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        is_equal = master_count == slave_count if master_count is not None and slave_count is not None else False
        query = "INSERT INTO dothis_mng.validate_replication_cnt (table_name, master_count, slave_count, is_equal, timestamp, error_msg) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.execute(query, (table_name, master_count, slave_count, is_equal, timestamp, error_msg))
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Master에 결과를 저장하는 중 오류 발생: {e}")

def make_msg():
    connection = pymysql.connect(**mysql_config_master)
    query = f"""select (select count(*) from dothis_mng.validate_replication_cnt where error_msg is not null) as error_msg_cnt,
                (select count(*) from dothis_mng.validate_replication_cnt where master_count >0 and is_equal != 1) as diff_replica_cnt,
                (select max(table_name) from dothis_mng.validate_replication_cnt where error_msg is not null) as present_error_table,
                (select max(table_name) from dothis_mng.validate_replication_cnt where master_count >0 and is_equal != 1) as present_diff_table"""
    cursor = connection.cursor()
    cursor.execute(query)
    msg_txt = cursor.fetchone()
    cursor.close()
    msg_txt = 'replica validation\n[err_msg_cnt,diff_replica_cnt,present_err_tab,present_diff_tab]\n' + ', '.join(map(str, msg_txt))
    return msg_txt

# 메인 실행 부
if __name__ == "__main__":
    
    tables = get_tab_list()
    tab_num = len(tables)
    tab_idx = 0
    tab_pct_str = ""
    for table_name in tables:
        tab_idx = tab_idx + 1
        tab_pct_str = f"({tab_idx}/{tab_num}"
        master_count, master_error = get_table_count('master', table_name)
        slave_count, slave_error = get_table_count('slave', table_name)
        if master_error or slave_error:
            error_msg = f"Master 오류: {master_error} / Slave 오류: {slave_error}"
            print(f"[{tab_pct_str}]테이블: {table_name} 처리 중 오류 발생. 오류 메시지: {error_msg}")
            save_result(table_name, master_count, slave_count, error_msg)
        else:
            print(f"[{tab_pct_str}]테이블: {table_name}, Master 데이터 수: {master_count}, Slave 데이터 수: {slave_count}")
            save_result(table_name, master_count, slave_count)
    
    msg_txt = make_msg();
    post_message(myToken,chnl,msg_txt)
