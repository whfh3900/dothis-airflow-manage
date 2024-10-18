
import re
import pymysql
from pyignite import Client
import datetime
from datetime import timedelta
import sys
import csv
import pandas as pd

now = datetime.datetime.now()
today_str=now.strftime('%Y-%m-%d')

one_day_before = now - timedelta(days=1)
one_day_before_str = one_day_before.strftime('%Y%m%d')

two_days_before = now - timedelta(days=2)
two_days_before_str = two_days_before.strftime('%Y%m%d')


REMARKS_STR='airflow-batch'

#SELECT * FROM dothis_exp.channel_data WHERE crawled_date = {today_str}

# MySQL과 Ignite 연결 설정
mysql_config = {
    'host': '192.168.0.132',
    'user': 'etluser',
    'password': 'data123!',
    'database': 'dothis_svc',
    'port': 2300
}

ignite_config = {
    'host': '192.168.0.31',
    'user': 'ignite',
    'password': 'data123!',
    'port': 10800
}

mysql_config_master = {
    'host': '192.168.0.131',
    'user': 'etluser',
    'password': 'data123!',
    'database': 'dothis_svc',
    'port': 2300
}

# 테이블 매핑 정보: 초기적재는 전체 건수를 가져와야 하므로 where 절에 1=1으로 처리
table_mapping = {
    'dothis_exp.id_mapper': {'target_table': 'dothis.id_mapper', 'columns': ['channel_id', 'video_id', 'etl_ymd'], 'where_clause': "etl_ymd='" + today_str+"'"}
}

# 로그 기록 함수 수정
def log_to_db_and_csv(mysql_conn, cursor, log_filename, source_table, target_table, transferred_count, source_count, target_count, counts_match, remarks, where_clause, select_query):
    insert_query = """
    INSERT INTO dothis_mng.log_table (timestamp, source_table, target_table, transferred_count, source_count, target_count, counts_match, remarks, where_clause, select_query)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cursor.execute(insert_query, (datetime.datetime.now(), source_table, target_table, transferred_count, source_count, target_count, counts_match, remarks, where_clause, select_query))
        mysql_conn.commit()
        """
        with open(log_filename, 'a', newline='', encoding='utf-8') as logfile:
            csv_writer = csv.writer(logfile)
            csv_writer.writerow([datetime.datetime.now(), source_table, target_table, transferred_count, source_count, target_count, counts_match, remarks, where_clause, select_query])
        """
    except Exception as e:
        print(f"로그 기록 중 오류 발생: {e}")


def transfer_data(mysql_cursor, ignite_client, source_table, mapping_info):
    target_table = mapping_info['target_table']
    columns = mapping_info['columns']
    where_clause = mapping_info['where_clause']
    select_query = f"SELECT {', '.join(columns)} FROM {source_table} WHERE {where_clause}" # limit N

    mysql_cursor.execute(select_query)
    rows = mysql_cursor.fetchall()

    cleaned_rows = [[x if x is not None else '' for x in row] for row in rows]

    for row in cleaned_rows:
        try:
            #values_str = ', '.join([f"'{escape_str(str(x))}'" for x in row])
            values_str = ', '.join([f"'{escape_str(str(x))}'" for x in row])
            values_str = ', '.join([re.sub(r'[\x00-\x08\x0E-\x1F\x7F]+', '', item) for item in values_str.split(', ')])
            query_str = f"/* SQL00101 */MERGE INTO {target_table} ({', '.join(columns)}) VALUES ({values_str})"
            ignite_client.sql(query_str)
        except Exception as e:
            print(f"데이터 전송 중 오류 발생: {e}")
            print(query_str)
            sys.exit(1)

    # MySQL에서 데이터 건수 확인: limit을 걸어서 select하더라도 count_match가 잘 동작하도록 함.
    #mysql_cursor.execute(f"SELECT COUNT(*) FROM {source_table} WHERE {mapping_info['where_clause']}")
    #source_count = mysql_cursor.fetchone()[0]

    mysql_cursor.execute(select_query)
    source_count = mysql_cursor.rowcount

    # Ignite에서 데이터 건수 확인
    target_count_query = f"SELECT COUNT(*) FROM {target_table} WHERE {mapping_info['where_clause']}"
    target_count_result = ignite_client.sql(target_count_query)
    target_count_result = pd.DataFrame(target_count_result)
    #print(target_count_result)
    target_count = target_count_result[0][0]
    #target_count_result = ignite_client.sql(target_count_query).fetchone()
    #target_count = target_count_result[0] if target_count_result else 0

    counts_match = source_count == target_count

    return len(cleaned_rows), source_count, target_count, counts_match, where_clause, select_query

def escape_str(s):
    return s.replace("'", "''").replace("\n", "\\n")

def main():
    mysql_conn = pymysql.connect(**mysql_config)
    mysql_cursor = mysql_conn.cursor()


    mysql_conn_master = pymysql.connect(**mysql_config_master)
    mysql_cursor_master = mysql_conn_master.cursor()


    ignite_client = Client(username=ignite_config['user'], password=ignite_config['password'])
    ignite_client.connect(ignite_config['host'], ignite_config['port'])

    log_filename = f"data_transfer_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    for source_table, target_info in table_mapping.items():
        transferred_count, mysql_count, ignite_count, counts_match, where_clause, select_query = transfer_data(mysql_cursor, ignite_client, source_table, target_info)
        print('check: ',transferred_count, mysql_count, ignite_count, counts_match, where_clause, select_query )
        log_to_db_and_csv(mysql_conn_master, mysql_cursor_master, log_filename, source_table, target_info['target_table'], transferred_count, mysql_count, ignite_count, counts_match, REMARKS_STR, where_clause, select_query)


    mysql_conn_master.commit()
    mysql_cursor_master.close()
    mysql_conn_master.close()

    mysql_conn.commit()
    mysql_cursor.close()
    mysql_conn.close()

    ignite_client.close()

if __name__ == "__main__":
    main()

