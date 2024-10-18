from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
}

dag = DAG(
	# firstDAG_test는 DAG의 이름. unique한 값을 가져야 함
    'get_mariadb_logs',
    # dag에서 사용할 기본적인 파라미터 값. 위에서 정의한 내용의 일부를 사용
    default_args=default_args,
    # DAG가 언제 실행될지 설정. "@once" : 한 번만 실행
    schedule_interval="@once",
)


t1 = BashOperator(
    task_id='get_data',
    bash_command='scp root@mariadb:/mariadb/logs/general.log /data_store/logs/mariadb/',
    dag=dag,
)

t2 = BashOperator(
    task_id='compress',
    bash_command='tar -zcvf /data_store/logs/mariadb/general.tar.gz  /data_store/logs/mariadb/general.log',
    dag=dag,
)


t3 = BashOperator(
    task_id='remove.plain-text.file',
    bash_command='rm -rf /data_store/logs/mariadb/general.log',
    dag=dag,
)


t1 >> t2 >> t3
