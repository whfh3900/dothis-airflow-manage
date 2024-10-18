import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator  # Airflow 2.0 이상에서 python_operator 대신 python 사용
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator

import subprocess
local_tz = pendulum.timezone("Asia/Seoul")
args = {
    "owner": "kts",
    "depends_on_past": False,
    #"retries": 3,
    }#"retry_delay": timedelta(minutes=1),
      

def run_etl():
    command = ['/app/miniconda3/condabin/conda', 'run', '-n', 'igniteClient', '--no-capture-output', '/app/miniconda3/envs/igniteClient/bin/python', '/app/airflow/dags/to_ignite_etl/etl-one-day-channel_data.py']
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    for line in process.stdout:
        print(line)
    process.wait()

# DAG 정의
dag = DAG(
    'etl_after_test2',
    default_args = args,
    description='선행 DAG etl_first_test1이 성공한 후 실행되는 DAG',
    schedule_interval=None, #"@once", # None,  # ExternalTaskSensor에 의해 트리거되므로 스케줄 설정 필요 없다고 되어 있으나, once로 해야 trigger가 잘됨.
    start_date=datetime(2024, 4, 25, tzinfo=local_tz),
    catchup=False,
)
"""
# 선행 DAG가 성공적으로 완료되었는지 확인하는 ExternalTaskSensor
wait_for_etl_test_once = ExternalTaskSensor(
    task_id='wait_for_etl_test_once',
    external_dag_id='etl_before_test2',
    external_task_id=None,  # 선행 DAG의 모든 태스크가 성공했는지 확인
    #execution_delta=timedelta(minutes=1),  # 선행 DAG의 실행 시간과의 시간 차이 조정. 요구사항에 따라 조정하세요.
    dag=dag,
)
"""
# 실제로 ETL 작업을 수행하는 PythonOperator
run_etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl,
    dag=dag,
)

do_something = EmptyOperator(task_id='do_something')

# 작업 순서 정의
run_etl_task >> do_something

