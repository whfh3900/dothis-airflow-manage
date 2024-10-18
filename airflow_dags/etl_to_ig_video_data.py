import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator  # Airflow 2.0 이상에서 python_operator 대신 python 사용
from airflow.sensors.external_task_sensor import ExternalTaskSensor
import subprocess
local_tz = pendulum.timezone("Asia/Seoul")

args = {
    "owner": "kts",
    "depends_on_past": False,
    #"retries": 3,
    }#"retry_delay": timedelta(minutes=1),
      

def run_etl():
    command = ['/app/miniconda3/condabin/conda', 'run', '-n', 'igniteClient', '--no-capture-output', '/app/miniconda3/envs/igniteClient/bin/python', '/app/airflow/dags/to_ignite_etl/etl-one-day_video_data.py']
    try:
        # subprocess.run을 사용하여 명령어 실행, check=True로 설정하여 실패 시 예외 발생
        completed_process = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        print(completed_process.stdout)  # 성공 시, 표준 출력 내용을 로그로 출력
    except subprocess.CalledProcessError as e:
        print(e.output)  # 실패 시, 에러 내용을 로그로 출력
        raise  # 현재 예외를 다시 발생시켜 Airflow에 task 실패를 알림

# DAG 정의
dag = DAG(
    'etl_to_ig_video_data',
    default_args = args,
    description='video_data  ETL from mariadb to ignite ',
    schedule_interval=None, #"@once", # None,  # ExternalTaskSensor에 의해 트리거되므로 스케줄 설정 필요 없다고 되어 있으나, once로 해야 trigger가 잘됨.
    start_date=datetime(2024, 4, 25, tzinfo=local_tz),
    catchup=False,
)

# 실제로 ETL 작업을 수행하는 PythonOperator
run_etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl,
    dag=dag,
)

# 작업 순서 정의
run_etl_task
