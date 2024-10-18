import pendulum
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator  # Import TriggerDagRunOperator

from airflow.utils.dates import days_ago
import subprocess
import sys
args = {
    "owner": "kts",
    "depends_on_past": False,
    #"retries": 3,
    }#"retry_delay": timedelta(minutes=1),
      

local_tz = pendulum.timezone("Asia/Seoul")

def run_etl():
    command = ['/app/miniconda3/condabin/conda', 'run', '-n', 'igniteClient', '--no-capture-output', '/app/miniconda3/envs/igniteClient/bin/python', '/app/airflow/dags/to_ignite_etl/etl-one-day-channel_data.py']
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    for line in process.stdout:
        print(line)
    process.wait()

dag = DAG(
    'etl_before_test2',
    default_args = args,
    schedule_interval="@once",
    start_date=datetime(2024, 4, 25, tzinfo=local_tz),
)

t1 = PythonOperator(
    task_id='etl_to_ignite_from_exp',
    python_callable=run_etl,
    dag=dag,
)



trigger_follow_up_dag = TriggerDagRunOperator(
    task_id="trigger_follow_up_dag",
    trigger_dag_id="etl_after_test2",  # The ID of the downstream DAG to trigger
    dag=dag,
)


t1 >> trigger_follow_up_dag
