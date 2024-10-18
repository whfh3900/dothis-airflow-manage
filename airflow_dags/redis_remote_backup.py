import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
import subprocess

local_tz = pendulum.timezone("Asia/Seoul")

args = {
    "owner": "kts",
    "depends_on_past": False
}                                     
                                                                     
dag = DAG(                                                           
    dag_id="redis_backup",                        
    default_args = args,
    description = "redis_remote_backup_daily_to_nfs",
    schedule_interval = '0 0 * * *',
    start_date = datetime(2024, 4, 25, tzinfo=local_tz),
    catchup = False,
    tags=["backup", "redis", "kts"]
)


ssh_redis1 = "root@192.168.0.121" # redis1
backup_cli = "/app/mng/redis-backup.sh"

redis1_remote_backup = BashOperator(
    task_id='redis1_remote_backup',
    bash_command=f"ssh {ssh_redis1} '{backup_cli}'",
    dag=dag
)

redis1_remote_backup
