import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from util.callback_util import on_success_callback, on_failure_callback

local_tz = pendulum.timezone("Asia/Seoul")

args = {
    "owner": "ybw",
    "depends_on_past": False,
    "on_success_callback": on_success_callback,
    "on_failure_callback": on_failure_callback
}


# DAG 정의
dag = DAG(
    'etl_to_os_channel_history',
    default_args=args,
    description="channel_history OpenSearch에 저장",
    schedule_interval=None,
    start_date=datetime(2024, 9, 11, tzinfo=local_tz),
    catchup=False,
)

today = datetime.now().strftime("%Y%m%d")
bash_command = f"/app/logstash/bin/logstash -f /app/logstash/config/etl_to_opensearch/channel_history/channel_history_{today}.conf"
ssh_domain = "svcmon@es-sa"

# BashOperator를 사용하여 명령어 실행
run_etl_task = BashOperator(
    task_id='run_os_ch_etl',
    bash_command=f'ssh {ssh_domain} {bash_command}',
    dag=dag,
)

run_etl_task