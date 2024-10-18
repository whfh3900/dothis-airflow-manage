import sys
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

sys.path.append('./')
load_dotenv(".")

args = {
    "owner" : "ybw",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    }

dag = DAG(
    "airflow_git_pulling",
    default_args = args,
    description="airflow_git_pulling",
    schedule_interval='0 1 * * *',
    start_date=datetime(2023, 12, 25),
    catchup=False,
    tags=["airflow", "git", "CI/CD", "pull"],
)

def get_git_code(repositery_name: str) -> str:
    git_secret = os.environ.get("GIT_SECRET")
    return f"https://Themath93:{git_secret}@github.com/dothis-world/{repositery_name}.git"

airflow_dags_dir = "/data_store/airflow_dir/"

airflow_git = get_git_code('airflow_manage')
etl_dir = "/home/etluser/ray_job/"
crawling_tool_git = get_git_code('crawling-tool')

airflow_pull = BashOperator(
    task_id='airflow_pull',
    bash_command=f"ssh air1 'cd {airflow_dags_dir} && git pull {airflow_git}'",
    dag=dag,
)
eltuser_1 = BashOperator(
    task_id='etl_1_pull',
    bash_command=f"ssh etluser@10.0.0.31 'cd {etl_dir} && git pull {crawling_tool_git}'",
    dag=dag,
)
etluser_2 = BashOperator(
    task_id='etl_2_pull',
    bash_command=f"ssh etluser@10.0.0.32 'cd {etl_dir} && git pull {crawling_tool_git}'",
    dag=dag,
)
airflow_db_migration = BashOperator(
    task_id='airflow_db_migration',
    bash_command=f"ssh air1 '/app/miniconda3/envs/airf/bin/python -m airflow db migrate'",
    dag=dag,
)


airflow_pull >> eltuser_1 >> etluser_2 >> airflow_db_migration
