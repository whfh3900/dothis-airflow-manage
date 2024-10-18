from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

args = {
        "depends_on_past": False,
        "email": ["lms46784678@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    }

dag = DAG(
    "ray_channel_history_crawling",
    default_args = args,
    description="Ray Channel History Crawling",
    schedule_interval='0 11 * * *',                                                        
    start_date=datetime(2023, 11, 28),                                                     
    catchup=False,                                                                         
    tags=["crawling","autoscaling","ec2","ray"],                                           
)       


working_dir = "/home/etluser/ray_job/ray-cluster/"
py_script_dir = f"/home/etluser/ray_job/crawler/youtube/aws/channel_history/"
arg = "/home/ubuntu"
ssh_user = "etluser@10.0.0.31"
ray_yaml_name = "ray-channel-history.yaml"

channel_history_task = BashOperator(
    task_id=f"channel_history_crawling_chunk",
    depends_on_past=False,
    bash_command=f"""
    ssh {ssh_user} ray down {working_dir}{ray_yaml_name} -y &&
    ssh {ssh_user} ray up {working_dir}{ray_yaml_name} --no-config-cache -y &&
    ssh {ssh_user} ray submit {working_dir}{ray_yaml_name}  {py_script_dir}channel_history_extract.py {arg} &&
    ssh {ssh_user} ray down --workers-only {working_dir}{ray_yaml_name} -y &&
    ssh {ssh_user} ray submit {working_dir}{ray_yaml_name}  {py_script_dir}channel_history_process.py {arg} &&
    ssh {ssh_user} ray down {working_dir}{ray_yaml_name} -y
    """ ,
    dag=dag
)



channel_history_task
