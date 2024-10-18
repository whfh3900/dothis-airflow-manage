from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain

args = {
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }

dag = DAG(
    "ray_history_crawling_s3",
    default_args = args,
    description="Ray History Crawling S3",
    schedule=None,
    start_date=datetime(2023, 11, 1),
    catchup=False,
    tags=["crwaling","autoscaling","ec2","ray","S3"],
)

task_list = []

working_dir = "/home/etluser/ray_job/ray-cluster/"
py_script_dir = f"/home/etluser/ray_job/aws/re/"



for i in range(110):
    
    ray_up = BashOperator(
        task_id=f"ray_up_{i}",
        bash_command=f"ssh etluser@10.0.0.31 ray up {working_dir}ray-autoscale.yaml --no-config-cache -y" ,
        dag=dag
        # Second argument is about quantity of each major's quetion. But real quantity is "2nd_arg * 10". So 10 is meaning 200ea
    )
    
    # 레이 히스토리 크롤링 시작 각 태스크당 5만개씩 크롤링 작업 수행함
    submit_task = BashOperator(
        task_id=f"job_begin_{i}",
        depends_on_past=False,
        bash_command=f"ssh etluser@10.0.0.31 ray submit {working_dir}ray-autoscale.yaml {py_script_dir}video_history_ray_cluster.py {i} {i+1}" ,
        retries=3,
        dag=dag
    )

    # ray down IP 초기화
    down_workers = BashOperator(
        task_id=f"ray_down_workers_{i}",
        depends_on_past=False,
        bash_command=f"ssh etluser@10.0.0.31 ray down {working_dir}ray-autoscale.yaml -y",
        dag=dag
    )
    
    task_list.append(ray_up)
    task_list.append(submit_task)
    task_list.append(down_workers)
    
# list 에 모든 태스크를 그냥 실행하면 모든 작업이 병렬로 실행되지만
# chain 매서드를 사용하면 병렬에서 직렬 작업으로 변경해준다.
chain(*task_list)

task_list
