from datetime import datetime, timedelta                             
from airflow import DAG                                              
from airflow.operators.bash import BashOperator                      
from airflow.models.baseoperator import chain                        
                                                                          
                                                                     
args = {                                                             
        "depends_on_past": False,                                    
        "email": ["lms46784678@example.com"],                        
        "email_on_failure": False,                                   
        "email_on_retry": False,                                     
        "retries": 3,                                                
        "retry_delay": timedelta(minutes=1),                         
    }                                                                
                                                                     
dag = DAG(                                                           
    "ray_video_history_crawling_seoul",                              
    default_args = args,                                             
    description="Ray History Crawling S3",                           
    schedule_interval='0 3 * * *',                                   
    start_date=datetime(2023, 11, 28),                               
    catchup=False,
    tags=["crawling","autoscaling","ec2","ray","S3"],
)

task_list = []

working_dir = "/home/etluser/ray_job/ray-cluster/"
py_script_dir = f"/home/etluser/ray_job/crawler/youtube/aws/"
ssh_user = "etluser@10.0.0.31"
ray_yaml_name = "ray-video-history-seoul.yaml"


for i in range(0,56,2):

    # Ray up -> submit -> down
    submit_task = BashOperator(
        task_id=f"history_crawling_{i}",
        depends_on_past=False,
        bash_command=f"""
        ssh {ssh_user} ray down {working_dir}{ray_yaml_name} -y &&
        ssh {ssh_user} ray up {working_dir}{ray_yaml_name} --no-config-cache -y &&
        ssh {ssh_user} ray submit {working_dir}{ray_yaml_name}  {py_script_dir}video_history_ray_cluster.py {i} {i+2} &&
        ssh {ssh_user} ray down {working_dir}{ray_yaml_name} -y
        """ ,
        dag=dag
    )
    task_list.append(submit_task)

# list 에 모든 태스크를 그냥 실행하면 모든 작업이 병렬로 실행되지만
# chain 매서드를 사용하면 병렬에서 직렬 작업으로 변경해준다.
chain(*task_list)

#task_list
