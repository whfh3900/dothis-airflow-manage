import os
import requests
from dotenv import load_dotenv
import pendulum
from airflow.models import Variable

load_dotenv()

def send_discord_message(dag_id, status: str, exec_date, task_id=None, additional_info=None):
    webhook_url = os.getenv("AIRFLOW_DISCORD_WEBHOOK_URL") if status.upper() == 'SUCCESS' else os.getenv("AIRFLOW_DISCORD_FAILURE_WEBHOOK_URL")
    kst = pendulum.timezone("Asia/Seoul")
    exec_date_kst = exec_date.in_timezone(kst)
    message = f"""
    ### {dag_id} - {status} - {exec_date_kst.strftime('%Y-%m-%d %H:%M:%S')}
    Task ID: {task_id}
    """
    
    if additional_info:
        message += f"\n{additional_info}"
    
    payload = {
        "content": message
    }
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(webhook_url, json=payload, headers=headers)
    if response.status_code != 204:
        raise Exception(f"Failed to send message to Discord: {response.status_code}, {response.text}")

def on_success_callback(context):
    try:
        kst = pendulum.timezone("Asia/Seoul")
        dag_id = context['dag_run'].dag_id
        exec_date = context['execution_date'].in_timezone(kst)
        task_id = context['task_instance'].task_id
        task_instance = context['task_instance']
        return_value = task_instance.xcom_pull(task_ids=task_id)
        
        additional_info = f"Success Time: {exec_date}\n"
        if return_value:
            additional_info += f"Return Value: {return_value}\n"
        
        send_discord_message(dag_id, 'Success', exec_date, task_id, additional_info)
    except Exception as e:
        print(f"Failed to send success message: {e}")

def on_failure_callback(context):
    try:
        kst = pendulum.timezone("Asia/Seoul")
        dag_id = context['dag_run'].dag_id
        exec_date = context['execution_date'].in_timezone(kst)
        task_id = context['task_instance'].task_id
        try_number = context['task_instance'].try_number
        max_tries = context['task_instance'].max_tries
        exception = context.get('exception')
        base_url = "http://dothis2.iptime.org:6800"
        
        additional_info = f"""
        Failure Time: {exec_date}
        DAG URL: {base_url}/admin/airflow/graph?dag_id={dag_id}
        Exception: {exception}
        Traceback: {context['task_instance'].log_url}
        Retry Count: {try_number - 1}/{max_tries}
        """
        
        send_discord_message(dag_id, 'Failed', exec_date, task_id, additional_info)
    except Exception as e:
        print(f"Failed to send failure message: {e}")