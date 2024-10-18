import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.providers.http.hooks.http import HttpHook
from util.callback_util import on_failure_callback
from datetime import datetime, timedelta
import json
import pytz

HTTP_CONN_ID = 'my_http_connection'
API_ENDPOINT = 'youtube-shorts/crawling/complete'

def send_http_request():
    http = HttpHook(method='POST', http_conn_id=HTTP_CONN_ID)
    headers = {'Content-Type': 'application/json'}

    # 현재 UTC 시간을 가져옵니다.
    current_time = datetime.utcnow()
    
    # 한국 시간(KST)으로 변환합니다.
    kst_tz = pytz.timezone('Asia/Seoul')
    current_time_kst = current_time.replace(tzinfo=pytz.utc).astimezone(kst_tz)

    # 하루 전의 시간을 KST 형식으로 생성합니다.
    target_time = (current_time_kst).strftime('%Y-%m-%dT%H:%M:%SZ')

    payload = {
        "jobId": "123456789",
        "status": "success",
        "message": "Crawling completed successfully",
        "crawledAt": target_time
    }

    try:
        response = http.run(API_ENDPOINT, json.dumps(payload), headers=headers)
        response_data = response.json()
        
        # 응답 데이터의 success 값 체크
        if not response_data.get("success", True):
            logging.error(f"Request to {API_ENDPOINT} failed: {response_data}")
            for failure in response_data.get("failures", []):
                logging.error(f"Request Index: {failure.get('requestIndex')}, "
                              f"Webhook URL: {failure.get('webhookUrl')}, "
                              f"Token: {failure.get('token')}, "
                              f"Message: {json.dumps(failure.get('message'), indent=2)}")
        
        return response_data

    except Exception as e:
        # 예외 로그 출력
        logging.error(f"An exception occurred while sending HTTP request: {str(e)}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'http_request_dag',
    default_args=default_args,
    description='A simple HTTP POST request DAG with 1 minute interval',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    request_task = PythonOperator(
        task_id="send_http_request_task",
        python_callable=send_http_request,
        on_failure_callback=on_failure_callback
    )

