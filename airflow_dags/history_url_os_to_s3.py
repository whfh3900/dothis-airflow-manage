from datetime import datetime, timedelta
import time
import json

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from util.os_client import opensearch_client,make_url_list_from_os

args = {                                             
        "depends_on_past": False,                    
        "email": ["lms46784678@example.com"],            
        "email_on_failure": True,                   
        "email_on_retry": True,                     
        "retries": 2,                                
        "retry_delay": timedelta(minutes=5),         
    }         

dag = DAG(                                           
    "history_url_os_to_s3",                       
    default_args = args,                             
    description="history_url_os_to_s3",           
    schedule=timedelta(days=1),                                   
    start_date=datetime(2023, 11, 1),                
    catchup=False,                                   
    tags=["crwaling","opensearch","S3"],
)            

def fill_s3_with_new_urls():
  tmp = []
  os_client = opensearch_client()
  s3_client = boto3.client('s3')
  bucket_name = 'ray-os-index'
  
  try:
      response = s3_client.list_objects_v2(Bucket=bucket_name)
      if 'Contents' in response:
        for obj in response['Contents']:
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
  except:
    print("nothing to delete")
    
  
  for i in range(72):
      result = make_url_list_from_os(os_client,f"video-{i}")
      tmp = tmp + result
      
  chunks = [tmp[i:i + 50000] for i in range(0, len(tmp), 50000)]

  for idx,chunk in enumerate(chunks):
      file_name = f'video-{idx}.json'

      s3_client.put_object(Body=json.dumps(chunk,ensure_ascii=False), Bucket=bucket_name, Key=file_name)

task1 = PythonOperator(
    task_id=f'video_url_os_to_s3',
    python_callable=fill_s3_with_new_urls,
    dag=dag
)

task1
