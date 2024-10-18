ssh root@air1 'systemctl start airflow-webserver'
ssh root@air1 'systemctl start airflow-scheduler'
ssh root@air1 'systemctl start airflow-flower'
ssh root@air1 'systemctl start airflow-trigger'

ssh root@air2 'systemctl start airflow-worker'
ssh root@air3 'systemctl start airflow-worker'
ssh root@air4 'systemctl start airflow-worker'

ssh root@air1 'systemctl restart airflow-flower'

