ssh root@air1 'systemctl stop airflow-webserver'
ssh root@air1 'systemctl stop airflow-scheduler'
ssh root@air1 'systemctl stop airflow-flower'
ssh root@air1 'systemctl stop airflow-trigger'

ssh root@air2 'systemctl stop airflow-worker'
ssh root@air3 'systemctl stop airflow-worker'
ssh root@air4 'systemctl stop airflow-worker'


