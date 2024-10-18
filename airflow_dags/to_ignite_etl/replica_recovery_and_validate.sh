#!/bin/bash
cd /app/airflow/dags/to_ignite_etl
sh loop_replica_include_db.sh
rm -rf *.sql
/app/miniconda3/envs/igniteClient/bin/python validate_replica-full.py
