#!/bin/bash

# db_tab.lst 파일 경로
input_file="db_tab.lst"

# 파일이 존재하는지 확인
if [[ ! -f "$input_file" ]]; then
    echo "파일이 존재하지 않습니다: $input_file"
    exit 1
fi

while read -r db_name table_name; do
    # mysqldump 및 mysql 명령 실행
    echo "${db_name}.${table_name} backup"
    mysqldump -u root -pdata123! -P2300 -h 192.168.0.131 "${db_name}" "${table_name}" --skip-column-statistics --single-transaction --max-allowed-packet=512M --compress --order-by-primary > "${db_name}.${table_name}.sql"
done < "$input_file"



while read -r db_name table_name; do
    echo "${db_name}.${table_name} drop"
    mysql -u root -pdata123! -P2300 -h 192.168.0.132 "${db_name}" -e "DROP TABLE IF EXISTS ${db_name}.${table_name};"
done < "$input_file"




while read -r db_name table_name; do
    echo "${db_name}.${table_name} restore"
    mysql -u root -pdata123! -P2300 -h 192.168.0.132 "${db_name}" < "${db_name}.${table_name}.sql"
done < "$input_file"



mysql -u root -pdata123! -P2300 -h 192.168.0.132  -e '
grant all privileges on *.* to etluser@`%`;
grant all privileges on *.* to maxscale@`%`;
grant all privileges on *.* to pmm@`%`;
grant all privileges on *.* to root@`%`;
grant all privileges on *.* to sqlmgr@`%`;
grant all privileges on *.* to root@`%`;
flush privileges;
'



# 후속 작업 수행
/app/miniconda3/envs/igniteClient/bin/python  push_msg.py "replica tables"



