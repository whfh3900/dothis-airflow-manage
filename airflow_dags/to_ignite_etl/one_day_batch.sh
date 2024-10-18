#!/bin/bash
/app/miniconda3/envs/igniteClient/bin/python etl-one-day-channel_data.py
/app/miniconda3/envs/igniteClient/bin/python etl-one-day-channel_history.py
/app/miniconda3/envs/igniteClient/bin/python etl-one-day-id_mapper.py
/app/miniconda3/envs/igniteClient/bin/python etl-one-day-invalid_video.py
/app/miniconda3/envs/igniteClient/bin/python etl-one-day_video_data.py
/app/miniconda3/envs/igniteClient/bin/python etl-one-day_video_data_shorts.py
/app/miniconda3/envs/igniteClient/bin/python etl-one-day_video_history.py
/app/miniconda3/envs/igniteClient/bin/python etl-one-day_video_history_shorts.py
/app/miniconda3/envs/igniteClient/bin/python validate_cnt.py
# /app/miniconda3/envs/igniteClient/bin/python validate_replica.py

/app/miniconda3/envs/igniteClient/bin/python push_msg.py "전체 일일적재"
