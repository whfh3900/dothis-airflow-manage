import os
import botocore

def local_folder_make(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

def s3_folder_make(bucket, s3, folder_path):
    # 폴더 경로에서 마지막 문자가 슬래시인지 확인하여 폴더가 이미 존재하는지 확인
    try:
        s3.head_object(Bucket=bucket, Key=(folder_path))
        print("기존에 폴더가 존재합니다.")
    except botocore.exceptions.ClientError as e:
        # head_object() 호출 시 ObjectNotFoundException이 발생하면 폴더가 존재하지 않음
        if e.response['Error']['Code'] == "404":
            # 폴더가 존재하지 않을 경우, put_object() 메서드로 빈 객체를 생성하여 폴더를 만듭니다.
            print("새롭게 폴더를 생성합니다.")
            s3.put_object(Bucket=bucket, Key=folder_path)

