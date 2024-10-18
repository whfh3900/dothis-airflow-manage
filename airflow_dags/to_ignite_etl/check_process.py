import psutil
import sys
import time
import requests
from dotenv import load_dotenv
import os
sys.path.append('./')
load_dotenv(".")
# Slack 설정
chnl = "#dothis알림"
myToken = os.environ.get('MYTOKEN')

def post_message(token, channel, text):
    response = requests.post("https://slack.com/api/chat.postMessage", headers={"Authorization": "Bearer " + token}, data={"channel": channel, "text": text})
    print(response)

def monitor_process(process_param):
    if process_param.isdigit():
        # 프로세스 ID로 입력된 경우
        process_id = int(process_param)
        try:
            process = psutil.Process(process_id)
            process_cmdline = ' '.join(process.cmdline())
            print(f"Monitoring process: {process_cmdline} (PID: {process_id})")
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            print(f"Process with ID {process_id} does not exist.")
            return
    else:
        # 프로세스 이름으로 입력된 경우
        process_name = process_param
        found = False

        # 프로세스 목록에서 이름을 확인하여 PID를 찾음
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            if proc.info['cmdline'] and process_name in proc.info['cmdline']:
                print(f"Monitoring process: {' '.join(proc.info['cmdline'])} (PID: {proc.info['pid']})")
                found = True
                break
        
        if not found:
            print(f"Process with name '{process_name}' does not exist.")
            return

    while True:
        # 1분마다 실행
        time.sleep(60)

        try:
            if process_param.isdigit():
                # 프로세스 ID로 입력된 경우
                process = psutil.Process(process_id)
                print("Process is still running.")
            else:
                # 프로세스 이름으로 입력된 경우
                for proc in psutil.process_iter(['cmdline']):
                    if proc.info['cmdline'] and process_name in proc.info['cmdline']:
                        print("Process is still running.")
                        break
                else:
                    # 프로세스가 더 이상 존재하지 않으면 출력 및 Slack 메시지 전송
                    print(f"{process_name} process has terminated.")
                    post_message(myToken, chnl, f"{process_name} process has terminated.")
                    break
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # 프로세스가 더 이상 존재하지 않으면 출력 및 Slack 메시지 전송
            if process_param.isdigit():
                print(f"Process with ID {process_id} has terminated.")
                post_message(myToken, chnl, f"Process {process_cmdline} ({process_id}) has terminated.")
            else:
                print(f"{process_name} process has terminated.")
                post_message(myToken, chnl, f"{process_name} process has terminated.")
            break

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <process_id_or_name>")
        sys.exit(1)

    process_param = sys.argv[1]
    monitor_process(process_param)

