import requests
import sys
from dotenv import load_dotenv
import os
sys.path.append('./')
load_dotenv(".")
msg_txt = "작업 완료"


if len(sys.argv) > 1:
    msg_txt = '[' + str(sys.argv[1]) + '] ' +msg_txt

# Slack 설정
chnl = "#dothis알림"
myToken = os.environ.get('MYTOKEN')

def post_message(token, channel, text):
    response = requests.post("https://slack.com/api/chat.postMessage", headers={"Authorization": "Bearer " + token}, data={"channel": channel, "text": text})
    print(response)


post_message(myToken,chnl,msg_txt)


