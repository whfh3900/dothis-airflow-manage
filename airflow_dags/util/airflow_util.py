from datetime import datetime, timedelta

def get_date_list(start_date: str):
    today = datetime.today().strftime("%Y-%m-%d")
    past_date = datetime.strptime(start_date, "%Y-%m-%d")
    date_list = [past_date.strftime("%Y-%m-%d")]
    while True:
        past_date = past_date + timedelta(days=1)
        str_past_date = past_date.strftime("%Y-%m-%d")
        if str_past_date == today:
            break
        date_list.append(str_past_date)
    date_list.append(today)
    return date_list