import requests
import pandas as pd
import json
from datetime import datetime
import DB_Connection as dbc

"""def does_exists_time(time_value,dict):
  if time_value in dict.keys():
    return dict[time_value]
  return '1970-01-01 00:00:00'

df_branches= pd.DataFrame(columns=["uuid","id","host_id","topic","type","start_time","duration","timezone","created_at","join_url","insert_ts"])
df_meetings= pd.DataFrame(columns=["id","uuid","start_time","insert_ts"])
df_meeting_instances = pd.DataFrame(columns=["uuid","id","host_id","topic","type","user_email","start_time","end_time","duration",
                                             "total_minutes","participants_count","insert_ts"])

branches = ["10","20","30","40","50","60","70","80","90","100","200","300"]
#branches = ["10"]
payload={}
headers = {
  'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhdWQiOm51bGwsImlzcyI6IncwT3luSWlVU2pLdGVwSlhMVXlvUFEiLCJleHAiOjQxMDI0Mzc2MDAsImlhdCI6MTYxNzQ2ODM4Nn0.LtNHeZz1vxxDx6pSIU2sQ92AJhT7pmnuIzjRAwkjkho',
  'Cookie': 'cred=B9A5509726F2D17EE944ECC47724ADBF'
}

for branch in branches:
  url = "https://api.zoom.us/v2/users/branch" + branch + "@she-codes.org/meetings?page_size=50"
  response = requests.request("GET", url, headers=headers, data=payload).text
  meetings = json.loads(response)["meetings"]
  for meeting in meetings:
    #print(meeting)
    df_branches = df_branches.append({
      "uuid": meeting["uuid"],
      "id": meeting["id"],
      "host_id": meeting["host_id"],
      "topic": meeting["topic"],
      "type": meeting["type"],
      "start_time": meeting["start_time"],
      "duration": meeting["duration"],
      "timezone": meeting["timezone"],
      "created_at": meeting["created_at"],
      "join_url": meeting["join_url"],
      "insert_ts": datetime.utcnow().strftime("%d-%m-%Y %H:%M:%S")
                    }, ignore_index=True)
  #df = pd.read_json(json.loads(response)["meetings"])
print(df_branches)
for branch in df_branches["id"]:
  url = "https://api.zoom.us/v2/past_meetings/"+str(branch)+"/instances"

  response = requests.request("GET", url, headers=headers, data=payload).text
  meetings = json.loads(response)
  if "meetings" in json.loads(response).keys() and json.loads(response)["meetings"] != []:
    meetings = json.loads(response)["meetings"]
    for meeting in meetings:
      # print(meeting)
      df_meetings = df_meetings.append({
        "id": branch,
        "uuid": meeting["uuid"],
        "start_time": meeting["start_time"],
        "insert_ts": datetime.utcnow().strftime("%d-%m-%Y %H:%M:%S")
      }, ignore_index=True)

print(df_meetings)

for meeting in df_meetings["uuid"]:
  url = "https://api.zoom.us/v2/past_meetings/"+meeting

  response = requests.request("GET", url, headers=headers, data=payload).text
  meeting_instances = json.loads(response)
  print(meeting_instances)
  if "uuid" in meeting_instances.keys():
    df_meeting_instances = df_meeting_instances.append({
    "id": meeting_instances["id"],
    "uuid": meeting_instances["uuid"],
    "host_id": meeting_instances["host_id"],
    "topic": meeting_instances["topic"],
    "type": meeting_instances["type"],
    "user_email": meeting_instances["user_email"],
    "start_time": does_exists_time("start_time",meeting_instances),
    "end_time": does_exists_time("end_time",meeting_instances),
    "duration": meeting_instances["duration"],
    "total_minutes": meeting_instances["total_minutes"],
    "participants_count": meeting_instances["participants_count"],
    "insert_ts": datetime.utcnow().strftime("%d-%m-%Y %H:%M:%S")
      }, ignore_index=True)

df_meeting_instances.to_csv(r'meetings_she_codes.csv',encoding='utf-8-sig')"""

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('PyCharm')
    con = dbc.DBconnection()

    csv_file = "./global-temp-monthly.csv"
    df = pd.read_csv(csv_file)
    df = df.rename(columns={
        "Source": "Source",
        "Date": "Date",
        "Mean": "Mean"
    })

    con.insert_into_db(df, "data_load","test")

    con.disconnect()

