import requests
import pandas as pd
import json
from datetime import datetime
import DB_Connection as dbc
from configparser import ConfigParser


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

