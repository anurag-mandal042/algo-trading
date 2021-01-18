import mysql.connector as mysql
import pandas as pd
from datetime import timedelta
import os
import stocks


def connect():
    # enter your server IP address/domain name
    HOST = "164.52.207.114"  # or "domain.com"
    # database name, if you want just to connect to MySQL server, leave it empty
    DATABASE = "stock_production"
    # this is the user you create
    USER = "stock"
    # user password
    PASSWORD = "stockdata@data"
    # connect to MySQL server
    db_connection = mysql.connect(host=HOST, user=USER, password=PASSWORD)
    print("Connected to:", db_connection.get_server_info())
    cursor = db_connection.cursor()  # get the cursor
    # TATA_MOTORS_DEC_FUT_ID =

    id_selected = stocks.selected_stock["id"]
    cursor.execute("use stock_production;")
    # cursor.execute("SELECT * FROM instrument_details LIMIT 100 ")
    # cursor.execute("SELECT * FROM instrument_details  where date(ins_date)>= '2020-11-30' and date(ins_date)<= '2020-12-02' and instrument_id in(select id from instruments where f_n_o=1) LIMIT 0,100")

    cursor.execute("SELECT * FROM instrument_details  where date(ins_date)>= '2020-12-22' and date(ins_date)<= '2020-12-24' and instrument_id = {} LIMIT 0,100".format(id_selected))
    df = pd.DataFrame(cursor.fetchall())
    df.columns = ['id', 'instrument_id', 'ins_date', 'open', 'high',
                  'low', 'close', 'volume', 'status', 'created_at', 'updated_at']

    return df


def addFinalDateTime(df):
    df['Final_DateTime'] = pd.to_datetime(
        df['ins_date']) + timedelta(seconds=0, minutes=30, hours=5)
    df['Final_Date'] = pd.to_datetime(df['Final_DateTime']).dt.date
    df['Final_Time'] = pd.to_datetime(df['Final_DateTime']).dt.time
    return df


def get_9_15_index(df):
    for i in df.index:
        if df["Final_Time"][i] == "09:15:00":
            # nine_fifteen_candle_index = i
            return i


if __name__ == "__main__":
    df = connect()
    df = addFinalDateTime(df)
    df.to_csv("csv_data\\{}.csv".format(stocks.selected_stock["filename"]))
    # # print(df['Final_DateTime'])
    # # df.to_csv("o.csv")

    # cursor.execute("use stock_production;")
    # # cursor.execute("SELECT * FROM instruments LIMIT 100 ")
    # cursor.execute("SELECT * FROM instrument_details  where date(ins_date)>= '2020-11-30' and date(ins_date)<= '2020-12-02' and instrument_id in(select id from instruments where f_n_o=1) LIMIT 0,100")
    # df = pd.DataFrame(cursor.fetchall())
    # df.columns = ['id', 'instrument_id', 'ins_date', 'open', 'high',
    #               'low', 'close', 'volume', 'status', 'created_at', 'updated_at']

    # df.to_csv("instruments.csv")
    # print("Done")
    # # os.system('cmd /k "start excel .\output.csv"')
    # os.system('cmd /c "start excel .\output.csv"')


# instrument_details daily data
# historical_data one time
