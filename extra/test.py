import os
import mysql.connector as mysql
from datetime import timedelta
import datetime
import pandas as pd
import strategyOne as s1

# global variables
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
cursor.execute("use stock_production;")


def reset():
    global stats, buy_trade, sell_trade, FIRST_TRADE, SECOND_TRADE

    stats = {
        "net_pnl": 0,
        "FIRST_TRADE_trigger_candle_index": 0,
        "FIRST_TRADE_close_candle_index": 0,
        # "SECOND_TRADE_trigger_candle_index": 0,
        "SECOND_TRADE_close_candle_index": 0,
        "nine_fifteen_candle_index": 0,
        "FIRST_TRADE": None,
        "SECOND_TRADE": None,
    }
    buy_trade = {
        "trade": "buy",
        'entry_at': 0,
        'exit_at': 0,
        "active": False,
        "stop_loss": 0,
        "stop_loss_hit": False,
        "pnl": 0,
        "volume": 100,
        "timestamp": None,
    }
    sell_trade = {
        "trade": "sell",
        'entry_at': 0,
        'exit_at': 0,
        "active": False,
        "stop_loss": 0,
        "stop_loss_hit": False,
        "pnl": 0,
        "volume": 100,
        "timestamp": None,
    }
    FIRST_TRADE = None
    SECOND_TRADE = None


def getDF(instrument_id, date):
    global cursor
    query = "SELECT * FROM instrument_details where date(ins_date) = '{}' and instrument_id = {}".format(
        date, instrument_id)
    cursor.execute(query)
    print(query)
    df = pd.DataFrame(cursor.fetchall())
    df.columns = ['id', 'instrument_id', 'ins_date', 'open', 'high',
                  'low', 'close', 'volume', 'status', 'created_at', 'updated_at']
    df = addFinalDateTimeColumns(df)
    # global df
    return df


def convert(df, minutes, save_file, open_file):
    '''
    function to convert df in specified time period
    parameters:
            df: dataframe to be converted
            minutes: time period (3 or 15)
            save_file: to save csv file after converting (True  /False)
            open_file: to open csv file after converting (True  /False)
    *** Warning: If file is never saved then it will be saved first and then opened ***
    '''
    opens = []
    close = []
    high = []
    low = []

    for i in range(0, df.index.stop, minutes):
        opened_at = df["open"][i]
        opens.append(opened_at)
        if i+minutes in df.index:
            # 0 to 3 | 4 to 7 | 8 to 11 ...
            closed_at = df["close"][i + minutes - 1]
        else:
            closed_at = df["close"][df.index.stop-1]
        close.append(closed_at)
        if i+minutes in df.index:
            min_low = min(df["low"][i:i + minutes])
            max_high = max(df["high"][i:i+minutes])
        else:
            # ------------ if index not multiple of minutes
            min_low = min(df["low"][i:i+df.index.stop])
            max_high = max(df["high"][i:i + df.index.stop])
        high.append(max_high)
        low.append(min_low)
        i += 3
    data = {
        'open': opens,
        'high': high,
        'low': low,
        'close': close,
    }
    converted_minutes_df = pd.DataFrame(data=data)
    # print(converted_minutes_df)

    if minutes == 3:
        file_name = "csv_Data\{}_3_minutes.csv".format(
            stocks.selected_stock["filename"])
    elif minutes == 15:
        file_name = "csv_Data\{}_15_minutes.csv".format(
            stocks.selected_stock["filename"])
    if save_file:
        converted_minutes_df.to_csv("{}".format(file_name))
    if open_file:
        converted_minutes_df.to_csv("{}".format(file_name))
        os.system('cmd /c "start excel {}"'.format(file_name))
    return converted_minutes_df


def addFinalDateTimeColumns(df):
    df['Final_DateTime'] = pd.to_datetime(
        df['ins_date']) + timedelta(seconds=0, minutes=30, hours=5)
    df['Final_Date'] = pd.to_datetime(df['Final_DateTime']).dt.date
    df['Final_Time'] = pd.to_datetime(df['Final_DateTime']).dt.time
    return df


def dictToDF(dict):
    data = {}
    for key, value in zip(dict.keys(), dict.values()):
        data[key] = [value]
    df = pd.DataFrame(data)
    return df


def singleDF(insturument_id, instrument_name, date, save_to="new", risk=1000):

    df = getDF(insturument_id, date)
    stats, FIRST_TRADE, SECOND_TRADE = s1.main(df)
    net_pnl = 0

    data = {
        "date": date,
        "stock_name": instrument_name,
        "stock_id": insturument_id,
        "signal": "",
        "lot_pnl": 0,
        "entry_at": 0,
        "exit_at": 0,
        "risk": risk,
        "stop_loss": 0,
        "volume": 0,
    }
    if stats["FIRST_TRADE"]["exit_at"] != 0:
        data["signal"] = stats["FIRST_TRADE"]["trade"]
        data["entry_at"] = stats["FIRST_TRADE"]["entry_at"]
        data["exit_at"] = stats["FIRST_TRADE"]["exit_at"]
        data["lot_pnl"] = stats["FIRST_TRADE"]["pnl"]
        data["stop_loss"] = abs(
            stats["FIRST_TRADE"]["entry_at"] - stats["FIRST_TRADE"]["stop_loss"])
        data["volume"] = data["risk"]/stats["FIRST_TRADE"]["stop_loss"]
        statsDF = dictToDF(data)
        net_pnl += data["lot_pnl"]
        temp_records = pd.concat([temp_records, statsDF])
    if (stats["SECOND_TRADE"] is not None) and (stats["SECOND_TRADE"]["exit_at"] != 0):
        data["signal"] = stats["SECOND_TRADE"]["trade"]
        data["entry_at"] = stats["SECOND_TRADE"]["entry_at"]
        data["exit_at"] = stats["SECOND_TRADE"]["exit_at"]
        data["lot_pnl"] = stats["SECOND_TRADE"]["pnl"]
        data["stop_loss"] = abs(
            stats["SECOND_TRADE"]["entry_at"] - stats["SECOND_TRADE"]["stop_loss"])
        data["volume"] = data["risk"] / \
            stats["SECOND_TRADE"]["stop_loss"]
        statsDF = dictToDF(data)
        net_pnl += data["lot_pnl"]
        temp_records = pd.concat([temp_records, statsDF])

    print(temp_records)
    print("overall netpnl :", net_pnl)
    temp_records.to_csv("{}.csv".format(save_to))
    print("results saved in {}.csv".format(save_to))


def multipleDFs():

    # stocks_data = pd.read_csv("stocks_data.csv")
    # stocks_data = pd.read_csv("fav_stocks.csv")
    # date = '2020-12-29'
    # absent_stocks = [1, 3, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 17, 19, 21, 22, 24, 26, 27, 28, 31, 32, 34, 35, 37, 39, 40, 43, 45, 46, 48, 50, 51, 52, 53, 54, 56, 58, 59, 60, 61, 62, 63, 64, 66, 67, 68, 69, 70, 71, 72, 73, 76, 77, 78, 79, 82, 83, 84, 85, 87, 88, 89, 91, 93, 95, 96, 97, 98, 99, 100, 102, 103, 104, 105, 107, 108, 109, 111, 113, 114, 115, 116, 117, 118, 120, 121, 123, 125, 126, 127, 129, 131, 132, 133, 134, 135, 137, 138, 139, 140, 141, 142, 145, 147, 149, 150, 152, 153, 154, 156, 157, 158, 161, 162, 163, 167, 169, 170, 173, 175, 176, 178, 180, 181, 183, 184, 185, 186, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 200, 202, 203, 204, 205, 206, 207, 208,
    #                  210, 211, 212, 213, 214, 215, 217, 218, 219, 220, 221, 223, 227, 229, 230, 231, 232, 233, 234, 235, 237, 238, 239, 241, 242, 244, 245, 246, 247, 251, 252, 254, 255, 258, 262, 263, 264, 265, 266, 267, 268, 269, 270, 272, 273, 276, 277, 278, 280, 281, 283, 284, 285, 286, 287, 288, 289, 291, 292, 293, 294, 296, 297, 300, 301, 302, 303, 306, 307, 310, 311, 312, 313, 314, 315, 317, 319, 320, 321, 322, 323, 324, 327, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 345, 346, 349, 350, 356, 358, 359, 360, 362, 363, 366, 367, 368, 371, 373, 374, 377, 378, 380, 381, 382, 383, 385, 387, 388, 391, 392, 394, 396, 397, 398, 399, 400, 402, 404, 406, 407, 408, 409, 410, 411, 413]
    # stocks_data = pd.read_csv("dated_stock_list.csv")
    # absent_stocks = [38, 44, 80, 64, 244, 259,
    #  120, 92, 128, 159, 174, 187, 277, 228, 334, 369, 197, 413]
    temp_records = pd.DataFrame()
    netpnl = 0

    date = '2020-12-29'

    code_start_time = datetime.datetime.now()
    df = getDF(75, date)
    # print(df)

    stats, FIRST_TRADE, SECOND_TRADE = s1.main(df)
    # print(stats, FIRST_TRADE, SECOND_TRADE)
    data = {
        "date": date,
        # "stock_name": stocks_data["instrument_name"][i],
        # "stock_id": stocks_data["instrument_id"][i],
        "signal": "",
        "lot_pnl": 0,
        "entry_at": 0,
        "exit_at": 0,
    }
    if stats["FIRST_TRADE"]["exit_at"] != 0:
        # data["signal"] = "buy"
        data["signal"] = stats["FIRST_TRADE"]["trade"]
        data["entry_at"] = stats["FIRST_TRADE"]["entry_at"]
        data["exit_at"] = stats["FIRST_TRADE"]["exit_at"]
        data["lot_pnl"] = stats["FIRST_TRADE"]["pnl"]
        statsDF = dictToDF(data)
        netpnl += data["lot_pnl"]
        temp_records = pd.concat([temp_records, statsDF])
    if (stats["SECOND_TRADE"] is not None) and (stats["SECOND_TRADE"]["exit_at"] != 0):
        # data["signal"] = "sell"
        data["signal"] = stats["SECOND_TRADE"]["trade"]
        data["entry_at"] = stats["SECOND_TRADE"]["entry_at"]
        data["exit_at"] = stats["SECOND_TRADE"]["exit_at"]
        data["lot_pnl"] = stats["SECOND_TRADE"]["pnl"]
        statsDF = dictToDF(data)
        netpnl += data["lot_pnl"]
        temp_records = pd.concat([temp_records, statsDF])

    # stock_id = stocks_data["instrument_id"][i]
        # if i % 10 == 0:
        # print("\t\t\t---------------------------------------------------------------------------------------------------------------------: ", i)
        # if stock_id not in absent_stocks:
        #     print(stocks_data["instrument_id"][i])
        #     # date = stocks_data["date"][i]
        #     print(stocks_data["instrument_name"][i])
        #     getDF(stock_id, date)
        #     stats = main()
        # print("Stats in multiple is:")
        # print("Stats: \n")
        # printDict(stats)
        # printDict(stats["FIRST_TRADE"])

    print(temp_records)
    print("overall netpnl :", netpnl)
    # overall netpnl : -226.97000000000074
    print("overall netpnl :", netpnl)
    result_filename = "test101"
    temp_records.to_csv("{}.csv".format(result_filename))
    code_end_time = datetime.datetime.now()
    print("code_start_time: ", code_start_time)
    print("code_end_time: ", code_end_time)
    code_runtime = code_end_time - code_start_time
    print("code runtime: ", code_runtime)
    print("results saved in {}.csv".format(result_filename))

    # if input("open results ? ") == 'y':
    #     os.system('cmd /k "start excel .\\results.csv"')
    # else:
    #     print("start excel .\\results.csv")
    # print("use command \nstart excel results.csv")
    # f.write(stats)


multipleDFs()
# INFY	224	14-05-2020
# AMARAJABAT	395	28-05-2020
#  all zeel
