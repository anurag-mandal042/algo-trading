import mysql.connector as mysql
import pandas as pd
from datetime import timedelta
import datetime
import os
from Trade import Trade
from invalid import exceptions

data_object_clone_0 = {
    "active": "-----",
    "trade": "-----",
    "entry_at": "-----",
    "exit_at": "-----",
    "stop_loss": "-----",
    "stop_loss_hit": "-----",
    "pnl": "-----",
    "volume": "-----",
    "timestamp": "-----",
}


def getDF(instrument_id, date, min_df_len=370, printdf=False):
    '''
    Input Parameters:
        instrument_id: to fetch data from mysql
        date: to fetch data from mysql
        min_df_len: it is a parameter to check validity of df returned from mysql, if len(df) < min_df_len then None is returned
        printdf: to print df returned from mysql        
        optional-parameters:
            min_df_len *default=370
            printdf *default=false
    Returns df
    '''
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

    start_time = '03:45:00'
    end_time = '04:00:00'
    query = "SELECT * FROM instrument_details where date(ins_date) = '{}' and instrument_id = {} and time(ins_date) >= '{}'  and time(ins_date) <= '{}' ".format(
        date, instrument_id, start_time, end_time)
    cursor.execute(query)
    print(query)
    df = pd.DataFrame(cursor.fetchall())
    cursor.close()
    db_connection.close()

    if printdf:
        print(df.info)
        # printArray(exceptions)

    if df.empty or len(df) < min_df_len:
        invalid_query = "\n{},{},empty or df len ({}) is less than 370,{},{}".format(
            instrument_id, query, len(df), datetime.date.today(), datetime.datetime.now().time())
        f = open("invalid_queries_{}_.txt".format("helper_functions"), "a")
        f.write(invalid_query)
        f.close()
        return None
    elif query in exceptions:

        print("\n\n\t\t\t\tquery in exceptions")
        invalid_query = "\n{},{},query in exceptions,{},{}".format(
            instrument_id, query, datetime.date.today(), datetime.datetime.now().time())
        f = open("invalid_queries_{}_.txt".format("helper_functions"), "a")
        f.write(invalid_query)
        f.close()
        return None
    else:
        df.columns = ['id', 'instrument_id', 'ins_date', 'open', 'high',
                      'low', 'close', 'volume', 'status', 'created_at', 'updated_at']
        df = addFinalDateTimeColumns(df)
    return df


def dictToDF(dict):
    '''
    Converts Dictinary into Pandas Data Frame
    returns DataFrame object
    '''
    data = {}
    for key, value in zip(dict.keys(), dict.values()):
        data[key] = [value]
    df = pd.DataFrame(data)
    return df


def printDict(dictionary):
    if dictionary is not None:
        for values, key in zip(dictionary.values(), dictionary.keys()):
            if type(values) == dict:
                print(key)
                printDict(values)
                continue
            print("\t", key, values)


def printArray(array):
    for i in range(0, len(array)):
        print("\nElement: ", i)
        if type(array[i]) == dict:
            printDict(array[i])
        else:
            print(array[i])


def searchTime(df, hour, minute):
    '''
    Input Parameters:
        df: pandas.DataFrame object, hour and minute: int
        Returns candle index according to time input
    '''
    for i in df.index:
        if df["Final_Time"][i] == datetime.time(hour=hour, minute=minute):
            return i


def nineFifteenCandleIndex(df):
    '''
        Returns 9:15 candle index
    '''
    for i in df.index:
        if df["Final_Time"][i] == datetime.time(hour=9, minute=15):
            return i


def getCandles(minutes, start_time=datetime.time(hour=9, minute=15), end_time=datetime.time(hour=15, minute=30)):
    '''
    Input parameters:
            minutes (int)
            start_time and end_time datetime.time objects
    Returns:
            array of candle start times
    *** Default value:
            for start_time: 09:15
            for end_time: 15:15
    '''
    candles = []
    time = start_time
    hr = start_time.hour
    mins = start_time.minute
    while time <= end_time:
        candles.append(time)
        mins += minutes
        if mins >= 60:
            hr += 1
            mins = 00
        time = datetime.time(hour=hr, minute=mins)
    return candles


def cleanDF(df, minutes):
    df = addFinalDateTimeColumns(df)
    id = df["id"][0]
    instrument_id = df["instrument_id"][0]
    date = df['Final_Date'][0]

    candle_close = datetime.time(hour=9, minute=30)
    candles = getCandles(minutes)
    groups = []
    for i in range(len(candles)-1):
        filt = (df['Final_Time'] >= candles[i]) & (
            df["Final_Time"] < candles[i+1])
        groups.append(df.loc[filt])

    final_df = []
    for i in range(len(groups)):
        df = groups[i]
        open_ = df.head(1)
        close_ = df.tail(1)

        o = float(open_['open'])
        h = float(df['high'].max())
        l = float(df['low'].min())
        c = float(close_['close'])
        v = float(df['volume'].sum())
        final_time = candles[i]
        final_df.append([id, instrument_id, date, o, h, l, c, v, final_time])
    df = pd.DataFrame(final_df)
    df.columns = ["id", "instrument_id", "date", "open",
                  "high",	"low", "close", "volume", "Final_Time"]
    # print(df)
    return df


def convert(df, minutes, save_file=False, open_file=False):
    '''
    function to convert df in specified time period
    parameters:
            df: dataframe to be converted
            minutes: time period (3 or 15)
            save_file: to save csv file after converting (True  /False)
            open_file: to open csv file after converting (True  /False)
    *** Warning: If file is never saved then it will be saved first and then opened ***
    '''
    start_at = nineFifteenCandleIndex(df)
    opens = []
    close = []
    high = []
    low = []
    # final_datetime = []
    # final_date = []
    final_time = []
    volume = []
    print(df)
    # print(start_at)
    for i in range(start_at, df.index.stop, minutes):
        #       0       15       30       45       60       75       90       105       120       135
        if i+minutes in df.index:
            # 0 to 3 | 4 to 7 | 8 to 11 ...
            closed_at = df["close"][i + minutes - 1]
        else:
            closed_at = df["close"][df.index.stop - 1]
        if i+minutes in df.index:
            min_low = min(df["low"][i: i + minutes])
            max_high = max(df["high"][i: i + minutes])
            volume.append(sum(df["volume"][i: i + minutes]))
        else:
            # ------------ if index not multiple of minutes
            min_low = min(df["low"][i: i+df.index.stop])
            max_high = max(df["high"][i: i + df.index.stop])
            volume.append(sum(df["volume"][i: i + df.index.stop]))

        opens.append(df["open"][i])
        # final_datetime.append(df["Final_DateTime"][i])
        # final_date.append(df['Final_Date'][i])
        final_time.append(df['Final_Time'][i])
        close.append(closed_at)
        high.append(max_high)
        low.append(min_low)
        i += 3
    data = {
        'open': opens,
        'high': high,
        'low': low,
        'close': close,
        'Final_DateTime': final_datetime,
        'Final_Date': final_date,
        'Final_Time': final_time,
        'volume': volume,
    }
    converted_minutes_df = pd.DataFrame(data=data)
    # print(converted_minutes_df)

    if minutes == 3:
        file_name = "csv_Data\{}_3_minutes.csv".format('temp')
    elif minutes == 15:
        file_name = "csv_Data\{}_15_minutes.csv".format('temp')
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


def recordTrade(RECORDS, Trade):
    '''
        Records Trade
            input:
                RECORDS (pd.DataFrame)
                Trade (Trade.Trade)
    '''
    tempTrade = pd.DataFrame(data={
        "instrument_id": [Trade["instrument_id"]],
        "instrument_name": [Trade["instrument_name"]],
        "date": [Trade["date"]],
        "trade": [Trade["trade"]],
        "entry_at": [Trade["entry_at"]],
        "active": [Trade["active"]],
        "stop_loss": [Trade["stop_loss"]],
        "intial_stop_loss": [Trade["intial_stop_loss"]],
        "stop_loss_hit": [Trade["stop_loss_hit"]],
        "exit_at": [Trade["exit_at"]],
        "pnl": [Trade["pnl"]],
        "volume": [Trade["volume"]],
        "timestamp": [Trade["timestamp"]],
        "lot_size": [Trade["lot_size"]],
    })
    RECORDS = pd.concat([RECORDS, tempTrade])
    return RECORDS


def convertToFinalCSV(filename):
    # filename = "output_15_min_candle_historic.csv"
    temp = pd.read_csv(filename)
    temp_records = pd.DataFrame()
    for i in temp.index:
        if temp["exit_at"][i] != 0:
            temp_dict = {
                'instrument_id': temp["instrument_id"][i],
                'instrument_name': temp["instrument_name"][i],
                'date': temp["date"][i],
                'trade': temp["trade"][i],
                'entry_at': temp["entry_at"][i],
                'active': temp["active"][i],
                "intial_stop_loss": temp["intial_stop_loss"][i],
                'stop_loss': temp["stop_loss"][i],
                'stop_loss_hit': temp["stop_loss_hit"][i],
                'exit_at': temp["exit_at"][i],
                'pnl': temp["pnl"][i],
                'volume': temp["volume"][i],
                'timestamp': temp["timestamp"][i],
                'lot_size': temp["lot_size"][i],
            }
            temp_df = dictToDF(temp_dict)
            temp_records = pd.concat([temp_records, temp_df])

    temp_records.to_csv("final_{}".format(filename))


def dayOfTheWeek(date):
    if date.weekday() == 0:
        return "Monday"
    elif date.weekday() == 1:
        return "Tuesday"
    elif date.weekday() == 2:
        return "Wednesday"
    elif date.weekday() == 3:
        return "Thursday"
    elif date.weekday() == 4:
        return "Friday"
    elif date.weekday() == 5:
        return "Saturday"
    elif date.weekday() == 6:
        return "Sunday"


def daysInMonth(date):
    thirty = [1, 3, 5, 7, 8, 10, 12]
    thirtyOne = [4, 6, 9, 11]
    if date.month == 2:
        return 28
    elif date.month in thirty:
        return 30
    elif date.month in thirtyOne:
        return 31


def tradingDays(date):
    print("in workingDays")
    trading_days = []
    holidays = [datetime.date(year=2020, month=12, day=25)]
    to_date = date
    from_date = to_date - timedelta(days=30)
    print("starting date", from_date)
    print("ending date", to_date)
    weekends = ["Saturday", "Sunday"]
    date = from_date
    while date != to_date:
        if dayOfTheWeek(date) not in weekends and date not in holidays:
            trading_days.append(date)
        date = date + timedelta(days=1)

    trading_days.append(to_date)
    # print(trading_days)
    return trading_days, from_date, to_date


def cummulativeVolume(trading_days, start_date, end_date):
    stocks = pd.read_csv("stocks_data.csv")

    start_time = datetime.time(hour=9, minute=15)
    end_time = datetime.time(hour=9, minute=30)

    volumes = []
    invalid = []
    # [1, 2, 3, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 17, 19, 21, 22, 24, 26, 27, 28, 31, 32, 34, 35, 37, 39, 40, 43, 45, 46, 48, 50, 51, 52, 53, 54, 56, 58, 59, 60, 61, 62, 63, 64, 66, 67, 68, 69, 70, 71, 72, 73, 76, 77, 78, 79, 80, 82, 83, 84, 85, 87, 88, 89, 91, 92, 93, 95, 96, 97, 98, 99, 100, 102, 103, 104, 105, 107, 108,
    #  109, 111, 113, 114, 115, 116, 117, 118, 120, 121, 123, 125, 126, 127, 128, 129, 131, 132, 133, 134, 135, 137, 138, 139, 140, 141, 142, 145, 147, 149, 150, 152,
    #  153, 154, 156, 157, 158, 161, 162, 163, 167, 169, 170, 171, 173, 175, 176, 178, 180, 181, 183, 184, 185, 186, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197,
    #  198, 200, 202, 203, 204, 205, 206, 207, 208, 210, 211, 212, 213, 214, 215, 217, 218, 219, 220, 221, 223, 227, 229, 230, 231, 232, 233, 234, 235, 237, 238, 239,
    #  241, 242, 244, 245, 246, 247, 251, 252, 254, 255, 258, 259, 262, 263, 264, 265, 266, 267, 268, 269, 270, 272, 273, 276, 277, 278, 280, 281, 283, 284, 285, 286,
    #  287, 288, 289, 291, 292, 293, 294, 296, 297, 300, 301, 302, 303, 306, 307, 310, 311, 312, 313, 314, 315, 317, 319, 320, 321, 322, 323, 324, 327, 331, 332, 333,
    #  334, 335, 336, 337, 338, 339, 340, 341, 342, 345, 346, 349, 350, 356, 358, 359, 360, 362, 363, 366, 367, 368, 371, 373, 374, 377, 378, 380, 381, 382, 383, 385,
    #  387, 388, 391, 392, 394, 396, 397, 398, 399, 400, 402, 404, 406, 407, 408, 409, 410, 411, 413]
    name = ""
    # calculate past_30_days_volume
    print("calculating past_30_days_volume")
    for stock_index in stocks.index:
        id = stocks["instrument_id"][stock_index]
        if id not in invalid:
            name = stocks["instrument_name"][stock_index]
            volume = 0
            for date in trading_days:
                df = getDF(id, date, min_df_len=0)
                if df is not None:
                    for j in df.index:
                        if df["Final_Time"][j] >= start_time and df["Final_Time"][j] < end_time:
                            volume += df["volume"][j]
                        if df["Final_Time"][j] > end_time:
                            break
                    # df.to_csv("./new/{}_{}_{}.csv".format(name, id, date))
                else:
                    f = open("df_none.txt", "a")
                    f.write("\ndf is None,{},{},{}".format(
                        name, start_date, end_date))
                    f.close()
                    print("df is None")
                    # invalid.append(id)
                    # print(invalid)
                    # break
            volumes.append([id, name, volume, start_date, end_date])
        print("\t\t{} done\n".format(name))
        if stock_index % 20 == 0:
            print("\n\t\t\t\t\t\t\t\t\t ", stock_index)
        # if stock_index == 15:
        #     break

    # convert volumes array to df
    volume_data = pd.DataFrame(volumes)
    # add column names to df
    volume_data.columns = ["instrument_id",
                           "instrument_name", "past_30_days_volume", "start_date", "end_date"]
    # remove data which have volume = 0 i.e invalid data
    volume_data = volume_data[
        volume_data["past_30_days_volume"] != 0]
    # find average of past 30 days
    volume_data['average_volume'] = pd.DataFrame(
        volume_data['past_30_days_volume']/30)

    # calculate todays_volume
    todays_volume = []
    print("calculating todays_volume")
    for stock_index in volume_data.index:
        print("in hf.outer")
        id = volume_data["instrument_id"][stock_index]
        name = volume_data["instrument_name"][stock_index]
        volume = 0
        date = volume_data["end_date"][stock_index]
        df = getDF(id, date, min_df_len=0)
        if df is not None:
            for j in df.index:
                print("in hf.inner")
                if df["Final_Time"][j] >= start_time and df["Final_Time"][j] < end_time:
                    volume += df["volume"][j]
                if df["Final_Time"][j] > end_time:
                    break
        else:
            f = open("df_none.txt", "a")
            f.write("\ndf is None in todays volume calculations,{},{},{}".format(
                name, start_date, end_date))
            f.close()
            print("df is None in todays volume calculations")
            # invalid.append(id)
            # print(invalid)
            # break
        todays_volume.append(volume)
    volume_data["todays_volume"] = todays_volume
    final_volume = []
    # compare past_30_days average volume with todays volume
    print("comparing past_30_days average volume with todays volume")
    for i in volume_data.index:
        final_volume.append(volume_data["todays_volume"][i] /
                            volume_data["average_volume"][i])
    volume_data["final_volume"] = final_volume
    # sort max volume index first
    print("sorting max volume index first")
    volume_data = volume_data.sort_values(by=[
        'final_volume'], ascending=False)
    volume_data.to_csv("top_three_for_{}.csv".format(end_date))
    return volume_data


def topThreeStocks(date, top=5):
    today = datetime.date.today()
    today = date
    todaysDay = dayOfTheWeek(today)
    # if today is Saturday or Sunday then go back to Friday
    if todaysDay == "Saturday":
        today = today - timedelta(days=1)
    elif todaysDay == "Sunday":
        today = today - timedelta(days=2)

    trading_days, start_date, end_date = tradingDays(today)
    volume_data = cummulativeVolume(
        trading_days, start_date, end_date)
    # top = 5
    print("top {} stocks are\n".format(top), volume_data[0:top])
    return volume_data[0:top]
    # volume_data.to_csv(" volume_data_sorted.csv")


def getLotSizeAndName(instrument_id):
    df = pd.read_csv("id_name_lot_size.csv")
    id_filter = (df["instrument_id"] == instrument_id)

    df = df.loc[id_filter]
    if df.empty:
        print("instrument_id {} not found".format(instrument_id))
        return None
    print(df.index)
    instrument_name = df['instrument_name'].to_string(index=False)
    lot_size = int(df['lot_size'].to_string(index=False))
    return lot_size, instrument_name


# getLotSizeAndName(376)
# convertToFinalCSV("output_5_min_candle_historic_data.csv")
# print("Done")
# convertToFinalCSV("output_10_min_candle_historic_data.csv")
# print("Done")
# convertToFinalCSV("output_15_min_candle_historic_data.csv")
# print("Done")
# convertToFinalCSV("output_30_min_candle_historic_data.csv")
# print("Done")

# topThreeStocks()
