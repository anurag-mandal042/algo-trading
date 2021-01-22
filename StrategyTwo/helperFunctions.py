# import mysql.connector as mysql
import pymysql.cursors
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
    db_connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD)
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

        print("\n---------------- query in exceptions")
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
        "initial_sl": [Trade["initial_sl"]],
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
                "initial_sl": temp["initial_sl"][i],
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
    temp_records = addPSColumns(temp_records)
    final_filename = os.path.join('final', "final_{}".format(filename))
    temp_records.to_csv(final_filename)


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
    filename = 'id_name_lot_size.csv'
    folder = 'csv'
    path = os.path.join(folder, filename)

    stocks = pd.read_csv(path)

    start_time = datetime.time(hour=9, minute=15)
    end_time = datetime.time(hour=9, minute=30)

    volumes = []
    invalid = []
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
            print("\n---------------- ", stock_index)
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
    # volume_data.to_csv("top_three_for_{}.csv".format(end_date))
    return volume_data


def topThreeStocks(date, top=5):
    day = dayOfTheWeek(date)
    # if today is Saturday or Sunday then go back to Friday
    holidays = ["Saturday", "Sunday"]
    if day not in holidays:
        trading_days, start_date, end_date = tradingDays(date)
        volume_data = cummulativeVolume(
            trading_days, start_date, end_date)
        # top = 5
        print("top {} stocks are\n".format(top), volume_data[0:top])
        return volume_data[0:top]
        # volume_data.to_csv(" volume_data_sorted.csv")
    return None


def getCustomDF(start_date, end_date, start_time, end_time, instrument_id, min_df_len=370, printdf=False):
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
    db_connection = pymysql.connect(host=HOST, user=USER, password=PASSWORD)
    print("Connected to:", db_connection.get_server_info())
    cursor = db_connection.cursor()  # get the cursor
    cursor.execute("use stock_production;")

    query = "SELECT * FROM instrument_details where instrument_id = {} and date(ins_date) >= '{}' and date(ins_date) <= '{}' and time(ins_date) >= '{}'  and time(ins_date) <= '{}' ".format(
        instrument_id, start_date, end_date,  start_time, end_time)
    cursor.execute(query)
    print(query)
    df = pd.DataFrame(cursor.fetchall())
    cursor.close()
    db_connection.close()

    if printdf:
        print(df.info)
        # printArray(exceptions)
    if not df.empty:
        df.columns = ["token_id", "instrument_id", "ins_date", "open", "high",
                      "low", "close", "volume", "status", "created_at", "updated_at"]
        duplicates = len(
            df)-len(df.drop_duplicates(subset=["open", "high", "low", "close"]))
        if duplicates > int(len(df) / 2):
            print("({}) duplicates dropped".format(duplicates))
            invalid_query = "\n{},{}, ({}) duplicates found,{},{}".format(
                instrument_id, query, duplicates, datetime.date.today(), datetime.datetime.now().time())
            f = open("invalid_queries_{}.txt".format("stratTwogetDF"), "a")
            f.write(invalid_query)
            f.close()
            return None

    if df.empty or len(df) < min_df_len:
        invalid_query = "\n{},{},empty or df len ({}) is less than 370,{},{}".format(
            instrument_id, query, len(df), datetime.date.today(), datetime.datetime.now().time())
        f = open("invalid_queries_{}_.txt".format("helper_functions"), "a")
        f.write(invalid_query)
        f.close()
        return None
    elif query in exceptions:

        print("\n---------------- query in exceptions")
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


def topStocks(date, top=5):
    # date = 2020-04-01
    start_date = date - datetime.timedelta(days=30)
    end_date = date

    start_time = '03:45:00'  # converts to 9:15
    end_time = '04:00:00'  # converts to 9:30

    filename = 'id_name_lot_size.csv'
    folder = 'input_csvs'
    path = os.path.join(folder, filename)

    stock_list = pd.read_csv(path)
    averages = []
    current_date_sums = []
    start = stock_list.index.start
    stop = stock_list.index.stop
    # start = 5 # debug
    # stop = 5  # debug
    for i in range(start, stop):
        instrument_id = stock_list["instrument_id"][i]
        lot_size, name = getLotSizeAndName(instrument_id)
        # print(instrument_id) # debug
        temp_df = getCustomDF(end_date, end_date, start_time,
                              end_time, instrument_id, min_df_len=0)
        if temp_df is not None:
            current_date_sum = temp_df['volume'].sum()
            current_date_sums.append(current_date_sum)
            # temp_df.to_csv("temp_df_{}.csv".format(name)) # debug

            df = getCustomDF(start_date, end_date, start_time,
                             end_time, instrument_id, min_df_len=0)
            if df is not None:
                sum = df['volume'].sum()
                averages.append([instrument_id, sum/30, current_date_sum])
                # df.to_csv("df_{}.csv".format(name))   # debug
    df = pd.DataFrame(averages)
    print(df)
    if not df.empty:
        df.columns = ["instrument_id", "average", "todays_total"]
        df["ratio"] = df['todays_total'] / df['average']

        df = df.sort_values(by=['ratio'], ascending=False)
        df = df.reset_index(drop=True)
        # print(df) # debug
        return df[0:top]
    else:
        return None


def getLotSizeAndName(instrument_id):
    filename = 'id_name_lot_size.csv'
    folder = 'input_csvs'
    path = os.path.join(folder, filename)
    df = pd.read_csv(path)
    id_filter = (df["instrument_id"] == instrument_id)

    df = df.loc[id_filter]
    if df.empty:
        print("instrument_id {} not found".format(instrument_id))
        return None, None
    # print(df.index)
    instrument_name = df['instrument_name'].to_string(index=False)
    lot_size = int(df['lot_size'].to_string(index=False))
    return lot_size, instrument_name


def outputToFile(filename, message):
    with open(filename, 'a', encoding='utf-8') as f:
        f.write(message)


def addCostColumn(df, cost_multiplier=0.001):
    df["cost"] = df['entry_at'] * cost_multiplier
    return df


def addPSRPTColumn(df, RPT=10000):
    rpt = [RPT] * len(df.index)
    df["RPT"] = rpt
    return df


def addPSDifferenceColumn(df):
    df["diff"] = abs(df["entry_at"] - df["initial_sl"])
    return df


def addPositionSizeColumn(df):
    df["PS"] = df["RPT"] / df["diff"]
    return df


def addPSPNLColumn(df):
    df["PS_PNL"] = df['PS'] * df['pnl']
    return df


def addPSCostColumn(df):
    df['PS_cost'] = df["cost"] * df['PS']
    return df


def addPSPNLAfterCostColumn(df):
    df['PS_pnl_a_cost'] = df['PS_PNL'] - df['PS_cost']
    return df


def addCummulativePNLColumn(df):
    # incomplete funtion
    df['PS_c_pnl'][0] = df["PS_pnl_a_cost"][0]
    # for i in df.index+1:
    # df['c_pnl'][i] = df['c_pnl'][i] + df['c_pnl'][i-1]


def addPSColumns(df, cost_multiplier=0.001, RPT=10000):
    df = addCostColumn(df, cost_multiplier)
    df = addPSRPTColumn(df, RPT)
    df = addPSDifferenceColumn(df)
    df = addPositionSizeColumn(df)
    df = addPSPNLColumn(df)
    df = addPSCostColumn(df)
    df = addPSPNLAfterCostColumn(df)
    # print(df) # debug
    return df


def addLSLotCostColumn(df):
    df["LS_lot_cost"] = df['cost'] * df["lot_size"]
    return df


def addLSPNLColumn(df):
    df["LS_pnl"] = df["lot_size"]*df['pnl']
    return df


def addLSPNLAfterCost(df):
    df['LS_pnl_after_cost'] = df['LS_pnl'] - df['LS_lot_cost']
    return df


def addLSColumns(df, cost_multiplier=0.001):
    df = addCostColumn(df, cost_multiplier)
    df = addLSLotCostColumn(df)
    df = addLSPNLColumn(df)
    df = addLSPNLAfterCost(df)
    return df
