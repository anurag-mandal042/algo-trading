import os
import mysql.connector as mysql
from datetime import timedelta
import datetime
import pandas as pd


# global vairables
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


RECORDS = pd.DataFrame()
df = pd.DataFrame()
stats = {
    "net_pnl": 0,
    "FIRST_TRADE_trigger_candle_index": 0,
    "FIRST_TRADE_close_candle_index": 0,
    "SECOND_TRADE_trigger_candle_index": 0,
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


def reset():
    global stats, buy_trade, sell_trade, FIRST_TRADE, SECOND_TRADE

    stats = {
        "net_pnl": 0,
        "FIRST_TRADE_trigger_candle_index": 0,
        "FIRST_TRADE_close_candle_index": 0,
        "SECOND_TRADE_trigger_candle_index": 0,
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
    invalid_queries = [
        # all these trades closed at end of day
        "SELECT * FROM instrument_details where date(ins_date) = '2020-05-14' and instrument_id = 127",
        "SELECT * FROM instrument_details where date(ins_date) = '2020-05-14' and instrument_id = 186",
        "SELECT * FROM instrument_details where date(ins_date) = '2019-02-08' and instrument_id = 44",
        "SELECT * FROM instrument_details where date(ins_date) = '2019-02-22' and instrument_id = 187",
        "SELECT * FROM instrument_details where date(ins_date) = '2019-03-18' and instrument_id = 174",
        "SELECT * FROM instrument_details where date(ins_date) = '2019-08-20' and instrument_id = 159",
        "SELECT * FROM instrument_details where date(ins_date) = '2019-12-31' and instrument_id = 369",
        "SELECT * FROM instrument_details where date(ins_date) = '2020-05-14' and instrument_id = 314",
        "SELECT * FROM instrument_details where date(ins_date) = '2018-03-13' and instrument_id = 38",
        "SELECT * FROM instrument_details where date(ins_date) = '2018-04-09' and instrument_id = 277",
        "SELECT * FROM instrument_details where date(ins_date) = '2018-04-30' and instrument_id = 228",
        "SELECT * FROM instrument_details where date(ins_date) = '2018-05-22' and instrument_id = 197"
    ]
    if query not in invalid_queries:
        cursor.execute(query)
        print(query)
        temp_df = pd.DataFrame(cursor.fetchall())
        # print(temp_df)
        if temp_df.empty or temp_df.index.stop < 300:
            print("\t\t***df has less than 300 entries, hence cannot process".upper())
            return None
        else:
            temp_df.columns = ['id', 'instrument_id', 'ins_date', 'open', 'high',
                               'low', 'close', 'volume', 'status', 'created_at', 'updated_at']
            temp_df = addFinalDateTimeColumns(temp_df)
            global df
            df = temp_df
            return temp_df
    else:
        return None


def addFinalDateTimeColumns(df):
    df['Final_DateTime'] = pd.to_datetime(
        df['ins_date']) + timedelta(seconds=0, minutes=30, hours=5)
    df['Final_Date'] = pd.to_datetime(df['Final_DateTime']).dt.date
    df['Final_Time'] = pd.to_datetime(df['Final_DateTime']).dt.time
    return df


def nineFifteenCandleIndex():
    '''
        returns 9:15 candle index
    '''
    for i in df.index:
        if df["Final_Time"][i] == datetime.time(hour=9, minute=15):
            stats["nine_fifteen_candle_index"] = i
            break
    date = df["Final_Date"][0]
    print("\nDate of stock is: ", date)
    return stats["nine_fifteen_candle_index"]


def printDictionary(dictionary):
    for values, key in zip(dictionary.values(), dictionary.keys()):
        if type(values) == dict:
            print(key)
            printDictionary(values)
            continue
        print("\t", key, values)


def triggeredTrade(df):
    # ------------ 9:15 + 3 = 9:18
    global buy_trade, sell_trade
    start_index = stats["nine_fifteen_candle_index"] + 3
    multiple = 0
    for i in range(start_index, df.index.stop):
        time = df["Final_Time"][i]
        remainder = time.minute % 3
        # ------------ step 2: check which order is triggered
        # ------------ BUY TRADE
        if df["high"][i] > buy_trade['entry_at']:
            # ------------ if buy order triggered cancel sell order
            buy_trade["active"] = True
            trigger_candle_index = i
            candle_start = trigger_candle_index - remainder
            # ------------------------ set stop loss to low of triggered candle (3 minutes candle)
            if remainder == 0:
                buy_trade['stop_loss'] = float(min(
                    df["low"][trigger_candle_index:trigger_candle_index+3])) - 0.05
            if remainder == 1:
                buy_trade['stop_loss'] = float(min(
                    df["low"][trigger_candle_index-1:trigger_candle_index+2])) - 0.05
            if remainder == 2:
                buy_trade['stop_loss'] = float(min(
                    df["low"][trigger_candle_index-2:trigger_candle_index+1])) - 0.05
            buy_trade['timestamp'] = df["Final_Time"][trigger_candle_index]
            return buy_trade, trigger_candle_index
        # ------------ SELL TRADE
        elif df["low"][i] < sell_trade['entry_at']:
            # ------------ else buy order triggered cancel sell order
            sell_trade["active"] = True
            trigger_candle_index = i
            candle_start = trigger_candle_index - remainder
            # ------------------------ set stop loss to high of triggered candle (3 minutes candle)
            if remainder == 0:
                sell_trade['stop_loss'] = float(max(
                    df["high"][trigger_candle_index:trigger_candle_index+3])) + 0.05
            if remainder == 1:
                sell_trade['stop_loss'] = float(max(
                    df["high"][trigger_candle_index-1:trigger_candle_index+2])) + 0.05
            if remainder == 2:
                sell_trade['stop_loss'] = float(max(
                    df["high"][trigger_candle_index-2:trigger_candle_index+1])) + 0.05
            sell_trade['timestamp'] = df["Final_Time"][trigger_candle_index]
            return sell_trade, trigger_candle_index
        multiple += 1


def recordTrade(Trade):
    global RECORDS
    tempTrade = pd.DataFrame(data={
        "trade": [Trade["trade"]],
        "entry_at": [Trade["entry_at"]],
        "active": [Trade["active"]],
        "stop_loss": [Trade["stop_loss"]],
        "stop_loss_hit": [Trade["stop_loss_hit"]],
        "exit_at": [Trade["exit_at"]],
        "pnl": [Trade["pnl"]],
        "volume": [Trade["volume"]],
        "timestamp": [Trade["timestamp"]],
    })
    RECORDS = pd.concat([RECORDS, tempTrade])


def addPnL():
    global FIRST_TRADE, SECOND_TRADE
    if SECOND_TRADE is None:
        stats["FIRST_TRADE"] = trade = FIRST_TRADE
    else:
        stats["SECOND_TRADE"] = trade = SECOND_TRADE
    if trade["trade"] == "buy":
        #     # Buy ke case mein exit - entry
        # printDictionary(trade)
        pnl = trade["pnl"] = (float(
            trade["exit_at"]) - float(trade["entry_at"]))
    elif trade["trade"] == "sell":
        #     # Sell ke case mein entry - exit
        # printDictionary(trade)
        pnl = trade["pnl"] = (float(
            trade["entry_at"]) - float(trade["exit_at"]))
    stats["net_pnl"] += pnl
    # print("\nTrade closed")
    # print("Trade details:")
    return trade


def processTrade(df, trade, start_index, step):
    '''
    After running triggeredTrade function, pass the df into processTrade function to find if the trade made profit or loss.
    Pass the following parameters:
        df: DataFrame to loop
        trade: current trade
        start_index: staring index of loop
        step: step of each loop
    Function returns: index of trade close candle
    '''
    p50, p75, p100 = calculateProfitMarks(trade=trade)
    # ------------ step 3: check is stop loss hit
    # ------------ if stop loss hit then close trade
    #
    # ------------ if trade is sell check stoploss hit and profit targets,
    # ------------ if hit then break loop and check profit or loss
    if trade["trade"] == 'sell':
        for i in range(start_index, df.index.stop, step):
            # print(df["Final_Time"][i], datetime.time(hour=9, minute=15))
            if df["Final_Time"][i] == datetime.time(hour=15, minute=10):
                trade["stop_loss_hit"] = True
                trade["exit_at"] = df["high"][i]
                trade["timestamp"] = df["Final_Time"][i]
                trade = addPnL()
                recordTrade(trade)
                return i
            if df["high"][i] >= trade["stop_loss"]:
                trade["stop_loss_hit"] = True
                trade["exit_at"] = trade["stop_loss"]
                trade['timestamp'] = df["Final_Time"][i]
                trade = addPnL()
                recordTrade(trade)
                return i
            elif df['low'][i] <= p100:
                # print("p100 hit", p100)
                # print("low = ", df['low'][i], df['Final_Time'][i])
                trade["exit_at"] = p100
                trade['timestamp'] = df["Final_Time"][i]
                trade = addPnL()
                recordTrade(trade)
                return i
            elif df['low'][i] <= p75 and trade["stop_loss"] > p75:
                # print("p75 hit", p75)
                # print("low = ", df['low'][i], df['Final_Time'][i])
                trade["stop_loss"] = p50
                trade['timestamp'] = df["Final_Time"][i]
                recordTrade(trade)
            elif df['low'][i] <= p50 and trade["stop_loss"] > p50:
                # print("p50 hit", p50)
                # print("low = ", df['low'][i], df['Final_Time'][i])
                trade["stop_loss"] = trade["entry_at"]
                trade['timestamp'] = df["Final_Time"][i]
                recordTrade(trade)
        # ------------ if trade is buy check stoploss hit and profit targets,
        # ------------ if hit then break loop and check profit or loss
    elif trade["trade"] == 'buy':
        for i in range(start_index, df.index.stop, step):
            if df["Final_Time"][i] == datetime.time(hour=15, minute=10):
                trade["stop_loss_hit"] = True
                trade["exit_at"] = df["low"][i]
                trade["timestamp"] = df["Final_Time"][i]
                trade = addPnL()
                recordTrade(trade)
                return i
            if df["low"][i] <= trade["stop_loss"]:
                trade["stop_loss_hit"] = True
                trade["exit_at"] = trade["stop_loss"]
                trade['timestamp'] = df["Final_Time"][i]
                trade = addPnL()
                recordTrade(trade)
                return i
            elif df['high'][i] >= p100:
                # print("p100 hit", p100)
                # print("high = ", df['high'][i], df['Final_Time'][i])
                trade["exit_at"] = p100
                trade['timestamp'] = df["Final_Time"][i]
                trade = addPnL()
                recordTrade(trade)
                return i
            elif df['high'][i] >= p75 and trade["stop_loss"] < p75:
                # print("p75 hit", p75)
                # print("high = ", df['high'][i], df['Final_Time'][i])
                trade["stop_loss"] = p50
                trade['timestamp'] = df["Final_Time"][i]
                recordTrade(trade)
            elif df['high'][i] >= p50 and trade["stop_loss"] < p50:
                # print("p50 hit", p50)
                # print("high = ", df['high'][i], df['Final_Time'][i])
                trade["stop_loss"] = trade["entry_at"]
                trade['timestamp'] = df["Final_Time"][i]
                recordTrade(trade)


def calculateProfitMarks(trade):
    entry_price = trade["entry_at"]
    if trade["trade"] == "buy":
        # ------------- set profit values
        p50 = float(entry_price) * 1.0050
        p75 = float(entry_price) * 1.0075
        p100 = float(entry_price) * 1.0100
    elif trade["trade"] == "sell":
        # ------------- set profit values
        p50 = float(entry_price) * 0.9950
        p75 = float(entry_price) * 0.9925
        p100 = float(entry_price) * 0.99
    return p50, p75, p100


def dictionaryToDF(dict):
    data = {}
    for key, value in zip(dict.keys(), dict.values()):
        data[key] = [value]
    df = pd.DataFrame(data)
    return df


def main():
    global df, stats, RECORDS, buy_trade, sell_trade, FIRST_TRADE, SECOND_TRADE
    # print(df)
    nine_fifteen_candle_index = nineFifteenCandleIndex()
    f_c_high = max(
        df["high"][nine_fifteen_candle_index:nine_fifteen_candle_index + 3])
    f_c_low = min(
        df['low'][nine_fifteen_candle_index:nine_fifteen_candle_index + 3])

    # ------------ step 1: place buy and sell order
    # ------------ buy order at high + 5 paise (0.05 Rs)
    # ------------ low order at low - 5 paise (0.05 Rs)
    buy_trade['entry_at'] = float(f_c_high) + 0.05
    sell_trade['entry_at'] = float(f_c_low) - 0.05
    # ------------ example:
    # --------------- first 3 min candle high = 180.65
    # --------------- first 3 min candle low = 120.35
    # --------------- then
    # ------------------- place buy order at 180.7
    # ------------------- place sell order at 120.3

    start_time = df["Final_Time"][nine_fifteen_candle_index]
    buy_trade['timestamp'] = start_time
    sell_trade['timestamp'] = start_time

    # ------------ ACTUAL MAIN FUNCTION STARTS HERE

    recordTrade(buy_trade)
    recordTrade(sell_trade)

    # ------------ first trade is set to buy or sell
    FIRST_TRADE, FIRST_TRADE_trigger_candle_index = triggeredTrade(df)
    stats["FIRST_TRADE"] = FIRST_TRADE
    stats["FIRST_TRADE_trigger_candle_index"] = FIRST_TRADE_trigger_candle_index
    recordTrade(FIRST_TRADE)
    # ------------ calculate profit marks
    p50, p75, p100 = calculateProfitMarks(trade=FIRST_TRADE)
    # ------------ get index of trigger_candle
    trigger_candle_index = stats["FIRST_TRADE_trigger_candle_index"]
    # ------------ process FIRST_TRADE
    stats["FIRST_TRADE_close_candle_index"] = processTrade(df, trade=FIRST_TRADE,
                                                           start_index=trigger_candle_index, step=1)

    # ------------ POTENTIAL SECOND TRADE STARTS
    # ------------ check if first trade closed because of stop loss hit
    # ------------ and then check if loss incured
    if (FIRST_TRADE["stop_loss_hit"]) and (FIRST_TRADE["pnl"] < 0):
        FIRST_TRADE["active"] = False
        print("SECOND_TRADE TRIGGERED")
        trigger_candle_index = stats["FIRST_TRADE_close_candle_index"]

        second_trade_trigger_timestamp = df["Final_Time"][trigger_candle_index]
        remainder = second_trade_trigger_timestamp.minute % 3
        if remainder == 0:
            sell_trade_stop_loss = float(df["high"][trigger_candle_index])+0.05
            buy_trade_stop_loss = float(df["low"][trigger_candle_index])-0.05
        else:
            candle_start = trigger_candle_index - remainder
            sell_trade_stop_loss = float(max(
                df["high"][candle_start:  trigger_candle_index]))+0.05
            buy_trade_stop_loss = float(min(
                df["low"][candle_start:  trigger_candle_index]))-0.05

        # if yes i.e stop loss hit and loss incured then open new trade
        if FIRST_TRADE["trade"] == "buy":
            # ------------ if closed trade was buy then open new sell trade
            sell_trade = {
                "trade": "sell",
                'entry_at': FIRST_TRADE["exit_at"],
                'exit_at': 0,
                "active": False,
                "stop_loss": sell_trade_stop_loss,
                "stop_loss_hit": False,
                "pnl": 0,
                "volume": FIRST_TRADE["volume"],
                "timestamp": FIRST_TRADE["timestamp"],
            }
            SECOND_TRADE = sell_trade
        elif FIRST_TRADE["trade"] == "sell":
            # ------------ if closed trade was sell then open new buy trade
            buy_trade = {
                "trade": "buy",
                'entry_at': FIRST_TRADE["exit_at"],
                'exit_at': 0,
                "active": False,
                "stop_loss": buy_trade_stop_loss,
                "stop_loss_hit": False,
                "pnl": 0,
                "volume": FIRST_TRADE["volume"],
                "timestamp": FIRST_TRADE["timestamp"],
            }
            SECOND_TRADE = buy_trade

        # ------------ by now second trade setup done
        recordTrade(SECOND_TRADE)
        trigger_candle_index = stats["FIRST_TRADE_close_candle_index"]

        stats["SECOND_TRADE"] = SECOND_TRADE
        # ------------ process SECOND_TRADE
        stats["SECOND_TRADE_close_candle_index"] = processTrade(df, trade=SECOND_TRADE,
                                                                start_index=trigger_candle_index, step=1)

        print("Order completed at ", df["Final_Time"]
              [stats["SECOND_TRADE_close_candle_index"]])
    # recordTrade.CSV contains stepwise trades of all the stocks COMPILED
    RECORDS.to_csv("recordTrade.csv")
    return stats


def multipleDFs():
    global df, FIRST_TRADE, SECOND_TRADE
    stocks_data = pd.read_csv("b.csv")
    temp_records = pd.DataFrame()
    netpnl = 0
    risk = 1000
    for i in stocks_data.index:
        reset()
        stock_id = stocks_data["instrument_id"][i]
        if stocks_data.index.stop >= 50 and i % 10 == 0:
            print("\t\t\t---------------------------------------------------------------------------------------------------------------------: ", i)

        print(stocks_data["instrument_id"][i])
        print(stocks_data["instrument_name"][i])
        date = stocks_data["date"][i]
        # date = '2020-01-01'
        temp_df = getDF(stock_id, date)
        if temp_df is not None:
            global df
            df = temp_df
            stats = main()
            data = {
                "date": date,
                "stock_name": stocks_data["instrument_name"][i],
                "stock_id": stocks_data["instrument_id"][i],
                "signal": "",
                "lot_pnl": 0,
                "entry_at": 0,
                "exit_at": 0,
                "risk": risk,
                "stop_loss": 0,
            }
            # printDictionary(stats)
            if stats["FIRST_TRADE"]["exit_at"] != 0:
                data["signal"] = stats["FIRST_TRADE"]["trade"]
                data["entry_at"] = stats["FIRST_TRADE"]["entry_at"]
                data["exit_at"] = stats["FIRST_TRADE"]["exit_at"]
                data["lot_pnl"] = stats["FIRST_TRADE"]["pnl"]
                data["stop_loss"] = abs(
                    stats["FIRST_TRADE"]["entry_at"] - stats["FIRST_TRADE"]["stop_loss"])
                statsDF = dictionaryToDF(data)
                netpnl += data["lot_pnl"]
                temp_records = pd.concat([temp_records, statsDF])
            if (SECOND_TRADE is not None) and (stats["SECOND_TRADE"]["exit_at"] != 0):
                data["signal"] = stats["SECOND_TRADE"]["trade"]
                data["entry_at"] = stats["SECOND_TRADE"]["entry_at"]
                data["exit_at"] = stats["SECOND_TRADE"]["exit_at"]
                data["lot_pnl"] = stats["SECOND_TRADE"]["pnl"]
                data["stop_loss"] = abs(float(
                    stats["SECOND_TRADE"]["entry_at"]) - stats["SECOND_TRADE"]["stop_loss"])
                statsDF = dictionaryToDF(data)
                netpnl += data["lot_pnl"]
                temp_records = pd.concat([temp_records, statsDF])
    print(temp_records)
    print("overall netpnl :", netpnl)
    result_filename = "results"
    temp_records.to_csv("{}.csv".format(result_filename))
    print("results saved in {}.csv".format(result_filename))


# CODE RUNS BECAUSE OF multipleDFs() FUNCTION CALL
multipleDFs()

# todo - check with time parameters
# todo - dynamic date
# avoiding global variable implementation is work in progress


# points to remember:
# stopless is always taken wrt to triggered candle
# sell: high + 5p
# buy: low - 5p
