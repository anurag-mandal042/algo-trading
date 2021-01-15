import math
import pandas as pd
import mysql.connector as mysql
import datetime
from datetime import timedelta
import os
import helperFunctions as hf
import decimal
from Trade import Trade
from invalid import exceptions


def getDF(instrument_id, date, min_df_len=370, printdf=False):
    '''
    Input Parameters:
        instrument_id: to fetch data from mysql
        date: to fetch data from mysql
        min_df_len: it is a parameter to check validity of df returned from mysql, if len(df) less than min_df_len then None is returned
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

    query = "SELECT * FROM instrument_details where date(ins_date) = '{}' and instrument_id = {}".format(
        date, instrument_id)
    cursor.execute(query)
    print(query)
    df = pd.DataFrame(cursor.fetchall())
    cursor.close()
    db_connection.close()

    if printdf:
        print(df.info)
        hf.printArray(exceptions)

    if df.empty or len(df) < min_df_len:
        invalid_query = "\n{},{},empty or df len ({}) is less than 370,{},{}".format(
            instrument_id, query, len(df), datetime.date.today(), datetime.datetime.now().time())
        f = open("invalid_queries_{}.txt".format("stratTwogetDF"), "a")
        f.write(invalid_query)
        f.close()
        return None
    elif query in exceptions:

        print("\n\n\t\t\t\tquery in exceptions")
        invalid_query = "\n{},{},query in exceptions,{},{}".format(
            instrument_id, query, datetime.date.today(), datetime.datetime.now().time())
        f = open("invalid_queries.txt", "a")
        f.write(invalid_query)
        f.close()
        return None
    else:
        df.columns = ['id', 'instrument_id', 'ins_date', 'open', 'high',
                      'low', 'close', 'volume', 'status', 'created_at', 'updated_at']
        df = hf.addFinalDateTimeColumns(df)
    return df


def calculateVwap(df):
    df['TP'] = (df['high'] + df['low'] + df['close'])/float(3.0)
    df['TradedValue'] = df['TP']*df['volume']
    df['CumVolume'] = df['volume'].cumsum()
    df['CumTradedValue'] = df['TradedValue'].cumsum()
    df['VWAP'] = df['CumTradedValue'] / df['CumVolume']
    return df


def nineFifteenCandleIndex(df):
    # ------------ get 9:15 candle index
    for i in df.index:
        if df["Final_Time"][i] == datetime.time(hour=9, minute=15):
            return i


def setPotentialTrade(df, records, data_object):
    '''
        checks with next candle if potential trade will be buy trade or sell trade w.r.t vwap value
        returns triggered trade
    '''
    current_candle_index = data_object["current_candle"]
    print("\nin potentialtrade candle time at",
          df["Final_Time"][current_candle_index])
    # FIRST TRADE CONSIDER 9:15-9:30 CANDLE
    #   check if potential trade is going to be buy or sell
    if df["Final_Time"][current_candle_index] >= datetime.time(hour=14, minute=45):
        print("DONT START NEW TRADE BECAUSE NEXT CANDLE IS EOD")
        data_object["continue"] = False
        current_trade = data_object["current_trade"]
        current_trade["timestamp"] = df["Final_Time"][current_candle_index]
        records = hf.recordTrade(records, current_trade)
        return data_object, records
    if df['close'][current_candle_index] > df['VWAP'][current_candle_index]:
        #   if potential trade is going to be buy
        print("\nclose:", df['close'][current_candle_index],
              "vwap:", df['VWAP'][current_candle_index])
        buy_trade = Trade.trade
        buy_trade['trade'] = "buy"
        buy_trade['entry_at'] = float(
            df['high'][current_candle_index] + float(0.05))
        buy_trade['stop_loss'] = float(
            df['low'][current_candle_index] - float(0.05))
        buy_trade['initial_sl'] = buy_trade['stop_loss']
        buy_trade['timestamp'] = df['Final_Time'][current_candle_index]
        data_object["current_candle"] = current_candle_index+1
        data_object["current_trade"] = buy_trade
        print("potential trade is buy trade at ",
              df["Final_Time"][current_candle_index])
        records = hf.recordTrade(records, buy_trade)
        return data_object, records
    elif df['close'][current_candle_index] < df['VWAP'][current_candle_index]:
        #   if potential trade is going to be sell
        print("\nclose:", df['close'][current_candle_index],
              "vwap:", df['VWAP'][current_candle_index])
        sell_trade = Trade.trade
        sell_trade['trade'] = "sell"
        sell_trade['entry_at'] = float(
            df['low'][current_candle_index] - float(0.05))
        sell_trade['stop_loss'] = float(
            df['high'][current_candle_index] + float(0.05))
        sell_trade['initial_sl'] = sell_trade['stop_loss']
        sell_trade['timestamp'] = df['Final_Time'][current_candle_index]
        data_object["current_candle"] = current_candle_index+1
        data_object["current_trade"] = sell_trade
        print("potential trade is sell trade at ",
              df["Final_Time"][current_candle_index])
        records = hf.recordTrade(records, sell_trade)
        return data_object, records


def checkIfTradeTriggered(df, records, data_object):
    '''
        checks if potential trade is triggered on next candle
        Returns true if triggered
    '''
    current_candle_index = data_object['current_candle']
    print("in checkIfTradeTriggered, current_candle at",
          df["Final_Time"][current_candle_index])
    current_trade = data_object['current_trade']
    if current_trade['trade'] == 'buy':
        # check if trade is triggered
        if df['high'][current_candle_index] >= current_trade['entry_at']:
            # if triggered then set stop loss at 5 paisa below current candle low
            current_trade['active'] = True
            current_trade['timestamp'] = df['Final_Time'][current_candle_index]
            data_object['current_trade'] = current_trade
            data_object['current_candle'] += 1
            print("potential buy triggered at {}, with entry price {}, sl at {}.".format(
                current_trade['timestamp'], current_trade['entry_at'], current_trade['stop_loss']))
            records = hf.recordTrade(records, current_trade)
            return True, data_object, records
        elif df['high'][current_candle_index] < current_trade['entry_at']:
            # if trade not triggered on next candle then find next potential trade
            print('potential buy trade not tiggered')
            print("WAIT FOR NEW SIGNAL")
            # hf.printDict(data_object["current_trade"])
            # records = hf.recordTrade(records, current_trade)
            return False, data_object, records
    elif current_trade['trade'] == 'sell':
        # check if trade is triggered
        if df['low'][current_candle_index] <= current_trade['entry_at']:
            # if triggered then set stop loss at 5 paisa above current candle high
            current_trade['active'] = True
            current_trade['timestamp'] = df['Final_Time'][current_candle_index]

            data_object['current_trade'] = current_trade
            data_object['current_candle'] += 1
            print("potential sell triggered at {}, with entry price {}, sl at {}.".format(
                current_trade['timestamp'], current_trade['entry_at'], current_trade['stop_loss']))
            records = hf.recordTrade(records, current_trade)
            return True, data_object, records
        elif df['low'][current_candle_index] > current_trade['entry_at']:
            # if trade not triggered on next candle then find next potential trade
            print('potential sell trade not tiggered')
            print("WAIT FOR NEW SIGNAL")
            # records = hf.recordTrade(records, current_trade)
            # hf.printDict(data_object["current_trade"])
            return False, data_object, records


def processTriggeredTrade(df, records, data_object):
    '''
        Returns True:
                if SL hit
        Returns False:
                if SL not hit by EOD
    '''
    current_candle_index = data_object['current_candle']
    print("in prcoesstriggeredtrade at ",
          df["Final_Time"][current_candle_index])
    current_trade = data_object["current_trade"]
    if current_trade['trade'] == 'sell':
        for i in range(current_candle_index, df.index.stop):
            # print(df['Final_Time'][i])
            # if sl hit code
            if df['high'][i] > current_trade['stop_loss']:
                if df["Final_Time"][i] >= datetime.time(hour=15, minute=00):
                    #  or df["Final_Time"][i] == datetime.time(hour=15, minute=00):
                    print("DONT START NEW TRADE")
                print("SL hit in sell trade at", df['Final_Time'][i])
                current_trade["timestamp"] = df['Final_Time'][i]
                current_trade["exit_at"] = current_trade['stop_loss']
                current_trade["stop_loss_hit"] = True
                current_trade["active"] = False
                current_trade["pnl"] = current_trade["entry_at"] - \
                    float(current_trade["exit_at"])

                data_object["current_trade"] = current_trade
                data_object["current_candle"] = i
                records = hf.recordTrade(records, current_trade)
                return True, data_object, records
                # EOD condition
            if df['Final_Time'][i] >= datetime.time(hour=15, minute=00):
                # or df['Final_Time'][i] == datetime.time(hour=15, minute=15):
                # closing trade code here
                current_trade["active"] = False
                current_trade["exit_at"] = df["close"][i]
                current_trade["pnl"] = current_trade["entry_at"] - \
                    float(current_trade["exit_at"])
                current_trade["timestamp"] = df["Final_Time"][i]
                current_trade["stop_loss_hit"] = "E.O.D"

                data_object["continue"] = False
                data_object["current_trade"] = current_trade
                print("END OF DAY")
                records = hf.recordTrade(records, current_trade)
                # returns False if SL not hit by EOD
                return False, data_object, records
            # check if vwap is lower than candle close then change stop loss to candle high
            if df['VWAP'][i] < df["close"][i]:
                current_trade["stop_loss"] = float(df["high"][i])+0.05
                current_trade["timestamp"] = df["Final_Time"][i]
                print("vwap condition, new stop loss: {} at {}".format(
                      current_trade["stop_loss"], df["Final_Time"][i]))
                records = hf.recordTrade(records, current_trade)
    elif current_trade['trade'] == 'buy':
        for i in range(current_candle_index, df.index.stop):
            # print(df['Final_Time'][i])
            # if sl hit code
            if df['low'][i] < current_trade['stop_loss']:
                if df["Final_Time"][i] >= datetime.time(hour=15, minute=00):
                    # or df["Final_Time"][i] == datetime.time(hour=15, minute=00):
                    print("DONT START NEW TRADE")
                print("SL hit in buy trade at", df['Final_Time'][i])
                current_trade["timestamp"] = df['Final_Time'][i]
                current_trade["exit_at"] = current_trade['stop_loss']
                current_trade["stop_loss_hit"] = True
                current_trade["active"] = False
                current_trade["pnl"] = float(current_trade["exit_at"]) - \
                    current_trade["entry_at"]
                data_object["current_trade"] = current_trade
                data_object["current_candle"] = i
                records = hf.recordTrade(records, current_trade)

                return True, data_object, records
            # EOD condition
            if df['Final_Time'][i] >= datetime.time(hour=15, minute=00):
                # or df['Final_Time'][i] == datetime.time(hour=15, minute=15):
                # closing trade code here
                current_trade["active"] = False
                current_trade["exit_at"] = df["close"][i]
                current_trade["pnl"] = float(current_trade["exit_at"]) - \
                    current_trade["entry_at"]
                current_trade["timestamp"] = df["Final_Time"][i]
                current_trade["stop_loss_hit"] = "E.O.D"

                data_object["continue"] = False
                data_object["current_trade"] = current_trade
                print("END OF DAY")
                records = hf.recordTrade(records, current_trade)
                return False, data_object, records
            # check if vwap is higher than candle close then change stop loss to candle low
            if df['VWAP'][i] > df["close"][i]:
                current_trade["stop_loss"] = float(df["low"][i])-0.05
                current_trade["timestamp"] = df["Final_Time"][i]
                print("vwap condition, new stop loss: {} at {}".format(
                      current_trade["stop_loss"], df["Final_Time"][i]))
                records = hf.recordTrade(records, current_trade)


def resetTrade(current_trade):
    current_trade['active'] = False
    current_trade['trade'] = None
    current_trade['entry_at'] = 0
    current_trade['exit_at'] = 0
    current_trade['stop_loss'] = 0
    current_trade['stop_loss_hit'] = False
    current_trade['pnl'] = 0
    current_trade['volume'] = 0
    current_trade['timestamp'] = None
    return current_trade


def main(instrument_id, instrument_name, lot_size, date, minutes):
    print("\n\n\n\t\t\t\tin main")
    print("Stock: ", instrument_name)
    df = getDF(instrument_id, date)
    if df is not None:
        df = hf.cleanDF(df, minutes)
        # if missing_entries:
        #     print("\n\n***\t\t\t MISSING ENTRY/IES WARNING, NUMBER OF ENTRIES: {}\t\t\t***\n\n".format(len(missing_entries)))
        #     hf.printArray(missing_entries)
        # df = hf.convert(df, minutes)
        df = calculateVwap(df)
        number_of_trades = 3

        records = pd.DataFrame()
        data_object = {
            "current_candle": 1,
            "net_pnl": 0,
            "continue": True,
            "current_trade": None,
        }
        data_object['current_trade'] = Trade.trade
        data_object['current_trade']["instrument_id"] = instrument_id
        data_object['current_trade']["instrument_name"] = instrument_name
        data_object['current_trade']["lot_size"] = lot_size
        data_object['current_trade']["date"] = date
        data_object_clone_0 = hf.data_object_clone_0
        temp_df_clone = hf.dictToDF(data_object_clone_0)
        temp_records = pd.DataFrame()
        trade_count = 0

        while trade_count < number_of_trades and data_object["continue"]:
            condition = False
            while not (condition):
                if data_object["continue"] and data_object["current_trade"] is not None and data_object["current_trade"]["exit_at"] != 0:
                    print("break because exit_at != 0")
                    break
                # function call
                data_object, records = setPotentialTrade(
                    df, records, data_object)

                temp_df = hf.dictToDF(data_object["current_trade"])
                temp_records = pd.concat([temp_records, temp_df])
                temp_records = pd.concat([temp_records, temp_df_clone])
                if not (data_object['continue']):
                    break
                # function call
                condition, data_object, records = checkIfTradeTriggered(
                    df, records, data_object)

                temp_df = hf.dictToDF(data_object["current_trade"])
                temp_records = pd.concat([temp_records, temp_df])
                temp_records = pd.concat([temp_records, temp_df_clone])
            if not (data_object['continue']):
                break
            # function call
            condition, data_object, records = processTriggeredTrade(
                df, records, data_object)
            temp_records = pd.concat([temp_records, temp_df_clone])
            temp_df = hf.dictToDF(data_object["current_trade"])
            temp_records = pd.concat([temp_records, temp_df])

            if trade_count == 0:
                print("\nFIRST TRADE ended")
            elif trade_count == 1:
                print("\nSECOND TRADE ended")
            elif trade_count == 2:
                print("\nTHIRD TRADE ended")
            data_object['current_trade'] = resetTrade(
                data_object['current_trade'])

            trade_count += 1
        return records
    else:
        print("INVALID DF\n")
