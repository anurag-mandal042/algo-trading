import os
import mysql.connector as mysql
from datetime import timedelta
import datetime
import pandas as pd


def getFirstCandleHighLow(df, nine_fifteen_candle_index):
    high = max(
        df["high"][nine_fifteen_candle_index:nine_fifteen_candle_index + 3])
    low = min(
        df['low'][nine_fifteen_candle_index:nine_fifteen_candle_index + 3])
    return high, low


def nineFifteenCandleIndex(df, stats):
    # ------------ get 9:15 candle index
    for i in df.index:
        if df["Final_Time"][i] == datetime.time(hour=9, minute=15):
            stats["nine_fifteen_candle_index"] = i
            break
    return stats


def printDict(dictionary):
    for values, key in zip(dictionary.values(), dictionary.keys()):
        if type(values) == dict:
            print(key)
            printDict(values)
            continue
        print("\t", key, values)


def triggeredTrade(df, stats, buy_trade, sell_trade):
    # ------------ 9:15 + 3 = 9:18
    # global buy_trade, sell_trade
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
                buy_trade['stop_loss'] = float(
                    df["low"][trigger_candle_index]) - 0.05
            else:
                buy_trade['stop_loss'] = float(min(
                    df["low"][candle_start:trigger_candle_index+1])) - 0.05
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
                sell_trade['stop_loss'] = float(
                    df["high"][trigger_candle_index]) + 0.05
            else:
                sell_trade['stop_loss'] = float(max(
                    df["high"][candle_start:trigger_candle_index+1])) + 0.05
            sell_trade['timestamp'] = df["Final_Time"][trigger_candle_index]
            return sell_trade, trigger_candle_index
        multiple += 1


def recordTrade(trade, RECORDS):
    # global RECORDS
    tempTrade = pd.DataFrame(data={
        "trade": [trade["trade"]],
        "entry_at": [trade["entry_at"]],
        "active": [trade["active"]],
        "stop_loss": [trade["stop_loss"]],
        "stop_loss_hit": [trade["stop_loss_hit"]],
        "exit_at": [trade["exit_at"]],
        "pnl": [trade["pnl"]],
        "volume": [trade["volume"]],
        "timestamp": [trade["timestamp"]],
    })
    RECORDS = pd.concat([RECORDS, tempTrade])
    return RECORDS


def addPnL(trade, first_trade, stats):
    # global FIRST_TRADE, SECOND_TRADE
    if first_trade:
        stats["FIRST_TRADE"] = trade
        # print("FIRST_TRADE")
    else:
        stats["SECOND_TRADE"] = trade
        # print("SECOND_TRADE")

    if trade["trade"] == "buy":
        #     # Buy ke case mein exit - entry
        # print("buy trade")
        # printDict(trade)
        pnl = trade["pnl"] = (float(
            trade["exit_at"]) - float(trade["entry_at"]))
        # *trade["volume"]
    elif trade["trade"] == "sell":
        #     # Sell ke case mein entry - exit
        # print("sell trade")
        # printDict(trade)
        pnl = trade["pnl"] = (float(
            trade["entry_at"]) - float(trade["exit_at"]))
    stats["net_pnl"] += pnl
    # print("\nTrade closed")
    # print("Trade details:")
    return trade, stats


def processTrade(df, RECORDS, trade, start_index, step,  stats, first_trade):
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
    for i in range(start_index, df.index.stop, step):
        # ------------ step 3: check is stop loss hit
        # ------------ if stop loss hit then close trade
        #
        # ------------ if trade is sell check stoploss hit and profit targets,
        # ------------ if hit then break loop and check profit or loss
        if trade["trade"] == 'sell':
            if df["high"][i] >= trade["stop_loss"]:
                trade["stop_loss_hit"] = True
                trade["exit_at"] = trade["stop_loss"]
                trade['timestamp'] = df["Final_Time"][i]
                trade, stats = addPnL(trade,  first_trade, stats)
                RECORDS = recordTrade(trade, RECORDS)
                return i, RECORDS, stats
            elif df['low'][i] <= p100:
                # print("p100 hit", p100)
                # print("low = ", df['low'][i], df['Final_Time'][i])
                trade["exit_at"] = p100
                trade['timestamp'] = df["Final_Time"][i]
                trade, stats = addPnL(trade, first_trade, stats)
                RECORDS = recordTrade(trade, RECORDS)
                return i, RECORDS, stats
            elif df['low'][i] <= p75 and trade["stop_loss"] > p75:
                # print("p75 hit", p75)
                # print("low = ", df['low'][i], df['Final_Time'][i])
                trade["stop_loss"] = p50
                trade['timestamp'] = df["Final_Time"][i]
                RECORDS = recordTrade(trade, RECORDS)
            elif df['low'][i] <= p50 and trade["stop_loss"] > p50:
                # print("p50 hit", p50)
                # print("low = ", df['low'][i], df['Final_Time'][i])
                trade["stop_loss"] = trade["entry_at"]
                trade['timestamp'] = df["Final_Time"][i]
                RECORDS = recordTrade(trade, RECORDS)
        # ------------ if trade is buy check stoploss hit and profit targets,
        # ------------ if hit then break loop and check profit or loss
        elif trade["trade"] == 'buy':
            if df["low"][i] <= trade["stop_loss"]:
                trade["stop_loss_hit"] = True
                trade["exit_at"] = trade["stop_loss"]
                trade['timestamp'] = df["Final_Time"][i]
                trade, stats = addPnL(trade, first_trade, stats)
                RECORDS = recordTrade(trade, RECORDS)
                return i, RECORDS, stats
            elif df['high'][i] >= p100:
                # print("p100 hit", p100)
                # print("high = ", df['high'][i], df['Final_Time'][i])
                trade["exit_at"] = p100
                trade['timestamp'] = df["Final_Time"][i]
                trade, stats = addPnL(trade, first_trade, stats)
                RECORDS = recordTrade(trade, RECORDS)
                return i, RECORDS, stats
            elif df['high'][i] >= p75 and trade["stop_loss"] < p75:
                # print("p75 hit", p75)
                # print("high = ", df['high'][i], df['Final_Time'][i])
                trade["stop_loss"] = p50
                trade['timestamp'] = df["Final_Time"][i]
                RECORDS = recordTrade(trade, RECORDS)
            elif df['high'][i] >= p50 and trade["stop_loss"] < p50:
                # print("p50 hit", p50)
                # print("high = ", df['high'][i], df['Final_Time'][i])
                trade["stop_loss"] = trade["entry_at"]
                trade['timestamp'] = df["Final_Time"][i]
                RECORDS = recordTrade(trade, RECORDS)


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


def dictToDF(dict):
    data = {}
    for key, value in zip(dict.keys(), dict.values()):
        data[key] = [value]
    df = pd.DataFrame(data)
    return df


def setSecondTrade(df, FIRST_TRADE, stats):
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
        return stats, sell_trade
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
        return stats, buy_trade


def main(df):

    RECORDS = pd.DataFrame()
    # df = pd.DataFrame()
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
    # global df, stats, RECORDS, buy_trade, sell_trade, FIRST_TRADE, SECOND_TRADE
    print(df)
    stats = nineFifteenCandleIndex(df, stats)
    nine_fifteen_candle_index = stats["nine_fifteen_candle_index"]
    date = df["Final_Date"][0]
    print("\nDate of stock is: ", date)

    first_3_minute_candle_high, first_3_minute_candle_low = getFirstCandleHighLow(
        df, nine_fifteen_candle_index)

    # ------------ step 1: place buy and sell order
    # ------------ buy order at high + 5 paise (0.05 Rs)
    # ------------ low order at low - 5 paise (0.05 Rs)
    buy_trade['entry_at'] = float(first_3_minute_candle_high) + 0.05
    sell_trade['entry_at'] = float(first_3_minute_candle_low) - 0.05
    # ------------ example:
    # --------------- first 3 min candle high = 180.65
    # --------------- first 3 min candle low = 120.35
    # --------------- then
    # ------------------- place buy order at 180.7
    # ------------------- place sell order at 120.3

    start_time = df["Final_Time"][nine_fifteen_candle_index]
    buy_trade['timestamp'] = start_time
    sell_trade['timestamp'] = start_time

    # ------------ MAIN FUNCTION START HERE

    RECORDS = recordTrade(buy_trade, RECORDS)
    RECORDS = recordTrade(sell_trade, RECORDS)

    # ------------ first trade is set to buy or sell
    FIRST_TRADE, FIRST_TRADE_trigger_candle_index = triggeredTrade(
        df, stats, buy_trade, sell_trade)
    stats["FIRST_TRADE"] = FIRST_TRADE
    stats["FIRST_TRADE_trigger_candle_index"] = FIRST_TRADE_trigger_candle_index
    RECORDS = recordTrade(FIRST_TRADE, RECORDS)
    # ------------ calculate profit marks
    p50, p75, p100 = calculateProfitMarks(trade=FIRST_TRADE)
    # ------------ get index of trigger_candle
    trigger_candle_index = stats["FIRST_TRADE_trigger_candle_index"]
    # ------------ process FIRST_TRADE

    stats["FIRST_TRADE_close_candle_index"], RECORDS, stats = processTrade(df, RECORDS, trade=FIRST_TRADE,
                                                                           start_index=trigger_candle_index, step=1, stats=stats, first_trade=True)

    # ------------ POTENTIAL SECOND TRADE STARTS
    # ------------ check if first trade closed because of stop loss hit
    # ------------ and then check if loss incured

    if (FIRST_TRADE["stop_loss_hit"]) and (FIRST_TRADE["pnl"] < 0):
        FIRST_TRADE["active"] = False
        stats, SECOND_TRADE = setSecondTrade(df, FIRST_TRADE, stats)
        # ------------ calculate new profit targets
        # p50, p75, p100 = calculateProfitMarks(trade=SECOND_TRADE)
        # ------------ by now second trade setup done
        RECORDS = recordTrade(SECOND_TRADE, RECORDS)
        trigger_candle_index = stats["FIRST_TRADE_close_candle_index"]

        stats["SECOND_TRADE"] = SECOND_TRADE
        # ------------ process SECOND_TRADE
        stats["SECOND_TRADE_close_candle_index"], RECORDS, stats = processTrade(df, RECORDS, trade=SECOND_TRADE,
                                                                                start_index=trigger_candle_index, step=1, stats=stats, first_trade=False)

        print("Order completed at ", df["Final_Time"]
              [stats["SECOND_TRADE_close_candle_index"]])
    RECORDS.to_csv("recordTrade.csv")
    print("\nMain ends")
    return stats, FIRST_TRADE, SECOND_TRADE


# if __name__ == "__main__":
    # main()


# stopless is always taken wrt to triggered candle
# sell: high + 5p
# buy: low - 5p

# check with time parameters
# 1753
