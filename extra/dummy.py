
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


for i in range(trigger_candle_index, df.index.stop):
    # ------------ step 3: check is stop loss hit
    # ------------ if stop loss not hit then process
    if second_trade["trade"] == 'sell':
        if df["high"][i] >= second_trade["stop_loss"]:
            second_trade["active"] = True
            second_trade["stop_loss_hit"] = True
            second_trade["exit_at"] = second_trade["stop_loss"]
            second_trade['timestamp'] = df["Final_Time"][i]
            recordTrade(second_trade)
            closeTrade()
            break
        elif df['low'][i] <= p50:
            second_trade["active"] = True
            second_trade["stop_loss"] = second_trade["entry_at"]
            second_trade['timestamp'] = df["Final_Time"][i]
            recordTrade(second_trade)
        elif df['low'][i] <= p75:
            second_trade["stop_loss"] = p50
            second_trade['timestamp'] = df["Final_Time"][i]
            recordTrade(second_trade)
        elif df['low'][i] <= p100:
            second_trade["exit_at"] = second_trade["stop_loss"]
            second_trade['timestamp'] = df["Final_Time"][i]
            recordTrade(second_trade)
            closeTrade()
            break
    elif second_trade["trade"] == 'buy':
        if df["low"][i] <= second_trade["stop_loss"]:
            second_trade["active"] = True
            second_trade["stop_loss_hit"] = True
            second_trade["exit_at"] = second_trade["stop_loss"]
            second_trade['timestamp'] = df["Final_Time"][i]
            recordTrade(second_trade)
            closeTrade()
            break
        elif df['high'][i] >= p50:
            second_trade["active"] = True
            second_trade["stop_loss"] = second_trade["entry_at"]
            second_trade['timestamp'] = df["Final_Time"][i]
            recordTrade(second_trade)
        elif df['high'][i] >= p75:
            second_trade["stop_loss"] = p50
            second_trade['timestamp'] = df["Final_Time"][i]
            recordTrade(second_trade)
        elif df['high'][i] >= p100:
            second_trade['timestamp'] = df["Final_Time"][i]
            second_trade["exit_at"] = second_trade["stop_loss"]
            recordTrade(second_trade)
            closeTrade()
            break


def secondTradeTriggerCandle(trade, start_index):
    global df
    multiple = 0
    if trade["trade"] == "buy":
        for i in range(start_index, df.index.stop):
            remainder = multiple % 3
            if df["high"][i] >= trade['entry_at']:
                # ------------ if buy order triggered cancel sell order
                trade["active"] = True
                trigger_candle_index = i
                lower_bound = trigger_candle_index - remainder
                # ------------------------ set stop loss to low of triggered candle (3 minutes candle)
                if remainder == 0:
                    trade['stop_loss'] = float(
                        df["low"][trigger_candle_index]) - 0.05
                else:
                    trade['stop_loss'] = float(min(
                        df["low"][lower_bound:trigger_candle_index])) - 0.05
                trade['timestamp'] = df["Final_Time"][trigger_candle_index]
                print("BUY TRADE triggered at index ", trigger_candle_index)
                recordTrade(trade)
                return trade, trigger_candle_index
            multiple += 1
    if trade["trade"] == "sell":
        for i in range(start_index, df.index.stop):
            remainder = multiple % 3
            if df["low"][i] <= trade['entry_at']:
                # ------------ else buy order triggered cancel sell order
                trade["active"] = True
                trigger_candle_index = i
                lower_bound = trigger_candle_index - remainder
                # ------------------------ set stop loss to high of triggered candle (3 minutes candle)
                if remainder == 0:
                    trade['stop_loss'] = float(
                        df["high"][trigger_candle_index]) + 0.05
                else:
                    trade['stop_loss'] = float(max(
                        df["high"][lower_bound:trigger_candle_index])) + 0.05
                trade['timestamp'] = df["Final_Time"][trigger_candle_index]
                print("SELL TRADE triggered at index ", trigger_candle_index)
                recordTrade(trade)
                return trade, trigger_candle_index
            multiple += 1

 # # A.csv contains no date column
    # # B.csv contains a date column
    # # todo - dynamic date yet to be handled
    # # as a.csv doesnt contain date column , the date has to be hard coded
    stocks_data = pd.read_csv("a.csv")
    date = '2020-05-14'
    # # the invalid stocks are also hard coded currently, and yet to be processed as a function
    # # i have made a list of trades which i have to check practically and it is mentioned in the file - invalid entries.txt
    # invalid_stocks_in_a =  [1, 2, 3, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 17, 19, 21, 22, 24, 26, 27, 28, 31, 32, 34, 35, 37, 39, 40, 43, 45, 46, 48, 50, 51, 52, 53, 54, 56, 58, 59, 60, 61, 62, 63, 64, 66, 67, 68, 69, 70, 71, 72, 73, 76, 77, 78, 79, 80, 82, 83, 84, 85, 87, 88, 89, 91, 93, 95, 96, 97, 98, 99, 100, 102, 103, 104, 105, 107, 108, 109, 111, 113, 114, 115, 116, 117, 118, 120, 121, 123, 125, 126, 127, 129, 131, 132, 133, 134, 135, 137, 138, 139, 140, 141, 142, 145, 147, 149, 150, 152, 153, 154, 156, 157, 158, 161, 162, 163, 167, 169, 170, 173, 175, 176, 178, 180, 181, 183, 184, 185, 186, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 200, 202, 203, 204, 205, 206, 207, 208,
    #  210, 211, 212, 213, 214, 215, 217, 218, 219, 220, 221, 223, 227, 229, 230, 231, 232, 233, 234, 235, 237, 238, 239, 241, 242, 244, 245, 246, 247, 251, 252, 254, 255, 258, 262, 263, 264, 265, 266, 267, 268, 269, 270, 272, 273, 276, 277, 278, 280, 281, 283, 284, 285, 286, 287, 288, 289, 291, 292, 293, 294, 296, 297, 300, 301, 302, 303, 306, 307, 310, 311, 312, 313, 314, 315, 317, 319, 320, 321, 322, 323, 324, 327, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 345, 346, 349, 350, 356, 358, 359, 360, 362, 363, 366, 367, 368, 371, 373, 374, 377, 378, 380, 381, 382, 383, 385, 387, 388, 391, 392, 394, 396, 397, 398, 399, 400, 402, 404, 406, 407, 408, 409, 410, 411, 413]
    # invalid_stocks = invalid_stocks_in_a

    # b.csv contains HISTORICAL DATA
    # if using b.csv uncomment below 4 lines
    # stocks_data = pd.read_csv("b.csv")
    invalid_stocks_in_b = []
    a = []
    #  [2, 224, 38, 44, 80, 64, 244, 259,                                       395, 120, 92, 128, 159, 174, 187, 277, 228, 334, 369, 197, 413]
    invalid_stocks = invalid_stocks_in_b

def none():
    # ------------------------------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------------------------------
    minutes = 10  # 5,15,10 is done
    start = 0
    stop = stocks_data.index.stop
    # stop = 2
    # number = 5
    # stop = start + number
    t1 = datetime.datetime.now()
    for i in range(start, stop):
        # if i == 10:
        # break
        print("\n\n\t\t\t\t\t\t\t********************************", i)
        instrument_id = stocks_data["instrument_id"][i]
        date = stocks_data["date"][i]
        print(stocks_data["date"][i])
        lot_size, instrument_name = hf.getLotSizeAndName(instrument_id)
        lot_size = stocks_data["lot_size"][i]
        # date = '2021-01-07'
        # print(instrument_name)
        records = strategyTwo.main(
            instrument_id, instrument_name, lot_size, date, minutes)

        final = pd.concat([final, records])

    if minutes == 5:
        extension = "5_min_candle"
    elif minutes == 10:
        extension = "10_min_candle"
    elif minutes == 15:
        extension = "15_min_candle"
    elif minutes == 30:
        extension = "30_min_candle"

    if date == today:
        extension += "_todays_data"
    else:
        extension += "_historic_data"
    filename = "output_{}.csv".format(extension)
    final.to_csv(filename)
    hf.convertToFinalCSV(filename)
    t2 = datetime.datetime.now()
    print(t1)
    print(t2)
    t3 = t2 - t1
    print(t3)
    time_taken = [start, stop, stop - start, t1, t2, t3]
    time_taken_df = pd.DataFrame([time_taken])
    time_taken_df.columns = ['start_index', "stop_index",
                             "total_indexes", "start_time", "end_time", 'code_runtime']
    time_taken_df.to_csv("{}_metadata_{}".format(minutes, filename))
    # ------------------------------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------------------------------
    minutes = 15  # 5,15,10 is done
    start = 0
    stop = stocks_data.index.stop
    # stop = 2
    # number = 5
    # stop = start + number
    t1 = datetime.datetime.now()
    for i in range(start, stop):
        # if i == 10:
        # break
        print("\n\n\t\t\t\t\t\t\t********************************", i)
        instrument_id = stocks_data["instrument_id"][i]
        date = stocks_data["date"][i]
        print(stocks_data["date"][i])
        lot_size, instrument_name = hf.getLotSizeAndName(instrument_id)
        lot_size = stocks_data["lot_size"][i]
        # date = '2021-01-07'
        # print(instrument_name)
        records = strategyTwo.main(
            instrument_id, instrument_name, lot_size, date, minutes)

        final = pd.concat([final, records])

    if minutes == 5:
        extension = "5_min_candle"
    elif minutes == 10:
        extension = "10_min_candle"
    elif minutes == 15:
        extension = "15_min_candle"
    elif minutes == 30:
        extension = "30_min_candle"

    if date == today:
        extension += "_todays_data"
    else:
        extension += "_historic_data"
    filename = "output_{}.csv".format(extension)
    final.to_csv(filename)
    hf.convertToFinalCSV(filename)
    t2 = datetime.datetime.now()
    print(t1)
    print(t2)
    t3 = t2 - t1
    print(t3)
    time_taken = [start, stop, stop - start, t1, t2, t3]
    time_taken_df = pd.DataFrame([time_taken])
    time_taken_df.columns = ['start_index', "stop_index",
                             "total_indexes", "start_time", "end_time", 'code_runtime']
    time_taken_df.to_csv("{}_metadata_{}".format(minutes, filename))
