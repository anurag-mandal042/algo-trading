import time
import strategyTwo
import pandas as pd
import helperFunctions as hf
import datetime


def main():

    # stocks_data = pd.read_csv(".\csv\dated_stock_list.csv")

    # for i in stocks_data:
    reliance = 90
    bataindia = 361
    titan = 33
    wipro = 4
    instrument_id = reliance
    instrument_name = "bataindia"
    today = datetime.date.today()

    # stocks_data = pd.read_csv("..\\csv\\fav_stocks.csv")
    stocks_data = pd.read_csv("..\\csv\\dated_stock_list_with_lot_size.csv")
    # stocks_data = pd.read_csv("..\\csv\\stocks_data.csv")
    final = pd.DataFrame()

    minutes = 10  # 5,15,10 is done
    start = 0
    stop = stocks_data.index.stop
    # stop = 2
    # number = 5
    # stop = start + number
    code_start_time = time.perf_counter()
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

    code_end_time = time.perf_counter()
    code_runtime = code_end_time-code_start_time
    print(code_start_time, code_end_time)
    print("coderuntime: ", round(code_runtime))


main()

# 10,VOLTAS
# 29,TORNTPOWER

# 3083.225084549710000563975979

# close 3082.00

# Stock:  NBCC
# SELECT * FROM instrument_details where date(ins_date) = '08-01-2018' and instrument_id = 140
# INVALID DF

# Stock:  NBCC
# SELECT * FROM instrument_details where date(ins_date) = '2018-01-08' and instrument_id = 140
# VALID DF
