import threading
import strategyTwo
import helperFunctions as hf
import pandas as pd
from helperFunctions import topStocks

import datetime


def func(date, top, minutes):
    final = pd.DataFrame()
    print('in temp.outer')
    df = topStocks(date, top)
    if df is not None:
        for j in df.index:
            print('in temp.inner')
            instrument_id = df["instrument_id"][j]
            lot_size, instrument_name = hf.getLotSizeAndName(
                instrument_id)
            message = "\n{},{},{},{},{}".format(
                instrument_id, instrument_name, j, date, minutes)
            filename = 'TopStocks.txt'
            hf.outputToFile(filename, message)
            minutes = 15
            records = strategyTwo.main(
                instrument_id, instrument_name, lot_size, date, minutes)
            final = pd.concat([final, records])

    extension = "{}_min_candle".format(minutes)
    extension += "_{}".format(date)
    filename = "output_{}.csv".format(extension)
    if not final.empty:
        final.to_csv(filename)
        hf.convertToFinalCSV(filename)


dates = []
start_ddmmyy = datetime.date(year=2016, month=4, day=1)
end_ddmmyy = datetime.date(year=2019, month=1, day=1)
# end_ddmmyy = datetime.date(year=2020, month=1, day=1) # debug
print(start_ddmmyy, end_ddmmyy)
current_date = start_ddmmyy
weekends = ["Saturday", "Sunday"]
while current_date <= end_ddmmyy:
    day = hf.dayOfTheWeek(current_date)
    if day not in weekends:
        dates.append(current_date)
    current_date += datetime.timedelta(days=1)

top = 3
minutes = 15

t1 = datetime.datetime.now()
# hf.printArray(dates)

threads = []
for i in range(len(dates)):
    if (i+1) % 3 != 0:
        date = dates[i]
        func(date, top, minutes)
        # threading implementation
        # t = threading.Thread(target=func, args=[date, top, minutes])
        # t.start()
        # threads.append(t)
    # elif (i+1) % 3 == 0:
    #     for thread in threads:
    #         thread.join()

    # if i == 2:        # debug
    #     break

t2 = datetime.datetime.now()
print(t2)
print(t1)
t3 = t2 - t1
print(t3)
# print(t2 - t1)

# print(top_three_stocks)
