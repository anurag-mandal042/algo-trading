import threading
import strategyTwo
import helperFunctions as hf
import pandas as pd
from helperFunctions import topThreeStocks as tts

import datetime
import time


def func(date, top, minutes):
    print('in temp.outer')
    df = tts(dates[i], top)
    final = pd.DataFrame()
    for j in df.index:
        print('in temp.inner')
        instrument_id = df["instrument_id"][j]
        date = df["end_date"][j]

        lot_size, instrument_name = hf.getLotSizeAndName(instrument_id)
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

    extension += "_{}".format(date)
    filename = "output_{}.csv".format(extension)
    final.to_csv(filename)
    hf.convertToFinalCSV(filename)


code_start_time = time.perf_counter()
dates = [
    datetime.date(year=2020, month=12, day=1),
    # datetime.date(year=2020, month=12, day=2),
    # datetime.date(year=2020, month=12, day=3),
]

top = 3
top_three_stocks = []
final = pd.DataFrame()
t1 = datetime.datetime.now()
minutes = 5
threads = []

for i in range(len(dates)):
    date = dates[i]
    func(date, top, minutes)


# for i in range(len(dates)):
#     date = dates[i]
#     t = threading.Thread(target=func, args=[date, top, minutes])
#     t.start()
#     threads.append(t)

# for thread in threads:
#     thread.join()


t2 = datetime.datetime.now()
print(t2)
print(t1)
t3 = t2 - t1
print(t3)

code_end_time = time.perf_counter()
code_runtime = code_end_time-code_start_time
print(code_start_time, code_end_time)
print("coderuntime: ", round(code_runtime))
# print(t2 - t1)
