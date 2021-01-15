import strategyTwo
import helperFunctions as hf
import pandas as pd
from helperFunctions import topThreeStocks as tts

import datetime
dates = []

start_ddmmyy = datetime.date(year=2015, month=1, day=1)
end_ddmmyy = datetime.date(year=2015, month=1, day=2)
# end_ddmmyy = datetime.date(year=2020, month=12, day=31)
print(start_ddmmyy, end_ddmmyy)
# for i in range(1, 30):
# dates.append(datetime.date(year=2020, month=9, day=i))

current_date = start_ddmmyy
while current_date < end_ddmmyy:
    dates.append(current_date)
    current_date += datetime.timedelta(days=1)
top = 3
top_three_stocks = []
final = pd.DataFrame()
t1 = datetime.datetime.now()
hf.printArray(dates)


for i in range(len(dates)):
    print('in temp.outer')
    df = tts(dates[i], top)
    if df is not None:
        for j in df.index:
            print('in temp.inner')
            instrument_id = df["instrument_id"][j]
            instrument_name = df["instrument_name"][j]
            date = df["end_date"][j]
            lot_size = 0
            minutes = 15
            records = strategyTwo.main(
                instrument_id, instrument_name, lot_size, date, minutes)
            final = pd.concat([final, records])

    extension = "{}_min_candle".format(minutes)
    extension += "_{}".format(date)
    filename = "output_{}.csv".format(extension)
    # final.to_csv(filename)
    hf.convertToFinalCSV(filename)

t2 = datetime.datetime.now()
print(t2)
print(t1)
t3 = t2 - t1
print(t3)
# print(t2 - t1)
'01: 29: 25.579790'
'00: 27: 21.211490'
# print(top_three_stocks)
