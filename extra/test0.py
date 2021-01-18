import numpy as np
from datetime import timedelta
import datetime
import pandas as pd

stats = {
    'profit': 0,

}
ENTRY = {
    'buy': 0,
    'sell': 0
}
df = pd.read_csv("output.csv")
prev_day_close = 4.00


# df['Final_DateTime'] = pd.to_datetime(
#     df['ins_date']) + timedelta(seconds=0, minutes=30, hours=5)
# df['Final_Date'] = pd.to_datetime(df['Final_DateTime']).dt.date
# df['Final_Time'] = pd.to_datetime(df['Final_DateTime']).dt.time
# print(df['Final_DateTime'])
# df.to_csv("o.csv")

first_candle_high = df['high'][0]
first_candle_low = df['low'][0]
start_time = df["ins_date"][0]
first_candle_open = df['open'][0]
first_candle_close = df['close'][0]
two_percent_of_prev_day_close = prev_day_close * 0.02


def convert_one_to_five(df):
    first_candle_high = df['high'][0]
    first_candle_low = df['low'][0]
    first_candle_open = df['open'][0]
    start_time = df["Final_DateTime"][0]
    end_time = start_time + timedelta(minutes=15)
    print("Starttime is", start_time)
    print("Endtime is", end_time)

    for i in df.index:
        if (start_time <= df["Final_DateTime"][i] <= end_time):
            first_candle_high = max(first_candle_high, df["high"][i])
            first_candle_low = min(first_candle_low, df["low"][i])
            first_candle_close = df['close'][i]
            mark = i  # the last index where time is less than 15 min
            # print(df["Final_DateTime"][i], first_candle_high, first_candle_low)

    return first_candle_high, first_candle_low, first_candle_open, first_candle_close, mark


first_candle_high, first_candle_low, first_candle_open, first_candle_close, mark = convert_one_to_five(
    df)
if (first_candle_open - first_candle_close) > 0 and (first_candle_high - first_candle_low) >= 0.02:
    print("(first_candle_open - first_candle_close) > 0 and (first_candle_high - first_candle_low) >= 0.02")


print("high is: ", first_candle_high)
print("low is: ", first_candle_low)
print("opened at: ", first_candle_open)
print("closed at: ", first_candle_close)
# first_candle after 15 min mark


start_time = datetime.time(9, 30, 00)
end_time = datetime.time(9, 45, 00)

for i in range(mark+1, int(df.index.stop)):
    count = 0
    high = df["high"][i]
    low = df["low"][i]
    open = df["open"][i]
    close = df["close"][i]
    print(df["Final_Time"][i])
    if (start_time <= df["Final_Time"][i] <= end_time):
        if open > close:
            count += 1
        if count == 2:
            print(count, True)
            break
        else:
            print(count, False)
    else:
        break


first_candle = df['open'][0] < df['close'][0]

if first_candle and first_candle_high - first_candle_low >= two_percent_of_prev_day_close:
    print("first_candle test passed")
else:
    print("first_candle test failed")
    # print(first_candle)
