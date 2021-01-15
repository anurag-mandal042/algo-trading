import strategyTwo
import helperFunctions as hf
import datetime


# instrument_id = 4
# dd = 15
# mm = 7
# yy = 2020
# minutes = 15
# NMDC
instrument_id = 130
dd = 9
mm = 1
yy = 2018
minutes = 15
lot_size, instrument_name = hf.getLotSizeAndName(instrument_id)
date = datetime.date(day=dd, month=mm, year=yy)
records = strategyTwo.main(
    instrument_id, instrument_name, lot_size, date, minutes)

records = records[records['exit_at'] != 0]

print(records)
records = hf.addAnalysisColumns(records)
# NMDC
# instrument_id = 130
# dd = 9
# mm = 1
# yy = 2018
# minutes = 15
