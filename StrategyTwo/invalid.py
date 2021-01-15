exceptions = [
    # # Stock:  RBLBANK - no graph on Zerodha no issue detected
    # # df is clean
    # # could be logical error
    "SELECT * FROM instrument_details where date(ins_date) = '2019-10-23' and instrument_id = 94",
    # # Stock:  AUROPHARMA - discuss with Sir candle very small
    "SELECT * FROM instrument_details where date(ins_date) = '2020-02-24' and instrument_id = 379",
    # Stock:  GLENMARK candle too small
    "SELECT * FROM instrument_details where date(ins_date) = '2020-06-22' and instrument_id = 282",
    # Stock:  INFY need to be checked
    "SELECT * FROM instrument_details where date(ins_date) = '2019-10-22' and instrument_id = 224",
]
[
    # # Stock:  SBIN
    # "SELECT * FROM instrument_details where date(ins_date) = '2018-08-13' and instrument_id = 80",
    # # Stock:  TITAN - time issues from 14:00
    # "SELECT * FROM instrument_details where date(ins_date) = '2019-05-06' and instrument_id = 33",
    # # Stock:  TATACHEM - time issues from 14:00
    # "SELECT * FROM instrument_details where date(ins_date) = '2019-05-06' and instrument_id = 49",
    # # Stock:  KSCL - time issues from 12:01
    # "SELECT * FROM instrument_details where date(ins_date) = '2019-06-04' and instrument_id = 184",
    # # Stock:  GODFRYPHLP - time issues from 10:46
    # "SELECT * FROM instrument_details where date(ins_date) = '2019-06-24' and instrument_id = 277",
    # # Stock:  BHARTIARTL - time issues from 14:01
    # "SELECT * FROM instrument_details where date(ins_date) = '2019-08-02' and instrument_id = 353",
    # # Stock:  VEDL
    # "SELECT * FROM instrument_details where date(ins_date) = '2019-08-02' and instrument_id = 16",
    # # Stock:  BPCL
    # "SELECT * FROM instrument_details where date(ins_date) = '2019-09-18' and instrument_id = 347",

    # # Stock:  TORNTPOWER - time issues from 11:16
    # "SELECT * FROM instrument_details where date(ins_date) = '2020-02-13' and instrument_id = 29",
    # # Stock:  INFRATEL - time issues from 11:46
    # "SELECT * FROM instrument_details where date(ins_date) = '2020-02-13' and instrument_id = 225",

    # # Stock:  GODREJPROP - time issues from 15:32
    # "SELECT * FROM instrument_details where date(ins_date) = '2020-08-27' and instrument_id = 274",
    # # Stock:  RBLBANK - time issues from 12:32
    # "SELECT * FROM instrument_details where date(ins_date) = '2020-08-27' and instrument_id = 94",
    # # Stock:  HDFC
    # "SELECT * FROM instrument_details where date(ins_date) = '2021-01-07' and instrument_id = 259",
    # Stock:  INFY need to be checked
    "SELECT * FROM instrument_details where date(ins_date) = '2019-10-22' and instrument_id = 224",
    # Stock:  AUROPHARMA
    "SELECT * FROM instrument_details where date(ins_date) = '2020-04-03' and instrument_id = 379",

]


[
    # Starting new trade at 14:45 issue
    # Stock:  TATACHEM
    "SELECT * FROM instrument_details where date(ins_date) = '2018-01-03' and instrument_id = 49",
    # Stock:  TORNTPOWER
    "SELECT * FROM instrument_details where date(ins_date) = '2018-02-08' and instrument_id = 29",
    # Stock:  GLENMARK
    "SELECT * FROM instrument_details where date(ins_date) = '2018-02-09' and instrument_id = 282",
    # Stock:  CANBK
    "SELECT * FROM instrument_details where date(ins_date) = '2018-03-13' and instrument_id = 343",
    # Stock:  BEML
    "SELECT * FROM instrument_details where date(ins_date) = '2018-03-21' and instrument_id = 356",
    # Stock:  BANKBARODA
    "SELECT * FROM instrument_details where date(ins_date) = '2018-03-27' and instrument_id = 364",
]
