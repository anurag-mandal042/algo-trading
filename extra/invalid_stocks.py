import pandas as pd
import mysql.connector as mysql


# enter your server IP address/domain name
HOST = "164.52.207.114"  # or "domain.com"
# database name, if you want just to connect to MySQL server, leave it empty
DATABASE = "stock_production"
# this is the user you create
USER = "stock"
# user password
PASSWORD = "stockdata@data"
# connect to MySQL server
db_connection = mysql.connect(
    host=HOST, user=USER, password=PASSWORD, database=DATABASE)
print("Connected to:", db_connection.get_server_info())
cursor = db_connection.cursor()  # get the cursor


stocks_data = pd.read_csv("dated_stock_list.csv")
print(stocks_data)

ab = []
# if data of specific date is mentioned uncomment below line and mention the date
date = '2020-12-29'
for i in stocks_data.index:
    id = stocks_data["instrument_id"][i]
    # if data of specific date is mentioned comment 2nd line below this
    # if df contains date column uncomment below line
    date = stocks_data["date"][i]
    query = "SELECT * FROM instrument_details  where date(ins_date)= '{}' and instrument_id = {} LIMIT 0,100".format(
        date, id)
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall())
    if i % 10 == 0:
        print(i, "done")

    if len(df) < 345:
        if id not in ab:
            ab.append(id)
        # print(id, query)
print(date)
print(ab)


# 2020-12-29
# [362, 367, 140, 220, 163, 291, 334, 197, 15, 331, 64, 356, 125, 96, 63, 342,
#     183, 277, 43, 244, 252, 21, 184, 313, 229, 333, 120, 398, 336, 339, 46, 413]

# 2020-12-04
# [362, 367, 25, 49, 33, 354, 110, 209, 140, 57, 130, 220, 163, 222, 23, 305, 357, 291, 201, 177, 403,
#     334, 197, 329, 29, 282, 55, 80, 41, 401, 390, 243, 224, 351, 15, 331, 343, 64, 379, 356, 386, 216,
#  125, 38, 96, 63, 376, 250, 364, 342, 44, 151, 183, 277, 16, 159, 43, 172, 106, 244, 330, 353,
#  4, 228, 166, 252, 21, 10, 249, 184, 236,
#     257, 240, 90, 347, 313, 260, 259, 370, 155, 18, 405, 101, 229, 179, 333, 308, 225, 375, 160, 326,
#  361, 187, 261, 120, 384, 355, 309, 344, 124, 174, 248, 146, 299, 316, 393, 36, 2, 398, 325, 6
#  5, 74, 92, 389, 199, 119, 395, 336, 339, 112, 168, 328, 290, 226, 318, 128, 30, 164, 94, 3
#  69, 298, 271, 46, 346, 253, 365, 47, 256, 143, 274, 413]
