# import thread
import pandas as pd
import mysql.connector as mysql
# import test1
# stocks_data = pd.read_csv("dated_stock_list.csv")
stocks_data = pd.read_csv("stocks_data.csv")
# stocks_data
print("\t\tstocks_data.csv")
print(stocks_data)


# # test1.multipleDFs()
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


# # [1, 3, 9, 15, 17, 19, 22, 24, 28, 29, 32, 35, 37, 40, 41, 43, 46, 48, 54, 56, 64, 73, 74, 79, 80, 85, 89, 91, 93, 100, 105, 109, 111, 118, 121, 123, 127, 129, 135, 142, 143, 145, 147, 150, 154, 158, 159, 163, 164, 165, 167, 170, 171, 173, 176, 178, 181, 186, 198, 200, 208, 215, 221, 223, 224, 225, 227, 235,
# #   237, 239, 242, 247, 248, 249, 252, 255, 256, 258, 259, 260, 270, 273, 274, 278, 281, 289, 294, 297, 298, 303, 304, 307, 308, 315, 317, 324, 325, 327, 328, 329, 342, 343, 345, 346, 347, 350, 351, 352, 353, 354, 356, 360, 363, 364, 368, 369, 371, 374, 375, 378, 383, 385, 388, 389, 392, 394, 400, 402, 404, 411, 413]
pr = []
# #  [0, 2, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 16, 18, 20, 21, 23, 25, 26, 27, 30, 31, 33, 34, 36, 38, 39, 42, 44, 45, 47, 49, 50, 51,
# #   52, 53, 55, 57, 58, 59, 60, 61, 62, 63, 65, 66, 67, 68, 69, 70, 71, 72, 75, 76, 77, 78, 81, 82, 83, 84, 86, 87, 88, 90, 92, 94, 95, 96, 97, 98, 99, 101, 102, 103, 104, 106, 107, 108, 110, 112, 113, 114, 115, 116, 117, 119, 120, 122, 124, 125, 126, 128, 130, 131, 132, 133, 134, 136, 137, 138, 139, 140, 141, 144, 146, 148, 149, 151, 152, 153, 155, 156, 157, 160, 161, 162, 166, 168, 169, 172, 174, 175, 177, 179, 180, 182, 183, 184, 185, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 199, 201, 202, 203, 204, 205, 206, 207, 209, 210, 211, 212, 213, 214, 216, 217, 218, 219, 220, 222, 226, 228, 229, 230, 231, 232, 233, 234, 236, 238, 240, 241, 243, 244, 245, 246, 250, 251, 253, 254, 257, 261, 262, 263, 264, 265, 266, 267, 268, 269, 271, 272, 275, 276, 277, 279, 280, 282, 283, 284, 285, 286, 287, 288, 290, 291, 292, 293, 295, 296, 299, 300, 301, 302, 305, 306, 309, 310, 311, 312, 313, 314, 316, 318, 319, 320, 321, 322, 323, 326, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 344, 348, 349, 355, 357, 358, 359, 361, 362, 365, 366, 367, 370, 372, 373, 376, 377, 379, 380, 381, 382, 384, 386, 387, 390, 391, 393, 395, 396, 397, 398, 399, 401, 403, 405, 406, 407, 408, 409, 410, 412]
# date = '2020-12-28'
# cursor.execute("use stock_production;")

ab = []
for i in stocks_data.index:
    id = stocks_data["instrument_id"][i]
    # date = stocks_data["date"][i]
    date = ' 2020-12-29'
    query = "SELECT * FROM instrument_details  where date(ins_date)= '{}' and instrument_id = {} LIMIT 0,100".format(
        date, id)
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall())
    if i % 10 == 0:
        print("\t\t\t\t\t\t\t", i)

    if len(df) < 22:
        if id not in ab:
            ab.append(id)
        # print(id, query)
print(date)
print(ab)
# print(len(pr), pr)


# def searchStock(stockname, df, fieldname, id_fieldname):

#     for i in df.index:
#         if stockname == df[fieldname][i]:
#             return df[id_fieldname][i]
#     return False


# def printDict(dictionary):
#     for values, key in zip(dictionary.values(), dictionary.keys()):
#         if type(values) == dict:
#             print(key)
#             printDict(values)
#             continue
#         print("\t", key, values)


# stock_list = pd.read_csv("Stock_List.csv")
# temp = []
# data = []
# for i in stock_list.index:
#     if i == 10:
#         break
#     if stock_list["Stocks"][i] in temp:
#         continue
#     else:
#         ret = searchStock(
#             stock_list["Stocks"][i], stocks_data, fieldname="instrument_name", id_fieldname="instrument_id")
#         if ret is False:
#             print(stock_list["Stocks"][i], " Not Found")
#         else:
#             temp.append({
#                 "instrument_id": ret,
#                 "instrument_name": stock_list["Stocks"][i],
#             })

# print("\tStock_List.csv")
# print(stock_list)


# def find():
#     thread.start_new_thread(find, ("Thread-1", 2, ))
#     thread.start_new_thread(find, ("Thread-2", 4, ))
#     for i in stock_list.index:
#     def innerFind():
#         # print(i)
#         if i % 20 == 0:
#             print(i, " stocks checked")
#         for j in stocks_data.index:
#             if stock_list["Stocks"][i] == stocks_data["instrument_name"][j]:
#                 data.append({
#                     "instrument_name": stock_list["Stocks"][i],
#                     "instrument_id": stocks_data["instrument_id"][j],
#                     "date": stock_list["DATE"][i],
#                 })
#                 break
#     # for i in len(data):
#     #     printDict(data[i])
#     finaldf = pd.DataFrame(data=data)

#     print(finaldf)
#     finaldf.to_csv("dated_stock_list.csv")

# 2020-12-24

# ab = [1, 3, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 17, 19, 21, 22, 24, 26, 27, 28, 31, 32, 34, 35, 37, 39, 40, 43, 45, 46, 48, 50, 51, 52, 53, 54, 56, 58, 59, 60, 61, 62, 63, 64, 66, 67, 68, 69, 70, 71, 72, 73, 76, 77, 78, 79, 82, 83, 84, 85, 87, 88, 89, 91, 92, 93, 95, 96, 97, 98, 99, 100, 102, 103, 104, 105, 107, 108, 109, 111, 113, 114, 115, 116, 117, 118, 120, 121, 123, 125, 126, 127, 128, 129, 131, 132, 133, 134, 135, 137, 138, 139, 140, 141, 142, 145, 147, 149, 150, 152, 153, 154, 156, 157, 158, 161, 162, 163, 167, 169, 170, 171, 173, 175, 176, 178, 180, 181, 183, 184, 185, 186, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 200, 202, 203, 204, 205, 206, 207, 208,
# 210, 211, 212, 213, 214, 215, 217, 218, 219, 220, 221, 223, 227, 229, 230, 231, 232, 233, 234, 235, 237, 238, 239, 241, 242, 244, 245, 246, 247, 251, 252, 254, 255, 258, 262, 263, 264, 265, 266, 267, 268, 269, 270, 272, 273, 276, 277, 278, 280, 281, 283, 284, 285, 286, 287, 288, 289, 291, 292, 293, 294, 296, 297, 300, 301, 302, 303, 306, 307, 310, 311, 312, 313, 314, 315, 317, 319, 320, 321, 322, 323, 324, 327, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 345, 349, 350, 356, 358, 359, 360, 362, 363, 366, 367, 368, 371, 373, 374, 377, 378, 380, 381, 382, 383, 385, 387, 388, 391, 392, 394, 396, 397, 398, 399, 400, 402, 404, 406, 407, 408, 409, 410, 411, 413]
