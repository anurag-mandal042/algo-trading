import pandas as pd
HDFC_BANK_DEC_FUT_STOCK = {
    'id': 257,
    "filename": 'hdfc_bank_dec_fut'
}
RELIANCE_STOCK = {
    'id': 90,
    "filename": 'reliance_stock'
}
TATASTEEL_STOCK = {
    'id': 41,
    "filename": "tata_steel"
}
WIPRO_STOCK = {
    'id': 4,
    "filename": "wipro"
}
AMBUJA_CEMENT_STOCK = {
    'id': 393,
    "filename": "ambuja_cement"
}
# BHARAT_FORG_STOCK = {
#     'id': 4,
#     "filename": "wipro"
# }


selected_stock = AMBUJA_CEMENT_STOCK
all_stocks = [AMBUJA_CEMENT_STOCK, HDFC_BANK_DEC_FUT_STOCK,
              RELIANCE_STOCK, TATASTEEL_STOCK, WIPRO_STOCK]
df = pd.read_csv("Stock_Insturment_IDs.csv")
# # df = pd.read_csv("stocks_data.csv")
# df.columns = [
#     'id', 'instrument_id',
#     'qw', 'open',
#     'instrument_name', 'low',
#     'close', 'volume',
#     'status', 'created_at',
#     'status', 'created_at',
#     'status', 'created_at',
#     'status', 'created_at',
#     'updated_at', "hgfg"
# ]
# data = {
#     "instrument_id": df["instrument_id"],
#     "instrument_name": df["instrument_name"]
# }
# new_df = pd.DataFrame(data=data)
# new_df.index += 1
# print(new_df)
# new_df.to_csv("stocks_data.csv")
# # print(df)
