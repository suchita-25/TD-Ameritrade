from influxdb import DataFrameClient
import os
import json
import pandas as pd


path1 = "/home/suchita/PycharmProjects/StreamingTDA/tryjson"
jsonfile = path1

# dict2 = {} # create an empty list

def create_dict(dict1, timestamp):
    cols = ["timestamp", "key", "BID_PRICE", "ASK_PRICE", "BID_SIZE", "ASK_SIZE", "ASK_ID", "BID_ID", "TOTAL_VOLUME",
            "LAST_SIZE", "TRADE_TIME", "QUOTE_TIME", "HIGH_PRICE", "LOW_PRICE", "BID_TICK", "CLOSE_PRICE",
            "EXCHANGE_ID",
            "MARGINABLE", "SHORTABLE", "ISLAND_BID_DEPRECATED", "ISLAND_ASK_DEPRECATED", "ISLAND_VOLUME_DEPRECATED",
            "QUOTE_DAY", "TRADE_DAY", "VOLATILITY", "DESCRIPTION", "LAST_ID", "DIGITS", "OPEN_PRICE",
            "NET_CHANGE", "HIGH_52_WEEK", "LOW_52_WEEK", "PE_RATIO", "DIVIDEND_AMOUNT", "DIVIDEND_YIELD",
            "ISLAND_BID_SIZE_DEPRECATED", "ISLAND_ASK_SIZE_DEPRECATED", "NAV", "FUND_PRICE",
            "EXCHANGE_NAME", "DIVIDEND_DATE", "IS_REGULAR_MARKET_QUOTE", "IS_REGULAR_MARKET_TRADE",
            "REGULAR_MARKET_LAST_PRICE", "REGULAR_MARKET_LAST_SIZE", "REGULAR_MARKET_TRADE_TIME",
            "REGULAR_MARKET_TRADE_DAY", "REGULAR_MARKET_NET_CHANGE", "SECURITY_STATUS", "MARK", "QUOTE_TIME_IN_LONG",
            "TRADE_TIME_IN_LONG","REGULAR_MARKET_TRADE_TIME_IN_LONG"]
    newdict = {}
    newdict = newdict.fromkeys(cols)
    newdict['timestamp'] = timestamp
    for k, v in dict1.items():
        if k in cols:
            newdict[k] = v
    return newdict

def get_list_of_json_files():
    list_of_files = os.listdir('/home/suchita/PycharmProjects/StreamingTDA/tryjson/')  # creates list of all the files in the folder
    return list_of_files

def write_db():

    cols1 = ["timestamp", "key", "BID_PRICE", "ASK_PRICE", "BID_SIZE", "ASK_SIZE", "ASK_ID", "BID_ID", "TOTAL_VOLUME",
        "LAST_SIZE", "TRADE_TIME", "QUOTE_TIME", "HIGH_PRICE", "LOW_PRICE", "BID_TICK", "CLOSE_PRICE", "EXCHANGE_ID",
        "MARGINABLE", "SHORTABLE", "ISLAND_BID_DEPRECATED", "ISLAND_ASK_DEPRECATED", "ISLAND_VOLUME_DEPRECATED",
        "QUOTE_DAY", "TRADE_DAY", "VOLATILITY", "DESCRIPTION", "LAST_ID", "DIGITS", "OPEN_PRICE",
        "NET_CHANGE", "HIGH_52_WEEK", "LOW_52_WEEK", "PE_RATIO", "DIVIDEND_AMOUNT", "DIVIDEND_YIELD",
        "ISLAND_BID_SIZE_DEPRECATED", "ISLAND_ASK_SIZE_DEPRECATED", "NAV", "FUND_PRICE",
        "EXCHANGE_NAME", "DIVIDEND_DATE", "IS_REGULAR_MARKET_QUOTE", "IS_REGULAR_MARKET_TRADE",
        "REGULAR_MARKET_LAST_PRICE", "REGULAR_MARKET_LAST_SIZE", "REGULAR_MARKET_TRADE_TIME",
        "REGULAR_MARKET_TRADE_DAY", "REGULAR_MARKET_NET_CHANGE", "SECURITY_STATUS", "MARK", "QUOTE_TIME_IN_LONG",
        "TRADE_TIME_IN_LONG","REGULAR_MARKET_TRADE_TIME_IN_LONG"]
    # with open('//home/suchita/PycharmProjects/StreamingTDA/output.csv', 'a', newline='') as c:
    #     writer = csv.DictWriter(c, fieldnames=cols1)
    #     writer.writeheader()
    dbClient = DataFrameClient('localhost', 8086, 'stockdata')
    dbClient.create_database('stockdata')
        # tagdic = {'key'}
    list_of_files = get_list_of_json_files()
    for file in list_of_files:
        print(file)
        with open(path1 + "/" + file) as f:
            data = json.loads(f)
        count = len(data['content'])

        for i in range(count):
            dict2 = {**data['content'][i]}
            dict3 = create_dict(dict2, data['timestamp'])
            df = pd.DataFrame(dict3)
            print(dict3)
            dbClient.write_points(pd, measurement = 'StockData', database= 'stockdata')


if __name__ == "__main__":
    write_db()