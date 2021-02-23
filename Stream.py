import asyncio
import json
from datetime import datetime

from tda.auth import easy_client
import tda.client
from tda.streaming import StreamClient
import pandas as pd
import config
from influxdb import DataFrameClient

client = easy_client(
    api_key=config.api_key,
    redirect_uri=config.redirect_uri,
    token_path=config.token_path)

stream_client = StreamClient(client, account_id=config.ACCOUNT_ID)

# dbclient = InfluxDBClient(host='localhost', port=8086)
# dbclient.create_database('nasdaqdump')

# dbClient = DataFrameClient(host='localhost', port=8086)
# dbClient.create_database(testdump1)

def order_book_handler(msg):
    # print(json.dumps(msg, indent=4))
    #   jsonobject = json.dumps(msg,indent=4)  #DUmp data in terminal
    # with open("Goog_quotes.txt","a") as outfile: # Write json in the file
    #  data1= json.dumps(msg, outfile, indent=4)
     print (type(msg))
     filename=datetime.now().strftime("%Y%m%d%I%M%S%f") + ".json"
     filepath = "/home/suchita/Downloads/jsondump/" + filename
     with open(filepath,"w") as outfile:  # Write json in the file
        json.dump(msg, outfile, indent=4)

        # pd.read_json(msg)
        # data1 = json.dumps(msg)
        # print(data1)
        # data = json.loads(data1)
        # print(type(data))
       # dbClient.write_points(data, database='nasdaqdump', protocol="Line")
     # dbclient.write([data])
    #Load in the DataFrame
        # data = json.loads(msg)
        # dbclient.write_points(data, database='nasdaqdump', protocol='Line')
    # print(data)

async def read_stream():
    await stream_client.login()
    await stream_client.quality_of_service(StreamClient.QOSLevel.EXPRESS)

    # Always add handlers before subscribing because many streams start sending
    # data immediately after success, and messages with no handlers are dropped.
    # stream_client.add_nasdaq_book_handler(
    #         lambda msg: print(json.dumps(msg, indent=4)))
    # await stream_client.nasdaq_book_subs(['GOOG']) #Nasdaq
    #  await stream_client.listed_book_subs(['GOOG']) #NYSE Bid & ASk
    # path4 = "/home/suchita/Downloads/nasdaq_screener_1613097106581.csv"
    path4 = "/home/suchita/Downloads/nasdaq102.csv"
    ftp1 = pd.read_csv(path4, usecols=['Symbol'])
    list1 = ftp1.Symbol.to_list()
    # set1=ftp1.Symbol.to_set()
    # ftp1 = pd.read_csv(path4, header=None)
    print(list1)

    await stream_client.level_one_equity_subs(list1)
    stream_client.add_level_one_equity_handler(order_book_handler)
    # stream_client.add_listed_book_handler(order_book_handler)
    while True:
        await stream_client.handle_message()
asyncio.run(read_stream())

# async def main():
#     print('Started, press ctrl+C')
#     asyncio.run(read_stream())
#
# if __name__ == '__main__':
#
#     async def close():
#         print('Finalizing...')
#         await asyncio.sleep(1)
#
#         loop = asyncio.get_event_loop()
#         try:
#             loop.run_until_complete(main())
#         except KeyboardInterrupt:
#             loop.run_until_complete(close())
#         finally:
#             print('Program finished')
#
#
#
