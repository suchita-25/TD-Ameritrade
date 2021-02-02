import asyncio
import json

from tda.auth import easy_client
import tda.client
from tda.streaming import StreamClient

import config

client = easy_client(
        api_key=config.api_key,
        redirect_uri=config.redirect_uri,
        token_path=config.token_path)

stream_client = StreamClient(client, account_id=config.ACCOUNT_ID)

def order_book_handler(msg):
    print(json.dumps(msg, indent=4))

async def read_stream():
    await stream_client.login()
    await stream_client.quality_of_service(StreamClient.QOSLevel.EXPRESS)

    # Always add handlers before subscribing because many streams start sending
    # data immediately after success, and messages with no handlers are dropped.
    # stream_client.add_nasdaq_book_handler(
    #         lambda msg: print(json.dumps(msg, indent=4)))
    await stream_client.nasdaq_book_subs(['GOOG'])

    stream_client.add_nasdaq_book_handler(order_book_handler)
    while True:
        await stream_client.handle_message()

asyncio.run(read_stream())