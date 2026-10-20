"""order-api: may only send to the `orders` stream."""
import asyncio
import json
import time

from apache_iggy import Partitioning, SendMessage

from common import STREAM, TOPIC, connect_with_token


async def main() -> None:
    client = connect_with_token()
    await client.connect()
    # A plain send needs only send_messages. (The high-level producer can also
    # create streams and topics, which this user is deliberately not allowed to do.)
    for n in range(1, 1_000_000):
        order = {"order_id": n, "amount_cents": 1999, "ts": time.time()}
        await client.send_messages(STREAM, TOPIC, Partitioning.messages_key(str(n)),
                                   [SendMessage(json.dumps(order))])
        await asyncio.sleep(1)


asyncio.run(main())
