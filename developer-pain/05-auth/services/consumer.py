"""billing-worker: may only read the `orders` stream."""
import asyncio
from datetime import timedelta

from apache_iggy import PollingStrategy

from common import STREAM, TOPIC, connect_with_token


async def main() -> None:
    client = connect_with_token()
    await client.connect()
    consumer = await client.consumer_group(
        "billing", STREAM, TOPIC,
        polling_strategy=PollingStrategy.Next(), poll_interval=timedelta(milliseconds=50))

    async def handle(message):
        print("billing", message.offset(), message.payload().decode())

    await consumer.consume_messages(handle, asyncio.Event())


asyncio.run(main())
