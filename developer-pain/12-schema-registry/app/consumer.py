"""Consume OrderPlaced events written with any registered schema version, and read them as v2."""
import asyncio
from datetime import timedelta

from apache_iggy import IggyClient, PollingStrategy

from .codec import SCHEMA_ID_HEADER, as_dict, decode, load_schema
from .registry import Registry

CONNECTION = "iggy+tcp://iggy:iggy@127.0.0.1:8090"
READER_SCHEMA = as_dict(load_schema("order_v2.avsc"))  # the version this service is written against


async def main() -> None:
    registry = Registry()
    client = IggyClient.from_connection_string(CONNECTION)
    await client.connect()
    consumer = await client.consumer_group(
        "analytics", "shop", "orders",
        polling_strategy=PollingStrategy.First(),
        poll_interval=timedelta(milliseconds=100),
    )

    async def on_message(message) -> None:
        headers = message.user_headers()
        if headers is None or SCHEMA_ID_HEADER not in headers.to_scalar_dict():
            print(f"offset {message.offset()}: no schema id, skipped")
            return
        schema_id = headers.to_scalar_dict()[SCHEMA_ID_HEADER]
        writer_schema = registry.schema_by_id(schema_id)
        order = decode(message.payload(), writer_schema, READER_SCHEMA)
        print(f"schema {schema_id}: {order}")

    stop = asyncio.Event()
    try:
        await consumer.consume_messages(on_message, stop)
    except (KeyboardInterrupt, asyncio.CancelledError):
        stop.set()


if __name__ == "__main__":
    asyncio.run(main())
