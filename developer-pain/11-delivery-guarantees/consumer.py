"""Consume payments effectively once: apply, then store the offset.

Delivery from Apache Iggy is at-least-once when the offset is stored after processing.
If the process dies between the database commit and store_offset(), the message is
delivered again and apply_payment() recognises it by id.
"""
import asyncio
import json
import os
import sys

from apache_iggy import AutoCommit, IggyClient, PollingStrategy

from ledger import apply_payment, open_db

URL = os.environ.get("IGGY_URL", "iggy+tcp://iggy:iggy@127.0.0.1:8090")
CRASH_AFTER = int(os.environ.get("CRASH_AFTER", "0"))  # crash test hook: die after N applies


async def run(db_path, url=URL, stream="payments", topic="events", stop=None):
    db = open_db(db_path)
    client = IggyClient.from_connection_string(url)
    await client.connect()
    consumer = await client.consumer_group(
        "ledger", stream, topic,
        polling_strategy=PollingStrategy.Next(),
        auto_commit=AutoCommit.Disabled(),  # we store the offset ourselves, after the DB commit
    )
    applied = 0
    stop = stop or asyncio.Event()
    while not stop.is_set():
        async for message in consumer.iter_messages():
            p = json.loads(message.payload())
            apply_payment(db, message.id(), p["account"], p["cents"])
            applied += 1
            if CRASH_AFTER and applied == CRASH_AFTER:
                os._exit(17)  # the worst moment: DB committed, offset not stored
            # Only now tell Apache Iggy this message is done.
            await consumer.store_offset(message.offset(), message.partition_id())
        await asyncio.sleep(0.05)  # caught up; poll again shortly


if __name__ == "__main__":
    asyncio.run(run(sys.argv[1] if len(sys.argv) > 1 else "ledger.db"))
