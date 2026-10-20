"""Send payments with ids derived from the payment id, so resends are recognisable."""
import asyncio
import json
import os
import sys

from apache_iggy import IggyClient, Partitioning, SendMessage

from ledger import message_id_for

URL = os.environ.get("IGGY_URL", "iggy+tcp://iggy:iggy@127.0.0.1:8090")


async def send_payments(payments, url=URL, stream="payments", topic="events"):
    client = IggyClient.from_connection_string(url)
    await client.connect()
    producer = await client.producer(stream, topic, topic_partitions_count=3)
    async with producer:
        for p in payments:
            msg = SendMessage(json.dumps(p), id=message_id_for(p["payment_id"]))
            # Same account, same partition: one account's payments stay in order.
            await producer.send_with_partitioning([msg], Partitioning.messages_key(p["account"]))


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    asyncio.run(send_payments([{"payment_id": f"p{i}", "account": f"acct-{i % 5}", "cents": 100} for i in range(n)]))
