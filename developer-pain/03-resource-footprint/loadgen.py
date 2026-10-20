"""Rate-controlled load for footprint runs, using the official Apache Iggy Python SDK.

Sends fixed-size messages at a target rate for a fixed time, in batches, and
consumes them with a consumer group so the server does the full round trip.
Run it on a separate machine (or disjoint CPUs) so the client is never the limit.
"""
import argparse, asyncio, os, time
from datetime import timedelta

from apache_iggy import IggyClient, PollingStrategy, SendMessage


async def produce(client, args):
    producer = await client.producer(args.stream, args.topic, topic_partitions_count=args.partitions)
    payload = os.urandom(args.size)
    batch = [SendMessage(payload) for _ in range(args.batch)]
    interval = args.batch / args.rate          # seconds per batch at the target rate
    sent, start = 0, time.perf_counter()
    async with producer:
        while time.perf_counter() - start < args.seconds:
            await producer.send(batch)
            sent += args.batch
            ahead = start + sent / args.rate - time.perf_counter()
            if ahead > 0:
                await asyncio.sleep(ahead)
    elapsed = time.perf_counter() - start
    print(f"produced {sent} messages in {elapsed:.1f}s = {sent / elapsed:,.0f} msg/s (target {args.rate:,})")
    return sent / elapsed


async def consume(client, args, stop):
    consumer = await client.consumer_group(
        "footprint", args.stream, args.topic,
        polling_strategy=PollingStrategy.Next(), batch_length=1000,
        poll_interval=timedelta(milliseconds=1))
    count = 0

    async def on_message(_):
        nonlocal count
        count += 1

    await consumer.consume_messages(on_message, stop)
    print(f"consumed {count} messages")


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="iggy+tcp://iggy:iggy@127.0.0.1:8090")
    p.add_argument("--rate", type=int, required=True, help="messages per second")
    p.add_argument("--seconds", type=int, default=300)
    p.add_argument("--size", type=int, default=256, help="payload bytes")
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--partitions", type=int, default=8)
    p.add_argument("--stream", default="footprint")
    p.add_argument("--topic", default="load")
    args = p.parse_args()
    client = IggyClient.from_connection_string(args.url)
    await client.connect()
    stop = asyncio.Event()
    consumer = asyncio.create_task(consume(client, args, stop))
    achieved = await produce(client, args)
    stop.set()
    await consumer
    if achieved < 0.95 * args.rate:
        print("WARNING: below 95% of target. Add client machines before reading the server numbers.")


if __name__ == "__main__":
    asyncio.run(main())
