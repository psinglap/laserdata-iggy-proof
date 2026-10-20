"""Checks the partition limits Apache Iggy enforces, against a running server.

1. One create call accepts at most 1,000 partitions (expected: 1,001 is refused).
2. A topic can grow past 1,000 by adding partitions in more calls.
Prints PASS/FAIL per check; it does not need any load.
"""
import argparse, asyncio, uuid

from apache_iggy import IggyClient


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="iggy+tcp://iggy:iggy@127.0.0.1:8090")
    p.add_argument("--grow-to", type=int, default=3000)
    args = p.parse_args()
    c = IggyClient.from_connection_string(args.url)
    await c.connect()
    stream = f"limits-{uuid.uuid4().hex[:6]}"
    await c.create_stream(stream)

    try:
        await c.create_topic(stream, "too-many", partitions_count=1001)
        print("FAIL  create_topic with 1,001 partitions was accepted")
    except (RuntimeError, ValueError) as e:
        print(f"PASS  create_topic with 1,001 partitions refused: {e}")

    await c.create_topic(stream, "grown", partitions_count=1000)
    total = 1000
    while total < args.grow_to:
        step = min(1000, args.grow_to - total)
        await c.create_partitions(stream, "grown", step)
        total += step
    topic = await c.get_topic(stream, "grown")
    ok = topic.partitions_count == args.grow_to
    print(f"{'PASS' if ok else 'FAIL'}  topic grown to {topic.partitions_count} partitions in {args.grow_to // 1000} calls")


if __name__ == "__main__":
    asyncio.run(main())
