"""Test 1: consumer memory while it catches up on a large backlog, at 2, 10 and 50 partitions.

The problem: a consumer that sat at about 800 MB grew past 6 GB after its topic went from
2 to 50 partitions with millions of messages of lag, and no single setting kept it bounded.

What this does, for one partition count:
  preload  a separate process writes N records (1 KiB each, default 7 million) with the Laser SDK
  consume  a fresh process joins a consumer group with the Laser SDK, using the SDK's default
           settings, and reads the whole backlog from the first offset
The sampler records the consumer process's memory every second. Every record carries its
sequence number, and the run fails if any record is missing.

Usage: python consumer_backlog.py preload --partitions 50
       python consumer_backlog.py consume --partitions 50
       python consumer_backlog.py cleanup --partitions 50
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time

import common as c

TEST = "consumer_backlog"


def names(partitions: int) -> tuple[str, str]:
    return f"fp-backlog-{partitions}", "events"


async def preload(partitions: int, total: int) -> None:
    stream, topic_name = names(partitions)
    c.set_phase(TEST, f"preload-{partitions}p")
    laser = await c.connect()
    try:
        topic = laser.stream(stream).topic(topic_name)
        producer = topic.producer(partitions=partitions, batch_length=c.BATCH,
                                  create_stream=True, never_expire=True)
        await producer.init()
        start = time.perf_counter()
        for first in range(0, total, c.BATCH):
            await producer.send_batch([c.payload(s) for s in range(first, min(first + c.BATCH, total))])
        took = time.perf_counter() - start
        c.event(TEST, "preloaded", partitions=partitions, records=total, seconds=round(took, 1))
        print(f"preloaded {total:,} records over {partitions} partitions in {took:.0f}s")
    finally:
        await laser.close()


async def consume(partitions: int, total: int) -> int:
    stream, topic_name = names(partitions)
    c.register_client("consumer")
    c.set_phase(TEST, f"consume-{partitions}p")
    laser = await c.connect()
    tracker = c.SeqTracker(total)
    idle_limit = 60.0
    try:
        group = laser.stream(stream).topic(topic_name).consumer_group("footprint")
        consumer = group.consumer(polling="first")  # every other setting is the SDK default
        await consumer.init()
        start = last = time.perf_counter()
        while tracker.unique < total:
            try:
                message = await consumer.next_within(int(idle_limit * 1000))
            except Exception as error:  # nothing arrived for idle_limit seconds
                print(f"stopped waiting: {error!r}")
                break
            if message is None:
                break
            tracker.add(c.seq_of(message.payload))
            last = time.perf_counter()
        took = last - start
        await consumer.shutdown()
    finally:
        await laser.close()
        c.unregister_client("consumer")
    result = {"partitions": partitions, "records": total, "received_unique": tracker.unique,
              "duplicates": tracker.duplicates, "missing": total - tracker.unique,
              "seconds": round(took, 1), "records_per_second": round(tracker.unique / took) if took else 0,
              "consumer_settings": "Laser SDK defaults, polling='first'"}
    c.write_result(f"{TEST}-{partitions}p", result)
    c.event(TEST, "consumed", **result)
    print(result)
    return 0 if result["missing"] == 0 else 1


async def cleanup(partitions: int) -> None:
    stream, _ = names(partitions)
    laser = await c.connect()
    try:
        await laser.stream(stream).delete()
    finally:
        await laser.close()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=["preload", "consume", "cleanup"])
    p.add_argument("--partitions", type=int, required=True)
    p.add_argument("--records", type=int, default=c.scaled(7_000_000))
    args = p.parse_args()
    if args.step == "preload":
        asyncio.run(preload(args.partitions, args.records))
    elif args.step == "consume":
        return asyncio.run(consume(args.partitions, args.records))
    else:
        asyncio.run(cleanup(args.partitions))
    return 0


if __name__ == "__main__":
    sys.exit(main())
