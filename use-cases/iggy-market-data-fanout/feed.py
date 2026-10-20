"""Synthetic exchange feed: N symbols, steady rate, optional market-open burst.

Ticks for one symbol always land on the same partition (keyed by bucket), so
per-symbol order and gap detection hold. Each tick carries its send time.
"""
import argparse
import asyncio
import random
import time
import zlib
from datetime import timedelta

from apache_iggy import DirectProducerConfig, Durability, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, STREAM, TOPIC, pack


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    producer = await client.producer(
        STREAM, TOPIC,
        mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(microseconds=100)),
        topic_partitions_count=a.partitions,
        topic_durability=Durability.REPLICATED if a.durability == "replicated" else Durability.PERSISTED,
    )
    symbols = [f"SYM{i:04d}" for i in range(a.symbols)]
    bucket = {s: zlib.crc32(s.encode()) % a.partitions for s in symbols}
    seq = {s: 0 for s in symbols}
    mid = {s: random.uniform(100, 3000) for s in symbols}
    started, sent = time.monotonic(), 0
    due = started
    async with producer:
        while a.seconds == 0 or time.monotonic() - started < a.seconds:
            t = time.monotonic() - started
            rate = a.rate * (a.burst if a.burst_at <= t < a.burst_at + a.burst_for else 1)
            per_ms = max(1, int(rate / 1000))
            batches = {}
            for _ in range(per_ms):
                s = random.choice(symbols)
                seq[s] += 1
                mid[s] *= 1 + random.gauss(0, 0.0002)
                batches.setdefault(bucket[s], []).append(
                    SendMessage(pack(s, seq[s], mid[s] - 0.05, mid[s] + 0.05, a.size)))
            for b, msgs in batches.items():
                await producer.send_with_partitioning(msgs, Partitioning.messages_key(f"bucket-{b}"))
            sent += per_ms
            due += per_ms / rate  # pace by schedule, so a burst does not stall the steady phase after it
            await asyncio.sleep(max(0.0, due - time.monotonic()))
    print(f"sent {sent:,} ticks")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--symbols", type=int, default=500)
    p.add_argument("--partitions", type=int, default=16)
    p.add_argument("--rate", type=int, default=20_000, help="ticks per second")
    p.add_argument("--size", type=int, default=128, help="bytes per tick (48 minimum)")
    p.add_argument("--seconds", type=int, default=0, help="0 = run forever")
    p.add_argument("--burst", type=float, default=1.0, help="rate multiplier during the burst")
    p.add_argument("--burst-at", type=float, default=30.0)
    p.add_argument("--burst-for", type=float, default=60.0)
    p.add_argument("--durability", choices=["replicated", "persisted"], default="replicated")
    asyncio.run(main(p.parse_args()))
