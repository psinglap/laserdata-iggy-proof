"""Viewer reactions (emoji taps) flowing into Iggy, with a burst when something happens.

In production these arrive through the gateways; here they are produced directly so the
burst can be sized independently of how many screens your laptop can hold open.
"""
import argparse
import asyncio
import random
import time

from apache_iggy import BackgroundProducerConfig, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, REACTIONS, STREAM

EMOJI = ["fire", "clap", "wow", "sad"]


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    producer = await client.producer(STREAM, REACTIONS, topic_partitions_count=a.partitions,
                                     mode=BackgroundProducerConfig())  # sends queue in the background: taps never block the UI
    t0, due = time.monotonic(), time.monotonic()
    async with producer:
        while True:
            t = time.monotonic() - t0
            rate = a.rate * (a.burst if a.burst_at and a.burst_at <= t < a.burst_at + a.burst_for else 1)
            per_tick = max(1, int(rate / 100))
            by_match = {}
            for _ in range(per_tick):
                m = f"M{random.randrange(a.matches):03d}"
                by_match.setdefault(m, []).append(SendMessage(f'{{"m":"{m}","r":"{random.choice(EMOJI)}","ns":{time.time_ns()}}}'))
            for m, msgs in by_match.items():
                await producer.send_with_partitioning(msgs, Partitioning.messages_key(m))
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--matches", type=int, default=50)
    p.add_argument("--partitions", type=int, default=16)
    p.add_argument("--rate", type=int, default=5000, help="reactions per second")
    p.add_argument("--burst", type=float, default=1.0)
    p.add_argument("--burst-at", type=float, default=0)
    p.add_argument("--burst-for", type=float, default=30)
    asyncio.run(main(p.parse_args()))
