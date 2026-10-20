"""Turns raw reactions into one count update per match every 250 ms, published as an event.

Screens receive "1,204 fire in the last 250 ms" instead of 1,204 messages. It also prints
reaction ingest lag (tap to aggregator) so the burst test shows whether Iggy kept up.
"""
import argparse
import asyncio
import json
import time
from collections import Counter, defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, EVENTS, REACTIONS, STREAM, Window, event

async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    out = await client.producer(STREAM, EVENTS)
    consumer = await client.consumer_group("aggregator", STREAM, REACTIONS,
                                           polling_strategy=PollingStrategy.Next(), batch_length=5000,
                                           auto_commit=AutoCommit.Interval(timedelta(seconds=1)),
                                           poll_interval=timedelta(milliseconds=1))
    counts, lag = defaultdict(Counter), Window()

    async def on_reaction(m):
        r = json.loads(m.payload())
        counts[r["m"]][r["r"]] += 1
        lag.add((time.time_ns() - r["ns"]) / 1e6)

    async def flush():
        async with out:
            while True:
                await asyncio.sleep(0.25)
                snap = dict(counts)
                counts.clear()
                for match, c in snap.items():  # seq 0: counts are a live snapshot, never replayed on resume
                    await out.send_with_partitioning(
                        [SendMessage(event(match, 0, "reactions", counts=dict(c)))], Partitioning.messages_key(match))
                if lag.ready():
                    r = lag.flush()
                    print(f"{r['n']:>9,} reactions/s ingested  lag p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms", flush=True)

    await asyncio.gather(consumer.consume_messages(on_reaction, asyncio.Event()), flush())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
