"""Velocity features: how many authorizations each card, device and IP made in the last minute.

Reads every authorization, updates sliding one-minute counts, and publishes each changed
count to the features topic keyed by entity, so every scorer keeps a fresh local copy.
Prints authorization-to-feature latency: how long until a transaction is counted.
"""
import argparse
import asyncio
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import AUTHS, DEFAULT_URL, FEATURES, STREAM, Window, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    auths = await client.consumer_group("features", STREAM, AUTHS, polling_strategy=PollingStrategy.Next(), batch_length=1000,
                                        auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(microseconds=100))
    out = await client.producer(STREAM, FEATURES, topic_partitions_count=16,
                                mode=DirectProducerConfig(batch_length=256, linger_time=timedelta(microseconds=100)))
    seen, lat = defaultdict(deque), Window()

    async def on_auth(m):
        x = load(m.payload())
        now = time.monotonic()
        updates = []
        for kind in ("card", "device", "ip"):
            key = f"{kind}:{x[kind]}"
            q = seen[key]
            q.append(now)
            while q and now - q[0] > 60:
                q.popleft()
            updates.append((key, SendMessage(msg(k=key, f=f"{kind}_1m", v=len(q), auth_ns=x["ns"]))))
        for key, u in updates:
            await out.send_with_partitioning([u], Partitioning.messages_key(key))
        lat.add((time.time_ns() - x["ns"]) / 1e6)

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            print(f"features {r['n']:>6}/s  auth-to-counted p50 {r['p50']:6.2f}ms  p99 {r['p99']:6.2f}ms  p99.9 {r['p999']:6.2f}ms", flush=True)

    async with out:
        await asyncio.gather(auths.consume_messages(on_auth, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
