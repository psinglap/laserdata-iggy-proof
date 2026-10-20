"""Positions and buying power, rebuilt purely from the order log.

On start it replays the log from the first retained event, at full speed, and reports how
long the rebuild took; then it follows live, reporting fill-to-position latency. Risk,
surveillance and statements can each do the same through their own consumer groups.
"""
import argparse
import asyncio
import os
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy

from common import DEFAULT_URL, ORDERS, STREAM, Window, load


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    log = await client.consumer_group(f"positions-{os.getpid()}", STREAM, ORDERS, polling_strategy=PollingStrategy.First(),
                                      batch_length=10000, auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=200))
    pos, n, lat, t0, live = defaultdict(int), 0, Window(), time.monotonic(), False

    async def on_event(m):
        nonlocal n, live
        e = load(m.payload())
        n += 1
        if e["t"] == "filled":
            pos[(e["acct"], e["sym"])] += e["qty"]
            age = (time.time_ns() - e["ns"]) / 1e6
            if not live and age < 100:
                live = True
                print(f"rebuilt {len(pos):,} positions from {n:,} events in {time.monotonic() - t0:.2f}s; now following live", flush=True)
            if live:
                lat.add(age)

    async def report():
        while True:
            await asyncio.sleep(1)
            if live:
                r = lat.flush()
                print(f"positions  fill-to-position p50 {r['p50']:6.2f}ms  p99 {r['p99']:6.2f}ms  accounts with positions "
                      f"{len({k[0] for k in pos}):,}", flush=True)

    await asyncio.gather(log.consume_messages(on_event, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
