"""Proves the bill: replays raw usage for the last N minutes, recounts it with deduplication,
and compares every closed tenant-minute with what the meter published. Billing teams run this
before invoices go out. Expected result: 0 mismatches.
"""
import argparse
import asyncio
import os
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy

from common import DEFAULT_URL, ROLLUPS, STREAM, USAGE, load


async def read_all(client, topic, since_us, handler):
    c = await client.consumer_group(f"reconcile-{topic}-{os.getpid()}", STREAM, topic, polling_strategy=PollingStrategy.Timestamp(since_us),
                                    batch_length=10000, auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(milliseconds=1))
    idle, stop = [time.monotonic()], asyncio.Event()

    async def on(m):
        idle[0] = time.monotonic()
        handler(load(m.payload()))

    async def until_idle():
        while time.monotonic() - idle[0] < 2:
            await asyncio.sleep(0.5)
        stop.set()

    await asyncio.gather(c.consume_messages(on, stop), until_idle())


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    since = int((time.time() - a.minutes * 60) * 1e6)
    now_min = int(time.time() // 60)
    raw, ids, published = defaultdict(int), set(), {}

    def on_usage(e):
        if e["id"] in ids:
            return
        ids.add(e["id"])
        raw[(e["tenant"], int(e["ns"] / 1e9 // 60))] += e["units"]

    def on_rollup(r):
        published[(r["tenant"], r["minute"])] = published.get((r["tenant"], r["minute"]), 0) + r["units"]   # corrections add up

    await read_all(client, USAGE, since - 60_000_000, on_usage)
    await read_all(client, ROLLUPS, since, on_rollup)
    first = min((k[1] for k in published), default=now_min)
    checked = [k for k in published if first < k[1] < now_min]       # whole minutes only
    bad = [k for k in checked if raw.get(k, 0) != published[k]]
    print(f"checked {len(checked):,} tenant-minutes from {len(ids):,} unique usage events: {len(bad)} mismatches")
    for k in bad[:10]:
        print(f"  {k[0]} minute {k[1]}: meter {published[k]:,} vs recount {raw.get(k, 0):,}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--minutes", type=int, default=10)
    asyncio.run(main(p.parse_args()))
