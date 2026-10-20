"""Runs in the cloud: reads every forwarded reading and checks, per device, that nothing was
lost or reordered across outages. Repeats (a reading resent after a reconnect) are skipped.
Prints edge-to-cloud latency while live, and gaps and repeats so far.
"""
import argparse
import asyncio
import time
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy

from common import CLOUD_URL, READINGS, STREAM, Window, load


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    c = await client.consumer_group("cloud-check", STREAM, READINGS, polling_strategy=PollingStrategy.Next(), batch_length=5000,
                                    auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(milliseconds=1))
    last, lat, stats = {}, Window(), {"gaps": 0, "repeats": 0}

    async def on_reading(m):
        r = load(m.payload())
        prev = last.get(r["dev"], 0)
        if r["seq"] <= prev:
            stats["repeats"] += 1
            return
        if prev and r["seq"] > prev + 1:
            stats["gaps"] += 1
        last[r["dev"]] = r["seq"]
        lat.add((time.time_ns() - r["ns"]) / 1e6)

    async def report():
        while True:
            await asyncio.sleep(1)
            x = lat.flush()
            print(f"cloud  {x['n']:>6} readings/s  edge-to-cloud p50 {x['p50']:8.1f}ms  p99 {x['p99']:8.1f}ms  "
                  f"devices {len(last)}  gaps {stats['gaps']}  repeats skipped {stats['repeats']}", flush=True)

    await asyncio.gather(c.consume_messages(on_reading, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=CLOUD_URL)
    asyncio.run(main(p.parse_args()))
