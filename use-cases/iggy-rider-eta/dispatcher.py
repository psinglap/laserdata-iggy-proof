"""Assigns each new order to the nearest free courier, using the latest position of every courier.

Prints how old the courier position was at the moment of dispatch (p50 / p99). A dispatcher
working from 4-second-old positions sends the wrong courier. On restart it rebuilds every
courier's last position by replaying the last 30 seconds of pings, so it holds no state.
"""
import argparse
import asyncio
import os
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, ETA, ORDERS, PINGS, STREAM, Window, cell, km, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    poll = dict(batch_length=2000, poll_interval=timedelta(milliseconds=1))
    pings = await client.consumer_group(f"dispatch-pings-{os.getpid()}", STREAM, PINGS,
                                        polling_strategy=PollingStrategy.Timestamp(int((time.time() - 30) * 1e6)),
                                        auto_commit=AutoCommit.Disabled(), **poll)
    orders = await client.consumer_group("dispatch-orders", STREAM, ORDERS, polling_strategy=PollingStrategy.Next(),
                                         auto_commit=AutoCommit.Interval(timedelta(seconds=1)), **poll)
    out = await client.producer(STREAM, ETA, topic_partitions_count=8)
    last = {}                      # courier -> (lat, lon, ns, seq)
    grid = defaultdict(set)        # cell -> couriers
    busy, stale = set(), Window()

    async def on_ping(m):
        p = load(m.payload())
        old = last.get(p["c"])
        if old and p["seq"] <= old[3]:
            return                 # out-of-date or repeated ping: latest wins
        if old:
            grid[cell(old[0], old[1])].discard(p["c"])
        last[p["c"]] = (p["lat"], p["lon"], p["ns"], p["seq"])
        grid[cell(p["lat"], p["lon"])].add(p["c"])

    async def on_order(m):
        o = load(m.payload())
        cy, cx = cell(o["lat"], o["lon"])
        best = None
        for r in range(0, 6):                       # widen the search ring until someone is free
            for dy in range(-r, r + 1):
                for dx in range(-r, r + 1):
                    for c in grid.get((cy + dy, cx + dx), ()):
                        if c in busy:
                            continue
                        d = km(o["lat"], o["lon"], *last[c][:2])
                        if best is None or d < best[0]:
                            best = (d, c)
            if best:
                break
        if not best:
            return
        c = best[1]
        busy.add(c)
        stale.add((time.time_ns() - last[c][2]) / 1e6)
        await out.send_with_partitioning([SendMessage(msg(order=o["order"], c=c, lat=o["lat"], lon=o["lon"], km=round(best[0], 2)))],
                                         Partitioning.messages_key(o["order"]))
        asyncio.get_running_loop().call_later(a.trip_s, busy.discard, c)

    async def report():
        while True:
            await asyncio.sleep(1)
            r = stale.flush()
            print(f"{r['n']:>5} dispatched/s  position age at dispatch p50 {r['p50']:7.0f}ms  p99 {r['p99']:7.0f}ms  "
                  f"couriers known {len(last):,}", flush=True)

    async with out:
        await asyncio.gather(pings.consume_messages(on_ping, asyncio.Event()),
                             orders.consume_messages(on_order, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--trip-s", type=float, default=60, help="seconds a courier stays busy per order")
    asyncio.run(main(p.parse_args()))
