"""Recomputes the arrival time for every active delivery each time its courier moves, and
measures ping-to-ETA latency: from the courier's phone sending a position to the new ETA
being ready for the customer's map. Every reader (maps, notifications, support tools)
reads the same ETA topic through its own consumer group.
"""
import argparse
import asyncio
import os
import time
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, ETA, PINGS, STREAM, Window, km, load, msg

SPEED_KMH = 18.0


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    poll = dict(batch_length=2000, poll_interval=timedelta(milliseconds=1))
    assigned = await client.consumer_group(f"eta-assign-{os.getpid()}", STREAM, ETA,
                                           polling_strategy=PollingStrategy.Timestamp(int((time.time() - 120) * 1e6)),
                                           auto_commit=AutoCommit.Disabled(), **poll)
    pings = await client.consumer_group("eta", STREAM, PINGS, polling_strategy=PollingStrategy.Last(),
                                        auto_commit=AutoCommit.Interval(timedelta(seconds=1)), **poll)
    out = await client.producer(STREAM, ETA)
    active, lat = {}, Window()     # courier -> (order, dest_lat, dest_lon)

    async def on_assign(m):
        e = load(m.payload())
        if "c" in e and "eta_s" not in e:
            active[e["c"]] = (e["order"], e["lat"], e["lon"])

    async def on_ping(m):
        p = load(m.payload())
        job = active.get(p["c"])
        if not job:
            return
        eta_s = km(p["lat"], p["lon"], job[1], job[2]) / SPEED_KMH * 3600
        await out.send_with_partitioning([SendMessage(msg(order=job[0], eta_s=round(eta_s), ping_ns=p["ns"]))],
                                         Partitioning.messages_key(job[0]))
        lat.add((time.time_ns() - p["ns"]) / 1e6)

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"{r['n']:>6} ETA updates/s  ping-to-ETA p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  p99.9 {r['p999']:6.1f}ms  "
                  f"active deliveries {len(active):,}  {ok} (<{a.budget_ms}ms)", flush=True)

    async with out:
        await asyncio.gather(assigned.consume_messages(on_assign, asyncio.Event()),
                             pings.consume_messages(on_ping, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--budget-ms", type=int, default=500)
    asyncio.run(main(p.parse_args()))
