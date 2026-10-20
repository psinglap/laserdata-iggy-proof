"""The meter: counts each usage event once, closes per-tenant totals every minute, and cuts a
tenant off the moment it crosses its quota.

Delivery is at-least-once, so the meter remembers recent event ids and skips repeats; that is
what makes the totals exact without exactly-once delivery. Every minute it publishes one
rollup per tenant to the rollups topic. When a tenant crosses its quota it publishes a cutoff,
and it reports overage: units the tenant consumed after crossing, before the cutoff took effect.
"""
import argparse
import asyncio
import time
from collections import OrderedDict, defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, QUOTA, ROLLUPS, STREAM, USAGE, Window, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    usage = await client.consumer_group("meter", STREAM, USAGE, polling_strategy=PollingStrategy.Next(), batch_length=5000,
                                        auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(milliseconds=1))
    rollups = await client.producer(STREAM, ROLLUPS, topic_partitions_count=8)
    quota = await client.producer(STREAM, QUOTA)
    seen = OrderedDict()                       # recent event ids, for deduplication
    minute_total, minute_n, day_total = defaultdict(int), defaultdict(int), defaultdict(int)
    cut, overage, lag, dups, closed = set(), defaultdict(int), Window(), 0, set()
    late = 0

    async def closer():
        # close a minute only after a grace period, so events still in flight from other partitions are counted
        while True:
            await asyncio.sleep(1)
            done = [k for k in minute_total if time.time() > (k[1] + 1) * 60 + a.grace_s]
            for k in done:
                await rollups.send_with_partitioning(
                    [SendMessage(msg(tenant=k[0], minute=k[1], units=minute_total.pop(k), events=minute_n.pop(k)))],
                    Partitioning.messages_key(k[0]))
                closed.add(k[1])

    async def on_usage(mm):
        nonlocal dups, late
        e = load(mm.payload())
        if e["id"] in seen:
            dups += 1
            return
        seen[e["id"]] = True
        if len(seen) > 2_000_000:
            seen.popitem(last=False)
        lag.add((time.time_ns() - e["ns"]) / 1e6)
        m = int(e["ns"] / 1e9 // 60)
        t = e["tenant"]
        if m in closed:
            late += 1                                 # arrived after its minute closed: goes out as a correction rollup
        minute_total[(t, m)] += e["units"]
        minute_n[(t, m)] += 1
        day_total[t] += e["units"]
        if t in cut:
            overage[t] += e["units"]                # consumed after the quota was crossed
        elif day_total[t] > a.quota:
            cut.add(t)
            await quota.send([SendMessage(msg(tenant=t, action="cutoff", total=day_total[t]))])

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lag.flush()
            print(f"{r['n']:>7,} events/s  event-to-meter p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"duplicates skipped {dups:,}  late {late}  tenants cut off {len(cut)}  overage units {sum(overage.values()):,}", flush=True)

    async with rollups, quota:
        await asyncio.gather(usage.consume_messages(on_usage, asyncio.Event()), closer(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--grace-s", type=float, default=5, help="seconds after a minute ends before it is closed")
    p.add_argument("--quota", type=int, default=5_000_000, help="units per tenant before cutoff")
    asyncio.run(main(p.parse_args()))
