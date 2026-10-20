"""The storefront: a search index of what each store has in stock, kept current from the
stock topic, plus shoppers who only ever order what the storefront shows as available.

Prints change-to-storefront latency every second. Run several with different --name to
model search, product pages and carts: each reads every change through its own group.
On restart it rebuilds the whole index by replaying stock changes from the start of the topic.
"""
import argparse
import asyncio
import os
import random
import time
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, ORDERS, STOCK, STREAM, Window, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    view = await client.consumer_group(f"{a.name}-{os.getpid()}", STREAM, STOCK, polling_strategy=PollingStrategy.First(),
                                       batch_length=5000, auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(microseconds=500))
    orders = await client.producer(STREAM, ORDERS, topic_partitions_count=8)
    seq, in_stock, lat = {}, set(), Window()

    async def on_change(m):
        c = load(m.payload())
        if c["seq"] <= seq.get(c["k"], 0):
            return                          # older or repeated change: the newest level wins
        seq[c["k"]] = c["seq"]
        (in_stock.add if c["level"] > 0 else in_stock.discard)(c["k"])
        lat.add((time.time_ns() - c["ns"]) / 1e6)

    async def shop():
        n = 0
        async with orders:
            while True:
                await asyncio.sleep(1 / a.orders_per_s)
                if not in_stock:
                    continue
                k = random.choice(tuple(in_stock))
                n += 1
                await orders.send_with_partitioning([SendMessage(msg(order=f"{a.name}-{n}", k=k))], Partitioning.messages_key(k))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
            print(f"[{a.name}] {r['n']:>7,} changes/s  change-to-storefront p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  in stock {len(in_stock):,}  {ok} (<{a.budget_ms}ms)", flush=True)

    await asyncio.gather(view.consume_messages(on_change, asyncio.Event()), shop(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="search")
    p.add_argument("--orders-per-s", type=float, default=200)
    p.add_argument("--budget-ms", type=int, default=100)
    asyncio.run(main(p.parse_args()))
