"""The source of truth: stock per store and SKU, price changes, and order acceptance.

Every change is published to the stock topic, keyed by store:sku, with a per-key
sequence number and the resulting level. Orders arrive on the orders topic and are
accepted or rejected against the true stock. A rejection means the shopper was shown
something that had already sold out: that is the cost of a stale storefront.
"""
import argparse
import asyncio
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, Durability, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, ORDERS, STATUS, STOCK, STREAM, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    cfg = DirectProducerConfig(batch_length=500, linger_time=timedelta(microseconds=500))
    stock_p = await client.producer(STREAM, STOCK, topic_partitions_count=a.partitions, mode=cfg, topic_durability=Durability.REPLICATED)
    status_p = await client.producer(STREAM, STATUS, topic_partitions_count=8, mode=cfg, topic_durability=Durability.REPLICATED)
    orders = await client.consumer_group("store-ops", STREAM, ORDERS, polling_strategy=PollingStrategy.Next(), batch_length=1000,
                                         auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(microseconds=500))
    keys = [f"S{s:03d}:K{k:04d}" for s in range(a.stores) for k in range(a.skus)]
    level = {k: random.randint(0, 8) for k in keys}
    price = {k: round(random.uniform(1, 40), 2) for k in keys}
    seq, stats = defaultdict(int), defaultdict(int)

    def change(k, **extra):
        seq[k] += 1
        return SendMessage(msg(k=k, seq=seq[k], level=level[k], price=price[k], **extra))

    async def on_order(m):
        o = load(m.payload())
        k = o["k"]
        if level[k] > 0:
            level[k] -= 1
            stats["accepted"] += 1
            await stock_p.send_with_partitioning([change(k)], Partitioning.messages_key(k))
            await status_p.send_with_partitioning([SendMessage(msg(order=o["order"], status="confirmed"))], Partitioning.messages_key(o["order"]))
        else:
            stats["rejected"] += 1      # shown as in stock, already gone
            await status_p.send_with_partitioning([SendMessage(msg(order=o["order"], status="rejected_sold_out"))], Partitioning.messages_key(o["order"]))

    async def churn():
        t0, due = time.monotonic(), time.monotonic()
        async with stock_p, status_p:
            while True:
                t = time.monotonic() - t0
                sale = a.sale if a.sale_at and a.sale_at <= t < a.sale_at + a.sale_for else 1
                by_key = {}
                for _ in range(max(1, int(a.rate * sale / 100))):
                    k = random.choice(keys)
                    if random.random() < 0.7 and level[k] > 0:
                        level[k] -= 1                      # in-store pick or another channel's sale
                    elif random.random() < 0.5:
                        level[k] += random.randint(1, 6)   # restock
                    else:
                        price[k] = round(price[k] * random.choice([0.9, 1.05]), 2)
                    by_key.setdefault(k, []).append(change(k))
                for k, ms in by_key.items():
                    await stock_p.send_with_partitioning(ms, Partitioning.messages_key(k))
                due += 0.01
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            total = stats["accepted"] + stats["rejected"]
            rate = 100 * stats["rejected"] / total if total else 0
            print(f"orders accepted {stats['accepted']:,}  rejected as sold out {stats['rejected']:,}  "
                  f"stale-availability rate {rate:.3f}%", flush=True)

    await asyncio.gather(churn(), orders.consume_messages(on_order, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--stores", type=int, default=100)
    p.add_argument("--skus", type=int, default=2000)
    p.add_argument("--rate", type=int, default=5000, help="stock and price changes per second")
    p.add_argument("--partitions", type=int, default=16)
    p.add_argument("--sale", type=float, default=1.0, help="multiplier for changes during the sale")
    p.add_argument("--sale-at", type=float, default=0)
    p.add_argument("--sale-for", type=float, default=120)
    asyncio.run(main(p.parse_args()))
