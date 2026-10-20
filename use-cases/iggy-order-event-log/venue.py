"""A simulated venue: reads accepted orders and writes fills (or cancels) back to the same log,
keyed by the same account, so the account's whole history stays in one ordered sequence.
"""
import argparse
import asyncio
import random
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, ORDERS, STREAM, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    orders = await client.consumer_group("venue", STREAM, ORDERS, polling_strategy=PollingStrategy.Last(), batch_length=2000,
                                         auto_commit=AutoCommit.Interval(timedelta(milliseconds=500)), poll_interval=timedelta(microseconds=200))
    out = await client.producer(STREAM, ORDERS)
    seq = defaultdict(lambda: 10**9)          # venue events use their own sequence range per account

    async def on_order(m):
        o = load(m.payload())
        if o["t"] != "accepted":
            return
        seq[o["acct"]] += 1
        kind = "filled" if random.random() < 0.9 else "cancelled"
        await out.send_with_partitioning([SendMessage(msg(acct=o["acct"], seq=seq[o["acct"]], order=o["order"], t=kind,
                                                          sym=o["sym"], qty=o["qty"], px=o["px"], order_ns=o["ns"]))],
                                         Partitioning.messages_key(o["acct"]))

    async with out:
        await orders.consume_messages(on_order, asyncio.Event())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
