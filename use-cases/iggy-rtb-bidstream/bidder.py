"""A bidder. Run several: they share the request stream through one consumer group.

Each bidder also reads every impression and every budget update through its own
consumer groups, so its frequency caps and paused campaigns are seconds-fresh, not
minutes-old. --think-ms adds model time so you can see how much of tmax is left.
"""
import argparse
import asyncio
import os
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import BIDS, BUDGET, CAMPAIGNS, DEFAULT_URL, IMPRESSIONS, REQUESTS, STREAM, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    me = f"bidder-{os.getpid()}"
    out = await client.producer(STREAM, BIDS, topic_partitions_count=16,
                                mode=DirectProducerConfig(batch_length=256, linger_time=timedelta(microseconds=200)))
    poll = dict(batch_length=1000, poll_interval=timedelta(microseconds=200))
    requests = await client.consumer_group("bidders", STREAM, REQUESTS, polling_strategy=PollingStrategy.Last(),
                                           auto_commit=AutoCommit.Interval(timedelta(seconds=1)), **poll)
    imps = await client.consumer_group(f"{me}-imp", STREAM, IMPRESSIONS, polling_strategy=PollingStrategy.Timestamp(int((time.time() - 60) * 1e6)),
                                       auto_commit=AutoCommit.Disabled(), **poll)
    budget = await client.consumer_group(f"{me}-budget", STREAM, BUDGET, polling_strategy=PollingStrategy.Timestamp(int((time.time() - 60) * 1e6)),
                                         auto_commit=AutoCommit.Disabled(), **poll)
    seen = defaultdict(deque)   # (user, campaign) -> impression times
    paused = set()

    async def on_imp(m):
        i = load(m.payload())
        seen[(i["user"], i["campaign"])].append(time.monotonic())

    async def on_budget(m):
        b = load(m.payload())
        (paused.add if b["paused"] else paused.discard)(b["campaign"])

    async def on_request(m):
        r = load(m.payload())
        now, choices = time.monotonic(), []
        for c, spec in CAMPAIGNS.items():
            if c in paused:
                continue
            q = seen[(r["user"], c)]
            while q and now - q[0] > 60:
                q.popleft()
            if len(q) < spec["cap"]:
                choices.append((c, spec["cpm"]))
        if a.think_ms:
            await asyncio.sleep(a.think_ms / 1000)
        if choices:
            c, cpm = random.choice(choices)
            bid = msg(id=r["id"], user=r["user"], campaign=c, price=round(cpm / 1000, 5))
        else:
            bid = msg(id=r["id"], user=r["user"], campaign=None, price=0)
        await out.send_with_partitioning([SendMessage(bid)], Partitioning.messages_key(r["id"]))

    async with out:
        await asyncio.gather(requests.consume_messages(on_request, asyncio.Event()),
                             imps.consume_messages(on_imp, asyncio.Event()),
                             budget.consume_messages(on_budget, asyncio.Event()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--think-ms", type=float, default=0, help="simulated model time per request")
    asyncio.run(main(p.parse_args()))
