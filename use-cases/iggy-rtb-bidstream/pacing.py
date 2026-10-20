"""Budget pacing: sums winning prices per campaign per minute and pauses a campaign
the moment it reaches its budget, by publishing to the budget topic every bidder reads.

Prints overspend: money won after the budget was reached, because bids were already
in flight or the pause had not reached the bidders yet. Slow streams show up here as
real money.
"""
import argparse
import asyncio
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy, SendMessage

from common import BUDGET, CAMPAIGNS, DEFAULT_URL, STREAM, WINS, Window, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    out = await client.producer(STREAM, BUDGET, topic_partitions_count=1)
    wins = await client.consumer_group("pacing", STREAM, WINS, polling_strategy=PollingStrategy.Next(), batch_length=1000,
                                       auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(microseconds=200))
    spend, over, paused, lag = defaultdict(float), defaultdict(float), set(), Window()
    minute = int(time.time() // 60)

    async def on_win(m):
        nonlocal minute
        w = load(m.payload())
        lag.add((time.time_ns() - w["ns"]) / 1e6)
        if int(time.time() // 60) != minute:          # new minute: budgets reset, campaigns resume
            minute = int(time.time() // 60)
            for c in list(paused):
                await out.send([SendMessage(msg(campaign=c, paused=False))])
            spend.clear()
            paused.clear()
        c = w["campaign"]
        if c in paused:
            over[c] += w["price"]
            return
        spend[c] += w["price"]
        if spend[c] >= CAMPAIGNS[c]["budget_per_min"]:
            paused.add(c)
            await out.send([SendMessage(msg(campaign=c, paused=True))])

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lag.flush()
            print(f"win-to-pacing lag p99 {r['p99']:5.1f}ms  paused {len(paused):>2}/20  "
                  f"overspend this run ${sum(over.values()):,.2f}", flush=True)

    async with out:
        await asyncio.gather(wins.consume_messages(on_win, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
