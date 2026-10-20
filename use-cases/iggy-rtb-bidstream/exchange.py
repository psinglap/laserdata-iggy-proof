"""The exchange side of the auction: sends bid requests, waits up to tmax for bids,
picks winners, and reports wins and rendered impressions back into the log.

Prints per second: requests, auction round trip p50/p99/p99.9 (request out, bid back),
timeouts (bids that arrived after tmax and were thrown away), and frequency-cap
violations (a user shown the same campaign more than its cap within a minute).
"""
import argparse
import asyncio
import random
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, DirectProducerConfig, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import BIDS, CAMPAIGNS, DEFAULT_URL, IMPRESSIONS, REQUESTS, STREAM, WINS, Window, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    fast = DirectProducerConfig(batch_length=256, linger_time=timedelta(microseconds=200))
    req = await client.producer(STREAM, REQUESTS, topic_partitions_count=16, mode=fast)
    win_p = await client.producer(STREAM, WINS, topic_partitions_count=8, mode=fast)
    imp_p = await client.producer(STREAM, IMPRESSIONS, topic_partitions_count=16, mode=fast)
    bids = await client.consumer_group("exchange", STREAM, BIDS, polling_strategy=PollingStrategy.Next(),
                                       batch_length=1000, auto_commit=AutoCommit.Interval(timedelta(seconds=1)),
                                       poll_interval=timedelta(microseconds=200))
    pending, rtt, stats = {}, Window(), defaultdict(int)
    shown = defaultdict(deque)   # (user, campaign) -> impression times in the last minute

    async def on_bid(m):
        b = load(m.payload())
        sent = pending.pop(b["id"], None)
        if sent is None:
            return
        ms = (time.time_ns() - sent) / 1e6
        if ms > a.tmax:
            stats["timeouts"] += 1
            return
        rtt.add(ms)
        if b.get("campaign") and random.random() < a.win_rate:
            c, u = b["campaign"], b["user"]
            await win_p.send_with_partitioning([SendMessage(msg(campaign=c, price=b["price"]))], Partitioning.messages_key(c))
            await imp_p.send_with_partitioning([SendMessage(msg(campaign=c, user=u))], Partitioning.messages_key(u))
            q, now = shown[(u, c)], time.monotonic()
            q.append(now)
            while q and now - q[0] > 60:
                q.popleft()
            if len(q) > CAMPAIGNS[c]["cap"]:
                stats["cap_violations"] += 1

    async def send_requests():
        due, n = time.monotonic(), 0
        async with req, win_p, imp_p:
            while True:
                per_tick = max(1, a.rate // 1000)
                by_user = {}
                for _ in range(per_tick):
                    n += 1
                    rid, user = f"r{n}", f"u{random.randrange(a.users)}"
                    pending[rid] = time.time_ns()
                    by_user.setdefault(user, []).append(SendMessage(msg(id=rid, user=user, tmax=a.tmax, floor=1.5)))
                for user, ms in by_user.items():
                    await req.send_with_partitioning(ms, Partitioning.messages_key(user))
                for rid in [r for r, t in list(pending.items())[:per_tick] if (time.time_ns() - t) / 1e6 > a.tmax]:
                    pending.pop(rid, None)
                    stats["no_bid"] += 1
                due += 0.001
                await asyncio.sleep(max(0.0, due - time.monotonic()))

    async def report():
        while True:
            await asyncio.sleep(1)
            r = rtt.flush()
            print(f"{r['n']:>7,} auctions/s  round trip p50 {r['p50']:5.1f}ms  p99 {r['p99']:5.1f}ms  p99.9 {r['p999']:5.1f}ms  "
                  f"timeouts {stats['timeouts']}  no-bid {stats['no_bid']}  cap violations {stats['cap_violations']}", flush=True)

    await asyncio.gather(send_requests(), bids.consume_messages(on_bid, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--rate", type=int, default=5000, help="bid requests per second")
    p.add_argument("--users", type=int, default=20000)
    p.add_argument("--tmax", type=int, default=100, help="auction deadline, ms")
    p.add_argument("--win-rate", type=float, default=0.2)
    asyncio.run(main(p.parse_args()))
