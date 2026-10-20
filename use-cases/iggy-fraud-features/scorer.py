"""The authorization decision: approve or block each payment using velocity rules, reading
features from a local copy kept fresh from the features topic.

The question that matters is not average latency but how many attack transactions get
approved before the features catch up. Prints decision latency and, during an attack,
how many attempts were approved versus blocked.
"""
import argparse
import asyncio
import os
import time
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import AUTHS, DECISIONS, DEFAULT_URL, FEATURES, RULES, STREAM, Window, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    poll = dict(batch_length=1000, poll_interval=timedelta(microseconds=100))
    auths = await client.consumer_group("scoring", STREAM, AUTHS, polling_strategy=PollingStrategy.Last(),
                                        auto_commit=AutoCommit.Interval(timedelta(seconds=1)), **poll)
    feats = await client.consumer_group(f"scorer-feats-{os.getpid()}", STREAM, FEATURES,
                                        polling_strategy=PollingStrategy.Timestamp(int((time.time() - 60) * 1e6)),
                                        auto_commit=AutoCommit.Disabled(), **poll)
    out = await client.producer(STREAM, DECISIONS, topic_partitions_count=16)
    cache, lat, stats = {}, Window(), {"attack_approved": 0, "attack_blocked": 0, "blocked": 0}

    async def on_feature(m):
        f = load(m.payload())
        cache[(f["k"], f["f"])] = f["v"]

    async def on_auth(m):
        x = load(m.payload())
        risky = any(cache.get((f"{kind}:{x[kind]}", f"{kind}_1m"), 0) > RULES[f"{kind}_1m"] for kind in ("card", "device", "ip"))
        if x.get("attack"):
            stats["attack_blocked" if risky else "attack_approved"] += 1
        stats["blocked"] += risky
        await out.send_with_partitioning([SendMessage(msg(id=x["id"], ok=not risky))], Partitioning.messages_key(x["card"]))
        lat.add((time.time_ns() - x["ns"]) / 1e6)

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            print(f"decisions {r['n']:>6}/s  p50 {r['p50']:6.2f}ms  p99 {r['p99']:6.2f}ms  |  attack approved "
                  f"{stats['attack_approved']}  blocked {stats['attack_blocked']}", flush=True)

    async with out:
        await asyncio.gather(feats.consume_messages(on_feature, asyncio.Event()),
                             auths.consume_messages(on_auth, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
