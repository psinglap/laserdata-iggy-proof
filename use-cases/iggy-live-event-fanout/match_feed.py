"""Live data provider: score changes, odds moves and market suspensions for N matches.

Every event for a match goes to one partition (keyed by match), so its order holds.
--goal-at simulates the moment that matters: a goal, a suspension, then a burst of odds moves.
"""
import argparse
import asyncio
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, Durability, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, EVENTS, STREAM, event


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    producer = await client.producer(STREAM, EVENTS, topic_partitions_count=a.partitions,
                                     mode=DirectProducerConfig(batch_length=500, linger_time=timedelta(microseconds=500)),
                                     topic_durability=Durability.REPLICATED)
    matches = [f"M{i:03d}" for i in range(a.matches)]
    seq = {m: 0 for m in matches}
    score = {m: [0, 0] for m in matches}
    t0, due, goal_done = time.monotonic(), time.monotonic(), False

    def nxt(m, kind, **d):
        seq[m] += 1
        return SendMessage(event(m, seq[m], kind, **d))

    async with producer:
        while a.seconds == 0 or time.monotonic() - t0 < a.seconds:
            batch = {}
            for m in random.sample(matches, k=max(1, a.rate // 100)):
                batch.setdefault(m, []).append(nxt(m, "odds", home=round(random.uniform(1.2, 4), 2), away=round(random.uniform(1.2, 4), 2)))
            if not goal_done and a.goal_at and time.monotonic() - t0 >= a.goal_at:
                goal_done = True
                for m in matches:  # every match scores at once: the worst case for the gateways
                    score[m][0] += 1
                    batch.setdefault(m, []).extend([nxt(m, "suspend"), nxt(m, "score", home=score[m][0], away=score[m][1])]
                                                   + [nxt(m, "odds", home=1.5, away=3.1) for _ in range(20)])
            for m, msgs in batch.items():
                await producer.send_with_partitioning(msgs, Partitioning.messages_key(m))
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--matches", type=int, default=50)
    p.add_argument("--partitions", type=int, default=8)
    p.add_argument("--rate", type=int, default=2000, help="odds updates per second across all matches")
    p.add_argument("--goal-at", type=float, default=0, help="seconds after start; 0 = never")
    p.add_argument("--seconds", type=int, default=0)
    asyncio.run(main(p.parse_args()))
