"""Users doing things in an app: opening it, viewing products, adding to cart, buying.

Events are keyed by user, so one user's actions arrive in order. --blast simulates a
campaign send: a burst of app opens and views as millions of notifications land at once.
"""
import argparse
import asyncio
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, Durability, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, EVENTS, STREAM, msg

CATEGORIES = ["shoes", "jackets", "bags", "watches", "phones", "books"]


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    p = await client.producer(STREAM, EVENTS, topic_partitions_count=a.partitions, topic_durability=Durability.REPLICATED,
                              mode=DirectProducerConfig(batch_length=1000, linger_time=timedelta(milliseconds=1)))
    seq, t0, due = {}, time.monotonic(), time.monotonic()
    async with p:
        while True:
            t = time.monotonic() - t0
            blast = a.blast if a.blast_at and a.blast_at <= t < a.blast_at + a.blast_for else 1
            by_user = {}
            for _ in range(max(1, int(a.rate * blast / 100))):
                u = f"u{random.randrange(a.users)}"
                kind = random.choices(["open", "view", "add_to_cart", "purchase"], [2, 6, 1.2, 0.4])[0]
                seq[u] = seq.get(u, 0) + 1
                by_user.setdefault(u, []).append(SendMessage(msg(u=u, seq=seq[u], e=kind, cat=random.choice(CATEGORIES))))
            for u, ms in by_user.items():
                await p.send_with_partitioning(ms, Partitioning.messages_key(u))
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--users", type=int, default=50000)
    ap.add_argument("--rate", type=int, default=10000, help="events per second")
    ap.add_argument("--partitions", type=int, default=32)
    ap.add_argument("--blast", type=float, default=1.0, help="multiplier during a campaign send")
    ap.add_argument("--blast-at", type=float, default=0)
    ap.add_argument("--blast-for", type=float, default=120)
    asyncio.run(main(ap.parse_args()))
