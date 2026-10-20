"""Payment authorizations arriving from merchants and apps.

Each authorization carries card, device and IP. --attack-at starts a card-testing attack:
one device and IP trying many stolen cards with small amounts, fast. Attack transactions
are marked only so the scorer can count how many it let through; the rules never see the mark.
"""
import argparse
import asyncio
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, Durability, IggyClient, Partitioning, SendMessage

from common import AUTHS, DEFAULT_URL, STREAM, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    p = await client.producer(STREAM, AUTHS, topic_partitions_count=a.partitions, topic_durability=Durability.REPLICATED,
                              mode=DirectProducerConfig(batch_length=256, linger_time=timedelta(microseconds=200)))
    t0, due, n = time.monotonic(), time.monotonic(), 0
    async with p:
        while True:
            t = time.monotonic() - t0
            attack = a.attack_at and a.attack_at <= t < a.attack_at + a.attack_for
            batch = []
            for _ in range(max(1, a.tps // 1000)):
                n += 1
                batch.append(dict(id=f"a{n}", card=f"c{random.randrange(a.cards)}", device=f"d{random.randrange(a.cards)}",
                                  ip=f"ip{random.randrange(a.cards // 3)}", amount=round(random.lognormvariate(3.5, 1), 2)))
            if attack:
                for _ in range(a.attack_rate // 1000 or 1):
                    n += 1
                    batch.append(dict(id=f"a{n}", card=f"stolen{random.randrange(10**6)}", device="d-attacker", ip="ip-attacker",
                                      amount=round(random.uniform(0.5, 2), 2), attack=True))
            for b in batch:
                await p.send_with_partitioning([SendMessage(msg(**b))], Partitioning.messages_key(b["card"]))
            due += 0.001
            await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--tps", type=int, default=2000, help="normal authorizations per second")
    ap.add_argument("--cards", type=int, default=300000)
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--attack-at", type=float, default=0)
    ap.add_argument("--attack-for", type=float, default=30)
    ap.add_argument("--attack-rate", type=int, default=200, help="attack attempts per second")
    asyncio.run(main(ap.parse_args()))
