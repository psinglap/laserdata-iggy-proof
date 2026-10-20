"""API gateways and inference servers reporting usage: one event per request, keyed by tenant,
with a unique event id. --dup-rate resends a share of events, as client retries do in real
life, so the meter has to cope with duplicates. --burst simulates a tenant's batch job.
"""
import argparse
import asyncio
import random
import time
import uuid
from datetime import timedelta

from apache_iggy import DirectProducerConfig, Durability, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, STREAM, USAGE, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    p = await client.producer(STREAM, USAGE, topic_partitions_count=a.partitions,
                              topic_durability=Durability.PERSISTED if a.persisted else Durability.REPLICATED,
                              mode=DirectProducerConfig(batch_length=500, linger_time=timedelta(milliseconds=1)))
    t0, due, recent = time.monotonic(), time.monotonic(), []
    async with p:
        while True:
            t = time.monotonic() - t0
            burst = a.burst if a.burst_at and a.burst_at <= t < a.burst_at + a.burst_for else 1
            by_tenant = {}
            for _ in range(max(1, int(a.rate * burst / 100))):
                tenant = "t000" if burst > 1 and random.random() < 0.5 else f"t{random.randrange(a.tenants):03d}"
                ev = dict(id=uuid.uuid4().hex, tenant=tenant, units=random.randint(100, 4000), kind="tokens")
                recent.append(ev)
                by_tenant.setdefault(tenant, []).append(SendMessage(msg(**ev)))
                if random.random() < a.dup_rate and recent:           # a retry of an earlier request
                    d = random.choice(recent[-1000:])
                    by_tenant.setdefault(d["tenant"], []).append(SendMessage(msg(**d)))
            del recent[:-5000]
            for tenant, ms in by_tenant.items():
                await p.send_with_partitioning(ms, Partitioning.messages_key(tenant))
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--tenants", type=int, default=500)
    ap.add_argument("--rate", type=int, default=10000, help="usage events per second")
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--dup-rate", type=float, default=0.01, help="share of events resent as retries")
    ap.add_argument("--burst", type=float, default=1.0, help="multiplier; half the burst goes to tenant t000")
    ap.add_argument("--burst-at", type=float, default=0)
    ap.add_argument("--burst-for", type=float, default=60)
    ap.add_argument("--persisted", action="store_true", help="fsync before acknowledging, as billing often requires")
    asyncio.run(main(ap.parse_args()))
