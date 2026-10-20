"""Services emitting logs and metrics: one JSON event per request, keyed by service.

--incident-at makes a few services start failing: their error rate jumps and their log
volume multiplies, which is what really happens when something breaks. The first event
of the incident is marked, so the alerter can measure time-to-alert from that moment.
"""
import argparse
import asyncio
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, Durability, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, EVENTS, STREAM, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    p = await client.producer(STREAM, EVENTS, topic_partitions_count=a.partitions, topic_durability=Durability.REPLICATED,
                              mode=DirectProducerConfig(batch_length=2000, linger_time=timedelta(milliseconds=2)))
    services = [f"svc-{i:03d}" for i in range(a.services)]
    broken = set(random.sample(services, k=max(1, a.services // 20)))
    t0, due, marked = time.monotonic(), time.monotonic(), False
    async with p:
        while True:
            t = time.monotonic() - t0
            incident = a.incident_at and a.incident_at <= t < a.incident_at + a.incident_for
            by_svc = {}
            for _ in range(max(1, a.rate // 100)):
                s = random.choice(services)
                bad = incident and s in broken
                for _ in range(a.log_burst if bad else 1):
                    err = random.random() < (0.4 if bad else 0.005)
                    extra = {}
                    if bad and not marked:
                        extra, marked = {"incident_start": True}, True
                    by_svc.setdefault(s, []).append(SendMessage(msg(
                        svc=s, level="error" if err else "info", status=500 if err else 200,
                        ms=round(random.lognormvariate(3, 0.5), 1), msg="upstream timeout" if err else "ok", **extra)))
            for s, ms in by_svc.items():
                await p.send_with_partitioning(ms, Partitioning.messages_key(s))
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--services", type=int, default=200)
    ap.add_argument("--rate", type=int, default=20000, help="requests per second across services")
    ap.add_argument("--partitions", type=int, default=32)
    ap.add_argument("--incident-at", type=float, default=0, help="seconds after start; 0 = never")
    ap.add_argument("--incident-for", type=float, default=60)
    ap.add_argument("--log-burst", type=int, default=10, help="log lines per request from a failing service")
    asyncio.run(main(ap.parse_args()))
