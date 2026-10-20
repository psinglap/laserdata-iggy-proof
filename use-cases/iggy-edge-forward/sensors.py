"""Devices behind one edge gateway: sensors writing readings to the gateway's local Iggy.

They write locally, so they keep working whether or not the gateway has a link to the cloud.
Readings are keyed by device and numbered per device.
"""
import argparse
import asyncio
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, Durability, IggyClient, Partitioning, SendMessage

from common import EDGE_URL, READINGS, STREAM, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    p = await client.producer(STREAM, READINGS, topic_partitions_count=a.partitions, topic_durability=Durability.PERSISTED,
                              mode=DirectProducerConfig(batch_length=500, linger_time=timedelta(milliseconds=2)))
    devices = [f"{a.site}-dev{i:03d}" for i in range(a.devices)]
    seq, due = {d: 0 for d in devices}, time.monotonic()
    async with p:
        while True:
            by_dev = {}
            for _ in range(max(1, a.rate // 100)):
                d = random.choice(devices)
                seq[d] += 1
                by_dev.setdefault(d, []).append(SendMessage(msg(site=a.site, dev=d, seq=seq[d], temp=round(random.gauss(4, 0.6), 2),
                                                                vib=round(abs(random.gauss(0.2, 0.05)), 3))))
            for d, ms in by_dev.items():
                await p.send_with_partitioning(ms, Partitioning.messages_key(d))
            due += 0.01
            await asyncio.sleep(max(0.0, due - time.monotonic()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=EDGE_URL)
    ap.add_argument("--site", default="store-0412")
    ap.add_argument("--devices", type=int, default=200)
    ap.add_argument("--rate", type=int, default=2000, help="readings per second at this site")
    ap.add_argument("--partitions", type=int, default=4)
    asyncio.run(main(ap.parse_args()))
