"""N couriers driving around the city, each sending its position every few seconds,
plus a stream of new orders. --transport quic uses Iggy's QUIC listener, which copes
better with the packet loss mobile networks have.
"""
import argparse
import asyncio
import random
import time
from datetime import timedelta

from apache_iggy import DirectProducerConfig, Durability, IggyClient, Partitioning, SendMessage

from common import CITY, DEFAULT_URL, ORDERS, PINGS, QUIC_URL, STREAM, msg


async def main(a):
    client = IggyClient.from_connection_string(QUIC_URL if a.transport == "quic" else a.url)
    await client.connect()
    cfg = DirectProducerConfig(batch_length=500, linger_time=timedelta(milliseconds=1))
    pings = await client.producer(STREAM, PINGS, topic_partitions_count=a.partitions, mode=cfg, topic_durability=Durability.REPLICATED)
    orders = await client.producer(STREAM, ORDERS, topic_partitions_count=4, mode=cfg, topic_durability=Durability.REPLICATED)
    lat0, lon0, lat1, lon1 = CITY
    pos = {f"c{i}": [random.uniform(lat0, lat1), random.uniform(lon0, lon1)] for i in range(a.couriers)}
    seq, n_order, t0 = {c: 0 for c in pos}, 0, time.monotonic()
    async with pings, orders:
        while True:
            surge = a.surge if a.surge_at and a.surge_at <= time.monotonic() - t0 < a.surge_at + a.surge_for else 1
            tick, by_courier = time.monotonic(), {}
            for c in random.sample(list(pos), k=max(1, int(len(pos) * 0.1 / a.every * surge))):
                p = pos[c]
                p[0] = min(lat1, max(lat0, p[0] + random.gauss(0, 0.0004)))
                p[1] = min(lon1, max(lon0, p[1] + random.gauss(0, 0.0004)))
                seq[c] += 1
                by_courier[c] = [SendMessage(msg(c=c, seq=seq[c], lat=round(p[0], 6), lon=round(p[1], 6)))]
            for c, m in by_courier.items():
                await pings.send_with_partitioning(m, Partitioning.messages_key(c))
            for _ in range(int(a.orders_per_s * 0.1 * surge)):
                n_order += 1
                await orders.send([SendMessage(msg(order=f"o{n_order}", lat=random.uniform(lat0, lat1), lon=random.uniform(lon0, lon1)))])
            await asyncio.sleep(max(0.0, 0.1 - (time.monotonic() - tick)))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--transport", choices=["tcp", "quic"], default="tcp")
    p.add_argument("--couriers", type=int, default=20000)
    p.add_argument("--every", type=float, default=5.0, help="seconds between pings per courier")
    p.add_argument("--orders-per-s", type=int, default=50)
    p.add_argument("--partitions", type=int, default=16)
    p.add_argument("--surge", type=float, default=1.0, help="multiplier for pings and orders during the rush")
    p.add_argument("--surge-at", type=float, default=0)
    p.add_argument("--surge-for", type=float, default=120)
    asyncio.run(main(p.parse_args()))
