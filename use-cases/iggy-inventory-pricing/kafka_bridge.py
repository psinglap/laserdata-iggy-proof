"""Shadow an existing Kafka topic of stock and price changes into Iggy, without touching Kafka,
so the storefront can be compared side by side.

Map your records to the kit's JSON shape in to_msg(): changes need k (store:sku), seq, level and price.
Needs: pip install confluent-kafka apache-iggy==0.9.0
"""
import argparse
import asyncio
import json
import time

from apache_iggy import IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, STREAM


def to_msg(value: bytes) -> bytes:
    d = json.loads(value)
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    producer = await iggy.producer(STREAM, a.iggy_topic, topic_partitions_count=8)
    kafka = Consumer({"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"})
    kafka.subscribe([a.kafka_topic])
    async with producer:
        while True:
            by_key = {}
            for rec in kafka.consume(num_messages=1000, timeout=0.001):
                if not rec.error():
                    by_key.setdefault(rec.key() or b"none", []).append(SendMessage(to_msg(rec.value())))
            for key, msgs in by_key.items():
                await producer.send_with_partitioning(msgs, Partitioning.messages_key(key))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--kafka-topic", default="stock")
    p.add_argument("--iggy-topic", default="stock")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
