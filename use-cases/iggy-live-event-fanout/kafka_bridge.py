"""Shadow an existing Kafka topic of live events into Iggy, keyed by match, without touching Kafka.

The gateways expect the kit's JSON event shape ({"m","seq","t","ns",...}); map your events to it
in to_event() below. Needs: pip install confluent-kafka apache-iggy==0.9.0
"""
import argparse
import asyncio
import json
import time

from apache_iggy import Durability, IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, EVENTS, STREAM


def to_event(key: bytes, value: bytes) -> bytes:
    ev = json.loads(value)
    ev.setdefault("m", key.decode() if key else "M000")
    ev.setdefault("ns", time.time_ns())
    return json.dumps(ev, separators=(",", ":")).encode()


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    producer = await iggy.producer(STREAM, EVENTS, topic_partitions_count=8, topic_durability=Durability.REPLICATED)
    kafka = Consumer({"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow", "auto.offset.reset": "latest"})
    kafka.subscribe([a.kafka_topic])
    async with producer:
        while True:
            by_key = {}
            for rec in kafka.consume(num_messages=500, timeout=0.001):
                if not rec.error():
                    by_key.setdefault(rec.key() or b"M000", []).append(SendMessage(to_event(rec.key(), rec.value())))
            for key, msgs in by_key.items():
                await producer.send_with_partitioning(msgs, Partitioning.messages_key(key))
            await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--kafka-topic", default="live-events")
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
