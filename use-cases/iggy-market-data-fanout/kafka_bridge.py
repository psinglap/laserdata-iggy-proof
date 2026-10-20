"""Shadow an existing Kafka tick topic into Iggy, so desks can be compared side by side.

Nothing changes on the Kafka side: this is one more Kafka consumer group. Each record's key
(the symbol) becomes the Iggy partition key, so per-symbol order is kept. The desks expect the
kit's tick format: adapt common.unpack() to your feed's encoding (it needs symbol, sequence, send time).
Needs: pip install confluent-kafka apache-iggy==0.9.0
"""
import argparse
import asyncio

from apache_iggy import Durability, IggyClient, Partitioning, SendMessage
from confluent_kafka import Consumer

from common import DEFAULT_URL, STREAM, TOPIC


async def main(a):
    iggy = IggyClient.from_connection_string(a.url)
    await iggy.connect()
    producer = await iggy.producer(STREAM, TOPIC, topic_partitions_count=a.partitions,
                                   topic_durability=Durability.REPLICATED)
    kafka = Consumer({"bootstrap.servers": a.bootstrap, "group.id": "iggy-shadow",
                      "auto.offset.reset": "latest", "enable.auto.commit": True})
    kafka.subscribe([a.kafka_topic])
    async with producer:
        while True:
            batch = kafka.consume(num_messages=500, timeout=0.001)
            by_key = {}
            for rec in batch:
                if rec.error():
                    continue
                by_key.setdefault(rec.key() or b"none", []).append(SendMessage(rec.value()))
            for key, msgs in by_key.items():
                await producer.send_with_partitioning(msgs, Partitioning.messages_key(key))
            if not batch:
                await asyncio.sleep(0)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--kafka-topic", default="ticks")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--partitions", type=int, default=16)
    asyncio.run(main(p.parse_args()))
