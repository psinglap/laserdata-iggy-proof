"""Produce OrderPlaced events. Pass v1 or v2 to choose the schema version the producer writes."""
import asyncio
import sys
import uuid

from apache_iggy import IggyClient, Partitioning

from .codec import as_dict, encode, load_schema
from .registry import Registry

CONNECTION = "iggy+tcp://iggy:iggy@127.0.0.1:8090"
ARTIFACT = "OrderPlaced"


async def main(version: str) -> None:
    schema_text = load_schema(f"order_{version}.avsc")
    registry = Registry()
    schema_id = registry.register(ARTIFACT, schema_text)
    registry.require_backward_compatibility(ARTIFACT)
    print(f"writing with schema {version}, globalId {schema_id}")

    client = IggyClient.from_connection_string(CONNECTION)
    await client.connect()
    producer = await client.producer("shop", "orders", topic_partitions_count=3)
    async with producer:
        for i in range(5):
            record = {"order_id": str(uuid.uuid4()), "customer_id": f"c-{i % 3}", "amount_cents": 1000 + i}
            if version == "v2":
                record["currency"] = "EUR"
            msg = encode(record, as_dict(schema_text), schema_id)
            await producer.send_with_partitioning([msg], Partitioning.messages_key(record["customer_id"]))
    print("sent 5 events")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "v1"))
