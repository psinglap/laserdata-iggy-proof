import asyncio
from datetime import timedelta

from apache_iggy import IggyClient, PollingStrategy, SendMessage

from testcontainers.community.iggy import IggyContainer


async def basic_example():
    with IggyContainer() as iggy:
        client = IggyClient.from_connection_string(iggy.get_connection_string())
        await client.connect()

        producer = await client.producer("orders", "created", topic_partitions_count=1)
        async with producer:
            await producer.send([SendMessage(f"order-{i}") for i in range(3)])
        print("Sent 3 messages")

        received, done = [], asyncio.Event()
        consumer = await client.consumer_group(
            "billing", "orders", "created", polling_strategy=PollingStrategy.First(), poll_interval=timedelta(milliseconds=10)
        )

        async def on_message(message):
            received.append(message.payload().decode())
            if len(received) == 3:
                done.set()

        await consumer.consume_messages(on_message, done)
        print(f"Received: {received}")


if __name__ == "__main__":
    asyncio.run(basic_example())
