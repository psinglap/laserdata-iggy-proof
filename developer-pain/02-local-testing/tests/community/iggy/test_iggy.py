import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest
from apache_iggy import IggyClient, PollingStrategy, SendMessage

from testcontainers.community.iggy import IggyContainer


def test_connection_details():
    with IggyContainer() as iggy:
        host, port = iggy.get_tcp_host_and_port()
        assert host
        assert port > 0
        assert iggy.get_connection_string() == f"iggy+tcp://iggy:iggy@{host}:{port}"
        assert iggy.get_http_url().startswith("http://")


@pytest.mark.asyncio
async def test_produce_and_consume():
    stream, topic = f"s-{uuid4().hex[:8]}", "events"
    sent = [f"message-{i}" for i in range(10)]
    with IggyContainer() as iggy:
        client = IggyClient.from_connection_string(iggy.get_connection_string())
        await client.connect()

        producer = await client.producer(stream, topic, topic_partitions_count=1)
        async with producer:
            await producer.send([SendMessage(m) for m in sent])

        received, done = [], asyncio.Event()
        consumer = await client.consumer_group(
            "test", stream, topic, polling_strategy=PollingStrategy.First(), poll_interval=timedelta(milliseconds=10)
        )

        async def on_message(message):
            received.append(message.payload().decode())
            if len(received) == len(sent):
                done.set()

        await asyncio.wait_for(consumer.consume_messages(on_message, done), timeout=30)
        assert received == sent
