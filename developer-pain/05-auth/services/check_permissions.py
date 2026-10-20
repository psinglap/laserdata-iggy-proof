"""Proves least privilege: billing-worker's token cannot send, and cannot create streams."""
import asyncio
import os

from apache_iggy import SendMessage

from common import STREAM, TOPIC, connect_with_token


async def expect_denied(label, coro) -> None:
    try:
        await coro
    except RuntimeError as err:
        print(f"OK   {label}: denied ({err})")
    else:
        raise SystemExit(f"FAIL {label}: was allowed")


async def main() -> None:
    os.environ.setdefault("IGGY_PAT_FILE", "/secrets/billing-worker.pat")
    client = connect_with_token()
    await client.connect()
    await expect_denied("billing-worker sends a message",
                        client.send_messages(STREAM, TOPIC, 0, [SendMessage("forged")]))
    await expect_denied("billing-worker creates a stream", client.create_stream("evil"))


asyncio.run(main())
