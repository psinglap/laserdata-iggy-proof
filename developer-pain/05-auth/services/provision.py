"""One-time setup, run as the admin user.

1. Creates the `orders` stream and its `created` topic.
2. Creates one user per service, with only the permissions that service needs.
3. Mints a personal access token (PAT) for each service user through the
   HTTPS API and writes it to /secrets/<user>.pat. Services never see a password.
"""
import asyncio
import json
import os
import secrets
import ssl
import urllib.request
from pathlib import Path

from apache_iggy import (AutoLogin, IggyClient, Permissions, StreamPermissions,
                         TcpConfig)

HOST, CA = "iggy", "/certs/ca.pem"
STREAM, TOPIC = "orders", "created"
TLS = ssl.create_default_context(cafile=CA)

# The only two things each service may do. Everything else defaults to False.
SERVICES = {
    "order-api": StreamPermissions(send_messages=True),
    "billing-worker": StreamPermissions(read_topics=True),  # includes poll_messages
}


def https(path: str, body: dict, bearer: str | None = None) -> dict:
    req = urllib.request.Request(
        f"https://{HOST}:3000{path}", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json",
                 **({"Authorization": f"Bearer {bearer}"} if bearer else {})})
    with urllib.request.urlopen(req, context=TLS) as resp:
        return json.loads(resp.read() or b"{}")


async def main() -> None:
    admin = IggyClient(TcpConfig(
        server_address=f"{HOST}:8090", tls_enabled=True, tls_domain=HOST, tls_ca_file=CA,
        auto_login=AutoLogin.username_password(
            os.environ["IGGY_ADMIN_USERNAME"], os.environ["IGGY_ADMIN_PASSWORD"])))
    await admin.connect()

    if await admin.get_stream(STREAM) is None:
        await admin.create_stream(STREAM)
        await admin.create_topic(STREAM, TOPIC, partitions_count=3)
    stream_id = (await admin.get_stream(STREAM)).id

    for user, stream_perms in SERVICES.items():
        password = secrets.token_urlsafe(32)          # used once, below, then discarded
        if await admin.get_user(user) is None:
            await admin.create_user(user, password,
                                    permissions=Permissions(streams={stream_id: stream_perms}))
        else:
            print(f"{user} exists; delete it to re-provision")
            continue
        session = https("/users/login", {"username": user, "password": password})
        jwt = session["access_token"]["token"]
        # expiry is in microseconds; 90 days. Rotate by minting a new token and deleting the old.
        pat = https("/personal-access-tokens",
                    {"name": f"{user}-1", "expiry": 90 * 24 * 3600 * 1_000_000}, bearer=jwt)
        path = Path(f"/secrets/{user}.pat")
        path.write_text(pat["token"])
        path.chmod(0o600)
        print(f"{user}: stream {stream_id} permissions set, token written to {path}")


asyncio.run(main())
