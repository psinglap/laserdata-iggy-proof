"""Shared connection helper: TLS with our CA, login with a personal access token."""
import os
from pathlib import Path

from apache_iggy import IggyClient

HOST = os.environ.get("IGGY_HOST", "iggy")
CA_FILE = os.environ.get("IGGY_CA_FILE", "/certs/ca.pem")
STREAM, TOPIC = "orders", "created"


def connect_with_token() -> IggyClient:
    token = Path(os.environ["IGGY_PAT_FILE"]).read_text().strip()
    # A connection string with one credential part (no ':') is read as a
    # personal access token. tls_domain must match a name on the server cert.
    return IggyClient.from_connection_string(
        f"iggy://{token}@{HOST}:8090?tls=true&tls_domain={HOST}&tls_ca_file={CA_FILE}"
    )
