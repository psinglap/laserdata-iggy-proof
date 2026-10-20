"""Integration: kill the consumer between DB commit and offset store, restart, check totals.

Needs Docker (uses the Apache Iggy Testcontainers module) or IGGY_URL pointing at a server.
"""
import asyncio
import os
import subprocess
import sys

import pytest

from ledger import balance, open_db
from producer import send_payments

pytestmark = pytest.mark.integration
N, ACCOUNTS = 200, 5


@pytest.fixture(scope="module")
def iggy_url():
    if os.environ.get("IGGY_URL"):
        yield os.environ["IGGY_URL"]
        return
    from testcontainers.community.iggy import IggyContainer
    with IggyContainer() as iggy:
        yield iggy.get_connection_string()


def run_consumer(db, url, crash_after, timeout=20):
    env = {**os.environ, "IGGY_URL": url, "CRASH_AFTER": str(crash_after)}
    try:
        return subprocess.run([sys.executable, "consumer.py", db], env=env, timeout=timeout).returncode
    except subprocess.TimeoutExpired:
        return None  # still consuming: it caught up and is waiting for more


def test_crash_and_resend(iggy_url, tmp_path):
    db = str(tmp_path / "ledger.db")
    payments = [{"payment_id": f"p{i}", "account": f"acct-{i % ACCOUNTS}", "cents": 100} for i in range(N)]
    asyncio.run(send_payments(payments, url=iggy_url))
    asyncio.run(send_payments(payments[:50], url=iggy_url))  # the producer resends 50 payments

    assert run_consumer(db, iggy_url, crash_after=73) == 17  # dies after a commit, before storing the offset
    run_consumer(db, iggy_url, crash_after=0, timeout=15)    # restarts and catches up

    ledger = open_db(db)
    assert sum(balance(ledger, f"acct-{k}") for k in range(ACCOUNTS)) == N * 100
    assert ledger.execute("SELECT COUNT(*) FROM processed").fetchone()[0] == N
