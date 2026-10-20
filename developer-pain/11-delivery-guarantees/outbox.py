"""Transactional outbox: change your database and queue the event in one transaction.

Apache Iggy has no transactions that span your database and the stream, so write the event
to an outbox table in the same transaction as the business change, then relay it. If the relay
crashes after sending but before marking the row sent, it sends again with the same message id,
and the idempotent consumer drops the copy.
"""
import json
import sqlite3

from apache_iggy import Partitioning, SendMessage

from ledger import message_id_for

OUTBOX = "CREATE TABLE IF NOT EXISTS outbox (payment_id TEXT PRIMARY KEY, body TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0)"


def record_payment(db: sqlite3.Connection, payment: dict) -> None:
    db.execute(OUTBOX)
    db.execute("BEGIN IMMEDIATE")
    # ... your business write goes here, in the same transaction ...
    db.execute("INSERT OR IGNORE INTO outbox (payment_id, body) VALUES (?, ?)",
               (payment["payment_id"], json.dumps(payment)))
    db.execute("COMMIT")


async def relay_once(db: sqlite3.Connection, producer) -> int:
    rows = db.execute("SELECT payment_id, body FROM outbox WHERE sent = 0 ORDER BY rowid LIMIT 100").fetchall()
    for payment_id, body in rows:
        account = json.loads(body)["account"]
        await producer.send_with_partitioning(
            [SendMessage(body, id=message_id_for(payment_id))], Partitioning.messages_key(account))
        db.execute("UPDATE outbox SET sent = 1 WHERE payment_id = ?", (payment_id,))
    return len(rows)
