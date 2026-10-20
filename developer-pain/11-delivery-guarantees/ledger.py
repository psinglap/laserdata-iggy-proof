"""The idempotent part: apply a payment to the ledger exactly once, however often it arrives.

The message id is the deduplication key. Producers set it from the business key, so a
resend of the same payment carries the same id, and the consumer can recognise it.
"""
import sqlite3
import uuid

SCHEMA = """
CREATE TABLE IF NOT EXISTS processed (message_id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS balances (account TEXT PRIMARY KEY, cents INTEGER NOT NULL);
"""


def open_db(path: str) -> sqlite3.Connection:
    db = sqlite3.connect(path, isolation_level=None)  # explicit BEGIN/COMMIT below
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript(SCHEMA)
    return db


def message_id_for(payment_id: str) -> int:
    """A stable 128-bit id derived from the business key (Apache Iggy ids are u128)."""
    return uuid.uuid5(uuid.NAMESPACE_URL, f"payment:{payment_id}").int


def apply_payment(db: sqlite3.Connection, message_id: int, account: str, cents: int) -> bool:
    """Record the message id and update the balance in one transaction.

    Returns False when the message was already applied. Either both writes commit or
    neither does, so a crash at any point leaves the ledger consistent.
    """
    db.execute("BEGIN IMMEDIATE")
    try:
        cur = db.execute("INSERT OR IGNORE INTO processed VALUES (?)", (str(message_id),))
        if cur.rowcount == 0:
            db.execute("ROLLBACK")
            return False
        db.execute(
            "INSERT INTO balances VALUES (?, ?) "
            "ON CONFLICT(account) DO UPDATE SET cents = cents + excluded.cents",
            (account, cents),
        )
        db.execute("COMMIT")
        return True
    except BaseException:
        db.execute("ROLLBACK")
        raise


def balance(db: sqlite3.Connection, account: str) -> int:
    row = db.execute("SELECT cents FROM balances WHERE account = ?", (account,)).fetchone()
    return row[0] if row else 0
