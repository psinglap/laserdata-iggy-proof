"""Runs without a server: checks the dedup logic under redelivery and resends."""
import random
import sqlite3

import pytest

from ledger import apply_payment, balance, message_id_for, open_db


def test_same_message_applied_once(tmp_path):
    db = open_db(str(tmp_path / "l.db"))
    mid = message_id_for("p1")
    assert apply_payment(db, mid, "a", 100) is True
    assert apply_payment(db, mid, "a", 100) is False
    assert balance(db, "a") == 100


def test_ids_are_stable_u128():
    assert message_id_for("p1") == message_id_for("p1")
    assert message_id_for("p1") != message_id_for("p2")
    assert 0 < message_id_for("p1") < 2**128


def test_random_redelivery_gives_exact_totals(tmp_path):
    db = open_db(str(tmp_path / "l.db"))
    payments = [(f"p{i}", f"acct-{i % 5}", 100) for i in range(500)]
    deliveries = payments + random.Random(7).choices(payments, k=300)  # 300 duplicates
    random.Random(8).shuffle(deliveries)
    applied = sum(apply_payment(db, message_id_for(p), a, c) for p, a, c in deliveries)
    assert applied == 500
    assert sum(balance(db, f"acct-{k}") for k in range(5)) == 500 * 100


def test_failure_mid_transaction_leaves_nothing(tmp_path):
    db = open_db(str(tmp_path / "l.db"))
    db.execute("CREATE TRIGGER boom BEFORE INSERT ON balances BEGIN SELECT RAISE(ABORT, 'disk full'); END")
    with pytest.raises(sqlite3.IntegrityError):
        apply_payment(db, message_id_for("p1"), "a", 100)
    db.execute("DROP TRIGGER boom")
    assert apply_payment(db, message_id_for("p1"), "a", 100) is True  # id was not recorded on failure
    assert balance(db, "a") == 100
