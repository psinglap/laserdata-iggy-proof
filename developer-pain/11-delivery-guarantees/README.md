> **LaserData proof · Developer pain 11: Exactly-once / transactions**  
> Blog: [Effectively-once processing with Apache Iggy: what the broker guarantees, and the twenty lines you add](https://claude.ai/artifact/AWTyiQyE39wfmukJRK9jqH) · Doc: [Delivery guarantees in Apache Iggy](https://claude.ai/artifact/Ve5w5gKoPr81vzNSt2F9xJ) · Posts: [ready-to-publish drafts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#eos)  
> Where this code goes: LaserData repo: an example with a crash test.  
> Status: not yet run against a live Apache Iggy server unless a checklist below says so. Nothing has been opened upstream.

# Effectively-once processing with Apache Iggy

A small, runnable example from LaserData. A ledger consumer applies payments from
[Apache Iggy](https://iggy.apache.org) so that each payment changes the balance exactly once, even
when the consumer crashes mid-stream and the producer sends some payments twice.

Apache Iggy delivers **at least once** when you store the offset after processing. It has no
transactions spanning partitions, topics or your database. This repo shows the two patterns that
close the gap.

## The two patterns

1. **Idempotent consumer** (`ledger.py`, `consumer.py`). The producer sets each message's id from
   the business key (`uuid5(payment_id)`, a 128-bit id). The consumer records the id and applies the
   payment in **one database transaction**, then stores the offset. Redelivered or resent messages
   find their id already recorded and change nothing.
2. **Transactional outbox** (`outbox.py`). Write the business change and the outgoing event in one
   database transaction, then relay the outbox to Apache Iggy. A relay that crashes after sending
   sends again with the same id, and the idempotent consumer drops the copy.

```
producer ──(id = uuid5(payment_id))──▶ Apache Iggy ──▶ consumer
                                                       BEGIN
                                                         INSERT processed(id)   ← duplicate? stop
                                                         UPDATE balances
                                                       COMMIT
                                                       store_offset()           ← crash here = redelivery, harmless
```

## Run it

```bash
pip install -e ".[test]"
docker run -d -p 8090:8090 --cap-add SYS_NICE --security-opt seccomp=unconfined \
  --ulimit memlock=-1:-1 -e IGGY_ROOT_USERNAME=iggy -e IGGY_ROOT_PASSWORD=iggy apache/iggy:0.9.0
python producer.py 100
python consumer.py ledger.db
```

## Tests

| Test | What it shows | Needs |
|---|---|---|
| `tests/test_ledger.py` | 500 payments delivered 800 times in random order give exact totals; a failed transaction records nothing | nothing |
| `tests/test_crash.py` | consumer killed after a commit but before storing its offset, plus 50 producer resends: totals still exact | Docker or `IGGY_URL` |

```bash
pytest -m "not integration"   # unit tests
pytest -m integration         # crash test against a real server
```

- [x] Unit tests pass (`pytest -m "not integration"`, Python 3.12, apache-iggy 0.9.0)
- [ ] Crash test run against a live Apache Iggy server

## What Apache Iggy guarantees, briefly

- A send is acknowledged after a quorum commit (`replicated` topics) or after stable storage on a
  quorum (`persisted` topics). Order is kept within a partition.
- SDK retries over TCP are protected by the server within a client session. An application resend
  is a new request and creates a new copy, which is why the message id matters.
- With `AutoCommit.Disabled()` and `store_offset()` after processing, every message is delivered at
  least once. Storing before processing gives at most once.

Full details: LaserData's guide *Delivery guarantees in Apache Iggy*.

Maintained by LaserData. I work at LaserData, which builds and runs Apache Iggy.
