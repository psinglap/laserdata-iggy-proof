# Benchmark plan: fraud and risk feature streams

**Question it answers:** at 100,000 authorizations a second, is every transaction counted in the
velocity features within 5 ms at p99, so the next authorization from the same card, device or IP
is scored on current counts, on Apache Iggy and on Kafka tuned for latency, on the same machines?

## Parameters, and why each one

| Parameter | Value | Why |
|---|---|---|
| Event size | 500 B | Authorization with card, device, IP, merchant, amount and timestamp. |
| Rate | 10K → 50K → 100K → 250K auths/s | National payment rails and large processors peak in the tens of thousands of TPS; features multiply events. |
| Partitions | 32, keyed by card | One card's authorizations stay in order. |
| Readers | 4 consumer groups | Feature builder, scorer, case management and the audit lake. |
| Durability | Iggy replicated × 3 · Kafka acks=all, RF 3, ISR 2, idempotent | Payments and decisions are audit records. |
| Kafka tuning | linger.ms=0, fetch.max.wait.ms=0 | Kafka on its best latency settings. |
| Attack | 500 attempts/s from one device for 60 s | Card testing is fast and small; the test is how many get through. |
| Persisted run | 100K/s with fsync before ack | Many payment teams require it. |
| Failover | Kill a leader at minute 15 | Report time until features flow again and attack approvals during the gap. |
| Duration | 30 min per step + 24 h soak | Payments never stop. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

## Hardware

3 × `c6id.4xlarge` brokers, 2 × `c7i.4xlarge` load generators. Same kernel and filesystem for both.

## Pass lines

1. Auth-to-feature p99 under 5 ms at 100K auths/s with 4 readers, replicated.
2. In the kit, attack approvals no more than 2 above the rule threshold (the ideal is exactly the threshold).

## What we publish

p99 vs authorization rate (one line per system, 5 ms line drawn), attack approvals per system, the
persisted run, broker CPU and node count, and every config and raw output.
