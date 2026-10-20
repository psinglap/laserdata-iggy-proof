# Stopping card-testing attacks before the next charge goes through, with LaserData

Payment companies score every authorization against features like how many times this card, device or IP was used in the last minute. This is how to keep those features current with LaserData on Apache Iggy, the open-source streaming engine LaserData builds, why an attack in progress is the real test, and how to try it yourself.

> Status: draft. Code targets the `apache-iggy` 0.9.0 Python SDK and the `apache/iggy:0.9.0` image. Benchmark runs are pending; no results are published yet.

**In short**
- **The job:** count every authorization into velocity features within a few milliseconds, so the next payment from the same card, device or IP is scored on numbers that include the one before it.
- **Why Iggy:** a Rust, thread-per-core log with no garbage collector keeps each hop to a few milliseconds even at peak, authorizations keyed by card stay in order, and the same log replays history for backtesting rule changes.
- **What you get:** a repo with payments, a feature builder, a scorer that counts how many card-testing attempts it let through, and a backtest, plus three tests and a benchmark against latency-tuned Kafka anyone can rerun.

## The problem: an attack finishes before slow features notice it

At 3 am, a script starts trying stolen card numbers against a small merchant: one device, one IP address, hundreds of one-dollar charges a minute.

  
Card testing is fast on purpose. Each attempt is small enough to look normal on its own. What gives it away is velocity: the same device, or the same IP, appearing far more often than any real customer would. Fraud systems catch it with features such as "authorizations from this device in the last minute", computed as transactions arrive and read by the model on every new authorization.

  
The catch is timing. If a transaction takes two seconds to be counted, the model scores the next fifty attempts on numbers that do not include the last fifty. By the time the count crosses the threshold, the attacker has a list of working cards. The fraud loss, the chargebacks and the network fines all come from the gap between a payment happening and the features knowing about it.

This post walks through building that feature path on Apache Iggy. We start with what the job demands and where the time goes, then show how the design keeps features current during an attack, and finally measure it against Kafka in a way you can rerun. Everything here runs from the companion repo.

| Requirement | What it means |
|---|---|
| Counted before the next charge | A transaction must reach the features before the next authorization from the same card, device or IP is scored. |
| Inside the authorization deadline | Every hop sits inside the payment's own time budget, alongside the model and the issuer. |
| Never lose a decision | Authorizations and decisions are audit records for regulators and chargeback disputes. |
| Testable on real history | A new rule should be tried on last week's traffic before it blocks anyone today. |

## The budget: a decision in about 100 ms, and a few of them for the features

Fraud scoring sits inside the authorization. Stripe describes Radar scoring over a thousand signals in under 100 ms. That decision includes fetching features, running the model and applying rules, so each streaming hop that feeds it needs to be in single-digit milliseconds.

  
The chart counts two hops, because the feature path crosses the log twice: once from the authorization to the feature builder, and once from the feature builder to the scorers.

The milliseconds matter less than what they cause. At 500 attempts a second, every 10 ms of feature lag lets about five more attempts be scored on stale counts. Object-storage designs, fine for the audit lake, would let hundreds through. The repo turns this into one number: attack attempts approved after the rule should have fired.

## Why the slowest updates, not the average, decide what gets through

Where do slow updates come from? In a JVM broker, a garbage-collection pause stops every partition at once, so every card's features go stale together for the length of the pause, which an attacker running at hundreds of attempts a second will hit. A shared request pool lets one slow disk write hold up unrelated partitions. And audit or analytics readers catching up compete with the feature path.

  
Iggy removes the first two by design: written in Rust with no garbage collector, running thread-per-core on `io_uring`, so each core owns its partitions and its own I/O. Separate consumer groups keep audit reads off the feature path.

## The design: fraud features with LaserData

- **Key authorizations by card, for order.** All of a card's authorizations go to one partition, in order, so its count is never computed out of sequence. Device and IP counts are published as features keyed by device and IP.
- **Publish features as events, keep a local copy in every scorer.** The feature builder publishes each changed count to a features topic. Every scorer reads all of it through its own consumer group and keeps the counts in memory, so a decision reads a local value instead of calling a feature store over the network.
- **Measure what gets through, not just latency.** Payments in the attack carry a test-only marker the rules never see. The scorer counts how many it approved. With a device threshold of 20 per minute, the ideal is exactly 20; anything above that is the cost of lag.
- **Replay history to test new rules.** The backtest reads the last ten minutes by timestamp at full speed and applies several candidate thresholds side by side, reporting attacks caught and good payments blocked for each.
- **Replicate every authorization and decision.** Authorizations and decisions are written to replicated topics, and the benchmark compares them with Kafka under `acks=all` and an idempotent producer. A persisted run adds an fsync before each acknowledgement, which many payment teams require.

## Build it: payments, features, a scorer and a backtest

The repo needs Docker and Python 3.10 or later. It starts Iggy 0.9.0, a stream of 2,000 authorizations a second, the feature builder and the scorer.

  
```
git clone https://github.com/laserdata/iggy-fraud-features
cd iggy-fraud-features
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python features.py &
python scorer.py &
python payments.py --tps 2000
```

  
The feature builder counts each authorization and publishes the new count, keyed by entity:

  
```
for kind in ("card", "device", "ip"):
    key = f"{kind}:{auth[kind]}"
    window[key].append(now)
    await features.send_with_partitioning(
        [SendMessage(msg(k=key, f=f"{kind}_1m", v=len(window[key])))],
        Partitioning.messages_key(key))
```

  
The scorer decides from its local copy of the features:

  
```
risky = any(cache.get((f"{k}:{auth[k]}", f"{k}_1m"), 0) > RULES[f"{k}_1m"]
            for k in ("card", "device", "ip"))
```

  
Both print a line a second:

  
```
features   2,000/s  auth-to-counted p50 …ms  p99 …ms  p99.9 …ms
decisions  2,000/s  p50 …ms  p99 …ms  |  attack approved …  blocked …
```

  
To plug in real traffic, point `kafka_bridge.py` at your authorization topic and run the feature builder and scorer against the copy. Keep two rules: key authorizations by card, and publish features keyed by the entity they describe.

## Test it under attack

### Card testing

One minute in, a single device and IP start trying 200 stolen cards a second for 30 seconds.

```
python payments.py --tps 2000 --attack-at 60 --attack-rate 200
```

**Pass:** the scorer approves no more than 22 attack attempts: the 20 the rule allows, plus at most two that arrived before their predecessors were counted.

### Peak traffic and an attack together

Five times the normal authorization rate while the attack runs.

```
python payments.py --tps 10000 --attack-at 60 --attack-rate 500
```

**Pass:** feature p99 stays under 5 ms and the attack approvals stay within two of the threshold.

### Backtest a rule change

After the attack, replay the last ten minutes against four device thresholds.

```
python backtest.py --minutes 10 --device-thresholds 5 10 20 40
```

**Pass:** it replays every authorization in seconds and reports, for each threshold, attacks caught and good payments blocked.

## Benchmark it against latency-tuned Kafka

A benchmark that only its author can reproduce is marketing. This one is designed to be rerun by anyone, and its rules are fixed before any results exist. The question: at 100,000 authorizations a second, is every transaction counted within 5 ms at p99, with four readers on the same log?

| Parameter | Value | Why |
|---|---|---|
| Event size | 500 B | An authorization with card, device, IP, merchant and amount. |
| Rate | 10K → 250K auths/s | National payment rails and large processors peak in the tens of thousands a second. |
| Batching | 1 message per batch | Authorizations are sent as they happen; no waiting to fill batches. |
| Readers | 4 consumer groups | Features, scoring, case management and the audit lake. |
| Durability | replicated × 3 | Kafka runs `acks=all`, three replicas, idempotent. Payments are audit records. |
| Kafka tuning | linger 0 · fetch wait 0 | Kafka on its best latency settings. Beating an untuned Kafka proves nothing. |
| Attack | 500 attempts/s, 60 s | Card testing is fast and small; the test is how many get through. |
| Persisted run | 100K/s, fsync before ack | Many payment teams require data on disk before the acknowledgement. |
| Failover | kill a leader at 15 min | Reports the time until features flow again and attack approvals during the gap. |
| Duration | 30 min/step + 24 h | Payments never stop. |

Iggy is driven by its own `iggy-bench` tool and Kafka by the OpenMessaging Benchmark, with the same workload shape. Each step runs three times and the median run is reported. The kit also runs at each rate, so the post can report attack approvals next to broker latency. Every config, version and raw output is committed. If Kafka wins a step, that row is published.

**The pass line: auth-to-feature p99 under 5 ms at 100,000 authorizations a second with four readers, replicated, and no more than two attack approvals above the rule threshold.** Full plan: [`bench/PLAN.md`](bench/PLAN.md).

## Where it fits in your workloads

- **Payment processors and wallets: Every authorization, scored.** Stripe's stream infrastructure team runs dozens of Kafka clusters, about 700 TB a day, for payments and risk signals. PayPal has written about partition reassignments that once left Kafka clusters unavailable. Block reworked cross-region Afterpay Kafka processing to cut cost.
- **Fintech and lending: Underwriting and spend in real time.** Affirm's streaming team runs underwriting and payments events on Kafka and Flink. Ramp authorizes card transactions against spend policies in real time. Robinhood and Coinbase run trading risk paths with sub-second budgets.
- **India's payment rails: UPI at national scale.** PhonePe runs one of India's largest Kafka estates, around a trillion messages a day. Razorpay's anomaly detection processes over 5 billion events a day on MSK and Flink. Juspay's open-source Rust payments switch already ships with Kafka.

## Running it in production

A demo on a laptop answers whether the idea works. A payment company needs to know what happens on its busiest day and under audit. Here is what Apache Iggy 0.9 provides today, what LaserData adds, and what is still missing for regulated buyers.

| Area | Apache Iggy 0.9 today |
|---|---|
| Replication | Clusters use Viewstamped Replication. Three nodes tolerate one failure, five tolerate two. Every SDK follows the leader and fails over on its own. |
| Durability per topic | `replicated` acknowledges once a quorum holds the write, like Kafka's `acks=all`. `persisted` also waits for stable storage. |
| Retention and replay | Set message expiry per topic. Weeks of authorizations support backtests and disputes; S3 archiving and lake connectors keep the years regulators require. |
| Security | TLS on TCP, QUIC, WebSocket and HTTP. Users, permissions and access tokens. Optional AES-256-GCM encryption of message payloads. |
| Observability | Prometheus metrics from the server. The kit adds auth-to-feature latency and attack approvals from the risk side. |
| Clients | Official SDKs for Rust, Go, Java, C#, Python and Node.js, so producers and consumers keep their languages. |

**LaserData Cloud** runs the same engine as a managed service on AWS or GCP, or inside your own cloud account (BYOC), on three dedicated nodes with local NVMe for the lowest tail latency. It adds a console and CLI, RBAC and audit logs, metrics, snapshots and upgrades. LaserData's founders lead the Apache Iggy project, so fixes land upstream, in the open.

## Frequently Asked Questions

**Is LaserData ready for a regulated payment environment?** Not as a hosted service yet: LaserData does not publish SOC 2 or PCI attestations today. That is why this use case starts with a design partner. Apache Iggy runs self-hosted in your own environment, and LaserData Cloud's BYOC option runs inside your own cloud account, so cardholder data does not leave it.

**Why not read features from Redis or a feature store?** Many teams do, and the feature builder in the repo could write to one. Keeping a local copy in each scorer, fed by the features topic, removes a network call from every decision and keeps working if the store is slow. Either way, the log is what gets each transaction counted quickly.

**Does Iggy do windowed aggregation like Flink?** No; Iggy is the log, not a stream processor. Simple sliding counts like these fit in a plain consumer. For complex windowed joins, Flink or another processor can read from Iggy.

**Is "replicated" the same guarantee as Kafka's acks=all?** Close to it. Both acknowledge a write once a majority of replicas hold it, and neither forces an fsync per message by default. That is why the main benchmark compares them directly. For teams that require data on disk before the ack, the benchmark adds a persisted run.

**Iggy consumers poll. Doesn't polling add latency?** Yes, up to the poll interval. The consumers in the repo poll every 100 µs to 1 ms, which keeps the added delay small but costs client CPU. The benchmark reports client CPU next to latency. Deferred polling, where the server holds a poll until data arrives, is on the Iggy roadmap.

**Can I keep my Kafka producers?** Not directly yet. Iggy's Kafka-compatible gateway is in progress and does not accept produce or fetch requests in 0.9. To trial it beside an existing pipeline, the repo includes `kafka_bridge.py`, a Kafka consumer group that copies a topic into Iggy without touching the Kafka side.

**Is there exactly-once delivery?** No. Delivery is at-least-once. Every message in the repo carries a sequence number, so consumers skip anything they have already seen. Iggy can also deduplicate on the server by message ID.

**Is 0.9 mature enough for production?** Clustering shipped in September 2026, and 0.9 changed the wire protocol and on-disk format. Iggy's own team says Kafka's replication has a decade more production behind it. That is why the adoption path starts with a shadow run beside your existing system. Decide on evidence from your own peaks, not from this post.

## Where it does not fit

Ledgers and payment switches that need transactions and exactly-once delivery should stay on systems built for that until Iggy's Kafka-compatible gateway and its transaction support arrive. The model itself and long-term audit storage live outside the log. Iggy's place is the feature path: every authorization counted before the next one is scored.
