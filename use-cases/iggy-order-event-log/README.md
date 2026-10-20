# Recording every order the moment it is placed, and acknowledging it in milliseconds, with LaserData

Brokers and exchanges write every order, fill and cancel to a durable log before telling the customer it was accepted, and rebuild positions from that log after any failure. This is how to build that log with LaserData on Apache Iggy, the open-source streaming engine LaserData builds, why the market open is the real test, and how to try it yourself.

> Status: draft. Code targets the `apache-iggy` 0.9.0 Python SDK and the `apache/iggy:0.9.0` image. Benchmark runs are pending; no results are published yet.

**In short**
- **The job:** write each order event durably and in order per account before acknowledging it, keep that acknowledgement to a few milliseconds at the market open, and rebuild positions from the log quickly after any failure.
- **Why Iggy:** a Rust, thread-per-core log with no garbage collector keeps single-order writes fast and steady, account histories stay ordered on one partition, and every downstream system replays the same record.
- **What you get:** a repo with an order management service, a venue and a positions service that print order acknowledgement latency and rebuild time, three tests, and a benchmark against latency-tuned Kafka anyone can rerun.

## The problem: the trader waits while the order is written down

The opening bell rings and a hundred thousand customers hit "buy" in the same minute.

  
Before a broker tells a customer their order is accepted, it has to be sure it will not forget it. So the order management system runs a pre-trade risk check, writes the order to a durable log, and only then acknowledges it. Everything after that, the route to the exchange, the fill, the position update, the statement, reads from the same record.

  
That makes the log's write latency something customers feel directly. A slow write at the open is a spinning button and an order that fills at a worse price. And because the log is the record, it has to be complete and in order: if a node fails, no order can be lost or applied twice, and the firm must be able to rebuild every account's position from it.

This post walks through building that log on Apache Iggy. We start with what the job demands and where the time goes, then show how the design keeps acknowledgements fast and the record complete, and finally measure it against Kafka in a way you can rerun. Everything here runs from the companion repo.

| Requirement | What it means |
|---|---|
| Fast single writes | Each order is acknowledged on its own. There is no time to wait for a batch to fill. |
| Ordered per account | An account's orders, fills and cancels must be applied in the order they happened. |
| Nothing lost, nothing doubled | Through a node failure, every acknowledged order must survive, exactly once in effect. |
| Rebuildable | Positions, risk and statements must be reproducible from the log after a crash or for an audit. |

## The budget: a few milliseconds for each order's write

The tier-1 bank target published by Confluent, under 5 ms at p99 end to end with `acks=all`, is a useful bar for order events too. Brokers that publish their numbers sit in the same range: Dhan reports 95% of orders executed in under 25 ms end to end, and Alpaca cut order processing from about 150 ms to under 1.5 ms after moving its write-ahead log to Redpanda.

  
The log write is one piece of that, and it happens on every single order, unbatched.

This use case is contested, and it is worth saying why. Order logs usually depend on Kafka tooling and audit features that firms already trust, and Iggy's Kafka-compatible gateway is not ready yet. Where Iggy has a case is new order paths and new venues, where single-write latency and a steady tail matter more than an existing ecosystem. The benchmark measures exactly that, with batching turned off on both sides.

## Why the slowest orders, not the average, decide the open

Where do slow writes come from? In a JVM broker, a garbage-collection pause stops every partition at once, so every order in flight waits. A shared request pool lets one slow disk write hold up unrelated accounts. And replicas that fall behind during a burst delay the acknowledgement for `acks=all` writes.

  
Iggy removes the first two by design: written in Rust with no garbage collector, running thread-per-core on `io_uring`, so each core owns its partitions and its own I/O. Replication in 0.9 uses Viewstamped Replication with a separate consensus group per partition.

## The design: an order event log with LaserData

- **Write before acknowledging, one order at a time.** Order management runs its risk check, writes the order event, and acknowledges the client only after the log has. Batching is off: each order is its own write, and the repo measures the time from receiving the order to the log's acknowledgement.
- **Key everything by account.** New orders, acceptances, rejections, fills and cancels for an account all go to one partition, so that account's history is one ordered sequence that any reader can apply directly.
- **Give every event a per-account sequence.** Each event carries a sequence number, so a reader can tell a repeat after a retry or a failover from a new event, and the record can be checked for gaps.
- **Rebuild state from the log.** The positions service starts from the first retained event and replays at full speed, then follows live. It reports how long the rebuild took, which is the recovery time after a crash.
- **Let every downstream system read the same record.** The venue gateway, positions, risk and surveillance read through their own consumer groups. Surveillance can lag without slowing the venue.

## Build it: order management, a venue and positions

The repo needs Docker and Python 3.10 or later. It starts Iggy 0.9.0, an order management service taking 2,000 orders a second, a simulated venue and a positions service.

  
```
git clone https://github.com/laserdata/iggy-order-event-log
cd iggy-order-event-log
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python venue.py &
python oms.py --rate 2000 &
python positions.py
```

  
Order management writes each order on its own and acknowledges only afterwards:

  
```
log = await client.producer("trading", "orders",
    topic_durability=Durability.REPLICATED,
    mode=DirectProducerConfig(batch_length=1))

await log.send_with_partitioning([SendMessage(order_event)], Partitioning.messages_key(account))
ack_latency.add((time.time_ns() - received) / 1e6)     # then tell the client
```

  
Positions rebuild from the start of the log, then follow live:

  
```
log = await client.consumer_group("positions", "trading", "orders",
    polling_strategy=PollingStrategy.First())
```

  
Each prints a line a second:

  
```
  2,000 orders/s  order ack p50 …ms  p99 …ms  p99.9 …ms  replicated  PASS (<5ms)
rebuilt … positions from … events in …s; now following live
positions  fill-to-position p50 …ms  p99 …ms  accounts with positions …
```

  
To plug in your own order flow, point `kafka_bridge.py` at a copy of your order topic and run positions against it. Keep two rules: key by account, and number each account's events.

## Test it under market-open conditions

### The open

Multiply the order rate fivefold for the first minute after the bell.

```
python oms.py --rate 2000 --burst 5 --open-at 30
```

**Pass:** order acknowledgement p99 stays under 5 ms through the burst.

### Orders on disk

Repeat with every order written to disk before it is acknowledged.

```
python oms.py --rate 2000 --persisted
```

**Pass:** acknowledgement p99 stays within budget, and the difference from the replicated run is the cost of the fsync.

### Crash and rebuild

Stop the positions service after a few minutes of trading and start it again.

```
# Ctrl-C positions.py, then
python positions.py
```

**Pass:** it replays the full log, reports the rebuild time, and matches the positions it had before the crash.

## Benchmark it against latency-tuned Kafka

A benchmark that only its author can reproduce is marketing. This one is designed to be rerun by anyone, and its rules are fixed before any results exist. The question: with one order per write and replicated durability, is the acknowledgement under 5 ms at p99 at 100,000 orders a second, and how fast does a day's log rebuild positions?

| Parameter | Value | Why |
|---|---|---|
| Event size | 300 B | Account, sequence, order, lifecycle step, symbol, quantity and price. |
| Rate | 10K → 250K orders/s | Brokers and exchanges see tens of thousands of orders a second at the open. |
| Batching | 1 order per write | Each order is acknowledged on its own, on both systems. |
| Partitions | 32, by account | An account's history stays one ordered sequence. |
| Durability | replicated × 3 | Kafka runs `acks=all`, three replicas, idempotent producer. |
| Persisted run | 100K/s, fsync before ack | Some brokers require orders on disk before acknowledging. |
| Market open | 5× for 60 s | The first minute after the bell. |
| Rebuild | one trading day | Recovery time after a crash, and how fast new services can start. |
| Failover | kill a leader at the open | Reports the longest acknowledgement and any lost or duplicated orders. |
| Hardware | 3 × c6id.4xlarge | Same machines for both systems, plus two load generators. |

Iggy is driven by its own `iggy-bench` tool and Kafka by the OpenMessaging Benchmark, with the same workload shape. Each step runs three times and the median run is reported. Every config, version and raw output is committed. If Kafka wins a step, that row is published.

**The pass line: order acknowledgement p99 under 5 ms at 100,000 single-order writes a second, replicated, through the open, with zero lost or duplicated orders across a failover.** Full plan: [`bench/PLAN.md`](bench/PLAN.md).

## Where it fits in your workloads

- **Retail brokers: A hundred thousand buttons at the bell.** Robinhood runs every trade order through Kafka and over-provisions for market hours. Alpaca moved its order management write-ahead log to Redpanda and cut processing from about 150 ms to under 1.5 ms, which shows the log's latency is visible to customers.
- **Exchanges and crypto: Order events, all day and night.** Coinbase runs sub-10 ms services on a 30-broker MSK cluster. Kraken rewrote its core services in Rust. Kalshi processes orders for thousands of regulated event markets.
- **Brokers in India: The 9:15 rush.** Dhan built its own trading system and reports 95% of orders under 25 ms. Groww consumes order updates from Kafka in its order-latency path. Angel One runs exchange data through on-prem Kafka for its charts.

## Running it in production

A demo on a laptop answers whether the idea works. An order log needs to know what happens when a node dies at the open. Here is what Apache Iggy 0.9 provides today, and what LaserData adds when you would rather not run it yourself.

| Area | Apache Iggy 0.9 today |
|---|---|
| Replication | Clusters use Viewstamped Replication. Three nodes tolerate one failure, five tolerate two. Every SDK follows the leader and fails over on its own. |
| Durability per topic | `replicated` acknowledges once a quorum holds the write, like Kafka's `acks=all`. `persisted` also waits for stable storage. |
| Retention and replay | Keep at least the days you need for rebuilds and corrections hot; regulators require years, which belong in S3 or a lakehouse written by Iggy's connectors. |
| Security | TLS on TCP, QUIC, WebSocket and HTTP. Users, permissions and access tokens. Optional AES-256-GCM encryption of message payloads. |
| Observability | Prometheus metrics from the server. The kit adds order acknowledgement latency and rebuild time from the trading side. |
| Clients | Official SDKs for Rust, Go, Java, C#, Python and Node.js, so producers and consumers keep their languages. |

**LaserData Cloud** runs the same engine as a managed service on AWS or GCP, or inside your own cloud account (BYOC), on three dedicated nodes with local NVMe for the lowest tail latency. It adds a console and CLI, RBAC and audit logs, metrics, snapshots and upgrades. LaserData's founders lead the Apache Iggy project, so fixes land upstream, in the open.

## Frequently Asked Questions

**Why would a broker trust a 0.9 log with orders?** For existing order flows, it probably should not yet: clustering shipped in September 2026 and the ecosystem around Kafka is far deeper. That is why this use case is marked contested, and why the path starts with a shadow and with new flows. The benchmark and the failover test are what change that answer.

**Is there exactly-once for orders?** Not in the transport. Each order has an id and a per-account sequence, so a retried write is recognised and applied once by every reader. Ledgers that need transactions across accounts should stay on systems built for them.

**Can surveillance and regulatory reporting read this log?** Yes, through their own consumer groups, without affecting the order path. For long retention, Iggy's connectors write the same events to Iceberg or Delta Lake.

**Is "replicated" the same guarantee as Kafka's acks=all?** Close to it. Both acknowledge a write once a majority of replicas hold it, and neither forces an fsync per message by default. That is why the main benchmark compares them directly. For teams that require data on disk before the ack, the benchmark adds a persisted run.

**Iggy consumers poll. Doesn't polling add latency?** Yes, up to the poll interval. The consumers in the repo poll every 100 µs to 1 ms, which keeps the added delay small but costs client CPU. The benchmark reports client CPU next to latency. Deferred polling, where the server holds a poll until data arrives, is on the Iggy roadmap.

**Can I keep my Kafka producers?** Not directly yet. Iggy's Kafka-compatible gateway is in progress and does not accept produce or fetch requests in 0.9. To trial it beside an existing pipeline, the repo includes `kafka_bridge.py`, a Kafka consumer group that copies a topic into Iggy without touching the Kafka side.

**Is 0.9 mature enough for production?** Clustering shipped in September 2026, and 0.9 changed the wire protocol and on-disk format. Iggy's own team says Kafka's replication has a decade more production behind it. That is why the adoption path starts with a shadow run beside your existing system. Decide on evidence from your own peaks, not from this post.

## Where it does not fit

The exchange matching engine runs in memory at microseconds and stays there. Ledgers that need cross-account transactions belong in a database or ledger. Order flows that depend on Kafka Connect, Kafka transactions or existing Kafka audit tooling should wait for Iggy's Kafka-compatible gateway. Iggy's place is new order paths where single-write latency and a complete, replayable record matter most.
