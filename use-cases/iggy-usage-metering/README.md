# Billing every API call and AI token exactly once, and cutting off quotas on time, with LaserData

Usage-based businesses count every request, token and minute, then turn the counts into invoices and quota limits. This is how to build that metering path with LaserData on Apache Iggy, the open-source streaming engine LaserData builds, how to get exact totals without exactly-once delivery, and how to try it yourself.

> Status: draft. Code targets the `apache-iggy` 0.9.0 Python SDK and the `apache/iggy:0.9.0` image. Benchmark runs are pending; no results are published yet.

**In short**
- **The job:** count every usage event once, close per-customer totals on schedule for invoices, and stop a customer at their quota before they run far past it.
- **Why Iggy:** usage keyed by customer stays in order, persisted writes are acknowledged only once on disk, and the same log replays raw usage so every invoice can be reconciled before it goes out.
- **What you get:** a repo with usage emitters that send retries on purpose, a meter that deduplicates and enforces quotas, and a reconciler that proves the totals, plus three tests and a benchmark anyone can rerun.

## The problem: billing has to be exact, and quotas have to be fast

An AI startup's customer runs a batch job overnight. By 2 am it has used its entire monthly token quota, and it is still going.

  
Usage-based pricing turns every API call, model token and compute minute into a metered event. Those events feed two very different things. Invoices need totals that are exactly right: counted once, no more and no less, and provable when a customer disputes them. Quotas and free-tier limits need to act quickly: every second between crossing a limit and enforcing it is usage nobody will pay for.

  
The hard part is that the network delivers some events twice. Clients retry, producers retry, consumers restart and re-read. A metering pipeline that counts every delivery overbills; one that drops anything underbills. And as AI workloads grow, one customer's runaway job can produce more usage in an hour than most customers do in a month.

This post walks through building that metering path on Apache Iggy. We start with what the job demands and where the time goes, then show how the design gets exact totals without exactly-once delivery, and finally measure it against Kafka in a way you can rerun. Everything here runs from the companion repo.

| Requirement | What it means |
|---|---|
| Count each event once | Retries and restarts deliver some events twice. Totals must count each one exactly once. |
| Never lose usage | Every lost event is unbilled revenue. Writes must be durable before they are acknowledged. |
| Enforce quotas fast | Usage after a limit is crossed is usage nobody pays for. The cutoff must follow within a second. |
| Provable totals | When a customer disputes an invoice, the raw events must reproduce the bill. |

## The budget: about a second to enforce a quota, and no budget for being wrong

There is no published standard for quota enforcement, so here is the practitioner view: within about a second of a customer crossing a limit, the gateway should know. For a customer consuming a million tokens a minute, every extra second of delay is roughly 17,000 tokens of overage.

  
Invoices have no latency budget at all; they have a correctness budget of zero. That is why the benchmark runs with persisted writes, where the broker acknowledges only after the data is on disk, as billing teams usually require.

Every option here fits the latency budget, and Cursor, for example, runs its metering-adjacent streams on object-storage Kafka. The deciding questions for metering are different: whether persisted writes stay fast enough at your peak, whether totals are exactly right after retries and failovers, and whether you can prove them. The repo and benchmark focus on those.

## Why the slowest events, not the average, decide overage

Where do slow events come from? In a JVM broker, a garbage-collection pause stops every partition at once. With persisted writes, a shared request pool means one slow fsync can hold up requests for unrelated partitions. And when analytics or reconciliation reads history, it competes with the live meter.

  
Iggy removes the first two by design: written in Rust with no garbage collector, running thread-per-core on `io_uring`, so each core owns its partitions and its own disk I/O, including fsyncs. Reconciliation reads through its own consumer group and never slows the meter.

## The design: usage metering with LaserData

- **Give every usage event a unique id.** The gateway that serves a request creates its id, and every retry of that request reuses it. Duplicates are then easy to recognise wherever they appear.
- **Deduplicate in the meter, not in the transport.** Delivery through the log is at-least-once. The meter remembers recent ids and skips repeats, which gives exact totals without needing exactly-once delivery from the log. The meter reports how many it skipped.
- **Key usage by customer, close minutes after a grace period.** All of a customer's usage goes to one partition and one meter. Each minute's total is published a few seconds after the minute ends, so events still in flight are counted; anything later goes out as a correction rather than being lost.
- **Publish quota cutoffs as events.** When a customer crosses its limit, the meter writes a cutoff that gateways read. It also counts the overage, the usage that arrived after the limit, which is the business cost of any delay.
- **Prove the bill by replaying the raw usage.** The reconciler reads the last N minutes of raw usage and the published totals, recounts with deduplication, and compares every customer-minute. Zero mismatches is the bar before invoices go out.

## Build it: usage emitters, a meter and a reconciler

The repo needs Docker and Python 3.10 or later. It starts Iggy 0.9.0, sends 10,000 usage events a second from 500 customers with one percent retries, and runs the meter.

  
```
git clone https://github.com/laserdata/iggy-usage-metering
cd iggy-usage-metering
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python meter.py &
python usage_emitter.py --rate 10000 --dup-rate 0.01 --persisted
```

  
Each usage event carries an id that retries reuse, and is keyed by customer:

  
```
await usage.send_with_partitioning(
    [SendMessage(msg(id=request_id, tenant="t042", units=1830, kind="tokens"))],
    Partitioning.messages_key("t042"))
```

  
The meter skips any id it has already counted:

  
```
if event["id"] in seen:
    duplicates += 1
    return
seen[event["id"]] = True
```

  
It prints a line a second, and after a few minutes the reconciler checks every closed minute:

  
```
 10,000 events/s  event-to-meter p50 …ms  p99 …ms  duplicates skipped …  late 0  tenants cut off 0  overage units 0
checked … tenant-minutes from … unique usage events: 0 mismatches
```

  
To plug in your own platform, point `kafka_bridge.py` at your usage topic, or send from your gateway with any Iggy SDK. Keep two rules: key by customer, and give each request one id that retries reuse.

## Test it under billing conditions

### Heavy retries

Resend five percent of all usage, as a flaky network or a client bug would.

```
python usage_emitter.py --rate 10000 --dup-rate 0.05 --persisted
```

**Pass:** the meter reports the duplicates it skipped, and `python reconcile.py` finds 0 mismatches.

### A runaway customer

One customer suddenly sends half of ten times the normal load, and crosses its quota.

```
python usage_emitter.py --rate 10000 --burst 10 --burst-at 30 --persisted
```

**Pass:** the cutoff is published within a second of crossing, and overage stays below one second of that customer's usage.

### Restart the meter

Stop the meter mid-minute and start it again; it re-reads from its last committed offset.

```
# Ctrl-C meter.py, then
python meter.py
python reconcile.py --minutes 10
```

**Pass:** events re-read after the restart are skipped as duplicates, and reconciliation still finds 0 mismatches.

## Benchmark it against Kafka, with writes on disk

A benchmark that only its author can reproduce is marketing. This one is designed to be rerun by anyone, and its rules are fixed before any results exist. The question: at a million usage events a second, with every write on disk before it is acknowledged, does every event reach the meter within 20 ms at p99?

| Parameter | Value | Why |
|---|---|---|
| Event size | 200 B | Event id, customer, units, kind and timestamp. |
| Rate | 100K → 2M events/s | API and inference platforms meter every request. |
| Partitions | 32, by customer | A customer's usage stays in order and with one meter. |
| Readers | 3 consumer groups | Meter, quota enforcement and analytics. |
| Durability | persisted (fsync) | Kafka runs `acks=all`, three replicas, flushing every message. Usage is money. |
| Replicated run | same steps, no fsync | Shows the cost of persisting for teams that accept replication alone. |
| Runaway customer | one tenant 10× for 10 min | A batch job or an agent loop. |
| Failover | kill a leader at 15 min | Reports the time until the meter receives again and the overage during the gap. |
| Duration | 30 min/step + 24 h | Billing closes daily; the soak ends with a full reconciliation. |
| Hardware | 3 × c6id.4xlarge | Same machines for both systems, local NVMe, plus two load generators. |

Iggy is driven by its own `iggy-bench` tool and Kafka by the OpenMessaging Benchmark, with the same workload shape. Each step runs three times and the median run is reported. The 24-hour soak ends with the reconciler over the full day. Every config, version and raw output is committed. If Kafka wins a step, that row is published.

**The pass line: event-to-meter p99 under 20 ms at a million usage events a second with persisted writes and three readers, and zero reconciliation mismatches after a 24-hour soak that includes a failover.** Full plan: [`bench/PLAN.md`](bench/PLAN.md).

## Where it fits in your workloads

- **AI and inference platforms: Tokens, seconds and runaway jobs.** Cursor and Perplexity run usage metering for their products. Deepgram is building the pipeline that ingests usage from inference servers, in Rust. Baseten and Together AI meter GPU inference. One customer's agent loop can dwarf the rest.
- **Developer and data platforms: Every API call, every event.** Stripe runs usage and billing data through dozens of Kafka clusters. PostHog meters the events it ingests. ClickHouse Cloud and Atlassian run multi-tenant event buses where usage per tenant has to be exact.
- **Billing and messaging in India: Per-message, per-transaction.** Chargebee meters customer usage for subscription billing. Cashfree collects usage through Kafka into its billing system. Gupshup and Netcore produce delivery receipts for billions of messages that are billed per message.

## Running it in production

A demo on a laptop answers whether the idea works. A billing system needs to know what happens at month-end and during an outage. Here is what Apache Iggy 0.9 provides today, and what LaserData adds when you would rather not run it yourself.

| Area | Apache Iggy 0.9 today |
|---|---|
| Replication | Clusters use Viewstamped Replication. Three nodes tolerate one failure, five tolerate two. Every SDK follows the leader and fails over on its own. |
| Durability per topic | `replicated` acknowledges once a quorum holds the write, like Kafka's `acks=all`. `persisted` also waits for stable storage. |
| Retention and replay | Keep raw usage for at least the billing and dispute window with message expiry; S3 archiving and lake connectors keep it for audits. |
| Security | TLS on TCP, QUIC, WebSocket and HTTP. Users, permissions and access tokens. Optional AES-256-GCM encryption of message payloads. |
| Observability | Prometheus metrics from the server. The kit adds duplicates skipped, late events, overage and reconciliation mismatches. |
| Clients | Official SDKs for Rust, Go, Java, C#, Python and Node.js, so producers and consumers keep their languages. |

**LaserData Cloud** runs the same engine as a managed service on AWS or GCP, or inside your own cloud account (BYOC), on three dedicated nodes with local NVMe for the lowest tail latency. It adds a console and CLI, RBAC and audit logs, metrics, snapshots and upgrades. LaserData's founders lead the Apache Iggy project, so fixes land upstream, in the open.

## Frequently Asked Questions

**Billing needs exactly-once. Is at-least-once good enough?** Yes, with ids. Exactly-once delivery would stop duplicates in transport, but retries also happen before the log, at the client and the gateway. Deduplicating by a request id in the meter handles both, and the reconciler proves it. That is how the totals in the repo stay exact.

**How long must the meter remember ids?** Longer than any retry can take, typically minutes. The repo keeps the last two million ids in memory. For longer windows, keep ids in a key-value store, or use Iggy's server-side deduplication by message id, which drops repeats within a configured window.

**Can audit logs use the same design?** Yes. Audit events keyed by customer, written persisted, in order and replayable, are the same pattern without the totals. Keep them for the audit retention period with message expiry and archive them to S3.

**Is "replicated" the same guarantee as Kafka's acks=all?** Close to it. Both acknowledge a write once a majority of replicas hold it, and neither forces an fsync per message by default. That is why the main benchmark compares them directly. For teams that require data on disk before the ack, the benchmark adds a persisted run.

**Iggy consumers poll. Doesn't polling add latency?** Yes, up to the poll interval. The consumers in the repo poll every 100 µs to 1 ms, which keeps the added delay small but costs client CPU. The benchmark reports client CPU next to latency. Deferred polling, where the server holds a poll until data arrives, is on the Iggy roadmap.

**Can I keep my Kafka producers?** Not directly yet. Iggy's Kafka-compatible gateway is in progress and does not accept produce or fetch requests in 0.9. To trial it beside an existing pipeline, the repo includes `kafka_bridge.py`, a Kafka consumer group that copies a topic into Iggy without touching the Kafka side.

**Is 0.9 mature enough for production?** Clustering shipped in September 2026, and 0.9 changed the wire protocol and on-disk format. Iggy's own team says Kafka's replication has a decade more production behind it. That is why the adoption path starts with a shadow run beside your existing system. Decide on evidence from your own peaks, not from this post.

## Where it does not fit

Ledgers that move money between accounts need transactions and belong in a database or a ledger system built for them. Pricing, rating and invoice generation live in your billing system, which reads the meter's totals. Pipelines that depend on Kafka Connect or Kafka transactions should wait for Iggy's Kafka-compatible gateway. Iggy's place is the path from request to exact, provable usage.
