# Alerting on logs, metrics and traces within seconds, even while the system is on fire, with LaserData

Observability platforms and internal platform teams move gigabytes of logs, metrics and traces every second, and alerts depend on the freshest of it. This is how to build that hot tier with LaserData on Apache Iggy, the open-source streaming engine LaserData builds, why an incident is the real test, and how to try it yourself.

> Status: draft. Code targets the `apache-iggy` 0.9.0 Python SDK and the `apache/iggy:0.9.0` image. Benchmark runs are pending; no results are published yet.

**In short**
- **The job:** carry every log line, metric and span to alerting and storage fast enough that alerts fire within seconds, keep it writable and readable through maintenance, and do it at a cost per GB that makes sense.
- **Why Iggy:** a Rust, thread-per-core log with no garbage collector keeps the hot tier steady when an incident multiplies log volume, rules run on the stream before indexing, and connectors land the same data in ClickHouse or S3.
- **What you get:** a repo with service emitters, an alerter and a ClickHouse sink that print time-to-alert and event-to-queryable latency, three incident tests, and a benchmark against Kafka anyone can rerun.

## The problem: telemetry spikes exactly when you need it most

At 2:14 am a dependency starts timing out. Within seconds, the services that call it are logging errors ten times faster than normal.

  
This is the moment the observability pipeline exists for. The alert rule needs to see the error rate climb and page someone. The on-call engineer needs to query the last five minutes of logs and traces. Dashboards need to show which services are affected. All of it depends on the freshest data, and all of it arrives in the middle of the biggest spike the pipeline sees all week.

  
Teams learn this the hard way. Honeycomb published that routine Kafka maintenance in May 2026 left recent data unqueryable for 25 minutes in one region and about 90 minutes in another. A hot tier that stalls during maintenance or bends under an incident fails exactly when it is needed.

This post walks through building that hot tier on Apache Iggy. We start with what the job demands and where the time goes, then show how the design keeps alerts and queries fresh during an incident, and finally measure it against Kafka in a way you can rerun. Everything here runs from the companion repo.

| Requirement | What it means |
|---|---|
| Fresh enough to alert | Alert rules and on-call queries need the last few seconds, not the last few minutes. |
| Steady during incidents | Log volume multiplies when things break. The tier must not slow down at that moment. |
| Always writable and readable | Upgrades, restarts and node failures must not leave recent data unqueryable. |
| Cheap per GB | Telemetry is the largest data stream most companies run. Cost per GB decides how much is kept. |

## The budget: seconds to alert, and about one second to queryable

Alerting pipelines are usually designed to page within a minute of a problem, and most of that is the rule's evaluation window: you need some seconds of data to know an error rate is real. What on-call engineers notice is freshness: when they query, are the last few seconds there? A common target is about one second from an event being emitted to it being queryable.

  
That second covers the agent sending the event, the log, the sink's batching and the database's insert. The log is one piece of it.

Here is the honest picture. For telemetry, every option in the chart fits the latency budget, and object-storage designs are often the cheapest way to keep a lot of data. Robinhood moved its logging to WarpStream and accepted higher latency for a 45% saving. Iggy does not have tiered storage yet. Its case for telemetry is the hot tier: a steady few hours on local NVMe that stays fast and available during incidents and maintenance, with alert rules running on the stream, and connectors moving everything older to ClickHouse, Iceberg or S3.

## Why the incident, not the average, decides whether alerts fire

Where do slow events come from? In a JVM broker, a garbage-collection pause stops every partition at once, and an incident's flood of log lines allocates heavily. A shared request pool lets one slow disk write hold up unrelated partitions. And when the ClickHouse sink or archive falls behind, its catch-up reads compete with the alerter reading the live edge.

  
Iggy removes the first two by design: written in Rust with no garbage collector, running thread-per-core on `io_uring`, so each core owns its partitions and its own I/O. Separate consumer groups keep the alerter at the live edge while sinks catch up.

## The design: a telemetry hot tier with LaserData

- **Key events by service, so rules see a service's whole picture.** All events from one service go to one partition, so an alerter instance sees that service's complete error rate without coordination. Alerters scale by sharing services through one consumer group.
- **Run alert rules on the stream, before indexing.** The alerter reads events as they arrive and evaluates a ten-second error-rate rule per service. It does not wait for a database to index the data, so time-to-alert is set by the rule window, not the pipeline.
- **Start the alerter at the live edge.** After a restart the alerter starts from the newest events, not from where it left off. For alerting, what is happening now matters more than catching up on the last ten minutes; the sinks handle completeness.
- **Let sinks catch up on their own.** The ClickHouse sink and the archive read through their own consumer groups and commit their own offsets. If ClickHouse is slow during an incident, the sink falls behind without slowing the alerter.
- **Keep hours hot, move the rest.** Iggy keeps a few hours to a couple of days on local disk with message expiry. Connectors write the same events to ClickHouse for queries and to S3, Iceberg or Delta Lake for months of retention.

## Build it: emitters, an alerter and a ClickHouse sink

The repo needs Docker and Python 3.10 or later. It starts Iggy 0.9.0 and ClickHouse, simulates 200 services, and runs the alerter and the sink.

  
```
git clone https://github.com/laserdata/iggy-telemetry-hot-tier
cd iggy-telemetry-hot-tier
docker compose up -d                 # Apache Iggy 0.9.0 :8090, ClickHouse :8123
pip install -r requirements.txt      # apache-iggy 0.9.0
python emitter.py --rate 20000 &
python alerter.py &
python sink.py
```

  
Emitters key every event by service:

  
```
await events.send_with_partitioning(
    [SendMessage(msg(svc="svc-042", level="error", status=500, ms=213.0))],
    Partitioning.messages_key("svc-042"))
```

  
The alerter evaluates its rule on each event as it arrives:

  
```
errors = sum(1 for _, bad in window[service] if bad)
if len(window[service]) >= 50 and errors / len(window[service]) > 0.05:
    await alerts.send_with_partitioning([SendMessage(alert)], Partitioning.messages_key(service))
```

  
Both print a line a second, and the alerter announces each alert with its time from the first failing request:

  
```
  20,000 events/s  ingest-to-alerter p50 …ms  p99 …ms  p99.9 …ms  firing 0
ALERT svc-042 error rate 38% …s after the first failing request
sink  event-to-queryable p50 …ms  p99 …ms  (flush every 500 ms)
```

  
To plug in your own telemetry, point an OpenTelemetry collector's exporter or `kafka_bridge.py` at Iggy, and keep one rule: key by service. In production, Iggy's ClickHouse sink connector replaces `sink.py`.

## Test it under incident conditions

### The incident

One minute in, ten services start failing: 40% errors and ten log lines per request.

```
python emitter.py --rate 20000 --incident-at 60 --incident-for 120
```

**Pass:** the alerter's ingest p99 stays under 100 ms through the flood, and each failing service alerts within about ten seconds of its first failing request.

### Slow storage

Pause ClickHouse for 30 seconds during the incident, then resume it.

```
docker pause clickhouse; sleep 30; docker unpause clickhouse
```

**Pass:** the alerter's p99 does not move; the sink falls behind, then catches up without losing events.

### Restart the alerter mid-incident

Stop and start the alerter while services are failing.

```
# Ctrl-C alerter.py, then
python alerter.py
```

**Pass:** it starts at the live edge and re-raises the firing alerts within one rule window.

## Benchmark it against Kafka

A benchmark that only its author can reproduce is marketing. This one is designed to be rerun by anyone, and its rules are fixed before any results exist. The question: at 1 GB/s, does every event reach the alerter within 100 ms at p99 through an incident, does the tier stay writable and readable through a rolling restart, and what does each GB/s cost?

| Parameter | Value | Why |
|---|---|---|
| Event size | 500 B | A structured log line or span. |
| Rate | 200 MB/s → 2 GB/s | Large platforms ingest GB/s of telemetry. |
| Partitions | 64, by service | One service's events stay together for per-service rules. |
| Readers | 3 consumer groups | Alerting on the stream, a ClickHouse sink and an S3 archive. |
| Durability | replicated × 3 | Kafka runs `acks=all`, three replicas. Incident telemetry is the telemetry you need. |
| Batching | linger 5 ms both sides | Small batching is normal for telemetry and keeps cost down; same on both systems. |
| Incident | 5× for 10 min | Error logs multiply exactly when alerts matter. |
| Rolling restart | each broker in turn | Maintenance must not make recent data unwritable or unqueryable. |
| Cost | CPU, nodes, disk per GB/s | For telemetry, cost per GB usually decides. |
| Duration | 30 min/step + 24 h | Telemetry never stops. |

Iggy is driven by its own `iggy-bench` tool and Kafka by the OpenMessaging Benchmark, with the same workload shape. Each step runs three times and the median run is reported. The rolling-restart run publishes a per-second timeline of produce errors and consumption. Every config, version and raw output is committed. If Kafka wins a step, or is cheaper per GB, that row is published.

**The pass line: ingest-to-alerter p99 under 100 ms at 1 GB/s with three readers, replicated, through a fivefold incident, and zero seconds of failed writes or stalled reads during a rolling restart.** Full plan: [`bench/PLAN.md`](bench/PLAN.md).

## Where it fits in your workloads

- **Observability vendors: Your customers' incidents are your peaks.** Honeycomb buffers all customer telemetry in Kafka and published two incidents where maintenance made recent data unqueryable. Sentry already consumes its Kafka topics with Rust consumers into ClickHouse. Conviva ingests live streaming quality data for events like the Super Bowl.
- **Platform teams at scale: Gigabytes a second, every second.** Shopify ingests about 100M observability events a second into ClickHouse at peak. DoorDash moves trillions of events a day. Jump Trading moved its telemetry pipeline off Kafka to avoid the JVM and reports p99 around 150 ms.
- **AI and inference platforms: Prompts, safety and GPU telemetry.** Character.AI streams 5 GiB/s of prompts and chat events to safety classifiers. Baseten and Together AI build inference telemetry and usage pipelines. Cursor runs telemetry on WarpStream and keeps latency-sensitive paths separate.

## Running it in production

A demo on a laptop answers whether the idea works. An observability pipeline needs to know what happens during a major incident and an upgrade. Here is what Apache Iggy 0.9 provides today, and what LaserData adds when you would rather not run it yourself.

| Area | Apache Iggy 0.9 today |
|---|---|
| Replication | Clusters use Viewstamped Replication. Three nodes tolerate one failure, five tolerate two. Every SDK follows the leader and fails over on its own. |
| Durability per topic | `replicated` acknowledges once a quorum holds the write, like Kafka's `acks=all`. `persisted` also waits for stable storage. |
| Retention and replay | Message expiry or a size cap per topic keeps hours to a couple of days hot on local disk. Tiered storage is not shipped yet; connectors and S3 archiving cover longer retention. |
| Security | TLS on TCP, QUIC, WebSocket and HTTP. Users, permissions and access tokens. Optional AES-256-GCM encryption of message payloads. |
| Observability of the pipeline | Prometheus metrics from the server; OpenTelemetry tracing covers the connectors runtime. The kit adds time-to-alert and event-to-queryable latency. |
| Clients | Official SDKs for Rust, Go, Java, C#, Python and Node.js, so producers and consumers keep their languages. |

**LaserData Cloud** runs the same engine as a managed service on AWS or GCP, or inside your own cloud account (BYOC), on three dedicated nodes with local NVMe for the lowest tail latency. It adds a console and CLI, RBAC and audit logs, metrics, snapshots and upgrades. LaserData's founders lead the Apache Iggy project, so fixes land upstream, in the open.

## Frequently Asked Questions

**Can the OpenTelemetry Collector write to Iggy?** Today the simplest path is a small exporter or the bridge in the repo; collectors that already export to Kafka can keep doing so while the bridge copies into Iggy. Once Iggy's Kafka-compatible gateway ships, the collector's Kafka exporter can point at Iggy directly.

**Why not alert from the database instead of the stream?** You can, and many teams do. Alerting on the stream removes the database's ingest and indexing delay from time-to-alert, and keeps alerting working when the database is the thing struggling during an incident, as the slow-storage test shows.

**Without tiered storage, how much can Iggy keep?** As much as its local disks hold, which on NVMe is typically hours to a couple of days of telemetry at GB/s rates. That is the hot tier. Everything older belongs in ClickHouse, Iceberg or S3 through connectors, where it is cheaper per GB.

**Is "replicated" the same guarantee as Kafka's acks=all?** Close to it. Both acknowledge a write once a majority of replicas hold it, and neither forces an fsync per message by default. That is why the main benchmark compares them directly. For teams that require data on disk before the ack, the benchmark adds a persisted run.

**Iggy consumers poll. Doesn't polling add latency?** Yes, up to the poll interval. The consumers in the repo poll every 100 µs to 1 ms, which keeps the added delay small but costs client CPU. The benchmark reports client CPU next to latency. Deferred polling, where the server holds a poll until data arrives, is on the Iggy roadmap.

**Can I keep my Kafka producers?** Not directly yet. Iggy's Kafka-compatible gateway is in progress and does not accept produce or fetch requests in 0.9. To trial it beside an existing pipeline, the repo includes `kafka_bridge.py`, a Kafka consumer group that copies a topic into Iggy without touching the Kafka side.

**Is there exactly-once delivery?** No. Delivery is at-least-once. Every message in the repo carries a sequence number, so consumers skip anything they have already seen. Iggy can also deduplicate on the server by message ID.

**Is 0.9 mature enough for production?** Clustering shipped in September 2026, and 0.9 changed the wire protocol and on-disk format. Iggy's own team says Kafka's replication has a decade more production behind it. That is why the adoption path starts with a shadow run beside your existing system. Decide on evidence from your own peaks, not from this post.

## Where it does not fit

Months of logs kept for compliance are cheaper on object storage, and object-storage Kafka designs are a reasonable choice for that cold path. Iggy does not replace your query engine or your SIEM. Pipelines that depend on Kafka Connect should wait for Iggy's Kafka-compatible gateway. Iggy's place is the hot tier: the last few hours, fast and available, especially during incidents.
