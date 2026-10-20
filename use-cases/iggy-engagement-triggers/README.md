# Reaching users with the right message while they are still in the app, with LaserData

Marketing and product platforms turn every tap, view and purchase into segments, triggers and messages, for billions of events a day. This is how to move those events with LaserData on Apache Iggy, the open-source streaming engine LaserData builds, why a campaign send is the real test, and how to try it yourself.

> Status: draft. Code targets the `apache-iggy` 0.9.0 Python SDK and the `apache/iggy:0.9.0` image. Benchmark runs are pending; no results are published yet.

**In short**
- **The job:** carry every app event to the trigger engine, segments and analytics fast enough that an in-session message lands while the user is still there, and cheaply enough to do it for billions of events a day.
- **Why Iggy:** events keyed by user arrive in order, every service reads one copy through its own consumer group, and a Rust, thread-per-core log with no garbage collector needs fewer machines for the same volume.
- **What you get:** a repo with app events, a trigger engine and a sender that prints in-session trigger latency, reminder lateness and suppressed repeats, three tests, and a benchmark against latency-tuned Kafka anyone can rerun.

## The problem: the moment to reach a user is short, and everyone arrives at once

A shopper browses running shoes three times in a minute, then hesitates. The best moment to show them an offer is now, while the app is open, not in tomorrow's email.

  
Engagement platforms are built to catch moments like this. Every app open, view, add-to-cart and purchase becomes an event. A trigger engine watches for patterns and fires messages. Segments update. Analytics and the warehouse keep a copy. A brand with tens of millions of users produces billions of these events a day.

  
Two things make it hard. First, the in-session window is short: if the trigger fires a minute late, the user has gone. Second, platforms create their own peaks. When a campaign notification reaches ten million phones, millions of people open the app in the same minute, and the event stream spikes exactly when every trigger matters most.

This post walks through building that event layer on Apache Iggy. We start with what the job demands and where the time goes, then show how the design keeps triggers in the moment during a campaign blast, and finally measure it against Kafka in a way you can rerun. Everything here runs from the companion repo.

| Requirement | What it means |
|---|---|
| Order per user | A purchase must cancel the cart reminder that came before it. One user's events must arrive in the order they happened. |
| In the moment | An in-session trigger has seconds, not minutes. The slowest events matter, because they are the missed moments. |
| Survive your own campaigns | A send to millions creates the biggest spike of the day, on purpose. |
| Cheap per billion events | Engagement volume grows with every user and every feature. Cost per event decides how much you can keep. |

## The budget: about a second from tap to trigger

There is no published standard, but practitioners treat about one second from the user's action to the trigger firing as the bar for in-session messaging; past a few seconds the user has often moved on. Klaviyo, rebuilding its event processing on Kafka, published service-level objectives in seconds for the same reason.

  
That second covers the SDK sending the event, the log, the trigger engine's decision and the sender's call to the push or email service. The log is one piece, but events cross it twice: once into the engine, once out as a trigger.

Engagement is a case where the honest answer is that most logs fit the latency budget on an ordinary day. A trigger path that crosses an object-storage log twice uses most of the second, which is why some platforms keep a fast path and a cheap path. The deciding questions are what the slowest events do during your own campaign sends, and how many machines it takes to carry billions of events a day. The benchmark reports both.

## Why the campaign spike, not the average, decides missed moments

Where do slow events come from? In a JVM broker, a garbage-collection pause stops every partition at once, and bursts of small events allocate heavily. A shared request pool lets one slow disk write hold up unrelated partitions. And on multi-tenant platforms, one customer's huge send becomes everyone's slow minute.

  
Iggy removes the first two by design: written in Rust with no garbage collector, running thread-per-core on `io_uring`, so each core owns its partitions and its own I/O. For the third, Iggy streams give each tenant its own namespace, permissions and topics.

## The design: real-time engagement with LaserData

- **Key events by user, so triggers see the true order.** All of a user's events go to one partition and arrive in order, so a purchase always cancels the cart reminder that came before it. Each event also carries a per-user sequence number, so an engine that restarts skips anything it already handled.
- **Share users across engines, keep each user in one place.** Trigger engines split the event stream through one consumer group. Because events are keyed by user, all of one user's state lives in one engine, with no shared database on the hot path.
- **Measure the two kinds of trigger separately.** In-session triggers fire on a pattern, like three views of one category in a minute, and are measured from the last event to the trigger. Timer triggers, like a cart reminder after 30 seconds, are measured by how late they fire. Both numbers are what users feel.
- **Send each trigger exactly once from the user's point of view.** Delivery through the log is at-least-once, so every trigger has an id, and the sender skips any id it has already sent. Nobody gets two identical pushes after a restart.
- **Let every other reader take its own copy.** Segments, analytics and the warehouse each read the same events through their own consumer group. Iggy's connectors write to ClickHouse, Apache Iceberg or Delta Lake without another pipeline.

## Build it: app events, a trigger engine and a sender

The repo needs Docker and Python 3.10 or later. It starts Iggy 0.9.0, simulates 50,000 users producing 10,000 events a second, and runs the trigger engine and the sender.

  
```
git clone https://github.com/laserdata/iggy-engagement-triggers
cd iggy-engagement-triggers
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python app_events.py --rate 10000 &
python trigger_engine.py &
python sender.py
```

  
Events are keyed by user, with a per-user sequence number:

  
```
await events.send_with_partitioning(
    [SendMessage(msg(u=user, seq=seq, e="add_to_cart", cat="shoes"))],
    Partitioning.messages_key(user))
```

  
The engine fires an in-session offer on the third view of a category within a minute, and measures how long that took:

  
```
if len(views[(user, category)]) == 3:
    await fire(user, "offer", cat=category)
    in_session.add((time.time_ns() - event["ns"]) / 1e6)
```

  
The sender skips any trigger id it has already sent, so restarts never cause duplicate messages:

  
```
if trigger["id"] in sent:
    duplicates += 1
    return
```

  
Both print a line a second:

  
```
in-session offers …/s  event-to-trigger p50 …ms  p99 …ms  | cart reminders …/s  late by p99 …ms  PASS (<1000ms)
sent …/s  trigger-to-send p50 …ms  p99 …ms  repeats suppressed 0
```

  
To plug in your own app, point `kafka_bridge.py` at your event topic, or send from your collector straight to Iggy with any of its SDKs. Keep two rules: key by user, and number each user's events.

## Test it under campaign conditions

### The campaign blast

Multiply events tenfold for two minutes, one minute in, as a send to millions does.

```
python app_events.py --rate 10000 --blast 10 --blast-at 60 --blast-for 120
```

**Pass:** in-session trigger p99 stays under one second through the blast, and cart reminders are never more than a second late at p99.

### Restart the engine

Stop the trigger engine mid-blast and start it again with the same group.

```
# Ctrl-C trigger_engine.py, then
python trigger_engine.py
```

**Pass:** it resumes from its committed offset, skips events it already handled, and the sender reports repeats suppressed rather than duplicate messages sent.

### Scale the engines

Start a second trigger engine during the blast; the group splits users between them.

```
python trigger_engine.py
```

**Pass:** combined trigger throughput rises, p99 falls back under budget, and no user gets a trigger twice.

## Benchmark it against latency-tuned Kafka

A benchmark that only its author can reproduce is marketing. This one is designed to be rerun by anyone, and its rules are fixed before any results exist. The question: during a campaign blast, do events reach five readers within 50 ms at p99, and how many machines does each system need per million events a second?

| Parameter | Value | Why |
|---|---|---|
| Event size | 300 B | User, sequence, event type, properties and timestamp. |
| Rate | 100K → 2M events/s | Engagement platforms ingest billions of events a day. |
| Partitions | 64, by user | One user's actions must arrive in order for triggers to be correct. |
| Readers | 5 consumer groups | Triggers, segments, personalization, analytics and a warehouse sink. |
| Durability | replicated × 3 | Kafka runs `acks=all`, three replicas. Purchases and consent changes cannot be lost. |
| Kafka tuning | linger 0 · fetch wait 0 | Kafka on its best latency settings. Beating an untuned Kafka proves nothing. |
| Campaign blast | 10× for 10 min | A send to millions makes them open the app together. |
| Cost | CPU and nodes per step | At billions of events a day, cost per billion decides the architecture. |
| Failover | kill a leader at 15 min | Reports the time until readers receive again and the next minute's p99. |
| Duration | 30 min/step + 24 h | Engagement never stops; daily patterns show over a day. |

Iggy is driven by its own `iggy-bench` tool and Kafka by the OpenMessaging Benchmark, with the same workload shape. Each step runs three times and the median run is reported. The kit also runs at each rate, so the post can report trigger latency and reminder lateness next to broker latency. Every config, version and raw output is committed. If Kafka wins a step, that row is published.

**The pass line: event-to-reader p99 under 50 ms at a million events a second with five readers, replicated, through a tenfold blast, with in-session triggers under one second at p99.** Full plan: [`bench/PLAN.md`](bench/PLAN.md).

## Where it fits in your workloads

- **Customer engagement platforms: Billions of events, many tenants.** Braze sends billions of messages a day from events published to Kafka per type and user. Klaviyo rebuilt its event processing on Kafka at 170,000 events a second. Attentive moved hundreds of streams to Pulsar, so it has switched before.
- **Product analytics: Every tap, kept and counted.** PostHog captures events in Rust and already splits traffic between MSK and WarpStream for cost. Amplitude has written about Kafka cost as volume grew. Segment's pipelines carried close to a million messages a second.
- **Consumer apps: Your own users, your own peaks.** Pinterest built its own log, MemQ, to cut cost, which shows it will run something other than Kafka. Spotify left Kafka once already. In India, Myntra and MakeMyTrip stream engagement events into personalization.

## Running it in production

A demo on a laptop answers whether the idea works. An engagement platform needs to know what happens during its biggest send. Here is what Apache Iggy 0.9 provides today, and what LaserData adds when you would rather not run it yourself.

| Area | Apache Iggy 0.9 today |
|---|---|
| Replication | Clusters use Viewstamped Replication. Three nodes tolerate one failure, five tolerate two. Every SDK follows the leader and fails over on its own. |
| Durability per topic | `replicated` acknowledges once a quorum holds the write, like Kafka's `acks=all`. `persisted` also waits for stable storage. |
| Retention and replay | Set message expiry per topic. A few days of events lets a new segment or model backfill; connectors keep the long history in ClickHouse or Iceberg. |
| Security | TLS on TCP, QUIC, WebSocket and HTTP. Users, permissions and access tokens. Optional AES-256-GCM encryption of message payloads. |
| Observability | Prometheus metrics from the server. The kit adds in-session trigger latency, reminder lateness and suppressed repeats. |
| Clients | Official SDKs for Rust, Go, Java, C#, Python and Node.js, so producers and consumers keep their languages. |

**LaserData Cloud** runs the same engine as a managed service on AWS or GCP, or inside your own cloud account (BYOC), on three dedicated nodes with local NVMe for the lowest tail latency. It adds a console and CLI, RBAC and audit logs, metrics, snapshots and upgrades. LaserData's founders lead the Apache Iggy project, so fixes land upstream, in the open.

## Frequently Asked Questions

**How do we keep one tenant's send from slowing everyone else?** Give each tenant its own Iggy stream, with its own topics, users and permissions. Partitions are owned by cores, so a busy tenant's partitions do not hold up another tenant's requests on other cores. The benchmark's blast step shows the effect on a shared cluster.

**Do we need a stream processor like Flink for triggers?** Not for triggers like these. Because events are keyed by user, a plain consumer can hold each user's state, as the repo does. For complex windowed joins across many streams, a stream processor still helps and can read from Iggy.

**What about consent and deletion requests?** Consent changes are events like any other and should travel on the same per-user key, so they apply in order. For deletion, set retention on raw events and delete from downstream stores; Iggy is not the long-term system of record.

**Is "replicated" the same guarantee as Kafka's acks=all?** Close to it. Both acknowledge a write once a majority of replicas hold it, and neither forces an fsync per message by default. That is why the main benchmark compares them directly. For teams that require data on disk before the ack, the benchmark adds a persisted run.

**Iggy consumers poll. Doesn't polling add latency?** Yes, up to the poll interval. The consumers in the repo poll every 100 µs to 1 ms, which keeps the added delay small but costs client CPU. The benchmark reports client CPU next to latency. Deferred polling, where the server holds a poll until data arrives, is on the Iggy roadmap.

**Can I keep my Kafka producers?** Not directly yet. Iggy's Kafka-compatible gateway is in progress and does not accept produce or fetch requests in 0.9. To trial it beside an existing pipeline, the repo includes `kafka_bridge.py`, a Kafka consumer group that copies a topic into Iggy without touching the Kafka side.

**Is there exactly-once delivery?** No. Delivery is at-least-once. Every message in the repo carries a sequence number, so consumers skip anything they have already seen. Iggy can also deduplicate on the server by message ID.

**Is 0.9 mature enough for production?** Clustering shipped in September 2026, and 0.9 changed the wire protocol and on-disk format. Iggy's own team says Kafka's replication has a decade more production behind it. That is why the adoption path starts with a shadow run beside your existing system. Decide on evidence from your own peaks, not from this post.

## Where it does not fit

Years of raw events for analytics belong in ClickHouse or a lakehouse, written there by Iggy's connectors. The decision logic of a journey builder lives in your application. Pipelines that depend on Kafka Connect or Kafka transactions should wait for Iggy's Kafka-compatible gateway. Iggy's place is the live path from tap to trigger.
