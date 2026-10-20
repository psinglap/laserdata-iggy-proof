# Live leaderboards, economy alerts and anti-cheat signals without ever slowing a game tick, with LaserData

Multiplayer games emit events every tick of every match, and leaderboards, live-ops and anti-cheat all run on them. This is how to carry those events with LaserData on Apache Iggy, the open-source streaming engine LaserData builds, why a tournament start is the real test, and how to try it yourself.

> Status: draft. Code targets the `apache-iggy` 0.9.0 Python SDK and the `apache/iggy:0.9.0` image. Benchmark runs are pending; no results are published yet.

**In short**
- **The job:** get every kill, pickup and currency change from game servers to leaderboards, economy monitoring and anti-cheat within a fraction of a second, without the game loop ever waiting on the network.
- **Why Iggy:** a background producer hands events off in microseconds and drops instead of stalling when full, events keyed by match stay in order, and a Rust log with no garbage collector stays steady when a tournament starts.
- **What you get:** a repo with a 128-tick game server, a live leaderboard and an economy watcher that print tick overruns, handoff time and time-to-flag an exploit, three tests, and a benchmark against Kafka anyone can rerun.

## The problem: the game cannot wait, but live-ops cannot fly blind

A new season launches at 6 pm. Within minutes, a hundred thousand matches are running, each server simulating the world 128 times a second.

  
Every tick produces events: damage, kills, item pickups, currency earned. Those events feed the things that keep a live game healthy. Leaderboards update in front of players. Live-ops watches the in-game economy, because a duplication bug can flood the market with currency in minutes. Anti-cheat looks for impossible patterns. Analytics decides what the next balance patch should be.

  
Two needs pull in opposite directions. The game server has 7.8 milliseconds per tick and cannot spend any of it waiting for telemetry to be acknowledged. Live-ops, though, needs those events fast, because an exploit caught in thirty seconds is a patch; one caught in thirty minutes is an economy rollback.

This post walks through building that telemetry layer on Apache Iggy. We start with what the job demands and where the time goes, then show how the design keeps the game loop untouched and live-ops informed, and finally measure it against Kafka in a way you can rerun. Everything here runs from the companion repo.

| Requirement | What it means |
|---|---|
| Never stall a tick | Handing an event to the telemetry pipeline must cost microseconds and never block, even if the network or broker is slow. |
| Fresh for live-ops | Leaderboards, economy alerts and anti-cheat need events within a fraction of a second, not minutes. |
| Survives launches | Season launches and tournaments start everyone at once. That peak is when exploits and cheats appear. |
| Order per match | Replays and anti-cheat need a match's events in the order they happened. |

## Two budgets: microseconds in the game loop, and a fraction of a second to live-ops

A 128 Hz server has 7.8 ms per tick to read inputs, simulate the world and send state to players. Studios publish those tick rates; Valorant runs at 128 Hz, many shooters at 64. The chart below shows what would happen if a server waited for each batch of telemetry to be acknowledged inside the tick.

  
The answer is that no log belongs inside the tick. The design uses a background producer instead, and the log's speed then decides something else: how fresh leaderboards and live-ops alerts are.

So the game server hands events to a background producer, which batches and sends them on its own. What the game loop pays is the handoff, measured in microseconds in the repo. What the broker's latency affects is everything after: how many milliseconds until a kill shows on the leaderboard, and how many seconds until a duplication exploit is flagged. That is where the steady tail of a Rust, thread-per-core log matters.

## Why the tournament start, not the average, decides what live-ops sees

Where do slow events come from? In a JVM broker, a garbage-collection pause stops every partition at once, and a launch's flood of small events allocates heavily. A shared request pool lets one slow disk write hold up unrelated matches. And when analytics backfills history, its reads compete with live-ops at the live edge.

  
Iggy removes the first two by design: written in Rust with no garbage collector, running thread-per-core on `io_uring`, so each core owns its partitions and its own I/O. Separate consumer groups keep analytics from slowing live-ops.

## The design: game telemetry with LaserData

- **Hand off, never wait.** The game server uses Iggy's background producer. Sending an event only places it in a buffer; batching and network I/O happen elsewhere. The repo measures the handoff per tick, in microseconds, next to tick overruns.
- **Drop rather than stall.** The producer is set to fail immediately when its buffer is full. In that case the server counts the dropped events and carries on. A lost damage event is acceptable; a stuttering match is not. The test below checks that drops stay at zero at normal load.
- **Key events by match, for replays and anti-cheat.** All of a match's events go to one partition, in order. Anti-cheat sees each match as it happened, and any match can be replayed by reading its key.
- **One consumer group per live-ops system.** Leaderboards, economy watch, anti-cheat and analytics each read every event independently. A slow analytics job falls behind on its own without delaying the leaderboard.
- **Measure what live-ops feels.** The leaderboard reports how long a kill takes to count on the board. The economy watcher reports how long after the first suspicious gain it flagged the player. Those are the numbers a live-ops team cares about.

## Build it: a game server, a leaderboard and an economy watch

The repo needs Docker and Python 3.10 or later. It starts Iggy 0.9.0 and runs a game server with 50 matches at 128 ticks a second, a leaderboard and an economy watcher.

  
```
git clone https://github.com/laserdata/iggy-game-telemetry
cd iggy-game-telemetry
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python leaderboard.py &
python economy_watch.py &
python game_server.py --matches 50
```

  
The server's producer never blocks and drops when full:

  
```
producer = await client.producer("game", "telemetry",
    mode=BackgroundProducerConfig(linger_time=timedelta(milliseconds=5),
                                  failure_mode=BackpressureMode.fail_immediately()))
try:
    await producer.send_with_partitioning(events, Partitioning.messages_key(match))
except RuntimeError:
    dropped += len(events)          # never stall the tick
```

  
Each component prints a line a second, and the economy watcher announces each flag:

  
```
  50 matches  ticks/s 128  overruns 0  event handoff per tick p50 …µs  p99 …µs  dropped 0
leaderboard  kill-to-board p50 …ms  p99 …ms  (board refresh 500 ms)
FLAG m0-p7 earned 5,000 in 10 s; caught … ms after the first suspicious gain
```

  
One note on the demo: Python adds its own scheduling jitter, so tick overruns in the demo are a ceiling, not what a C++ or Rust server would see. To plug in your own servers, use Iggy's Rust, C# or Go SDKs in background mode and keep one rule: key by match.

## Test it under launch conditions

### The tournament start

Five times as many matches for two minutes, starting one minute in.

```
python game_server.py --matches 50 --tournament 5 --tournament-at 60
```

**Pass:** no tick overruns caused by the handoff, zero dropped events, and kill-to-board p99 stays under one second.

### A duplication exploit

Two minutes in, one player starts earning 5,000 currency every tick.

```
python game_server.py --matches 50 --exploit-at 120
```

**Pass:** the economy watcher flags that player within a second of the first suspicious gain.

### Lose the broker for a moment

Pause Iggy for five seconds while matches run, then resume it.

```
docker pause iggy; sleep 5; docker unpause iggy
```

**Pass:** the game loop keeps 128 ticks a second throughout; the producer buffers, and drops only if its buffer fills, which the server reports.

## Benchmark it against Kafka

A benchmark that only its author can reproduce is marketing. This one is designed to be rerun by anyone, and its rules are fixed before any results exist. The question: at a million gameplay events a second, does every event reach four live-ops readers within 50 ms at p99, with both systems using non-blocking producers?

| Parameter | Value | Why |
|---|---|---|
| Event size | 200 B | Match, player, event type, tick and a few values. |
| Rate | 100K → 2M events/s | Large titles run tens of thousands of matches at once. |
| Partitions | 64, by match | A match's events stay in order for replays and anti-cheat. |
| Readers | 4 consumer groups | Leaderboards, economy watch, anti-cheat and the warehouse. |
| Durability | replicated × 3 | Kafka runs `acks=all`, three replicas. Economy and anti-cheat evidence cannot be lost. |
| Producer mode | background, 5 ms linger | The game loop never waits; same batching on both sides. |
| Tournament start | 5× for 10 min | Launches start everyone at once. |
| Failover | kill a leader at 15 min | Reports reader recovery time and producer drops on the game side. |
| Duration | 30 min/step + 24 h | Live-service games never close. |
| Hardware | 3 × c6id.4xlarge | Same machines for both systems, plus two load generators. |

Iggy is driven by its own `iggy-bench` tool and Kafka by the OpenMessaging Benchmark, with the same workload shape. Each step runs three times and the median run is reported. The kit also runs at each rate, so the post can report tick overruns, handoff time and drops next to broker latency. Every config, version and raw output is committed. If Kafka wins a step, that row is published.

**The pass line: event-to-reader p99 under 50 ms at a million events a second with four readers, replicated, through a fivefold tournament start, with zero tick overruns from the handoff and zero dropped events.** Full plan: [`bench/PLAN.md`](bench/PLAN.md).

## Where it fits in your workloads

- **Platforms and live-service titles: Economies with real money behind them.** Roblox runs Kafka and Flink for game telemetry, economy and safety ML, and delivers billions of chat messages a day. Scopely live-operates Monopoly GO! and Niantic's games on NATS JetStream and Kafka.
- **Competitive shooters: Anti-cheat and matchmaking.** Riot collects game-client telemetry for anti-cheat models and runs its publishing platform on Kafka. Activision publishes research on ping-driven matchmaking for Call of Duty. Epic Games hires Rust engineers for its desktop software.
- **Mobile and fantasy: Everyone joins at match start.** Dream11 rebuilt its Kafka consumers after delays and rebalancing during the spike before matches start. Krafton India runs live-ops for BGMI, and Nazara operates games across several studios.

## Running it in production

A demo on a laptop answers whether the idea works. A live-service game needs to know what happens on launch night. Here is what Apache Iggy 0.9 provides today, and what LaserData adds when you would rather not run it yourself.

| Area | Apache Iggy 0.9 today |
|---|---|
| Replication | Clusters use Viewstamped Replication. Three nodes tolerate one failure, five tolerate two. Every SDK follows the leader and fails over on its own. |
| Durability per topic | `replicated` acknowledges once a quorum holds the write, like Kafka's `acks=all`. `persisted` also waits for stable storage. |
| Retention and replay | Set message expiry per topic. Days of telemetry cover replays and anti-cheat reviews; connectors land the rest in ClickHouse, Iceberg or Delta Lake. |
| Security | TLS on TCP, QUIC, WebSocket and HTTP. Users, permissions and access tokens. Optional AES-256-GCM encryption of message payloads. |
| Observability | Prometheus metrics from the server. The kit adds tick overruns, handoff time, drops and time-to-flag from the game side. |
| Clients | Official SDKs for Rust, Go, Java, C#, Python and Node.js, so producers and consumers keep their languages. |

**LaserData Cloud** runs the same engine as a managed service on AWS or GCP, or inside your own cloud account (BYOC), on three dedicated nodes with local NVMe for the lowest tail latency. It adds a console and CLI, RBAC and audit logs, metrics, snapshots and upgrades. LaserData's founders lead the Apache Iggy project, so fixes land upstream, in the open.

## Frequently Asked Questions

**Which SDK should game servers use?** Iggy has official Rust, C#, Go, Java, Python and Node.js SDKs, all with a background producer mode. Unreal and custom C++ servers can use the Rust SDK through a thin wrapper; a C++ SDK is in progress in the Iggy project.

**Should match state go through the log?** No. Authoritative match state stays inside the game server, tick by tick. The log carries what happened, for everyone who needs to know about it afterwards: leaderboards, economy, anti-cheat, analytics and replays.

**What happens to events if a game server crashes?** Events still in the background producer's buffer are lost with the process, which is acceptable for most telemetry. For economy events that must not be lost, send them in direct mode from the service that owns the currency, not from the game loop.

**Is "replicated" the same guarantee as Kafka's acks=all?** Close to it. Both acknowledge a write once a majority of replicas hold it, and neither forces an fsync per message by default. That is why the main benchmark compares them directly. For teams that require data on disk before the ack, the benchmark adds a persisted run.

**Iggy consumers poll. Doesn't polling add latency?** Yes, up to the poll interval. The consumers in the repo poll every 100 µs to 1 ms, which keeps the added delay small but costs client CPU. The benchmark reports client CPU next to latency. Deferred polling, where the server holds a poll until data arrives, is on the Iggy roadmap.

**Can I keep my Kafka producers?** Not directly yet. Iggy's Kafka-compatible gateway is in progress and does not accept produce or fetch requests in 0.9. To trial it beside an existing pipeline, the repo includes `kafka_bridge.py`, a Kafka consumer group that copies a topic into Iggy without touching the Kafka side.

**Is there exactly-once delivery?** No. Delivery is at-least-once. Every message in the repo carries a sequence number, so consumers skip anything they have already seen. Iggy can also deduplicate on the server by message ID.

**Is 0.9 mature enough for production?** Clustering shipped in September 2026, and 0.9 changed the wire protocol and on-disk format. Iggy's own team says Kafka's replication has a decade more production behind it. That is why the adoption path starts with a shadow run beside your existing system. Decide on evidence from your own peaks, not from this post.

## Where it does not fit

The simulation itself, player input and state sync stay in the game server and its netcode. Years of telemetry for analytics are cheaper in a warehouse or lakehouse, written there by Iggy's connectors. Pipelines that depend on Kafka Connect should wait for Iggy's Kafka-compatible gateway. Iggy's place is the live path from game servers to the people and systems running the game.
