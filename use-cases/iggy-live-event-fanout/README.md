# Pushing live scores, odds and reactions to millions of screens the moment they happen, with LaserData

Sports apps, streaming services and sportsbooks send every score change, odds move and reaction to every viewer during a live game. This is how to build that layer with LaserData on Apache Iggy, the open-source streaming engine LaserData builds, why the biggest moment of the game is the real test, and how to try it yourself.

> Status: draft. Code targets the `apache-iggy` 0.9.0 Python SDK and the `apache/iggy:0.9.0` image. Benchmark runs are pending; no results are published yet.

**In short**
- **The job:** deliver every score, odds change and suspension to every screen watching a game, in order, within about a second, including in the seconds after a goal when everything spikes at once.
- **Why Iggy:** stateless push gateways can each read every event from one log, resume a reconnecting screen from its last update, and rebuild after a restart by replaying the last two minutes.
- **What you get:** a repo with a match feed, WebSocket gateways, a reaction burst and a browser scoreboard, three big-moment tests, and a benchmark against latency-tuned Kafka anyone can rerun.

## The problem: every screen needs the same update, at the same moment

It is the fourth quarter of a Sunday night game. Thirty million people are watching on phones, TVs and laptops. A touchdown is scored.

  
In the next second, a lot has to happen. The scoreboard in every app changes. The sportsbook suspends every market on the game, recalculates odds, and reopens them. Millions of viewers tap a reaction. Highlights, push notifications and the live win-probability chart all update.

  
Each of those is a small event, and every one of them has to reach every screen watching that game, in order. If the odds on one screen lag the real game by two seconds, someone bets on a result that is already known. If the score jumps backwards after a reconnect, viewers notice and post about it.

This post walks through building that layer on Apache Iggy. We start with what the job demands, look at where the time actually goes, and then show how the design holds up at the moment that matters. Everything here runs from the companion repo.

| Requirement | What it means |
|---|---|
| Order per game | A screen must never show 21–17 and then 14–17. Across games, order does not matter. |
| Every gateway gets every event | A screen can connect to any gateway, so each gateway needs the full stream for every game. |
| Holds up at the big moment | A goal makes every market suspend and every viewer react at once. Average load is irrelevant. |
| Resume without gaps | Phones drop connections constantly. A screen that reconnects must get exactly what it missed, from any gateway. |

## The budget for a live update is about one second, event to screen

There is no published standard for live sports updates, so here is the practitioner norm. The video stream is already several seconds behind the stadium. In-app scores and odds should beat the video, which in practice means an update should reach the screen within about one second of the event. Sportsbooks want suspensions faster still, because a market left open after a goal is a market someone will exploit.

  
Most of that second belongs to the last mile: mobile networks, Wi-Fi, the app redrawing. The internal path, from the data provider through the log to the push gateway, should take a small, predictable slice. A common target is under 100 ms at p99, including during bursts.

At a quiet moment, any disk-based log fits this budget easily: both Iggy and Kafka use a few milliseconds of it. Designs that write to object storage to save cost do not fit, because their p99 alone is several times the budget. So for live events the deciding question is not steady-state speed. It is what happens to the slowest updates when every game and every viewer moves at once.

## Why the big moment, not the average, decides live updates

Where do slow updates come from in a burst? Three places. A garbage-collection pause in a JVM broker stops every partition at once, and bursts make pauses more likely because allocation spikes. Gateways that fall behind ask the broker for older data, which competes with live reads. And reconnect storms, when a network blip drops a million phones at once, turn into a wave of catch-up reads at the worst moment.

  
Iggy avoids the first by design: it is written in Rust, so there is no garbage collector, and it runs thread-per-core on `io_uring`, so a slow read on one partition does not hold up another. The design below handles the other two.

## The design: live updates with LaserData

- **Key every event by game, for order.** Iggy keeps order within a partition, so every event for a game goes to the same one. A screen watching one game sees its score, suspensions and odds in exactly the order they happened.
- **Give every gateway its own consumer group, so adding gateways is free.** Each gateway reads the whole stream through its own consumer group. The log is written once. Adding a gateway for a bigger audience adds a reader, not another copy of the data.
- **Put a sequence number in every event, so screens can resume.** Each game's events are numbered. A phone that reconnects sends the last number it saw, and whichever gateway it lands on sends everything after it from memory. No gaps, no duplicates, no special session routing.
- **Rebuild a gateway from the log, so gateways stay stateless.** A gateway that restarts replays the last two minutes from Iggy by timestamp and is ready to resume screens again. There is no gateway state to back up, and gateways can be added or removed during a game.
- **Count reactions before sending them out.** Raw taps go into their own topic. A counter turns them into one small update per game every 250 ms, so screens receive "1,204 fire" instead of 1,204 messages, and the reaction burst never competes with score updates.

## Build it: a match feed, push gateways and a scoreboard

The repo needs Docker and Python 3.10 or later. It starts Iggy 0.9.0, runs a feed for 50 games, starts two gateways and opens 2,000 simulated screens.

  
```
git clone https://github.com/laserdata/iggy-live-event-fanout
cd iggy-live-event-fanout
docker compose up -d                       # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt            # apache-iggy 0.9.0, websockets
python match_feed.py --rate 2000 &          # 50 games, odds moves and scores
python aggregator.py &                      # reactions → counts every 250 ms
python gateway.py --id g1 --port 8765 &
python gateway.py --id g2 --port 8766 &
python viewers.py --gateways ws://localhost:8765,ws://localhost:8766
```

  
Open `index.html` in a browser to watch one game's scoreboard update live.

  
The feed writes each game's events to one partition, keyed by the game:

  
```
await producer.send_with_partitioning(
    [SendMessage(event(game, seq, "score", home=21, away=17))],
    Partitioning.messages_key(game))
```

  
Each gateway reads every event through its own consumer group, starting two minutes back so it can serve resumes immediately:

  
```
consumer = await client.consumer_group(
    f"gateway-{id}", "live", "events",
    polling_strategy=PollingStrategy.Timestamp(two_minutes_ago_us),
)
```

  
A screen connects with the last sequence number it saw, and the gateway sends what it missed before going live:

  
```
# ws://gateway/?match=M007&after=1234
for raw in buffers[match]:
    if json.loads(raw)["seq"] > after:
        await ws.send(raw)
screens[match].add(ws)
```

  
The simulated screens print one line a second, measured from the moment the event was created to the moment it reached the screen:

  
```
  2,000 screens  …/s  p50 …ms  p99 …ms  p99.9 …ms  gaps 0  resumes 0  PASS (<100ms)
```

  
To plug in a real feed, replace the generator in `match_feed.py` with your provider's client. Keep two rules: key by game, and number each game's events. Python tops out at a few thousand screens per process on a laptop, so the demo shows behaviour; peak numbers come from the benchmark below.

## Test it under big-moment conditions

### The goal

At 60 seconds every game scores at once: a suspension, a score change and twenty odds moves per game, while reactions jump twentyfold.

```
python match_feed.py --rate 2000 --goal-at 60 &
python reactions.py --rate 5000 --burst 20 --burst-at 60
```

**Pass:** the screens' p99 stays under 100 ms through the burst, gaps stay 0, and the aggregator's reaction lag stays under 250 ms.

### Lose a gateway

Stop one gateway with Ctrl-C during the game. Its screens reconnect to the other gateway with their last sequence number.

```
# Ctrl-C gateway g1, then
python gateway.py --id g1 --port 8765
```

**Pass:** every screen resumes with 0 gaps and 0 duplicates, and the restarted gateway serves resumes as soon as its two-minute replay finishes.

### Reconnect storm

Drop every screen at once, as a network blip during a final does, and let them all reconnect with jitter.

```
python viewers.py --gateways ws://localhost:8765,ws://localhost:8766 --storm-at 30
```

**Pass:** all screens resume within a few seconds with 0 gaps, and the p99 for live updates recovers within 10 seconds.

## Benchmark it against latency-tuned Kafka

A benchmark that only its author can reproduce is marketing. This one is designed to be rerun by anyone, and its rules are fixed before any results exist. The question: when every game updates at once, does every gateway still receive every event within 100 ms at p99, and can reactions be ingested at 2M a second without falling behind?

| Parameter | Value | Why |
|---|---|---|
| Event size | 200 B JSON | A score, odds or suspension event with game, sequence and timestamp. |
| Event rate | 10K/s, 500 games | A busy evening of live sport. Odds move constantly; scores rarely. |
| Fan-out | 10 → 200 gateways | Every gateway reads every event. This is the number that grows with the audience. |
| Big moment | 20× for 30 s | A goal: suspensions, scores and odds moves everywhere at once. |
| Reactions | 100K → 2M taps/s | Tens of millions of viewers, a small share tapping after a goal. |
| Durability | replicated × 3 | Kafka runs `acks=all`, three replicas. A lost suspension is a real loss. |
| Kafka tuning | linger 0 · fetch wait 0 | Kafka on its best latency settings. Beating an untuned Kafka proves nothing. |
| Failover | kill the leader at 15 min | Reports the time until gateways receive again and the next minute's p99. |
| Duration | 30 min/step + 4 h | A full game, with the big moments inside it. |
| Hardware | 3 × c6id.4xlarge | Same machines for both systems, plus two load generators. |

Iggy is driven by its own `iggy-bench` tool and Kafka by the OpenMessaging Benchmark, with the same workload shape. Each step runs three times and the median run is reported. Every config file, software version and raw output is committed next to the results. If Kafka wins a step, that row is published.

**The pass line: event-to-gateway p99 under 100 ms with 200 gateways through a 20× burst, zero gaps, and reaction lag under 250 ms at 2M taps a second.** Full plan: [`bench/PLAN.md`](bench/PLAN.md).

## Where it fits in your workloads

- **Sportsbooks and prediction markets: Suspend fast, reopen everywhere.** FanDuel scales 5–10× for the Super Bowl across about 40 regulated sites and is exploring near-real-time replication between them. Kalshi updates live prices for thousands of event markets. DraftKings pushes in-play odds to millions.
- **Streaming and sports media: Scores that beat the video.** Peacock streams NFL playoff games and the Olympics. Disney+, ESPN and Hulu already write Rust streaming consumers. In India, Dream11 reworked its Kafka consumers to survive the spike before a match starts.
- **Live commerce and communities: Bids and reactions in real time.** Whatnot runs live auctions where every bid must reach every viewer. Discord fans out tens of millions of messages a second. StubHub is choosing a new streaming platform on AWS.

## Running it in production

A demo on a laptop answers whether the idea works. A live final needs to know what happens on a bad night. Here is what Apache Iggy 0.9 provides today, and what LaserData adds when you would rather not run it yourself.

| Area | Apache Iggy 0.9 today |
|---|---|
| Replication | Clusters use Viewstamped Replication. Three nodes tolerate one failure, five tolerate two. Every SDK follows the leader and fails over on its own. |
| Durability per topic | `replicated` acknowledges once a quorum holds the write, like Kafka's `acks=all`. `persisted` also waits for stable storage. |
| Retention and replay | Set message expiry per topic. A few hours covers resumes and gateway rebuilds; S3 archiving keeps full games for replays and settlement. |
| Security | TLS on TCP, QUIC, WebSocket and HTTP. Users, permissions and access tokens. Optional AES-256-GCM encryption of message payloads. |
| Observability | Prometheus metrics from the server. The simulated screens add per-second p99, gaps and resume counts from the viewer's side. |
| Clients | Official SDKs for Rust, Go, Java, C#, Python and Node.js, so producers and consumers keep their languages. |

**LaserData Cloud** runs the same engine as a managed service on AWS or GCP, or inside your own cloud account (BYOC), on three dedicated nodes with local NVMe for the lowest tail latency. It adds a console and CLI, RBAC and audit logs, metrics, snapshots and upgrades. LaserData's founders lead the Apache Iggy project, so fixes land upstream, in the open.

## Frequently Asked Questions

**Can phones and browsers connect to Iggy directly?** Iggy speaks WebSocket and QUIC natively, which is useful for internal dashboards, trading tools and partner feeds. For millions of public screens, run a thin push tier like the gateways in the repo. It handles authentication, connection limits and per-game subscriptions, and Iggy makes that tier stateless, so it scales by adding instances.

**How many WebSocket connections can one gateway hold?** That depends on the gateway, not on Iggy. Production push tiers written in Go, Rust or Elixir commonly hold hundreds of thousands of connections per instance. The repo's Python gateway is for showing the pattern. Because each gateway is a separate consumer group, the benchmark measures how many gateways Iggy can feed, which is the number that matters for the log.

**What happens if a gateway falls behind during the goal?** Its consumer group simply reads from further back in the log while the others continue at the live edge. One slow gateway does not slow the others. The gateway can also drop intermediate odds for a game and keep only the latest, since screens care about the current price.

**Is "replicated" the same guarantee as Kafka's acks=all?** Close to it. Both acknowledge a write once a majority of replicas hold it, and neither forces an fsync per message by default. That is why the main benchmark compares them directly. For teams that require data on disk before the ack, the benchmark adds a persisted run.

**Iggy consumers poll. Doesn't polling add latency?** Yes, up to the poll interval. The consumers in the repo poll every 100 µs to 1 ms, which keeps the added delay small but costs client CPU. The benchmark reports client CPU next to latency. Deferred polling, where the server holds a poll until data arrives, is on the Iggy roadmap.

**Can I keep my Kafka producers?** Not directly yet. Iggy's Kafka-compatible gateway is in progress and does not accept produce or fetch requests in 0.9. To trial it beside an existing pipeline, the repo includes `kafka_bridge.py`, a Kafka consumer group that copies a topic into Iggy without touching the Kafka side.

**Is there exactly-once delivery?** No. Delivery is at-least-once. Every message in the repo carries a sequence number, so consumers skip anything they have already seen. Iggy can also deduplicate on the server by message ID.

**Is 0.9 mature enough for production?** Clustering shipped in September 2026, and 0.9 changed the wire protocol and on-disk format. Iggy's own team says Kafka's replication has a decade more production behind it. That is why the adoption path starts with a shadow run beside your existing system. Decide on evidence from your own peaks, not from this post.

## Where it does not fit

The video stream itself belongs to a CDN, not a log. Long-term storage of every play for analytics is cheaper on object storage. Workflows that depend on Kafka Connect or Kafka transactions should wait for Iggy's Kafka-compatible gateway. Iggy's place is the live path: events in, every gateway out, resume and replay.
