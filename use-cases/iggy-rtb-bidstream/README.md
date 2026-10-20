# Keeping bids, wins and ad budgets in sync inside a 100 ms auction, with LaserData

Ad platforms run millions of auctions a second, and every auction leaves a trail of events: the bid, the win, the impression, the spend. This is how to move those events with LaserData on Apache Iggy, the open-source streaming engine LaserData builds, so budgets stop and frequency caps hold before the next auction, and how to test it yourself.

> Status: draft. Code targets the `apache-iggy` 0.9.0 Python SDK and the `apache/iggy:0.9.0` image. Benchmark runs are pending; no results are published yet.

**In short**
- **The job:** carry every bid, win and impression to pacing, frequency capping and billing fast enough that a campaign pauses at its budget and a user is not shown the same ad too often, at millions of events a second.
- **Why Iggy:** a Rust, thread-per-core log with no garbage collector keeps each hop to a few milliseconds even at peak, and every service reads one copy of the events through its own consumer group.
- **What you get:** a repo with an exchange, bidders and a pacing service that prints auction latency, timeouts, overspend and cap violations, three tests, and a benchmark against latency-tuned Kafka anyone can rerun.

## The problem: the auction is fast, but the money loop behind it lags

A shopper opens a news app. In the next 100 milliseconds, an exchange asks dozens of bidders what they will pay to show an ad, picks a winner, and renders it.

  
The auction itself gets the attention. The harder part is what happens after it. The win has to reach the pacing service, which tracks how much each campaign has spent. The impression has to reach the frequency-capping service, which tracks how many times this person has seen this ad. Billing and reporting need both.

  
If those events arrive late, the next auctions run on old information. A campaign that hit its budget keeps winning, and the advertiser either pays for it or the platform eats the cost. A person sees the same ad five times in a minute. At millions of auctions a second, a few hundred milliseconds of lag turns into real money every day.

This post walks through building that loop on Apache Iggy. We start with what the job demands and where the time goes, then show how the design keeps budgets and caps current, and finally measure it against Kafka in a way you can rerun. Everything here runs from the companion repo.

| Requirement | What it means |
|---|---|
| Fit inside the auction | Any hop on the request path must leave room in a 100 ms deadline that also covers networks and the model. |
| Close the loop in time | A win must pause its campaign, and an impression must update its cap, before the next request for that campaign or user. |
| Never lose a win | Wins and impressions are billing records. They need replicated, durable writes. |
| Cheap per billion events | Every auction produces several events. At millions of auctions a second, cost per event decides the architecture. |

## The budget: an auction is 100 ms, and each hop gets a few of them

OpenRTB lets the exchange set the deadline, `tmax`, on every request. Values around 80 to 120 ms are common in practice, and Google's buyer documentation allows anywhere from 80 ms to a second depending on format. Bids that arrive late are thrown away, and exchanges throttle bidders that time out often.

  
That deadline covers the network both ways, feature lookups and the model. What is left for each event hop inside the bidder is small. Practitioners budget roughly 1 to 10 ms per hop, which is the scale on the chart below.

Read the bars with the number of hops in mind. A bid request that crosses the log once uses a third of its hop budget on Iggy and half on Kafka at these published figures. The money loop crosses it several times: win to pacing, pause to bidders, impression to caps. Object-storage designs, built to cut cost, are hundreds of milliseconds per hop and cannot sit on this path at all. Neither published number answers the question for your traffic, which is why the second half of this post is a benchmark you can rerun.

## Why the slowest events, not the average, decide spend

Where do slow events come from? In a JVM broker, a garbage-collection pause stops every partition at once, and ad traffic, with its constant small messages, allocates heavily. A shared request pool lets one slow disk write hold up requests for unrelated partitions. And when one consumer, say reporting, falls behind, its catch-up reads compete with the live readers that pacing depends on.

  
Iggy removes the first two by design. It is written in Rust, so there is no garbage collector, and it runs thread-per-core on `io_uring`, so each core owns its partitions and its own I/O. A request is never handed between threads.

## The design: the bidding event loop with LaserData

- **Key impressions by user and wins by campaign, for correct counts.** Iggy keeps order within a partition. Keying impressions by user means one user's impressions arrive in order, so a cap count is never computed out of sequence. Keying wins by campaign does the same for spend.
- **Share the request stream, fan out the state.** Bidders split the bid requests between them through one consumer group, so adding a bidder adds capacity. Each bidder also reads every impression and every budget pause through its own consumer group, so its caps and pauses are current no matter which requests it gets.
- **Publish pauses as events, not as a database flag.** When pacing sees a campaign reach its budget, it writes a pause to a small topic every bidder reads. The pause reaches every bidder in one hop, instead of waiting for the next cache refresh.
- **Measure money, not just milliseconds.** The exchange counts timeouts and cap violations; pacing counts overspend, the money won after a campaign was already over budget. Those three numbers translate latency into what the business sees.
- **Replicate the billing events.** Wins and impressions are written to replicated topics. The benchmark compares them with Kafka under `acks=all` and three replicas, so neither side is fast because it is allowed to lose data.

## Build it: an exchange, bidders and a pacing service

The repo needs Docker and Python 3.10 or later. It starts Iggy 0.9.0, an exchange sending 5,000 bid requests a second, two bidders and the pacing service, with 20 campaigns that each have a budget per minute and a cap of three impressions per user per minute.

  
```
git clone https://github.com/laserdata/iggy-rtb-bidstream
cd iggy-rtb-bidstream
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python pacing.py &
python bidder.py &
python bidder.py &
python exchange.py --rate 5000 --tmax 100
```

  
A bidder takes its share of requests through the shared `bidders` group, and reads every pause through its own group:

  
```
requests = await client.consumer_group("bidders", "rtb", "requests",
    polling_strategy=PollingStrategy.Last())
budget = await client.consumer_group(f"{me}-budget", "rtb", "budget",
    polling_strategy=PollingStrategy.Timestamp(one_minute_ago_us))
```

  
Pacing pauses a campaign the moment its spend reaches the budget, with one event every bidder reads:

  
```
if spend[campaign] >= CAMPAIGNS[campaign]["budget_per_min"]:
    paused.add(campaign)
    await out.send([SendMessage(msg(campaign=campaign, paused=True))])
```

  
The exchange and pacing each print a line a second, so you see latency and its cost side by side:

  
```
  5,000 auctions/s  round trip p50 …ms  p99 …ms  p99.9 …ms  timeouts 0  no-bid 0  cap violations 0
win-to-pacing lag p99 …ms  paused  6/20  overspend this run $…
```

  
To plug in real traffic, point `kafka_bridge.py` at your existing wins or impressions topic and run `pacing.py` against the copy. Keep two rules: key impressions by user and wins by campaign. Python tops out at a few thousand auctions a second, so the demo shows behaviour; peak numbers come from the benchmark below.

## Test it under prime-time conditions

### Prime-time surge

Triple the request rate, as prime time or a live game does, and watch timeouts and overspend.

```
python exchange.py --rate 15000 --tmax 100
```

**Pass:** timeouts stay under 0.1% of requests, and overspend stays below one second of a campaign's spend.

### A slow model

Give the bidders 60 ms of model time, so the events have only what is left of the 100 ms.

```
python bidder.py --think-ms 60
```

**Pass:** timeouts stay under 1%, which means the event hops fit in the remaining 40 ms at p99.

### Lose the pacing service

Stop pacing for 30 seconds while auctions continue, then restart it with the same group name.

```
# Ctrl-C pacing.py, wait 30 s, then
python pacing.py
```

**Pass:** it resumes from its committed offset, catches up on every win it missed, and pauses over-budget campaigns within a second of catching up.

## Benchmark it against latency-tuned Kafka

A benchmark that only its author can reproduce is marketing. This one is designed to be rerun by anyone, and its rules are fixed before any results exist. The question: can every event in the money loop cross the log in under 10 ms at p99, at millions of events a second, with eight services reading it?

| Parameter | Value | Why |
|---|---|---|
| Event size | 300 B | A bid, win or impression record with ids, campaign, price and timestamp. |
| Rate | 250K → 4M /s | Large DSPs and SSPs see millions of requests a second, and each auction produces several events. |
| Partitions | 32, by user or campaign | Caps need a user's events in order; pacing needs a campaign's. |
| Readers | 8 consumer groups | Pacing, caps, fraud, reporting, billing and bidder caches all read the same events. |
| Durability | replicated × 3 | Kafka runs `acks=all`, three replicas. Wins are billing records. |
| Kafka tuning | linger 0 · fetch wait 0 | Kafka on its best latency settings. Beating an untuned Kafka proves nothing. |
| Burst | 3× for 120 s | Prime time and live sport push ad requests up sharply. |
| Persisted run | 1M/s, fsync before ack | Some billing teams require data on disk before the acknowledgement. |
| Failover | kill a leader at 15 min | Reports the time until consumers receive again and the next minute's p99. |
| Duration | 30 min/step + 24 h | Pacing runs all day; slow drift shows over hours. |

Iggy is driven by its own `iggy-bench` tool and Kafka by the OpenMessaging Benchmark, with the same workload shape. Each step runs three times and the median run is reported. The kit itself also runs at each rate, so the post can report timeouts, overspend and cap violations next to the latency. Every config, version and raw output is committed. If Kafka wins a step, that row is published.

**The pass line: produce-to-consume p99 under 10 ms at 2M events a second with eight readers, replicated, and zero lost wins.** Full plan: [`bench/PLAN.md`](bench/PLAN.md).

## Where it fits in your workloads

- **Demand-side platforms: Spend that stops on time.** The Trade Desk evaluates over 11M ad requests a second on Kafka and Flink. Moloco answers bids within 100 ms at millions of requests a second. Pacing and caps are where late events cost money.
- **Exchanges and SSPs: Trillions of events, priced per billion.** Magnite handles around 2 trillion transactions a day and already writes core services in Rust. PubMatic processes on the order of a trillion bids a day and is building a new buy-side platform.
- **Streaming and media with ads: Beacons in, decisions out.** Netflix's ad eventing team ingests billions of ad events a day under sub-second SLAs. Roku and Tubi feed ad systems from millions of devices. In India, JioHotstar makes ad decisions in under 100 ms during cricket finals.

## Running it in production

A demo on a laptop answers whether the idea works. A bidding platform needs to know what happens on its busiest day. Here is what Apache Iggy 0.9 provides today, and what LaserData adds when you would rather not run it yourself.

| Area | Apache Iggy 0.9 today |
|---|---|
| Replication | Clusters use Viewstamped Replication. Three nodes tolerate one failure, five tolerate two. Every SDK follows the leader and fails over on its own. |
| Durability per topic | `replicated` acknowledges once a quorum holds the write, like Kafka's `acks=all`. `persisted` also waits for stable storage. |
| Retention and replay | Set message expiry per topic. A day of wins and impressions covers replays for billing disputes; S3 archiving keeps longer history. |
| Security | TLS on TCP, QUIC, WebSocket and HTTP. Users, permissions and access tokens. Optional AES-256-GCM encryption of message payloads. |
| Observability | Prometheus metrics from the server. The kit adds timeouts, overspend and cap violations from the business side. |
| Clients | Official SDKs for Rust, Go, Java, C#, Python and Node.js, so producers and consumers keep their languages. |

**LaserData Cloud** runs the same engine as a managed service on AWS or GCP, or inside your own cloud account (BYOC), on three dedicated nodes with local NVMe for the lowest tail latency. It adds a console and CLI, RBAC and audit logs, metrics, snapshots and upgrades. LaserData's founders lead the Apache Iggy project, so fixes land upstream, in the open.

## Frequently Asked Questions

**Should bid requests really go through a log?** Only if each hop fits your deadline with room to spare, which is what the "slow model" test checks. Many platforms keep the request path as direct RPC and use the log for everything after the auction. The money loop is where most of the value is, and it is where the repo puts most of its events.

**Does Iggy speak OpenRTB or Protobuf?** Iggy stores bytes and does not enforce a schema. The repo uses JSON for readability; production systems usually send Protobuf or another compact encoding, which also lowers cost per event.

**What about reporting and long-term logs?** Reporting reads the same topics through its own consumer group. For months of history, Iggy's connectors write to ClickHouse, Apache Iceberg or Delta Lake, and older segments can be archived to S3.

**Is "replicated" the same guarantee as Kafka's acks=all?** Close to it. Both acknowledge a write once a majority of replicas hold it, and neither forces an fsync per message by default. That is why the main benchmark compares them directly. For teams that require data on disk before the ack, the benchmark adds a persisted run.

**Iggy consumers poll. Doesn't polling add latency?** Yes, up to the poll interval. The consumers in the repo poll every 100 µs to 1 ms, which keeps the added delay small but costs client CPU. The benchmark reports client CPU next to latency. Deferred polling, where the server holds a poll until data arrives, is on the Iggy roadmap.

**Can I keep my Kafka producers?** Not directly yet. Iggy's Kafka-compatible gateway is in progress and does not accept produce or fetch requests in 0.9. To trial it beside an existing pipeline, the repo includes `kafka_bridge.py`, a Kafka consumer group that copies a topic into Iggy without touching the Kafka side.

**Is there exactly-once delivery?** No. Delivery is at-least-once. Every message in the repo carries a sequence number, so consumers skip anything they have already seen. Iggy can also deduplicate on the server by message ID.

**Is 0.9 mature enough for production?** Clustering shipped in September 2026, and 0.9 changed the wire protocol and on-disk format. Iggy's own team says Kafka's replication has a decade more production behind it. That is why the adoption path starts with a shadow run beside your existing system. Decide on evidence from your own peaks, not from this post.

## Where it does not fit

Bid-request logs kept for months for analytics are cheaper on object storage, where latency does not matter. Pipelines that depend on Kafka Connect or Kafka transactions should wait for Iggy's Kafka-compatible gateway. Iggy's place is the live loop: requests, bids, wins, impressions and pauses, while they still affect the next auction.
