# Dispatching the nearest courier and keeping every delivery ETA current, with LaserData

Delivery, ride-hailing and logistics apps stream the location of every driver every few seconds, and dispatch, arrival times and customer maps all depend on it. This is how to build that location stream with LaserData on Apache Iggy, the open-source streaming engine LaserData builds, why the dinner rush is the real test, and how to try it yourself.

> Status: draft. Code targets the `apache-iggy` 0.9.0 Python SDK and the `apache/iggy:0.9.0` image. Benchmark runs are pending; no results are published yet.

**In short**
- **The job:** carry every courier's position to dispatch, arrival-time estimates and customer maps within a fraction of a second, so the nearest free courier gets the order and the ETA on the customer's screen is right.
- **Why Iggy:** pings keyed by courier keep each driver's positions in order, every service reads one copy through its own consumer group, services rebuild from a 30-second replay, and phones can send over QUIC, which handles lossy cellular networks better.
- **What you get:** a repo with simulated couriers, a dispatcher and an ETA service that print how old the dispatched position was and how fast ETAs update, three rush-hour tests, and a benchmark against latency-tuned Kafka anyone can rerun.

## The problem: dispatch is only as good as the last position it saw

It is 7 pm on a rainy Friday. Orders triple in twenty minutes, and thousands of couriers are criss-crossing the city.

  
Every courier's phone sends its position every few seconds. Those positions drive almost everything the customer experiences. Dispatch picks the nearest free courier for each new order. The ETA service recalculates the arrival time each time the courier moves. The customer's map shows the courier approaching. Geofences fire "your courier is outside" at the right moment.

  
All of it depends on how fresh the positions are. If dispatch is working from positions that are several seconds old, it sends a courier who has already driven the other way. If ETAs lag, customers watch a dot that jumps and call support. On the busiest evening of the week, both happen at once.

This post walks through building that location layer on Apache Iggy. We start with what the job demands and where the time goes, then show how the design keeps dispatch and ETAs fresh at the peak, and finally measure it against Kafka in a way you can rerun. Everything here runs from the companion repo.

| Requirement | What it means |
|---|---|
| Latest position wins | A courier's positions must be applied in order. An old ping arriving late must never move the dot backwards. |
| Every service sees every ping | Dispatch, ETA, maps and geofences all need the full stream, and adding a service must not slow the others. |
| Fresh at the rush | The promise is about the busiest half hour of the day, when pings and orders spike together. |
| Survive bad networks | Phones move between cell towers and lose packets. Late or repeated pings must not corrupt the picture. |

## The budget: under half a second from phone to every service

There is no single published standard, but location designs commonly target under 500 ms from the courier's phone to the services that act on it, with pings every five seconds or so. Ride-hailing operators describe exactly that interval.

  
Most of that half second belongs to the mobile network: a phone on a busy cell can take 100 to 300 ms just to deliver a packet. What is left for the log, the dispatcher and the ETA service is the rest. A sensible target for the broker hop is a small, predictable slice of it.

Location is a case where honesty matters. At these published figures, both Iggy and Kafka use about 1% of the budget, so raw broker speed is not the deciding factor. Object-storage designs, which some delivery platforms are evaluating to cut cost, use nearly all of it, leaving almost nothing for the mobile network. For location the questions are cost per active courier, behaviour at the rush, and how phones on poor networks reach the log. The rest of this post focuses on those.

## Why the rush, not the average, decides dispatch quality

Where do slow pings come from? In a JVM broker, a garbage-collection pause stops every partition at once, so every courier's position is late together. A shared request pool lets one slow disk write hold up unrelated partitions. And on the phone side, TCP over a lossy cellular link stalls every message behind one lost packet.

  
Iggy addresses the broker side by design: it is written in Rust with no garbage collector, and it runs thread-per-core on `io_uring`, so each core owns its partitions and its I/O. For the phone side, Iggy has a native QUIC listener; a lost packet on one stream does not block the others.

## The design: live location with LaserData

- **Key pings by courier, so the latest position wins.** All of a courier's pings go to one partition and arrive in order. Each ping also carries a sequence number, so a service ignores any ping older than the one it already has, even after a retry or a reconnect.
- **One consumer group per service, so adding a service is free.** Dispatch, ETA, maps and geofences each read every ping through their own group. The pings are written once; a new service is a new reader, not a new copy.
- **Index positions by grid cell for dispatch.** The dispatcher keeps the latest position of every courier in roughly 500 m cells and searches outward from the order's cell. It also records how old the chosen courier's position was, which is the number that tells you whether dispatch is working from fresh data.
- **Rebuild from a short replay, so services hold no state.** A restarted dispatcher or ETA service replays the last 30 seconds of pings by timestamp. With pings every five seconds, that is every active courier's latest position, and the service is ready again in seconds.
- **Let phones use QUIC.** Iggy listens for QUIC as well as TCP. On cellular networks with packet loss, QUIC avoids the head-of-line blocking that makes TCP stall, and its connections survive a phone changing networks.

## Build it: couriers, a dispatcher and an ETA service

The repo needs Docker and Python 3.10 or later. It starts Iggy 0.9.0 with TCP and QUIC listeners, simulates 20,000 couriers over Manhattan with 50 new orders a second, and runs a dispatcher and an ETA service.

  
```
git clone https://github.com/laserdata/iggy-rider-eta
cd iggy-rider-eta
docker compose up -d                 # Apache Iggy 0.9.0: TCP :8090, QUIC :8080
pip install -r requirements.txt      # apache-iggy 0.9.0
python couriers.py --couriers 20000 &
python dispatcher.py &
python eta.py
```

  
Couriers send each position keyed by courier, with a per-courier sequence number:

  
```
await pings.send_with_partitioning(
    [SendMessage(msg(c=courier, seq=seq, lat=lat, lon=lon))],
    Partitioning.messages_key(courier))
```

  
The dispatcher rebuilds every courier's position from the last 30 seconds, then applies only newer pings:

  
```
pings = await client.consumer_group("dispatch-pings", "mobility", "pings",
    polling_strategy=PollingStrategy.Timestamp(thirty_seconds_ago_us))

if old and ping["seq"] <= old.seq:
    return                     # latest position wins
```

  
Both services print a line a second:

  
```
   50 dispatched/s  position age at dispatch p50 …ms  p99 …ms  couriers known 20,000
 …  ETA updates/s  ping-to-ETA p50 …ms  p99 …ms  p99.9 …ms  active deliveries …  PASS (<500ms)
```

  
Note what "position age" includes: up to five seconds of waiting for the courier's next ping, which no broker can remove. The part the log controls is ping-to-ETA, the second line. To plug in a real fleet, point `kafka_bridge.py` at your existing ping topic and keep two rules: key by courier, and send a per-courier sequence number.

## Test it under dinner-rush conditions

### The dinner rush

Triple pings and orders for two minutes, starting one minute in.

```
python couriers.py --couriers 20000 --surge 3 --surge-at 60 --surge-for 120
```

**Pass:** ping-to-ETA p99 stays under 500 ms through the surge, and dispatch keeps assigning within a second of each order.

### A bad network

On Linux, add 5% packet loss and compare the courier simulator over TCP and over QUIC.

```
sudo tc qdisc add dev lo root netem loss 5%
python couriers.py --transport tcp     # then --transport quic
```

**Pass:** with QUIC, ping-to-ETA p99 stays under 500 ms; the TCP run shows how much head-of-line blocking costs.

### Restart dispatch mid-rush

Stop the dispatcher during the surge and start it again.

```
# Ctrl-C dispatcher.py, then
python dispatcher.py
```

**Pass:** within a few seconds it reports all 20,000 couriers known again and resumes assigning, with no order lost.

## Benchmark it against latency-tuned Kafka

A benchmark that only its author can reproduce is marketing. This one is designed to be rerun by anyone, and its rules are fixed before any results exist. The question: at a million active couriers, does every ping reach dispatch, ETA, maps and geofences within 50 ms at p99, leaving the rest of the 500 ms for the mobile network?

| Parameter | Value | Why |
|---|---|---|
| Ping size | 120 B | Courier id, sequence, latitude, longitude and timestamp. |
| Rate | 40K → 1M pings/s | 200K to 5M active couriers at one ping every five seconds. |
| Partitions | 32, by courier | Each courier's pings must stay in order so the latest position wins. |
| Readers | 4 consumer groups | Dispatch, ETA, customer maps and geofences all read every ping. |
| Durability | replicated × 3 | Kafka runs `acks=all`, three replicas. Positions feed disputes and safety reviews. |
| Kafka tuning | linger 0 · fetch wait 0 | Kafka on its best latency settings. Beating an untuned Kafka proves nothing. |
| Dinner rush | 3× for 30 min | Meal peaks and bad weather triple pings and orders together. |
| Lossy network | 5% loss, QUIC vs TCP | Phones on cellular networks lose packets. This run is Iggy-only, to show the transport choice. |
| Failover | kill a leader at 15 min | Reports the time until dispatch receives again and the next minute's p99. |
| Duration | 30 min/step + 24 h | Delivery runs all day with two peaks. |

Iggy is driven by its own `iggy-bench` tool and Kafka by the OpenMessaging Benchmark, with the same workload shape. Each step runs three times and the median run is reported. Every config, version and raw output is committed. If Kafka wins a step, that row is published, and broker CPU and node count are reported at every step so cost per courier can be compared.

**The pass line: ping-to-consumer p99 under 50 ms at a million pings a second with four readers, replicated, and QUIC within twice the loss-free p99 at 5% packet loss.** Full plan: [`bench/PLAN.md`](bench/PLAN.md).

## Where it fits in your workloads

- **Food and grocery delivery: Dinner rush, every night.** DoorDash runs dispatch and ETA events on a Kafka platform shared with Wolt and Deliveroo, and is evaluating object-storage brokers to cut cost. In India, Swiggy and Zomato run delivery-time predictions on Kafka at hundreds of millions of messages a minute.
- **Ride-hailing: Matching on fresh positions.** Uber built its own Kafka consumer proxy to contain hardware cost and head-of-line blocking. Lyft handles millions of route requests a day under a new CTO. Rapido matches riders to millions of captains in real time.
- **Fleets and logistics: Hundreds of thousands of devices.** Motive turns telemetry from hundreds of thousands of gateways into location and geofence events. Samsara streams GPS from millions of vehicles. Porter processes driver pings through Pub/Sub and watches cloud cost closely.

## Running it in production

A demo on a laptop answers whether the idea works. A delivery platform needs to know what happens on New Year's Eve. Here is what Apache Iggy 0.9 provides today, and what LaserData adds when you would rather not run it yourself.

| Area | Apache Iggy 0.9 today |
|---|---|
| Replication | Clusters use Viewstamped Replication. Three nodes tolerate one failure, five tolerate two. Every SDK follows the leader and fails over on its own. |
| Durability per topic | `replicated` acknowledges once a quorum holds the write, like Kafka's `acks=all`. `persisted` also waits for stable storage. |
| Retention and replay | Set message expiry per topic. A few hours of pings covers rebuilds and support lookups; S3 archiving keeps trips for disputes and model training. |
| Security | TLS on TCP, QUIC, WebSocket and HTTP. Users, permissions and access tokens. Optional AES-256-GCM encryption of message payloads. |
| Observability | Prometheus metrics from the server. The kit adds position age at dispatch and ping-to-ETA latency from the product side. |
| Clients | Official SDKs for Rust, Go, Java, C#, Python and Node.js, so producers and consumers keep their languages. |

**LaserData Cloud** runs the same engine as a managed service on AWS or GCP, or inside your own cloud account (BYOC), on three dedicated nodes with local NVMe for the lowest tail latency. It adds a console and CLI, RBAC and audit logs, metrics, snapshots and upgrades. LaserData's founders lead the Apache Iggy project, so fixes land upstream, in the open.

## Frequently Asked Questions

**Can courier phones connect to Iggy directly?** They can over TCP, QUIC or WebSocket, with per-user credentials and TLS. Many teams still put a thin ingest tier in front for authentication and rate limiting, and that tier writes to Iggy. Either way, QUIC is worth using from the phone side.

**Why not keep only the latest position in Redis?** Many teams do, and Redis is a good place to serve the latest position from. The log sits in front of it: every service gets every ping in order, a new service can start from history, and a crashed cache can be rebuilt by replaying the last 30 seconds.

**How do I handle millions of couriers on 32 partitions?** Each partition holds many couriers; ordering only needs to hold per courier, which keying guarantees. Increase partitions when one core can no longer keep up with a partition's ping rate, not per courier.

**Is "replicated" the same guarantee as Kafka's acks=all?** Close to it. Both acknowledge a write once a majority of replicas hold it, and neither forces an fsync per message by default. That is why the main benchmark compares them directly. For teams that require data on disk before the ack, the benchmark adds a persisted run.

**Iggy consumers poll. Doesn't polling add latency?** Yes, up to the poll interval. The consumers in the repo poll every 100 µs to 1 ms, which keeps the added delay small but costs client CPU. The benchmark reports client CPU next to latency. Deferred polling, where the server holds a poll until data arrives, is on the Iggy roadmap.

**Can I keep my Kafka producers?** Not directly yet. Iggy's Kafka-compatible gateway is in progress and does not accept produce or fetch requests in 0.9. To trial it beside an existing pipeline, the repo includes `kafka_bridge.py`, a Kafka consumer group that copies a topic into Iggy without touching the Kafka side.

**Is there exactly-once delivery?** No. Delivery is at-least-once. Every message in the repo carries a sequence number, so consumers skip anything they have already seen. Iggy can also deduplicate on the server by message ID.

**Is 0.9 mature enough for production?** Clustering shipped in September 2026, and 0.9 changed the wire protocol and on-disk format. Iggy's own team says Kafka's replication has a decade more production behind it. That is why the adoption path starts with a shadow run beside your existing system. Decide on evidence from your own peaks, not from this post.

## Where it does not fit

Months of trip history for analytics and model training are cheaper on object storage, written there by Iggy's connectors. Map tiles, routing and the ETA model itself are outside the log. Pipelines that depend on Kafka Connect or Kafka transactions should wait for Iggy's Kafka-compatible gateway. Iggy's place is the live stream: every ping, to every service, in order.
