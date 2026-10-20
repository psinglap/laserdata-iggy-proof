# Never losing a sensor reading when a vehicle, store or plant goes offline, with LaserData

Vehicles, stores, warehouses and factories produce data where the network is unreliable. This is how to keep every reading safe at the edge and deliver it to the cloud in order when the link returns, with LaserData on Apache Iggy, the open-source streaming engine LaserData builds, and how to try it yourself.

> Status: draft. Code targets the `apache-iggy` 0.9.0 Python SDK and the `apache/iggy:0.9.0` image. Benchmark runs are pending; no results are published yet.

**In short**
- **The job:** accept every reading at the edge whether or not the cloud is reachable, keep it safe on local disk, and forward it in order, with nothing lost, as soon as the link is back.
- **Why Iggy:** the same small single binary runs on an ARM gateway and in the cloud, writes to local disk before acknowledging, and a forwarder commits each reading only after the cloud has it.
- **What you get:** a repo with sensors, a local gateway log, a forwarder and a cloud checker that prints backlog and gaps through an outage, three tests, and a benchmark of footprint and catch-up against Kafka and NATS.

## The problem: the network fails at the edge, and the data keeps coming

A refrigerated truck drives into a valley with no signal for forty minutes. Its sensors keep reporting temperature every second.

  
Edge sites are full of moments like this. Vehicles pass through tunnels. Stores lose their uplink. Ships and mines have links that come and go. Meanwhile, sensors, cameras, point-of-sale terminals and machines keep producing readings, and someone in the cloud needs all of them: the cold-chain record for compliance, the vibration trend that predicts a failure, the transaction log for reconciliation.

  
Sending straight to the cloud loses everything produced while the link is down. Buffering in the device's memory loses it on the next power cut. What edge sites need is a small, durable log on the gateway that devices can always write to, and a forwarder that delivers the backlog in order, without gaps or duplicates, when the network comes back.

This post walks through building that pattern on Apache Iggy. We start with what the job demands, then show how the design keeps every reading through outages and restarts, and finally measure footprint and catch-up against Kafka and NATS in a way you can rerun. Everything here runs from the companion repo.

| Requirement | What it means |
|---|---|
| Always writable locally | Devices must be able to write whether or not the cloud is reachable. |
| Survives power cuts | Readings must be on disk before they are acknowledged. Edge power is unreliable. |
| Small enough for the gateway | Gateways have a few ARM cores and a few gigabytes of memory, shared with other software. |
| Complete and in order | After an outage, the cloud must receive every reading, once, in each device's order. |

## The budget: memory on the gateway, and seconds to the cloud when online

At the edge, the first budget is resources. A typical gateway has four ARM cores and a few gigabytes of memory, shared with the applications it runs. Whatever log sits on it has to stay small, start fast and run without tuning. A runtime that needs a large heap, or a separate coordination service, is hard to fit.

  
The second budget is freshness when the link is up. Monitoring and alerts in the cloud want readings within a second or so. The chart shows published broker p99 against that, for the times the link is healthy.

Latency is not what decides edge deployments: every option fits when the link is up. What decides them is footprint, resilience to power and network loss, and how quickly a backlog drains after an outage. This is a contested space. NATS JetStream, with its leaf nodes, is the established pattern, and the benchmark compares against it directly as well as against a single-node Kafka.

## Why the backlog, not the steady state, decides edge designs

What goes wrong at the edge? A JVM-based broker needs a sized heap and, for Kafka, a controller quorum, which is a lot to run on a small gateway. Buffering in memory, or in an application's own queue, loses data on a power cut. And a forwarder that commits before the cloud has acknowledged a reading will skip that reading after a reconnect.

  
Iggy addresses the first by design: it is one Rust binary with no garbage collector or JVM, and the published image runs on ARM64 as well as x86. The design below handles the other two.

## The design: edge store-and-forward with LaserData

- **Write locally, always.** Devices write to Iggy on the gateway, never directly to the cloud. The local log is always reachable, so devices never block or drop readings because the uplink is down.
- **Persist before acknowledging.** The gateway's topic uses Iggy's `persisted` durability: a reading is acknowledged only once it is on disk, so a power cut cannot lose acknowledged readings.
- **Commit only after the cloud has it.** The forwarder reads the local log through a consumer group and commits each reading only after the cloud acknowledges it. During an outage it simply waits and retries; the backlog stays on the gateway's disk.
- **Key by device and number readings, so the cloud can check.** Readings are keyed by device and carry a per-device sequence number. The cloud skips any repeat sent again after a reconnect and counts any gap, so completeness is measured, not assumed.
- **Same engine at the edge and in the cloud.** The gateway and the cloud region run the same Iggy, with the same SDKs and tools. Commands such as configuration changes can flow back down the same way.

## Build it: sensors, a gateway log, a forwarder and a cloud check

The repo needs Docker and Python 3.10 or later. It starts two Iggy servers, one playing the edge gateway with a 512 MB memory limit and one playing the cloud, then runs 200 sensors, the forwarder and the cloud checker.

  
```
git clone https://github.com/laserdata/iggy-edge-forward
cd iggy-edge-forward
docker compose up -d                 # Iggy edge :8091 (512 MB), Iggy cloud :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python sensors.py --rate 2000 &
python forwarder.py &
python cloud_check.py
```

  
The forwarder commits a reading on the gateway only after the cloud has acknowledged it, and retries with backoff while the cloud is unreachable:

  
```
local = await edge.consumer_group("forwarder", "fleet", "readings",
    auto_commit=AutoCommit.After(AutoCommitAfter.ConsumingEachMessage()))

async def on_reading(m):
    while True:
        try:
            await cloud_out.send_with_partitioning([SendMessage(m.payload())],
                                                   Partitioning.messages_key(device))
            break
        except Exception:
            await asyncio.sleep(backoff)
```

  
Both sides print a line a second:

  
```
forwarded  2,000/s  cloud is behind by p50 …s  max …s  send retries 0  [live]
cloud   2,000 readings/s  edge-to-cloud p50 …ms  p99 …ms  devices 200  gaps 0  repeats skipped 0
```

  
To plug in your own devices, write to the gateway's Iggy with any SDK, or bridge MQTT into it, and keep two rules: key by device, and number each device's readings.

## Test it under edge conditions

### Lose the link for ten minutes

Take the cloud away while sensors keep writing, then bring it back.

```
bench/outage.sh 600
```

**Pass:** the forwarder reports "catching up", drains the 1.2 million-reading backlog, and the cloud checker reports 0 gaps.

### Pull the power

Kill the gateway's Iggy and the forwarder abruptly, then start them again.

```
docker kill iggy-edge; pkill -f forwarder.py
docker start iggy-edge; python forwarder.py
```

**Pass:** every acknowledged reading is still on the gateway and reaches the cloud; repeats are skipped and gaps stay 0.

### Measure the footprint

Sample the gateway container's memory and CPU for ten minutes at the current rate.

```
bench/footprint.sh iggy-edge 600
```

**Pass:** memory stays well inside the 512 MB limit, with CPU to spare for the gateway's other work.

## Benchmark footprint and catch-up against Kafka and NATS

A benchmark that only its author can reproduce is marketing. This one is designed to be rerun by anyone, and its rules are fixed before any results exist. The question: on a small ARM gateway, how much memory and CPU does each system need, and how fast does each drain a backlog with nothing lost?

| Parameter | Value | Why |
|---|---|---|
| Gateway | 4-core ARM64, 4 GB | A typical store, vehicle or plant gateway, such as a Raspberry Pi 5 or a small Graviton instance. |
| Reading size | 150 B | Device, sequence, a few values and a timestamp. |
| Rate | 1K → 20K readings/s | From a small store to a busy plant floor. |
| Durability | persisted on the gateway | Edge power is unreliable; readings must be on disk before the ack. |
| Outages | 10 min · 1 h · 6 h | Tunnels, bad links, ships at sea. |
| Measures | RSS, CPU, catch-up, gaps | Footprint and correctness decide edge deployments, not latency. |
| Comparison | Iggy · Kafka KRaft · NATS | Kafka is the familiar default; NATS JetStream leaf nodes are the established edge pattern. |

Each configuration runs three times and the median run is reported. Every config, version and raw output is committed. If NATS or Kafka is smaller or faster on any measure, that row is published.

**The pass line: Iggy stays within 512 MB on the gateway at 10,000 readings a second, and a one-hour backlog of 36 million readings forwards with zero gaps, in order per device.** Full plan: [`bench/PLAN.md`](bench/PLAN.md).

## Where it fits in your workloads

- **Vehicles and fleets: Tunnels, valleys and borders.** Rivian moves vehicle and cloud messages across NATS and Kafka and forks Rust Kafka clients. Motive and Samsara turn telemetry from hundreds of thousands of gateways into location and alert events. Lucid is building a vehicle telemetry platform.
- **Stores, warehouses and sites: Keep working when the uplink drops.** Chewy runs event-driven warehouse systems across cloud and on-site edge that must keep running through network failures. Target streams in-store device telemetry through Kafka. Verkada ingests events from a global fleet of cameras and sensors.
- **Connected devices in India: Scooters, trucks and terminals.** Ather and Ola Electric stream telemetry from connected scooters. BlackBuck tracks trucks through GPS and FASTag events. Pine Labs runs a large fleet of payment terminals.

## Running it in production

A demo on a laptop answers whether the idea works. A fleet needs to know what happens on ten thousand gateways after a regional outage. Here is what Apache Iggy 0.9 provides today, and what LaserData adds when you would rather not run the cloud side yourself.

| Area | Apache Iggy 0.9 today |
|---|---|
| Replication | Clusters use Viewstamped Replication. Three nodes tolerate one failure, five tolerate two. Every SDK follows the leader and fails over on its own. |
| Durability per topic | `replicated` acknowledges once a quorum holds the write, like Kafka's `acks=all`. `persisted` also waits for stable storage. |
| Retention on the gateway | Set a size cap or expiry on the gateway topic so the backlog never fills the disk; size it for the longest outage you need to survive. |
| Security | TLS on TCP, QUIC, WebSocket and HTTP. Users, permissions and access tokens. Optional AES-256-GCM encryption of message payloads. |
| Observability | Prometheus metrics from each server. The kit adds backlog age, send retries, gaps and repeats from the forwarding path. |
| Clients | Official SDKs for Rust, Go, Java, C#, Python and Node.js, so producers and consumers keep their languages. |

**LaserData Cloud** runs the same engine as a managed service on AWS or GCP, or inside your own cloud account (BYOC), on three dedicated nodes with local NVMe for the lowest tail latency. It adds a console and CLI, RBAC and audit logs, metrics, snapshots and upgrades. LaserData's founders lead the Apache Iggy project, so fixes land upstream, in the open.

## Frequently Asked Questions

**Does Iggy run on ARM gateways?** Yes. Iggy is a single Rust binary, and the published Docker image is built for linux/arm64 as well as x86. It uses io_uring, so it needs Linux kernel 5.19 or newer, which current Raspberry Pi OS and most gateway distributions provide.

**How is this different from NATS leaf nodes?** NATS JetStream leaf nodes are a mature, widely used way to do this, and the benchmark compares against them directly. Iggy's case is a single engine and SDK set from the gateway to a replicated cloud cluster, with partitioned, replayable topics on both sides. Which one wins on footprint and catch-up is what the benchmark is for.

**What about MQTT devices?** Many devices speak MQTT. Run a small MQTT-to-Iggy bridge on the gateway, or keep your MQTT broker for devices and forward from it into the gateway's Iggy. The rest of the design is unchanged.

**What happens if the gateway disk fills during a long outage?** The size cap or expiry on the gateway topic decides: the oldest readings are removed first. Size the cap for your longest expected outage, and monitor backlog age so the cloud knows when a site is at risk.

**Is "replicated" the same guarantee as Kafka's acks=all?** Close to it. Both acknowledge a write once a majority of replicas hold it, and neither forces an fsync per message by default. That is why the main benchmark compares them directly. For teams that require data on disk before the ack, the benchmark adds a persisted run.

**Is there exactly-once delivery?** No. Delivery is at-least-once. Every message in the repo carries a sequence number, so consumers skip anything they have already seen. Iggy can also deduplicate on the server by message ID.

**Is 0.9 mature enough for production?** Clustering shipped in September 2026, and 0.9 changed the wire protocol and on-disk format. Iggy's own team says Kafka's replication has a decade more production behind it. That is why the adoption path starts with a shadow run beside your existing system. Decide on evidence from your own peaks, not from this post.

## Where it does not fit

Device firmware, local control loops and video streams stay where they are; the log carries readings and events. Months of history belong in the cloud's lakehouse, written there by Iggy's connectors. If your fleet already runs NATS leaf nodes happily, the benchmark is the place to decide whether a change is worth it. Iggy's place is the durable hop between the edge and the cloud.
