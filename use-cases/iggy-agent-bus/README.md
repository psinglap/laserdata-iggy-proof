# Keeping AI voice agents fast, recoverable and auditable on every call, with LaserData

Voice and chat agents pass transcripts, tool calls and decisions between services many times per turn, and a person is waiting on the other end. This is how to connect those services with LaserData on Apache Iggy, the open-source streaming engine LaserData builds, why the 800 ms turn is the real test, and how to try it yourself.

> Status: draft. Code targets the `apache-iggy` 0.9.0 Python SDK and the `apache/iggy:0.9.0` image. Benchmark runs are pending; no results are published yet.

**In short**
- **The job:** carry every utterance, tool call and answer between the parts of an agent fast enough that the caller never notices the plumbing, while keeping a complete, replayable record of every call.
- **Why Iggy:** a Rust, thread-per-core log with no garbage collector keeps each hop to a few milliseconds, a call's events stay in order on one partition, crashed agents resume from the log, and Iggy's MCP server lets agents read and write streams as tools.
- **What you get:** a repo with callers, an agent and tool workers that print turn latency and orchestration overhead, an audit script that replays any call, three tests, and a benchmark against latency-tuned Kafka anyone can rerun.

## The problem: the caller hears every millisecond the plumbing adds

A customer calls about a late order. They finish speaking, and an AI agent has under a second to answer before the pause starts to feel wrong.

  
In that second, a lot happens. Speech-to-text finishes the transcript. The model decides it needs the order status and calls a tool. The tool queries an internal API and returns. The model writes the answer, and text-to-speech starts speaking. Meanwhile a supervisor agent watches for escalation, a QA service scores the call, and compliance needs a record of everything that was said and done.

  
Each step is a separate service, and the events between them cross some kind of bus several times per turn. If one of those hops stalls for 200 milliseconds, the caller hears a gap and starts talking over the agent. If an agent process crashes mid-call, the call should continue where it was, not start over. And when a call goes wrong, someone will need to replay exactly what happened.

This post walks through building that bus on Apache Iggy. We start with what the job demands and where the time goes, then show how the design keeps turns fast and calls recoverable, and finally measure it against Kafka in a way you can rerun. Everything here runs from the companion repo.

| Requirement | What it means |
|---|---|
| Fast at every hop | A turn crosses the bus several times. Each hop has to be small and, above all, predictable. |
| Order per call | The tool result must never arrive before the tool call, and the agent must see the caller's words in order. |
| Recover mid-call | If an agent crashes, the call continues from where it was, with no repeated or lost turns. |
| A complete record | Evals, QA and compliance need every utterance, tool call and answer of a call, replayable in order. |

## The budget: about 800 ms per turn, and only tens of them for the plumbing

Voice-agent builders work to about 800 ms from the end of the caller's speech to the first audio of the answer. Published breakdowns put speech-to-text at 100 to 300 ms, the model's time to first token at 200 to 600 ms, text-to-speech at 150 to 400 ms, and the network at 20 to 100 ms. That leaves roughly 20 to 50 ms for orchestration: every hop between services.

  
The chart below multiplies each system's published p99 by four, because a turn with one tool call crosses the bus four times.

Two things stand out. Object-storage designs, which some AI companies use for training data and telemetry, cannot sit on the live path of a voice turn at all; even one hop blows the budget. And because the hops add up, the gap between a few milliseconds and tens of milliseconds per hop is the difference between a comfortable turn and one that feels slow. Agent platforms like Vapi say it plainly: a p99 spike means callers drop.

## Why the slowest turns, not the average, decide whether callers stay

Where do slow hops come from? In a JVM broker, a garbage-collection pause stops every partition at once, so every call on the cluster stalls together. A shared request pool lets one slow disk write hold up unrelated calls. And when an eval or analytics job reads history, its reads compete with the live agents.

  
Iggy removes the first two by design: written in Rust with no garbage collector, running thread-per-core on `io_uring`, so each core owns its partitions and its own I/O. Separate consumer groups keep evals from slowing the live path.

## The design: an agent bus with LaserData

- **One session log, keyed by call.** Every event of a call (what the caller said, each tool call and result, each answer) goes to the same partition, in order. Any service can reconstruct a call by reading its key.
- **Share calls across agents, keep each call with one agent.** Agents split the calls through one consumer group. Because events are keyed by call, a call's turns stay with one agent, which keeps its context local and avoids a shared state store on the hot path.
- **Let tools be just another consumer.** Tool workers take tool calls from the same log and post results back. Adding capacity for a slow API means starting more workers, not changing the agent.
- **Recover from the log, not from memory.** Agents commit their position every half second. An agent that crashes is replaced, and the replacement continues from the last committed event; the caller sees each reply once because replies carry the turn number.
- **Give agents the bus as a tool.** Iggy's MCP server exposes streams and topics to MCP-capable agents, so a supervisor or eval agent can read a call's history or post an event without custom glue.

## Build it: callers, an agent and tool workers

The repo needs Docker and Python 3.10 or later. It starts Iggy 0.9.0 and runs 500 simulated calls of 20 turns each, an agent with 150 ms of simulated model time per step, and tool workers with 40 ms of simulated API time.

  
```
git clone https://github.com/laserdata/iggy-agent-bus
cd iggy-agent-bus
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python tools.py &
python agent.py --llm-ms 150 &
python caller.py --calls 500
```

  
Every event of a call is keyed by the call, so it stays in order:

  
```
await bus.send_with_partitioning(
    [SendMessage(msg(call=call, turn=3, t="tool_call", tool="lookup_order"))],
    Partitioning.messages_key(call))
```

  
The caller measures each turn, and subtracts the model and tool time the agent reports, so what remains is the cost of orchestration:

  
```
turn_ms = (time.time_ns() - started) / 1e6
overhead.add(turn_ms - reply["work_ms"])
```

  
It prints a line a second:

  
```
  …  turns/s  turn p50 …ms  p99 …ms  |  orchestration overhead p50 …ms  p99 …ms  PASS (<800ms)
```

  
Any call can be replayed in order, with timings, from the log:

  
```
python audit.py --call call-7
```

  
To plug in your own agents, send each service's events to the session topic keyed by call, and keep one rule: every event carries the call and turn, so replies can be matched and replays are complete.

## Test it under call-center conditions

### The surge

Triple the number of concurrent calls, as an outage or a launch does.

```
python caller.py --calls 1500
```

**Pass:** turn p99 stays under 800 ms and orchestration overhead p99 stays under 40 ms.

### Crash the agent mid-call

Kill the agent during the run and start a new one; it continues from the last committed event.

```
# Ctrl-C agent.py, then
python agent.py --llm-ms 150
```

**Pass:** every call continues; the turns in flight are answered once, late but not lost, and no caller hears a reply twice.

### Replay a call

Pick any call and replay its full history from the log.

```
python audit.py --call call-7
```

**Pass:** every utterance, tool call, result and answer appears once, in order, with timings.

## Benchmark it against latency-tuned Kafka

A benchmark that only its author can reproduce is marketing. This one is designed to be rerun by anyone, and its rules are fixed before any results exist. The question: with tens of thousands of live calls, does each hop on the bus stay under 10 ms at p99, so four hops fit comfortably in the orchestration budget?

| Parameter | Value | Why |
|---|---|---|
| Event size | 1 KB | Transcript chunks, tool calls and results are small JSON documents. |
| Rate | 25K → 500K events/s | 2,500 to 50,000 concurrent calls at about ten bus events per call per second. |
| Partitions | 64, by call | A call's events stay in order and with one agent. |
| Readers | 4 consumer groups | Agents, tool workers, the telephony gateway and evals. |
| Durability | replicated × 3 | Kafka runs `acks=all`, three replicas. A call must survive a crash mid-sentence. |
| Kafka tuning | linger 0 · fetch wait 0 | Kafka on its best latency settings. Beating an untuned Kafka proves nothing. |
| Call surge | 3× for 10 min | Outages and launches flood contact centers at once. |
| Failover | kill a leader at 15 min | Reports the time until agents receive again and how many turns went over budget. |
| Duration | 30 min/step + 24 h | Contact centers run around the clock. |
| Hardware | 3 × c6id.4xlarge | Same machines for both systems, plus two load generators. |

Iggy is driven by its own `iggy-bench` tool and Kafka by the OpenMessaging Benchmark, with the same workload shape. Each step runs three times and the median run is reported. The kit also runs at each rate, so the post can report turn latency and orchestration overhead next to per-hop latency. Every config, version and raw output is committed. If Kafka wins a step, that row is published.

**The pass line: per-hop p99 under 10 ms at 250,000 events a second with four readers, replicated, through a threefold surge, and orchestration overhead under 40 ms per turn at p99.** Full plan: [`bench/PLAN.md`](bench/PLAN.md).

## Where it fits in your workloads

- **Voice agent platforms: Every call, every turn.** Vapi runs live phone calls on a Kafka cluster per region and says a p99 spike means callers drop. Deepgram builds its voice-agent engine in Rust. In India, Sarvam AI is building telephony voice agents on its own speech models.
- **Customer-service agents: Chat and voice at enterprise scale.** Sierra's context engine processes agent conversation data with event streaming. Decagon builds real-time ingestion on Kafka and Flink with tight p95 and p99 targets. Yellow.ai and Observe.AI run agents on live contact-center traffic.
- **AI-native products: Agents, tools and feedback loops.** Cursor keeps latency-sensitive agent paths separate from its WarpStream pipelines. Glean is building an agentic runtime and asks for low-latency event systems. FourKites is wiring agents into its Kafka event platform through MCP.

## Running it in production

A demo on a laptop answers whether the idea works. A contact center needs to know what happens when an agent host dies mid-call. Here is what Apache Iggy 0.9 provides today, and what LaserData adds when you would rather not run it yourself.

| Area | Apache Iggy 0.9 today |
|---|---|
| Replication | Clusters use Viewstamped Replication. Three nodes tolerate one failure, five tolerate two. Every SDK follows the leader and fails over on its own. |
| Durability per topic | `replicated` acknowledges once a quorum holds the write, like Kafka's `acks=all`. `persisted` also waits for stable storage. |
| Retention and replay | Set message expiry per topic. Days of session logs cover evals and disputes; S3 archiving and the Iceberg sink keep calls for training and compliance. |
| Security | TLS on TCP, QUIC, WebSocket and HTTP. Users, permissions and access tokens. Optional AES-256-GCM encryption of message payloads. |
| Observability | Prometheus metrics from the server. The kit adds turn latency and orchestration overhead per turn from the caller's side. |
| Clients | Official SDKs for Rust, Go, Java, C#, Python and Node.js, so producers and consumers keep their languages. |

**LaserData Cloud** runs the same engine as a managed service on AWS or GCP, or inside your own cloud account (BYOC), on three dedicated nodes with local NVMe for the lowest tail latency. It adds a console and CLI, RBAC and audit logs, metrics, snapshots and upgrades. LaserData's founders lead the Apache Iggy project, so fixes land upstream, in the open.

## Frequently Asked Questions

**Why not call services directly over HTTP or gRPC?** Direct calls are the fastest single hop and are fine for simple agents. A log adds what direct calls lack: every step recorded in order, agents that can crash and resume, tools that scale by adding consumers, and evals that read the same history. The benchmark shows what that costs per hop, so you can decide with numbers.

**What does Iggy's MCP server do?** It exposes Iggy streams and topics to agents that speak the Model Context Protocol, so an agent can read messages from a topic or post one as a tool call, without custom integration code. It runs against the replicated server.

**Can the bus carry audio?** It can carry audio chunks as messages, but most teams send audio over a media path built for it and put transcripts and events on the bus. The repo follows that split.

**Is "replicated" the same guarantee as Kafka's acks=all?** Close to it. Both acknowledge a write once a majority of replicas hold it, and neither forces an fsync per message by default. That is why the main benchmark compares them directly. For teams that require data on disk before the ack, the benchmark adds a persisted run.

**Iggy consumers poll. Doesn't polling add latency?** Yes, up to the poll interval. The consumers in the repo poll every 100 µs to 1 ms, which keeps the added delay small but costs client CPU. The benchmark reports client CPU next to latency. Deferred polling, where the server holds a poll until data arrives, is on the Iggy roadmap.

**Can I keep my Kafka producers?** Not directly yet. Iggy's Kafka-compatible gateway is in progress and does not accept produce or fetch requests in 0.9. To trial it beside an existing pipeline, the repo includes `kafka_bridge.py`, a Kafka consumer group that copies a topic into Iggy without touching the Kafka side.

**Is there exactly-once delivery?** No. Delivery is at-least-once. Every message in the repo carries a sequence number, so consumers skip anything they have already seen. Iggy can also deduplicate on the server by message ID.

**Is 0.9 mature enough for production?** Clustering shipped in September 2026, and 0.9 changed the wire protocol and on-disk format. Iggy's own team says Kafka's replication has a decade more production behind it. That is why the adoption path starts with a shadow run beside your existing system. Decide on evidence from your own peaks, not from this post.

## Where it does not fit

Model inference itself, speech-to-text and text-to-speech run outside the bus. Large training datasets and long-term call archives are cheaper on object storage, written there by Iggy's connectors. Pipelines that depend on Kafka Connect or Kafka transactions should wait for Iggy's Kafka-compatible gateway. Iggy's place is the live path between an agent's parts, and the record of it.
