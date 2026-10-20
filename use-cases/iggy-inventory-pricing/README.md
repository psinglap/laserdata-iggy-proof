# Showing shoppers only what is really in stock during the biggest sale of the year, with LaserData

Retailers, marketplaces and quick-commerce apps change stock and prices thousands of times a second, and search, product pages and carts all have to reflect it. This is how to move those changes with LaserData on Apache Iggy, the open-source streaming engine LaserData builds, why the first minutes of a sale are the real test, and how to try it yourself.

> Status: draft. Code targets the `apache-iggy` 0.9.0 Python SDK and the `apache/iggy:0.9.0` image. Benchmark runs are pending; no results are published yet.

**In short**
- **The job:** carry every stock and price change from stores and warehouses to search, product pages and carts fast enough that shoppers are not shown items that just sold out, including in the minutes after a sale starts.
- **Why Iggy:** changes keyed by store and item apply in order, every storefront service reads one copy through its own consumer group, and a rebuilt search index can replay the whole topic.
- **What you get:** a repo with stores, a storefront and shoppers that prints how often a shopper orders something that was already gone, three sale-day tests, and a benchmark against latency-tuned Kafka anyone can rerun.

## The problem: the storefront shows what was in stock a moment ago

It is 9 am on Black Friday. A limited deal goes live, and within a minute thousands of shoppers are adding it to their carts.

  
Behind the page, stock is moving fast. Online orders, in-store sales, other marketplaces and warehouse restocks all change the count. Prices change too, as promotions start and dynamic pricing reacts. Every one of those changes has to reach search results, product pages, carts and recommendations.

  
When changes arrive late, the storefront shows items that are already gone. Shoppers order them, the order is rejected or cancelled later, and support picks up the pieces. In quick commerce, where each dark store holds only a few units of each item, a few hundred milliseconds of lag at the wrong moment means a cancelled delivery.

This post walks through building that change stream on Apache Iggy. We start with what the job demands and where the time goes, then show how the design keeps the storefront honest at the sale start, and finally measure it against Kafka in a way you can rerun. Everything here runs from the companion repo.

| Requirement | What it means |
|---|---|
| Newest level wins | An item's changes must apply in order. A late restock event must never show stock that has since sold. |
| Every storefront service sees every change | Search, product pages, carts and recommendations all need the stream, and adding one must not slow the others. |
| Holds at the sale start | The first minutes of a sale bring the most changes and the most shoppers at the same time. |
| Rebuildable | A search index that is rebuilt or added mid-season must catch up from history, not from a nightly export. |

## The budget: about 100 ms from the stockroom to the storefront

There is no published standard for this, so here is the practitioner norm. Shoppers add to cart within seconds of seeing a product, and on a sale day the last units go in seconds. A change that reaches search and product pages within about 100 ms keeps the storefront close enough to the truth that few shoppers are disappointed.

  
That budget covers the database or store system writing the change, the log carrying it, and the search index or cache applying it. The log should take a small, predictable part.

On a quiet day both Iggy and Kafka fit easily. Object-storage designs, attractive for their cost, spend several times the budget on one hop. The deciding questions for inventory are how the slowest changes behave when a sale starts, how many services can read the stream without slowing it down, and what it costs to run all year for a few days of peak.

## Why the sale start, not the average, decides cancellations

Where do slow changes come from? In a JVM broker, a garbage-collection pause stops every partition at once, and sale traffic, with its bursts of small messages, makes pauses more likely. A shared request pool lets one slow disk write hold up unrelated partitions. And when one reader, say analytics, falls behind, its catch-up reads compete with search and carts.

  
Iggy removes the first two by design: written in Rust with no garbage collector, running thread-per-core on `io_uring`, so each core owns its partitions and its own I/O.

## The design: live inventory with LaserData

- **Key changes by store and item, so the newest level wins.** All changes for one item in one store go to one partition and arrive in order. Each carries a sequence number and the resulting level, not just a delta, so a reader that sees a repeated or older change simply ignores it.
- **One consumer group per storefront service.** Search, product pages, carts and recommendations each read every change through their own group. Changes are written once; a new service is a new reader.
- **Rebuild an index by replaying the topic.** A new or rebuilt search index starts from the first retained change and catches up at full speed. Because every change carries the resulting level, the replay ends with the correct stock for every item.
- **Let the source of truth decide.** The storefront never decides whether an order succeeds; the stock owner does. The stream's job is to make the storefront's view close enough that rejections are rare. The kit counts them, so the cost of lag is visible.
- **Publish order status on the same log.** Accepted and rejected orders go to an order-status topic keyed by order, so apps, email and support tools read one stream instead of polling the order database.

## Build it: stores, a storefront and shoppers

The repo needs Docker and Python 3.10 or later. It starts Iggy 0.9.0, simulates 100 stores with 2,000 items each changing 5,000 times a second, and runs a storefront whose shoppers order 200 items a second from what it shows as in stock.

  
```
git clone https://github.com/laserdata/iggy-inventory-pricing
cd iggy-inventory-pricing
docker compose up -d                 # Apache Iggy 0.9.0 on :8090
pip install -r requirements.txt      # apache-iggy 0.9.0
python store_ops.py --rate 5000 &
python storefront.py --name search
```

  
Every change carries the item's sequence number and resulting level, keyed by store and item:

  
```
await stock.send_with_partitioning(
    [SendMessage(msg(k="S042:K0137", seq=seq, level=3, price=4.99))],
    Partitioning.messages_key("S042:K0137"))
```

  
The storefront applies only newer changes, so the newest level always wins:

  
```
if change["seq"] <= seq.get(change["k"], 0):
    return
(in_stock.add if change["level"] > 0 else in_stock.discard)(change["k"])
```

  
Both sides print a line a second:

  
```
[search]   5,000 changes/s  change-to-storefront p50 …ms  p99 …ms  p99.9 …ms  in stock …  PASS (<100ms)
orders accepted …  rejected as sold out …  stale-availability rate …%
```

  
To plug in your own stock system, point `kafka_bridge.py` at your existing stock topic or CDC stream and keep two rules: key by store and item, and send the resulting level with a sequence number, not only the delta.

## Test it under sale-day conditions

### The sale starts

Multiply stock changes tenfold for two minutes, one minute in, while shoppers keep ordering.

```
python store_ops.py --rate 5000 --sale 10 --sale-at 60 --sale-for 120
```

**Pass:** change-to-storefront p99 stays under 100 ms, and the stale-availability rate stays under 0.1%.

### More storefront readers

Add product pages and carts as two more storefronts reading the same changes.

```
python storefront.py --name pdp &
python storefront.py --name cart
```

**Pass:** the search storefront's p99 does not move when the other two join.

### Rebuild the index

Stop the search storefront mid-sale and start it again; it replays every change from the start of the topic.

```
# Ctrl-C storefront.py --name search, then
python storefront.py --name search
```

**Pass:** it catches up and its in-stock count matches the other storefronts within seconds.

## Benchmark it against latency-tuned Kafka

A benchmark that only its author can reproduce is marketing. This one is designed to be rerun by anyone, and its rules are fixed before any results exist. The question: when a sale starts, does every change reach six storefront services within 20 ms at p99, leaving most of the 100 ms for databases and indexes?

| Parameter | Value | Why |
|---|---|---|
| Change size | 150 B | Store, item, sequence, resulting level, price and timestamp. |
| Rate | 50K → 1M changes/s | Thousands of stores times tens of thousands of items, spiking on sale days. |
| Partitions | 32, by store:item | Each item's changes must apply in order. |
| Readers | 6 consumer groups | Search, product pages, carts, pricing, recommendations and analytics. |
| Durability | replicated × 3 | Kafka runs `acks=all`, three replicas. Stock and order status cannot be lost. |
| Kafka tuning | linger 0 · fetch wait 0 | Kafka on its best latency settings. Beating an untuned Kafka proves nothing. |
| Sale start | 10× for 5 min | Flash sales and holiday doors-open moments. |
| Failover | kill a leader at 15 min | Reports the time until readers receive again and the next minute's p99. |
| Duration | 30 min/step + 24 h | Stores trade all day; drift shows over hours. |
| Hardware | 3 × c6id.4xlarge | Same machines for both systems, plus two load generators. |

Iggy is driven by its own `iggy-bench` tool and Kafka by the OpenMessaging Benchmark, with the same workload shape. Each step runs three times and the median run is reported. The kit also runs at each rate, so the post can report the stale-availability rate next to latency. Every config, version and raw output is committed. If Kafka wins a step, that row is published.

**The pass line: change-to-reader p99 under 20 ms at 500,000 changes a second with six readers, replicated, through a tenfold sale start, and fewer than 0.1% of orders rejected as sold out.** Full plan: [`bench/PLAN.md`](bench/PLAN.md).

## Where it fits in your workloads

- **Marketplaces and retailers: Holiday peaks, all channels.** Shopify load-tests at 150% of its last Black Friday peak and found Kafka bottlenecks to fix. Walmart processes tens of billions of replenishment messages a day for about 100M items. Expedia runs pricing and availability for over a million transactions a second.
- **Quick commerce and delivery: A few units per dark store.** Instacart streams item availability through Kafka and Flink. In India, Zepto pushes per-store stock and price changes through MSK to keep search fresh and tracks p99, and Blinkit and Instamart run the same pattern.
- **Tickets and live auctions: Seats and lots that sell in seconds.** StubHub processes 10 to 30 billion listing, price and order events a day and is choosing a new streaming platform on AWS. Gametime reprices tickets in near real time. Copart runs live vehicle auctions worldwide.

## Running it in production

A demo on a laptop answers whether the idea works. A retailer needs to know what happens on Black Friday. Here is what Apache Iggy 0.9 provides today, and what LaserData adds when you would rather not run it yourself.

| Area | Apache Iggy 0.9 today |
|---|---|
| Replication | Clusters use Viewstamped Replication. Three nodes tolerate one failure, five tolerate two. Every SDK follows the leader and fails over on its own. |
| Durability per topic | `replicated` acknowledges once a quorum holds the write, like Kafka's `acks=all`. `persisted` also waits for stable storage. |
| Retention and replay | Set message expiry or a size cap per topic. A few days of changes rebuilds any index; S3 archiving keeps longer history for audits and forecasting. |
| Security | TLS on TCP, QUIC, WebSocket and HTTP. Users, permissions and access tokens. Optional AES-256-GCM encryption of message payloads. |
| Observability | Prometheus metrics from the server. The kit adds change-to-storefront latency and the stale-availability rate from the business side. |
| Clients | Official SDKs for Rust, Go, Java, C#, Python and Node.js, so producers and consumers keep their languages. |

**LaserData Cloud** runs the same engine as a managed service on AWS or GCP, or inside your own cloud account (BYOC), on three dedicated nodes with local NVMe for the lowest tail latency. It adds a console and CLI, RBAC and audit logs, metrics, snapshots and upgrades. LaserData's founders lead the Apache Iggy project, so fixes land upstream, in the open.

## Frequently Asked Questions

**Can the stream reserve stock, so nothing is ever oversold?** No, and it should not try. Reservations belong in the system that owns the stock, with a transaction. The stream's job is to keep every storefront's view close to that truth, which is what keeps rejections rare. The kit separates the two on purpose.

**Our stock changes come from database CDC. Does that work?** Yes, as long as each change carries the item key, a sequence and the resulting level. Iggy has a PostgreSQL source connector, and existing Debezium pipelines can be copied in with the bridge until Iggy's Kafka-compatible gateway lets Debezium write to it directly.

**How many readers can one stream support?** Each consumer group reads independently from the same log, so readers do not copy the data. The benchmark uses six; the practical limit is broker network and CPU, which the benchmark reports at each step.

**Is "replicated" the same guarantee as Kafka's acks=all?** Close to it. Both acknowledge a write once a majority of replicas hold it, and neither forces an fsync per message by default. That is why the main benchmark compares them directly. For teams that require data on disk before the ack, the benchmark adds a persisted run.

**Iggy consumers poll. Doesn't polling add latency?** Yes, up to the poll interval. The consumers in the repo poll every 100 µs to 1 ms, which keeps the added delay small but costs client CPU. The benchmark reports client CPU next to latency. Deferred polling, where the server holds a poll until data arrives, is on the Iggy roadmap.

**Can I keep my Kafka producers?** Not directly yet. Iggy's Kafka-compatible gateway is in progress and does not accept produce or fetch requests in 0.9. To trial it beside an existing pipeline, the repo includes `kafka_bridge.py`, a Kafka consumer group that copies a topic into Iggy without touching the Kafka side.

**Is there exactly-once delivery?** No. Delivery is at-least-once. Every message in the repo carries a sequence number, so consumers skip anything they have already seen. Iggy can also deduplicate on the server by message ID.

**Is 0.9 mature enough for production?** Clustering shipped in September 2026, and 0.9 changed the wire protocol and on-disk format. Iggy's own team says Kafka's replication has a decade more production behind it. That is why the adoption path starts with a shadow run beside your existing system. Decide on evidence from your own peaks, not from this post.

## Where it does not fit

The order and stock database itself stays where it is; the stream carries its changes. Years of change history for forecasting are cheaper on object storage, written there by Iggy's connectors. Pipelines that depend on Kafka Connect or Kafka transactions should wait for Iggy's Kafka-compatible gateway. Iggy's place is the live path from stockroom to storefront.
