# Benchmark plan: inventory, pricing and order status

**Question it answers:** when a sale starts and stock changes spike tenfold, does every stock and price
change reach search, product pages and carts within 20 ms at p99, so the storefront stops showing
items that are gone, on Apache Iggy and on Kafka tuned for latency?

## Parameters, and why each one

| Parameter | Value | Why |
|---|---|---|
| Change size | 150 B | Store, SKU, sequence, resulting level, price and timestamp. |
| Rate | 50K → 200K → 500K → 1M changes/s | Thousands of stores times tens of thousands of SKUs, moving all day and spiking on sale days. |
| Partitions | 32, keyed by store:sku | Each item's changes must apply in order so the newest level wins. |
| Readers | 6 consumer groups | Search, product pages, carts, pricing, recommendations and analytics all read every change. |
| Durability | Iggy replicated × 3 · Kafka acks=all, RF 3, ISR 2 | Stock levels and order status cannot be lost. |
| Kafka tuning | linger.ms=0, fetch.max.wait.ms=0 | Kafka on its best latency settings. |
| Sale start | 10× for 5 min | Flash sales and holiday doors-open moments. |
| Failover | Kill a leader at minute 15 | Report time until readers receive again and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Stores trade all day; drift shows over hours. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

## Hardware

3 × `c6id.4xlarge` brokers, 2 × `c7i.4xlarge` load generators. Same kernel and filesystem for both.

## Pass lines

1. Change-to-reader p99 under 20 ms at 500K changes/s with 6 readers, replicated, through the 10× sale start.
2. In the kit at the same rate, orders rejected as sold out stay under 0.1%.

## What we publish

p99 vs change rate (one line per system, 20 ms line drawn), the sale-start trace, the kit's
stale-availability rate, broker CPU and node count, and every config and raw output.
