# Benchmark plan: courier location, dispatch and ETA

**Question it answers:** at a million active couriers pinging every five seconds, does every location
reach dispatch, ETA and the customer map within 50 ms at p99, leaving most of a 500 ms
end-to-end budget for mobile networks, on Apache Iggy and on Kafka tuned for latency?

## Parameters, and why each one

| Parameter | Value | Why |
|---|---|---|
| Ping size | 120 B | Courier id, sequence, latitude, longitude, timestamp. |
| Rate | 40K → 200K → 500K → 1M pings/s | 200K to 5M active couriers at one ping every 5 s, the interval ride-hailing apps commonly use. |
| Partitions | 32, keyed by courier | Each courier's pings must stay in order so the latest position wins. |
| Readers | 4 consumer groups | Dispatch, ETA, customer maps and geofence alerts all read every ping. |
| Durability | Iggy replicated × 3 · Kafka acks=all, RF 3, ISR 2 | Positions feed disputes and safety reviews later. |
| Kafka tuning | linger.ms=0, fetch.max.wait.ms=0 | Kafka on its best latency settings. |
| Dinner rush | 3× for 30 min | Meal peaks and bad weather triple pings and orders at once. |
| Lossy network | 5% packet loss, QUIC vs TCP (Iggy) | Phones on cellular networks lose packets; QUIC avoids head-of-line blocking. |
| Failover | Kill a leader at minute 15 | Report time until dispatch receives again and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Delivery runs all day with two peaks. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

## Hardware

3 × `c6id.4xlarge` brokers, 2 × `c7i.4xlarge` load generators. Same kernel and filesystem for both.
The lossy-network run uses `tc qdisc add dev eth0 root netem loss 5%` on the load generator.

## Pass lines

1. Ping-to-consumer p99 under 50 ms at 1M pings/s with 4 readers, replicated.
2. Under 5% loss, QUIC producer p99 stays within 2× of the loss-free run.

## What we publish

p99 vs ping rate (one line per system, 50 ms line drawn), the rush trace, QUIC vs TCP under loss,
the kit's position-age-at-dispatch numbers, broker CPU and node count, and every config and raw output.
