#!/usr/bin/env bash
# Market-data fan-out on Apache Iggy, measured with iggy-bench (cargo install iggy-bench).
# Same steps as kafka/run-omb.sh: 128 B ticks, 16 partitions, 4 desks (consumer groups),
# replicated writes, rate steps up to the 1.6M msg/s a tier-1 bank published as its target.
set -euo pipefail
SERVER=${SERVER:-127.0.0.1:8090}
GIT=${GIT:-$(date +%Y%m%d)}
for RATE in 100000 500000 1000000 1600000; do
  MB=$(( RATE * 128 / 1000000 ))MB          # rate limit is in bytes/s for all producers
  iggy-bench --message-size 128 --messages-per-batch 16 --warmup-time 5m --total-data 200GiB \
    --rate-limit "$MB" --durability replicated \
    balanced-producer-and-consumer-group --streams 1 --partitions 16 --producers 4 \
      --consumers 16 --consumer-groups 4 --read-amplification 1.05 \
    tcp --server-address "$SERVER" \
    output --identifier "mkt-fanout-${RATE}" --remark "replicated,128B,16p,4 groups" --gitref "$GIT"
done
# Persisted variant at 1M/s: the write is acknowledged only after it reaches stable storage.
iggy-bench --message-size 128 --messages-per-batch 16 --warmup-time 5m --total-data 200GiB \
  --rate-limit 128MB --durability persisted \
  balanced-producer-and-consumer-group --streams 1 --partitions 16 --producers 4 \
    --consumers 16 --consumer-groups 4 --read-amplification 1.05 \
  tcp --server-address "$SERVER" \
  output --identifier mkt-fanout-1000000-persisted --remark "persisted,128B,16p,4 groups" --gitref "$GIT"

# Failover: during a 1M/s run, stop the leader node at minute 15 (e.g. docker stop / systemctl stop)
# and record time-to-resume and the next minute's p99 from desk.py output on the load generator.

# Market-open burst: 10x for 60 s on top of 160K msg/s is the same 1.6M ceiling, reached suddenly.
# Run feed.py --rate 160000 --burst 10 --burst-at 300 --burst-for 60 with desk.py x4 and record p99 per second.
