#!/usr/bin/env bash
# Order event log on Apache Iggy with iggy-bench (cargo install iggy-bench).
set -euo pipefail
SERVER=${SERVER:-127.0.0.1:8090}
GIT=${GIT:-$(date +%Y%m%d)}
for RATE in 10000 50000 100000 250000; do
  MB=$(( RATE * 300 / 1000000 ))MB
  iggy-bench --message-size 300 --messages-per-batch 1 --warmup-time 5m --total-data 200GiB \
    --rate-limit "$MB" --durability replicated \
    balanced-producer-and-consumer-group --streams 1 --partitions 32 --producers 16 \
      --consumers 32 --consumer-groups 4 --read-amplification 1.05 \
    tcp --server-address "$SERVER" \
    output --identifier "orders-${RATE}" --remark "replicated,300B,32p,4 groups,batch 1" --gitref "$GIT"
done
iggy-bench --message-size 300 --messages-per-batch 1 --warmup-time 5m --total-data 100GiB \
  --rate-limit 30MB --durability persisted \
  balanced-producer-and-consumer-group --streams 1 --partitions 32 --producers 16 \
    --consumers 32 --consumer-groups 4 --read-amplification 1.05 \
  tcp --server-address "$SERVER" \
  output --identifier orders-100000-persisted --remark "persisted,300B,32p,4 groups" --gitref "$GIT"
# Rebuild: after a day-long run, start positions.py and record the reported rebuild time.
