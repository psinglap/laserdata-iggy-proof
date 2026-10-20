#!/usr/bin/env bash
# Inventory and pricing changes on Apache Iggy with iggy-bench (cargo install iggy-bench).
set -euo pipefail
SERVER=${SERVER:-127.0.0.1:8090}
GIT=${GIT:-$(date +%Y%m%d)}
for RATE in 50000 200000 500000 1000000; do
  MB=$(( RATE * 150 / 1000000 ))MB
  iggy-bench --message-size 150 --messages-per-batch 16 --warmup-time 5m --total-data 200GiB \
    --rate-limit "$MB" --durability replicated \
    balanced-producer-and-consumer-group --streams 1 --partitions 32 --producers 8 \
      --consumers 24 --consumer-groups 6 --read-amplification 1.05 \
    tcp --server-address "$SERVER" \
    output --identifier "inv-${RATE}" --remark "replicated,150B,32p,6 groups" --gitref "$GIT"
done
# Sale start: store_ops.py --rate N --sale 10 --sale-at 300 --sale-for 300 with storefront.py x3;
# record change-to-storefront p99 per second and the stale-availability rate.
