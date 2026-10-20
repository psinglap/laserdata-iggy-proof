#!/usr/bin/env bash
# RTB money-loop events on Apache Iggy with iggy-bench (cargo install iggy-bench).
set -euo pipefail
SERVER=${SERVER:-127.0.0.1:8090}
GIT=${GIT:-$(date +%Y%m%d)}
for RATE in 250000 1000000 2000000 4000000; do
  MB=$(( RATE * 300 / 1000000 ))MB
  iggy-bench --message-size 300 --messages-per-batch 32 --warmup-time 5m --total-data 400GiB \
    --rate-limit "$MB" --durability replicated \
    balanced-producer-and-consumer-group --streams 1 --partitions 32 --producers 8 \
      --consumers 32 --consumer-groups 8 --read-amplification 1.05 \
    tcp --server-address "$SERVER" \
    output --identifier "rtb-${RATE}" --remark "replicated,300B,32p,8 groups" --gitref "$GIT"
done
# Persisted variant at 1M/s.
iggy-bench --message-size 300 --messages-per-batch 32 --warmup-time 5m --total-data 200GiB \
  --rate-limit 300MB --durability persisted \
  balanced-producer-and-consumer-group --streams 1 --partitions 32 --producers 8 \
    --consumers 32 --consumer-groups 8 --read-amplification 1.05 \
  tcp --server-address "$SERVER" \
  output --identifier rtb-1000000-persisted --remark "persisted,300B,32p,8 groups" --gitref "$GIT"
# Kit-level run: exchange.py --rate N with bidder.py x4 and pacing.py; record timeouts, overspend, cap violations.
