#!/usr/bin/env bash
# Usage metering on Apache Iggy with iggy-bench (cargo install iggy-bench).
set -euo pipefail
SERVER=${SERVER:-127.0.0.1:8090}
GIT=${GIT:-$(date +%Y%m%d)}
for DUR in persisted replicated; do
  for RATE in 100000 500000 1000000 2000000; do
    MB=$(( RATE * 200 / 1000000 ))MB
    iggy-bench --message-size 200 --messages-per-batch 64 --warmup-time 5m --total-data 400GiB \
      --rate-limit "$MB" --durability "$DUR" \
      balanced-producer-and-consumer-group --streams 1 --partitions 32 --producers 8 \
        --consumers 24 --consumer-groups 3 --read-amplification 1.05 \
      tcp --server-address "$SERVER" \
      output --identifier "meter-${DUR}-${RATE}" --remark "${DUR},200B,32p,3 groups" --gitref "$GIT"
  done
done
# Kit run: usage_emitter.py --rate N --dup-rate 0.01 --persisted with meter.py; after 24 h run reconcile.py --minutes 1440.
