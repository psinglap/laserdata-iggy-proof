#!/usr/bin/env bash
# Agent bus on Apache Iggy with iggy-bench (cargo install iggy-bench).
set -euo pipefail
SERVER=${SERVER:-127.0.0.1:8090}
GIT=${GIT:-$(date +%Y%m%d)}
for RATE in 25000 100000 250000 500000; do
  MB=$(( RATE * 1000 / 1000000 ))MB
  iggy-bench --message-size 1000 --messages-per-batch 4 --warmup-time 5m --total-data 400GiB \
    --rate-limit "$MB" --durability replicated \
    balanced-producer-and-consumer-group --streams 1 --partitions 64 --producers 8 \
      --consumers 32 --consumer-groups 4 --read-amplification 1.05 \
    tcp --server-address "$SERVER" \
    output --identifier "agent-${RATE}" --remark "replicated,1KB,64p,4 groups" --gitref "$GIT"
done
# Kit run: caller.py --calls N with agent.py x4 and tools.py x4; record turn p99 and orchestration overhead p99.
