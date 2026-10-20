#!/usr/bin/env bash
# Telemetry hot tier on Apache Iggy with iggy-bench (cargo install iggy-bench).
set -euo pipefail
SERVER=${SERVER:-127.0.0.1:8090}
GIT=${GIT:-$(date +%Y%m%d)}
for MBPS in 200 500 1000 2000; do
  iggy-bench --message-size 500 --messages-per-batch 200 --warmup-time 5m --total-data 2TiB \
    --rate-limit "${MBPS}MB" --durability replicated \
    balanced-producer-and-consumer-group --streams 1 --partitions 64 --producers 16 \
      --consumers 48 --consumer-groups 3 --read-amplification 1.05 \
    tcp --server-address "$SERVER" \
    output --identifier "tel-${MBPS}MBps" --remark "replicated,500B,64p,3 groups" --gitref "$GIT"
done
# Rolling restart: during the 1 GB/s step, restart node 1, 2, 3 in turn at minute 10 (systemctl restart iggy)
# and keep emitter.py + alerter.py running; record seconds with produce errors or no consumption.
