#!/usr/bin/env bash
# Engagement events on Apache Iggy with iggy-bench (cargo install iggy-bench).
set -euo pipefail
SERVER=${SERVER:-127.0.0.1:8090}
GIT=${GIT:-$(date +%Y%m%d)}
for RATE in 100000 500000 1000000 2000000; do
  MB=$(( RATE * 300 / 1000000 ))MB
  iggy-bench --message-size 300 --messages-per-batch 64 --warmup-time 5m --total-data 400GiB \
    --rate-limit "$MB" --durability replicated \
    balanced-producer-and-consumer-group --streams 1 --partitions 64 --producers 8 \
      --consumers 40 --consumer-groups 5 --read-amplification 1.05 \
    tcp --server-address "$SERVER" \
    output --identifier "eng-${RATE}" --remark "replicated,300B,64p,5 groups" --gitref "$GIT"
done
# Campaign blast: app_events.py --rate N --blast 10 --blast-at 300 --blast-for 600 with trigger_engine.py and sender.py;
# record event-to-trigger p99 and reminder lateness per second.
