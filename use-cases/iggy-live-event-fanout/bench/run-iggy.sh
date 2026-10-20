#!/usr/bin/env bash
# Live events on Apache Iggy with iggy-bench (cargo install iggy-bench).
set -euo pipefail
SERVER=${SERVER:-127.0.0.1:8090}
GIT=${GIT:-$(date +%Y%m%d)}
# 1. Fan-out: 10K events/s (200 B = 2MB/s) read by 10..200 gateways (consumer groups).
for G in 10 50 100 200; do
  iggy-bench --message-size 200 --messages-per-batch 8 --warmup-time 5m --total-data 20GiB \
    --rate-limit 2MB --durability replicated \
    balanced-producer-and-consumer-group --streams 1 --partitions 8 --producers 2 \
      --consumers "$G" --consumer-groups "$G" --read-amplification 1.05 \
    tcp --server-address "$SERVER" \
    output --identifier "live-fanout-${G}gw" --remark "replicated,200B,8p,${G} groups" --gitref "$GIT"
done
# 2. Reaction ingest: 100K..2M taps/s at 80 B, one aggregator group.
for RATE in 100000 500000 1000000 2000000; do
  MB=$(( RATE * 80 / 1000000 ))MB
  iggy-bench --message-size 80 --messages-per-batch 64 --warmup-time 5m --total-data 100GiB \
    --rate-limit "$MB" --durability replicated \
    balanced-producer-and-consumer-group --streams 1 --partitions 16 --producers 8 \
      --consumers 8 --consumer-groups 1 \
    tcp --server-address "$SERVER" \
    output --identifier "live-reactions-${RATE}" --remark "replicated,80B,16p" --gitref "$GIT"
done
# 3. The big moment: run match_feed.py --rate 10000 --goal-at 300 with gateway.py x N and viewers.py,
#    and record the per-second p99 printed by viewers.py through the burst.
