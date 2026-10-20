#!/usr/bin/env bash
# Game telemetry on Apache Iggy with iggy-bench (cargo install iggy-bench).
set -euo pipefail
SERVER=${SERVER:-127.0.0.1:8090}
GIT=${GIT:-$(date +%Y%m%d)}
for RATE in 100000 500000 1000000 2000000; do
  MB=$(( RATE * 200 / 1000000 ))MB
  iggy-bench --message-size 200 --messages-per-batch 100 --warmup-time 5m --total-data 400GiB \
    --rate-limit "$MB" --durability replicated \
    balanced-producer-and-consumer-group --streams 1 --partitions 64 --producers 16 \
      --consumers 32 --consumer-groups 4 --read-amplification 1.05 \
    tcp --server-address "$SERVER" \
    output --identifier "game-${RATE}" --remark "replicated,200B,64p,4 groups" --gitref "$GIT"
done
# Kit run: game_server.py x N (one per core) with leaderboard.py and economy_watch.py;
# record tick overruns, handoff p99 and drops, and kill-to-board p99.
