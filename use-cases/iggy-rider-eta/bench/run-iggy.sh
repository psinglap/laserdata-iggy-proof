#!/usr/bin/env bash
# Courier pings on Apache Iggy with iggy-bench (cargo install iggy-bench).
set -euo pipefail
SERVER=${SERVER:-127.0.0.1:8090}
QUIC=${QUIC:-127.0.0.1:8080}
GIT=${GIT:-$(date +%Y%m%d)}
for RATE in 40000 200000 500000 1000000; do
  MB=$(( RATE * 120 / 1000000 ))MB
  iggy-bench --message-size 120 --messages-per-batch 16 --warmup-time 5m --total-data 100GiB \
    --rate-limit "$MB" --durability replicated \
    balanced-producer-and-consumer-group --streams 1 --partitions 32 --producers 8 \
      --consumers 16 --consumer-groups 4 --read-amplification 1.05 \
    tcp --server-address "$SERVER" \
    output --identifier "geo-${RATE}" --remark "replicated,120B,32p,4 groups" --gitref "$GIT"
done
# Lossy network: same 200K/s step over QUIC and TCP, run with netem loss on the load generator.
for T in tcp quic; do
  ADDR=$([ "$T" = quic ] && echo "$QUIC" || echo "$SERVER")
  iggy-bench --message-size 120 --messages-per-batch 16 --warmup-time 2m --total-data 20GiB \
    --rate-limit 24MB --durability replicated \
    balanced-producer --streams 1 --partitions 32 --producers 8 \
    "$T" --server-address "$ADDR" \
    output --identifier "geo-loss5-${T}" --remark "netem loss 5%" --gitref "$GIT"
done
