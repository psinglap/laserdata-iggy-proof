#!/usr/bin/env bash
# Fraud feature streams on Apache Iggy with iggy-bench (cargo install iggy-bench).
set -euo pipefail
SERVER=${SERVER:-127.0.0.1:8090}
GIT=${GIT:-$(date +%Y%m%d)}
for RATE in 10000 50000 100000 250000; do
  MB=$(( RATE * 500 / 1000000 ))MB
  iggy-bench --message-size 500 --messages-per-batch 1 --warmup-time 5m --total-data 200GiB \
    --rate-limit "$MB" --durability replicated \
    balanced-producer-and-consumer-group --streams 1 --partitions 32 --producers 8 \
      --consumers 32 --consumer-groups 4 --read-amplification 1.05 \
    tcp --server-address "$SERVER" \
    output --identifier "fraud-${RATE}" --remark "replicated,500B,32p,4 groups,batch 1" --gitref "$GIT"
done
iggy-bench --message-size 500 --messages-per-batch 1 --warmup-time 5m --total-data 100GiB \
  --rate-limit 50MB --durability persisted \
  balanced-producer-and-consumer-group --streams 1 --partitions 32 --producers 8 \
    --consumers 32 --consumer-groups 4 --read-amplification 1.05 \
  tcp --server-address "$SERVER" \
  output --identifier fraud-100000-persisted --remark "persisted,500B,32p,4 groups" --gitref "$GIT"
# Kit run: payments.py --tps N --attack-at 300 with features.py and scorer.py; record attack approvals.
