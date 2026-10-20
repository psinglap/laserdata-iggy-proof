#!/usr/bin/env bash
# Footprint run: idle, then 10K, 100K and 1M msg/s, five minutes each.
# Usage: IGGY_CPUS=2 IGGY_MEM=2g ./run.sh   (run loadgen from another machine for the 1M step)
set -euo pipefail
URL=${IGGY_URL:-iggy+tcp://iggy:iggy@127.0.0.1:8090}
OUT=${OUT:-footprint-${IGGY_CPUS:-2}cpu.csv}
SECS=${SECS:-300}

docker compose up -d
until curl -fs http://127.0.0.1:3000/ping >/dev/null; do sleep 1; done

python sample.py --url "$URL" --phase idle --seconds 120 --out "$OUT"
for rate in 10000 100000 1000000; do
  python sample.py --url "$URL" --phase "${rate}" --seconds "$SECS" --out "$OUT" &
  python loadgen.py --url "$URL" --rate "$rate" --seconds "$SECS" --stream "fp-${rate}"
  wait
done
python summarize.py "$OUT" | tee "${OUT%.csv}.md"
docker compose down -v
