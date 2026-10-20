#!/usr/bin/env bash
# Samples memory and CPU of the edge container every 5 s while the kit runs. Repeat per rate step.
# Usage: ./footprint.sh iggy-edge 600 > footprint-iggy-10k.csv
set -euo pipefail
NAME=${1:-iggy-edge}
SECS=${2:-600}
echo "time,cpu_percent,mem_usage"
for _ in $(seq 1 $(( SECS / 5 ))); do
  docker stats --no-stream --format "{{.CPUPerc}},{{.MemUsage}}" "$NAME" | sed "s/^/$(date +%s),/"
  sleep 5
done
