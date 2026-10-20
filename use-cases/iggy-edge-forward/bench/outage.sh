#!/usr/bin/env bash
# Takes the cloud away for N seconds while sensors keep writing at the edge, then brings it back.
# Watch forwarder.py switch to "catching up" and cloud_check.py report 0 gaps once it is live again.
set -euo pipefail
SECS=${1:-600}
docker pause iggy-cloud
echo "cloud unreachable for ${SECS}s"
sleep "$SECS"
docker unpause iggy-cloud
echo "cloud back"
