#!/usr/bin/env bash
# Startup script for both GCP VMs: Docker, Python, the test code. The server VM also gets
# its SSD data disk (a normal mount, with write barriers) at /data.
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -y -q docker.io docker-compose-v2 python3-venv git curl
mkdir -p /opt/fp && cd /opt/fp
if [ -e /dev/disk/by-id/google-data ]; then
  mkfs.ext4 -q -F /dev/disk/by-id/google-data
  mkdir -p /data && mount /dev/disk/by-id/google-data /data
  mkdir -p /data/iggy /data/plane && chmod -R 777 /data
fi
# Which repository, commit and folder to use: run-gcp.sh passes the ones it was started from,
# so this works from LaserData's repository, a fork or a copy without edits.
md() { curl -fs -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"; }
REPO=$(md fp-repo); REF=$(md fp-ref || echo main); DIR=$(md fp-dir || echo .)
git clone -q "$REPO" repo && git -C repo checkout -q "$REF"
ln -sfn "/opt/fp/repo/$DIR" /opt/fp/kit
python3 -m venv venv && venv/bin/pip install -q -r kit/requirements.txt
chmod -R a+rwX /opt/fp
touch /opt/fp/ready
