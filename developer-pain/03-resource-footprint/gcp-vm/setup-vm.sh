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
REF=$(curl -fs -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/fp-ref || echo main)
git clone -q https://github.com/psinglap/laserdata-iggy-proof repo && git -C repo checkout -q "$REF"
python3 -m venv venv && venv/bin/pip install -q -r repo/developer-pain/03-resource-footprint/requirements.txt
chmod -R a+rwX /opt/fp
touch /opt/fp/ready
