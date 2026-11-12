#!/usr/bin/env bash
# Runs on the server VM: starts Laser Stack (LaserData's server and managed backend) with the
# same limits as the GitHub run, reachable from the client VM, and samples it once a second.
set -euo pipefail
PROFILE=${1:-sized-2g}
HERE=/opt/fp/repo/developer-pain/03-resource-footprint
STACK=/opt/fp/laser-stack
STACK_SHA=$(sed -n 's/^STACK_SHA=\([0-9a-f]*\).*/\1/p' "$HERE/run.sh")
[ -d "$STACK/.git" ] || git clone -q https://github.com/laserdata/laser-stack "$STACK"
git -C "$STACK" checkout -q "$STACK_SHA"
echo "$STACK_SHA" > /opt/fp/stack-sha
cp "$STACK/.env.example" "$STACK/.env"
sed -i 's/^LASER_BIND_ADDRESS=.*/LASER_BIND_ADDRESS=0.0.0.0/' "$STACK/.env"  # reachable from the client VM (internal network only)
export SERVER_CPUS=2,3 CLIENT_CPUS=0,1 SERVER_MEMORY=2g FOOTPRINT_DATA_DIR=/data
export COMPOSE_FILE="$STACK/compose.yaml:$HERE/stack/limits.yml:$HERE/stack/data-disk.yml"
[ "$PROFILE" = "shipped" ] || COMPOSE_FILE="$COMPOSE_FILE:$HERE/stack/$PROFILE.yml"
sudo -E env "PATH=$PATH" CI=true "$STACK/scripts/up"
mkdir -p /opt/fp/server-out
SERVER=$(sudo docker ps -qf name=laser-stack-iggy-1); PLANE=$(sudo docker ps -qf name=laser-stack-plane-1)
sudo nohup /opt/fp/venv/bin/python "$HERE/footprint/sampler.py" --out /opt/fp/server-out \
  --server "$SERVER" --plane "$PLANE" > /opt/fp/server-out/sampler.log 2>&1 < /dev/null &
echo "server up: $SERVER"
