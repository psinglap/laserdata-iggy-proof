#!/usr/bin/env bash
# LaserData footprint tests: one command, Linux with Docker.
#
#   ./run.sh                      full run, about 55 minutes
#   FOOTPRINT_SCALE=0.1 ./run.sh  quick trial, about 8 minutes
#   PROFILE=sized-2g ./run.sh     server budgets shrunk to fit a 2 GiB node (see stack/sized-2g.yml)
#   TESTS="restart" ./run.sh      one test only (idle, backlog-2, backlog-10, backlog-50, producer, restart)
#
# Starts Laser Stack (LaserData's server and managed backend) with the server capped at
# 2 cores and 2 GiB, runs the three tests with Laser SDK clients, and writes everything
# to results/local (or $FOOTPRINT_OUT): samples.csv, events.jsonl, one JSON per test, env.json.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
export FOOTPRINT_OUT=${FOOTPRINT_OUT:-$HERE/results/local}
export FOOTPRINT_SCALE=${FOOTPRINT_SCALE:-1}
# Which tests to run; the GitHub workflow runs each on its own machine in parallel.
TESTS=${TESTS:-idle backlog-2 backlog-10 backlog-50 producer restart}
export SERVER_CPUS=${SERVER_CPUS:-2,3} CLIENT_CPUS=${CLIENT_CPUS:-0,1} SERVER_MEMORY=${SERVER_MEMORY:-2g}
STACK=${LASER_STACK_DIR:-$HERE/.laser-stack}
STACK_SHA=46dd0168675b54ce3d9025dace26574603220d10  # laserdata/laser-stack, 2026-11-10
PY=${PYTHON:-python3}
OUT=$FOOTPRINT_OUT

rm -rf "$OUT" && mkdir -p "$OUT"
if [ -n "${REMOTE_SERVER:-}" ]; then
  # The server runs on another machine (gcp-vm/ starts it there). The tests reach its Docker over SSH.
  export LASER_CONNECTION_STRING=${LASER_CONNECTION_STRING:-iggy:laser@$REMOTE_SERVER:8090}
  export FOOTPRINT_PING=${FOOTPRINT_PING:-http://$REMOTE_SERVER:3000/ping}
  export FOOTPRINT_DOCKER="ssh -o StrictHostKeyChecking=no -o BatchMode=yes $REMOTE_SERVER sudo docker"
  D() { $FOOTPRINT_DOCKER $(printf '%q ' "$@"); }  # quote each argument for the remote shell
  until curl -fs "$FOOTPRINT_PING" >/dev/null; do sleep 1; done
  SERVER=$(D ps -qf name=laser-stack-iggy-1); PLANE=$(D ps -qf name=laser-stack-plane-1)
  PROFILE=${PROFILE:-shipped}
  STACK_SHA=$(ssh -o BatchMode=yes "$REMOTE_SERVER" cat /opt/fp/stack-sha 2>/dev/null || echo "$STACK_SHA")
  HOST_INFO="$(ssh -o BatchMode=yes "$REMOTE_SERVER" "lscpu | sed -n 's/^Model name:\s*//p' | head -1; nproc; free -m | awk '/Mem:/{print \$2}'; uname -r")"
else
  [ -d "$STACK/.git" ] || git clone -q https://github.com/laserdata/laser-stack "$STACK"
  git -C "$STACK" fetch -q origin "$STACK_SHA" 2>/dev/null; git -C "$STACK" checkout -q "$STACK_SHA"
  [ -f "$STACK/.env" ] || cp "$STACK/.env.example" "$STACK/.env"
  # Laser Stack's own start script, so the server and managed backend come up exactly as
  # LaserData ships them; it waits until both report healthy. Our only addition is limits.yml.
  export COMPOSE_FILE="$STACK/compose.yaml:$HERE/stack/limits.yml"
  # Server profile: "shipped" (Laser Stack as is) or "sized-2g" (budgets shrunk to fit 2 GiB).
  PROFILE=${PROFILE:-shipped}
  [ "$PROFILE" = "shipped" ] || COMPOSE_FILE="$COMPOSE_FILE:$HERE/stack/$PROFILE.yml"
  if [ -n "${FOOTPRINT_DATA_DIR:-}" ]; then
    mkdir -p "$FOOTPRINT_DATA_DIR/iggy" "$FOOTPRINT_DATA_DIR/plane"
    COMPOSE_FILE="$COMPOSE_FILE:$HERE/stack/data-disk.yml"
  fi
  compose() { docker compose --project-directory "$STACK" --env-file "$STACK/.env" "$@"; }
  CI=true "$STACK/scripts/up" || { compose logs --no-color --tail 200; exit 1; }
  until curl -fs http://127.0.0.1:3000/ping >/dev/null; do sleep 1; done
  SERVER=$(compose ps -q iggy); PLANE=$(compose ps -q plane)
  D() { docker "$@"; }
  HOST_INFO="$(lscpu | sed -n 's/^Model name:\s*//p' | head -1; nproc; free -m | awk '/Mem:/{print $2}'; uname -r)"
fi
export FOOTPRINT_SERVER=$SERVER

# What ran, recorded with the results.
{
  echo "{"
  echo "  \"server_image\": \"$(D inspect -f '{{.Image}}' "$SERVER")\","
  echo "  \"server_image_digest\": \"$(D image inspect -f '{{index .RepoDigests 0}}' "$(D inspect -f '{{.Config.Image}}' "$SERVER")")\","
  echo "  \"server_version\": \"$(D exec "$SERVER" sh -c 'iggy-server --version 2>/dev/null || true' | head -1 | tr -d '"')\","
  echo "  \"plane_image_digest\": \"$(D image inspect -f '{{index .RepoDigests 0}}' "$(D inspect -f '{{.Config.Image}}' "$PLANE")")\","
  echo "  \"laser_stack_commit\": \"$STACK_SHA\","
  echo "  \"laser_sdk\": \"$($PY -c 'import importlib.metadata as m; print(m.version("laser-sdk"))')\","
  echo "  \"python\": \"$($PY -c 'import platform; print(platform.python_version())')\","
  echo "  \"server_cpus\": \"$SERVER_CPUS\", \"server_memory_limit\": \"$SERVER_MEMORY\", \"client_cpus\": \"$CLIENT_CPUS\","
  { read -r HCPU; read -r HCORES; read -r HMEM; read -r HKERNEL; } <<< "$HOST_INFO"
  echo "  \"host_cpu\": \"$HCPU\", \"host_cores\": $HCORES, \"host_memory_mib\": $HMEM, \"kernel\": \"$HKERNEL\","
  echo "  \"client_host\": \"${REMOTE_SERVER:+separate VM, $(nproc) vCPU}\", \"scale\": $FOOTPRINT_SCALE,"
  echo "  \"tests\": \"$TESTS\", \"profile\": \"$PROFILE\","
  echo "  \"started\": \"$(date -u +%FT%TZ)\""
  echo "}"
} > "$OUT/env.json"
cat "$OUT/env.json"

if [ -n "${REMOTE_SERVER:-}" ]; then  # the server VM samples itself
  $PY "$HERE/footprint/sampler.py" --out "$OUT" &
else
  $PY "$HERE/footprint/sampler.py" --out "$OUT" --server "$SERVER" --plane "$PLANE" &
fi
SAMPLER=$!
run() { taskset -c "$CLIENT_CPUS" "$PY" "$HERE/footprint/$@"; }
phase() { printf '%s\t%s' "$1" "$2" > "$OUT/phase"
  printf '{"t": %s, "test": "%s", "event": "phase", "phase": "%s"}\n' "$(date +%s.%N)" "$1" "$2" >> "$OUT/events.jsonl"; }
STATUS=()
check() { if [ "$2" -ne 0 ]; then STATUS+=("\"$1\": \"failed\""); else STATUS+=("\"$1\": \"passed\""); fi; }
SCALE_SECS() { $PY -c "print(max(1,int($1*$FOOTPRINT_SCALE)))"; }

want() { case " $TESTS " in *" $1 "*) return 0;; *) return 1;; esac; }

if want idle; then
  echo "== idle"
  phase idle idle; sleep "$(SCALE_SECS 120)"
fi

for p in 2 10 50; do
  want "backlog-$p" || continue
  echo "== test 1: consumer backlog, $p partitions"
  run consumer_backlog.py preload --partitions "$p"
  phase consumer_backlog "settle-${p}p"; sleep 10
  run consumer_backlog.py consume --partitions "$p"; check "consumer_backlog_${p}p" $?
  run consumer_backlog.py cleanup --partitions "$p"
done

if want producer; then
  echo "== test 2: producer with server paused"
  run producer_pause.py consume & C=$!
  sleep 3
  run producer_pause.py produce; check producer_pause_producer $?
  wait $C; check producer_pause_consumer $?
  run producer_pause.py cleanup
fi

if want restart; then
  echo "== test 3: restart"
  run restart.py preload
  run restart.py consume & C=$!
  run restart.py produce & P=$!
  sleep 3
  run restart.py orchestrate; check restart_orchestrator $?
  wait $P; check restart_producer $?
  wait $C; check restart_consumer $?
  run restart.py cleanup
fi

phase done done
touch "$OUT/stop"; wait $SAMPLER
D inspect -f '{"oom_killed": {{.State.OOMKilled}}, "restart_count": {{.RestartCount}}}' "$SERVER" > "$OUT/server-state.json"
[ ${#STATUS[@]} -eq 0 ] && STATUS+=("\"idle\": \"passed\"")
D logs "$SERVER" > "$OUT/server.log" 2>&1
D logs "$PLANE" > "$OUT/backend.log" 2>&1
(IFS=,; echo "{${STATUS[*]}}") > "$OUT/status.json"
cat "$OUT/status.json" "$OUT/server-state.json"
[ -n "${REMOTE_SERVER:-}" ] || compose down -v
grep -q failed "$OUT/status.json" && exit 1 || exit 0
