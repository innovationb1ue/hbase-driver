#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/run_tests_docker.sh [pytest-args]
# Starts docker-compose with HBase (embedded ZK) and runs pytest inside the dev container.

COMPOSE="docker-compose"
if ! command -v docker-compose >/dev/null 2>&1; then
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
  else
    echo "docker-compose or 'docker compose' is required" >&2
    exit 1
  fi
fi

# Start stack (HBase with embedded ZK, dev container)
echo "Building and starting containers..."
$COMPOSE up --build -d

# Wait for HBase master HTTP endpoint to be healthy
echo "Waiting for HBase (http://localhost:16010)..."
MAX_WAIT=120
WAITED=0
SLEEP=2

while [ $WAITED -lt $MAX_WAIT ]; do
  if curl -fsS http://localhost:16010 >/dev/null 2>&1; then
    echo "HBase is up at http://localhost:16010"
    break
  fi
  sleep $SLEEP
  WAITED=$((WAITED+SLEEP))
done

if [ $WAITED -ge $MAX_WAIT ]; then
  echo "Timed out waiting for HBase" >&2
  $COMPOSE logs --tail=100 hbase || true
  exit 1
fi

# Wait for /hbase/master znode via ZK (localhost:2181 from dev container)
echo "Waiting for HBase znodes..."
ZK_WAIT=60
ZK_W=0
DEV_NAME="hbase_dev"

while [ $ZK_W -lt $ZK_WAIT ]; do
  if docker exec -u root ${DEV_NAME} bash -lc "python - <<'PY'
from kazoo.client import KazooClient
import sys
zk = KazooClient(hosts='hbase:2181')
try:
    zk.start(timeout=5)
    master_exists = zk.exists('/hbase/master')
    zk.stop()
    sys.exit(0 if master_exists else 1)
except Exception as e:
    sys.exit(2)
PY" >/dev/null 2>&1; then
    echo "HBase znodes ready"
    break
  fi
  sleep 2
  ZK_W=$((ZK_W+2))
done

# Run pytest
echo "Running pytest..."
TEST_HBASE_ZK="${HBASE_ZK:-hbase:2181}"

if [ $# -eq 0 ]; then
  docker exec -u root -i ${DEV_NAME} env HBASE_ZK="${TEST_HBASE_ZK}" pytest -q
else
  docker exec -u root -i ${DEV_NAME} env HBASE_ZK="${TEST_HBASE_ZK}" pytest -q "$@"
fi
