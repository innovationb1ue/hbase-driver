#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/run_tests_docker.sh [pytest-args]
# Starts docker-compose, waits for HBase master (http://localhost:16010) and runs pytest inside the dev container.
# Runs pytest inside the 'hbase_dev' container created by docker-compose.

COMPOSE="docker-compose"
if ! command -v docker-compose >/dev/null 2>&1; then
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
  else
    echo "docker-compose or 'docker compose' is required" >&2
    exit 1
  fi
fi

# Start stack
$COMPOSE up --build -d

# Wait for HBase master HTTP endpoint
echo "Waiting for HBase master (http://localhost:16010) up to 240s..."
MAX_WAIT=240
WAITED=0
SLEEP=2

# Get container id for hbase service (if available)
HBASE_CID=$($COMPOSE ps -q hbase 2>/dev/null || true)
if [ -z "$HBASE_CID" ]; then
  # fallback to container name if compose ps didn't return an id
  HBASE_CID=$(docker ps --filter "name=hbase_server" --format '{{.ID}}' || true)
fi

while [ $WAITED -lt $MAX_WAIT ]; do
  # 1) try host localhost
  if curl -fsS http://localhost:16010 >/dev/null 2>&1; then
    echo "HBase master is up at http://localhost:16010"
    break
  fi

  # 2) try mapped host port if container is present
  if [ -n "$HBASE_CID" ]; then
    HOST_PORT=$(docker port "$HBASE_CID" 16010/tcp 2>/dev/null | sed -n 's/.*:\([0-9]*\)$/\1/p' || true)
    if [ -n "$HOST_PORT" ]; then
      if curl -fsS "http://localhost:$HOST_PORT" >/dev/null 2>&1; then
        echo "HBase master is up at http://localhost:$HOST_PORT (mapped port)"
        break
      fi
    fi

    # 3) try container internal IP
    CONTAINER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$HBASE_CID" 2>/dev/null || true)
    if [ -n "$CONTAINER_IP" ]; then
      if curl -fsS "http://$CONTAINER_IP:16010" >/dev/null 2>&1; then
        echo "HBase master is up at http://$CONTAINER_IP:16010 (container IP)"
        break
      fi
    fi
  fi

  # 4) detect master abort via master logs (fail fast if master died)
  if [ -n "$HBASE_CID" ]; then
    if docker exec "$HBASE_CID" bash -lc 'grep -q -E "Master server abort|ABORTING master|SessionExpiredException|KeeperException" /hbase/logs/hbase--master-*.log 2>/dev/null' >/dev/null 2>&1; then
      echo "HBase Master appears to have aborted; printing recent master logs:" >&2
      docker exec "$HBASE_CID" bash -lc 'tail -n 200 /hbase/logs/hbase--master-*.log 2>/dev/null || true' >&2 || true
      exit 1
    fi
  fi

  sleep $SLEEP
  WAITED=$((WAITED+SLEEP))
done

if [ $WAITED -ge $MAX_WAIT ]; then
  echo "Timed out waiting for HBase master on 16010" >&2
  echo "Compose status:" >&2
  $COMPOSE ps || true
  $COMPOSE logs --tail=200 hbase || true
  exit 1
fi

# Ensure dev container is running
DEV_NAME="hbase_dev"
if ! docker ps --format '{{.Names}}' | grep -q "^${DEV_NAME}$"; then
  echo "Starting dev container..."
  $COMPOSE up -d dev
fi

# After HBase HTTP is reachable, wait for the ZooKeeper znode /hbase/master to appear (so tests can locate master)
# Use the dev container's python + kazoo to query zk via the docker network host 'zookeeper' or overridden HBASE_ZK
echo "Waiting for /hbase/master znode up to 120s..."
ZK_WAIT=120
ZK_W=0
ZK_HOST="${HBASE_ZK:-zookeeper:2181}"
while [ $ZK_W -lt $ZK_WAIT ]; do
  if docker exec -u root ${DEV_NAME} bash -lc "python - <<'PY'
from kazoo.client import KazooClient
import sys
hosts='${ZK_HOST}'
zk=KazooClient(hosts=hosts)
try:
    zk.start(timeout=5)
    if zk.exists('/hbase/master'):
        print('znode exists')
        zk.stop()
        sys.exit(0)
    else:
        zk.stop()
        sys.exit(2)
except Exception as e:
    print('zk-check-exception', e)
    sys.exit(3)
PY" >/dev/null 2>&1; then
    echo "/hbase/master znode present"
    break
  fi
  sleep 2
  ZK_W=$((ZK_W+2))
done
if [ $ZK_W -ge $ZK_WAIT ]; then
  echo "Timed out waiting for /hbase/master znode" >&2
  echo "Compose status:" >&2
  $COMPOSE ps || true
  $COMPOSE logs --tail=200 hbase || true
  exit 1
fi

# Run pytest inside dev container
echo "Running pytest inside ${DEV_NAME}..."

# Ensure pytest is available inside the dev container; if not, rebuild dev image and recreate container
if docker exec -u root ${DEV_NAME} bash -lc 'command -v pytest >/dev/null 2>&1' >/dev/null 2>&1; then
  echo "pytest found in ${DEV_NAME}"
else
  echo "pytest not found in ${DEV_NAME}, rebuilding dev image and recreating container..."
  # try to build without cache first, fallback to normal build
  $COMPOSE build --no-cache dev || $COMPOSE build dev || true
  # force recreate to ensure new image is used
  $COMPOSE up -d --force-recreate dev || $COMPOSE up -d dev || true
  sleep 2
  if ! docker exec -u root ${DEV_NAME} bash -lc 'command -v pytest >/dev/null 2>&1' >/dev/null 2>&1; then
    echo "pytest still missing in ${DEV_NAME} after rebuild; printing container info and exiting." >&2
    docker exec -u root ${DEV_NAME} bash -lc 'echo PATH=$PATH; ls -l /usr/local/bin | sed -n "1,200p" || true' >&2 || true
    exit 1
  fi
fi

# Determine HBASE_ZK to use inside container (use host env if set, otherwise zookeeper:2181)
TEST_HBASE_ZK="${HBASE_ZK:-zookeeper:2181}"
if [ $# -eq 0 ]; then
  docker exec -u root -i ${DEV_NAME} env HBASE_ZK="${TEST_HBASE_ZK}" pytest -q
else
  docker exec -u root -i ${DEV_NAME} env HBASE_ZK="${TEST_HBASE_ZK}" pytest -q -- "$@"
fi
