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

# Run pytest inside dev container
echo "Running pytest inside ${DEV_NAME}..."
if [ $# -eq 0 ]; then
  docker exec -u root -i ${DEV_NAME} pytest -q
else
  docker exec -u root -i ${DEV_NAME} pytest -q -- "$@"
fi
