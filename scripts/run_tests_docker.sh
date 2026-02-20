#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/run_tests_docker.sh [options] [pytest-args]
# 
# Starts docker-compose with HBase (embedded ZK) and runs pytest inside the dev container.
#
# Examples:
#   ./scripts/run_tests_docker.sh                                    # Run all tests
#   ./scripts/run_tests_docker.sh test/test_zk.py                    # Run single test file
#   ./scripts/run_tests_docker.sh test/test_zk.py::test_locate_meta  # Run single test
#   ./scripts/run_tests_docker.sh -v                                 # Run all tests verbose
#   ./scripts/run_tests_docker.sh --no-build                         # Skip docker build
#   ./scripts/run_tests_docker.sh --no-start                         # Use running containers

NO_BUILD=0
NO_START=0
PYTEST_ARGS=()

# Parse special flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-build)
      NO_BUILD=1
      shift
      ;;
    --no-start)
      NO_START=1
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [options] [pytest-args]"
      echo "Options:"
      echo "  --no-build    Skip docker image build"
      echo "  --no-start    Use running containers (don't start)"
      echo "  --help        Show this help message"
      echo ""
      echo "Examples:"
      echo "  $0                                    # Run all tests"
      echo "  $0 test/test_zk.py::test_locate_meta # Run single test"
      echo "  $0 -v                                 # Run all tests verbose"
      exit 0
      ;;
    *)
      PYTEST_ARGS+=("$1")
      shift
      ;;
  esac
done

COMPOSE="docker-compose"
if ! command -v docker-compose >/dev/null 2>&1; then
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
  else
    echo "docker-compose or 'docker compose' is required" >&2
    exit 1
  fi
fi

DEV_NAME="hbase_dev"
HBASE_NAME="hbase"

# Start or verify containers
if [ $NO_START -eq 0 ]; then
  if [ $NO_BUILD -eq 0 ]; then
    echo "Building and starting containers..."
    $COMPOSE up --build -d
  else
    echo "Starting containers (skip build)..."
    $COMPOSE up -d
  fi
else
  echo "Using running containers..."
  if ! docker ps --format '{{.Names}}' | grep -q "^${HBASE_NAME}$"; then
    echo "Error: HBase container '${HBASE_NAME}' is not running" >&2
    exit 1
  fi
  if ! docker ps --format '{{.Names}}' | grep -q "^${DEV_NAME}$"; then
    echo "Error: Dev container '${DEV_NAME}' is not running" >&2
    exit 1
  fi
fi

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

# Wait for /hbase/master znode via ZK
echo "Waiting for HBase znodes..."
ZK_WAIT=60
ZK_W=0

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

# Run pytest with user-provided arguments
echo ""
echo "Running pytest..."
TEST_HBASE_ZK="${HBASE_ZK:-hbase:2181}"

# If no arguments provided, run all tests with moderate verbosity
if [ ${#PYTEST_ARGS[@]} -eq 0 ]; then
  docker exec -u root -i ${DEV_NAME} env HBASE_ZK="${TEST_HBASE_ZK}" pytest test/
else
  # Run with user-provided arguments
  docker exec -u root -i ${DEV_NAME} env HBASE_ZK="${TEST_HBASE_ZK}" pytest "${PYTEST_ARGS[@]}"
fi
