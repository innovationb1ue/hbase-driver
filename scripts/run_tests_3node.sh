#!/usr/bin/env bash
set -euo pipefail

# Test runner for the custom 3-node HBase cluster.
#
# Cluster topology:
#   hbase-zk      – standalone ZooKeeper 3.8.4    (custom image)
#   hbase-master  – HBase 2.6.1 Master             (custom image)
#   hbase-rs1/2/3 – HBase 2.6.1 RegionServers      (custom image)
#   hbase_dev     – Python test runner              (dev image)
#
# Usage:
#   ./scripts/run_tests_3node.sh                          # build + start + run all tests
#   ./scripts/run_tests_3node.sh test/test_scan.py        # run a single test file
#   ./scripts/run_tests_3node.sh test/test_scan.py::test_scan  # run a single test
#   ./scripts/run_tests_3node.sh -v                       # all tests, verbose
#   ./scripts/run_tests_3node.sh -k scan                  # filter tests by keyword
#   ./scripts/run_tests_3node.sh --no-build               # skip docker build step
#   ./scripts/run_tests_3node.sh --no-start               # use already-running cluster
#   ./scripts/run_tests_3node.sh --down                   # tear down cluster after tests

NO_BUILD=0
NO_START=0
TEAR_DOWN=0
PYTEST_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-build)  NO_BUILD=1;  shift ;;
    --no-start)  NO_START=1;  shift ;;
    --down)      TEAR_DOWN=1; shift ;;
    -h|--help)
      echo "Usage: $0 [options] [pytest-args]"
      echo ""
      echo "Options:"
      echo "  --no-build    Skip docker image build (use cached images)"
      echo "  --no-start    Assume cluster is already running"
      echo "  --down        Tear down cluster (and remove volumes) after tests"
      echo "  -h, --help    Show this help message"
      echo ""
      echo "Examples:"
      echo "  $0                                         # full build + all tests"
      echo "  $0 --no-build                              # start + run (skip build)"
      echo "  $0 --no-start                              # run tests on live cluster"
      echo "  $0 test/test_scan.py                       # single test file"
      echo "  $0 test/test_scan.py::test_scan            # single test case"
      echo "  $0 -v -k scan                              # verbose, filter by keyword"
      echo "  $0 --down                                  # run all tests then tear down"
      exit 0
      ;;
    *)
      PYTEST_ARGS+=("$1")
      shift
      ;;
  esac
done

# ── resolve compose command ──────────────────────────────────────────────────
COMPOSE="docker-compose"
if ! command -v docker-compose >/dev/null 2>&1; then
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
  else
    echo "ERROR: docker-compose or 'docker compose' is required" >&2
    exit 1
  fi
fi

DEV_NAME="hbase_dev"
MASTER_NAME="hbase-master"
RS_NAMES=("hbase-rs1" "hbase-rs2" "hbase-rs3")
ZK_NAME="hbase-zk"

ZK_ADDR="hbase-zk:2181"
MASTER_HOST="hbase-master"

# ── start / verify cluster ───────────────────────────────────────────────────
if [ $NO_START -eq 0 ]; then
  echo "Cleaning up orphan containers..."
  $COMPOSE down --remove-orphans >/dev/null 2>&1 || true

  if [ $NO_BUILD -eq 0 ]; then
    echo "Building images..."
    $COMPOSE build
  fi

  echo "Starting 3-node cluster..."
  $COMPOSE up -d

else
  echo "Using running cluster..."
  MISSING=0
  for name in "${MASTER_NAME}" "${ZK_NAME}" "${DEV_NAME}" "${RS_NAMES[@]}"; do
    if ! docker ps --format '{{.Names}}' | grep -q "^${name}$"; then
      echo "  ERROR: container '${name}' is not running" >&2
      MISSING=1
    fi
  done
  [ $MISSING -eq 1 ] && exit 1
fi

# ── wait for ZooKeeper ───────────────────────────────────────────────────────
echo ""
echo "─── Waiting for ZooKeeper ───────────────────────────────────────────"
ZK_WAIT=60; ZK_W=0
while [ $ZK_W -lt $ZK_WAIT ]; do
  if docker exec "${ZK_NAME}" bash -c "echo ruok | nc localhost 2181 | grep -q imok" 2>/dev/null; then
    echo "ZooKeeper ready"
    break
  fi
  sleep 2; ZK_W=$((ZK_W+2))
done
if [ $ZK_W -ge $ZK_WAIT ]; then
  echo "ERROR: Timed out waiting for ZooKeeper" >&2
  $COMPOSE logs --tail=50 "${ZK_NAME}" || true
  exit 1
fi

# ── wait for HBase Master HTTP ───────────────────────────────────────────────
echo "─── Waiting for HBase Master Web UI (http://localhost:16010) ────────"
MAX_WAIT=180; WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
  if curl -fsS http://localhost:16010 >/dev/null 2>&1; then
    echo "HBase Master web UI up"
    break
  fi
  sleep 3; WAITED=$((WAITED+3))
done
if [ $WAITED -ge $MAX_WAIT ]; then
  echo "ERROR: Timed out waiting for HBase Master web UI" >&2
  $COMPOSE logs --tail=100 "${MASTER_NAME}" || true
  exit 1
fi

# ── wait for ZK znodes ───────────────────────────────────────────────────────
echo "─── Waiting for HBase ZK znodes ─────────────────────────────────────"
ZNODE_WAIT=120; ZNODE_W=0
while [ $ZNODE_W -lt $ZNODE_WAIT ]; do
  if docker exec -u root "${DEV_NAME}" \
      python -c "
from kazoo.client import KazooClient
zk = KazooClient(hosts='hbase-zk:2181')
zk.start(timeout=5)
ok = bool(zk.exists('/hbase/master') and zk.exists('/hbase/meta-region-server'))
zk.stop()
raise SystemExit(0 if ok else 1)
" >/dev/null 2>&1; then
    echo "ZK znodes ready (/hbase/master + /hbase/meta-region-server)"
    break
  fi
  sleep 3; ZNODE_W=$((ZNODE_W+3))
done
if [ $ZNODE_W -ge $ZNODE_WAIT ]; then
  echo "WARNING: ZK znodes timed out after ${ZNODE_WAIT}s, proceeding anyway..." >&2
fi

# ── wait for meta region ─────────────────────────────────────────────────────
echo "─── Waiting for meta region ─────────────────────────────────────────"
META_WAIT=60; META_W=0
while [ $META_W -lt $META_WAIT ]; do
  if docker exec -u root "${DEV_NAME}" \
      python -c "
import os; os.environ['HBASE_ZK'] = 'hbase-zk:2181'
from hbasedriver import zk as zk_mod
zk_mod.locate_master(['hbase-zk:2181'])
" >/dev/null 2>&1; then
    echo "Meta region ready"
    break
  fi
  sleep 2; META_W=$((META_W+2))
done

# ── wait for all 3 RegionServers ─────────────────────────────────────────────
echo "─── Waiting for all 3 RegionServers ─────────────────────────────────"
RS_WAIT=120; RS_W=0; RS_COUNT=0
while [ $RS_W -lt $RS_WAIT ]; do
  RS_COUNT=$(docker exec -u root "${DEV_NAME}" \
    python -c "
from kazoo.client import KazooClient
try:
    zk = KazooClient(hosts='hbase-zk:2181')
    zk.start(timeout=5)
    n = len(zk.get_children('/hbase/rs')) if zk.exists('/hbase/rs') else 0
    zk.stop()
    print(n)
except: print(0)
" 2>/dev/null || echo 0)
  if [ "${RS_COUNT:-0}" -ge 3 ]; then
    echo "All 3 RegionServers registered in ZooKeeper"
    break
  fi
  echo "  RegionServers registered: ${RS_COUNT:-0}/3 ..."
  sleep 3; RS_W=$((RS_W+3))
done
if [ $RS_W -ge $RS_WAIT ]; then
  echo "WARNING: Only ${RS_COUNT:-0}/3 RegionServers ready after ${RS_WAIT}s, proceeding anyway" >&2
fi

# ── wait for master DDL readiness ────────────────────────────────────────────
echo "─── Waiting for master DDL readiness ────────────────────────────────"
DDL_WAIT=180; DDL_W=0
while [ $DDL_W -lt $DDL_WAIT ]; do
  if docker exec -u root "${DEV_NAME}" \
      python -c "
import os; os.environ['HBASE_ZK'] = 'hbase-zk:2181'
from hbasedriver.client import Client
client = Client({'hbase.zookeeper.quorum': 'hbase-zk', 'hbase.zookeeper.property.clientPort': '2181'})
client.get_admin().list_tables()
" >/dev/null 2>&1; then
    echo "Master DDL ready"
    break
  fi
  sleep 3; DDL_W=$((DDL_W+3))
done
if [ $DDL_W -ge $DDL_WAIT ]; then
  echo "WARNING: Master DDL readiness timed out, proceeding anyway..." >&2
fi

# ── print cluster status ─────────────────────────────────────────────────────
echo ""
echo "─── Cluster status ──────────────────────────────────────────────────"
docker ps --format "  {{.Names}}: {{.Status}}" | grep -E "hbase|dev" || true
echo ""

# ── run pytest ───────────────────────────────────────────────────────────────
echo "─── Running tests ───────────────────────────────────────────────────"
EXIT_CODE=0
if [ ${#PYTEST_ARGS[@]} -eq 0 ]; then
  docker exec -u root -i "${DEV_NAME}" \
    env HBASE_ZK="${ZK_ADDR}" HBASE_MASTER="${MASTER_HOST}" \
    pytest test/ || EXIT_CODE=$?
else
  docker exec -u root -i "${DEV_NAME}" \
    env HBASE_ZK="${ZK_ADDR}" HBASE_MASTER="${MASTER_HOST}" \
    pytest "${PYTEST_ARGS[@]}" || EXIT_CODE=$?
fi

# ── optional teardown ────────────────────────────────────────────────────────
if [ $TEAR_DOWN -eq 1 ]; then
  echo ""
  echo "─── Tearing down cluster ────────────────────────────────────────────"
  $COMPOSE down -v
  echo "Cluster removed."
fi

exit $EXIT_CODE
