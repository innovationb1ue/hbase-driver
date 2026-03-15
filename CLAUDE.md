# Python HBase Driver - Development Guide

## Project Overview

A Python driver for Apache HBase 2.x using protobuf-based RPC communication. Supports table operations, data mutations, scanning, and admin operations.

## Project Structure

```
src/hbasedriver/
├── client/
│   ├── client.py      # Main Client class
│   └── admin.py       # Admin operations (tables, snapshots, cluster)
├── common/            # Shared utilities (TableName, etc.)
├── Connection.py      # RPC connection handling
├── master.py          # MasterService protobuf calls
├── region_client.py   # RegionServer client
├── response.py        # Response type mappings
└── zk.py              # ZooKeeper discovery

test/
├── test_admin_column.py    # Column family operations
├── test_admin_cluster.py   # Cluster status/balancer
├── test_admin_region.py    # Region management
├── test_admin_snapshot.py  # Snapshot operations
└── ...
```

## Running Tests

**Always run tests inside the dev container**, not locally.

**IMPORTANT: After implementing any new function, always run the related tests to verify the implementation works correctly.**

### Why?
The HBase cluster runs in Docker with internal hostnames (e.g., `hbase-rs1`, `hbase-rs2`). ZooKeeper returns these internal hostnames when discovering region servers. The dev container (`hbase_dev`) is on the same Docker network and can resolve these names, but your local machine cannot.

### Quick Commands

```bash
# Start the cluster (if not running)
docker compose up -d

# Run all tests
docker exec hbase_dev bash -c "cd /workspace && HBASE_ZK=hbase-zk:2181 pytest test/ -v"

# Run specific test file
docker exec hbase_dev bash -c "cd /workspace && HBASE_ZK=hbase-zk:2181 pytest test/test_admin_column.py -v"

# Run specific test class/method
docker exec hbase_dev bash -c "cd /workspace && HBASE_ZK=hbase-zk:2181 pytest test/test_admin_snapshot.py::TestSnapshotAPI -v"

# Run with coverage
docker exec hbase_dev bash -c "cd /workspace && HBASE_ZK=hbase-zk:2181 pytest test/ --cov=hbasedriver"
```

### Using the Test Script

```bash
./scripts/run_tests_docker.sh                    # Full build and test
./scripts/run_tests_docker.sh --no-build         # Skip image rebuild
./scripts/run_tests_docker.sh --no-start         # Use running containers
./scripts/run_tests_docker.sh test/test_zk.py    # Run specific test
```

## Docker Architecture

| Container | Purpose | Ports |
|-----------|---------|-------|
| `hbase-zk` | ZooKeeper | 2181 |
| `hbase-master` | HBase Master | 16000 (RPC), 16010 (Web UI) |
| `hbase-rs1` | RegionServer 1 | 16020, 16030 |
| `hbase-rs2` | RegionServer 2 | 16021, 16031 |
| `hbase-rs3` | RegionServer 3 | 16022, 16032 |
| `hbase_dev` | Python dev/test | - |

### Useful Docker Commands

```bash
docker compose up -d              # Start cluster
docker compose down               # Stop cluster
docker compose logs -f hbase-master  # View master logs
docker exec hbase_dev bash        # Shell into dev container
```

## Implemented Features

### Client Operations
- Get, Put, Delete, Scan
- Batch mutations
- Connection pooling
- **BufferedMutator** for efficient bulk writes (Java HBase compatible)
  - Configurable buffer size with auto-flush
  - Background flush thread with configurable interval
  - Exception listener callback for failed writes
  - Context manager support (auto-flush on close)

### Server-Side Atomic Operations
- **CheckAndPut** - atomic conditional put
- **CheckAndDelete** - atomic conditional delete
- **Increment** - atomic counter increment with result
- **Append** - atomic value append with result
- **RowMutations** - atomic multiple mutations on single row
- **Exists/ExistsAll** - server-side existence check

### Admin Operations
- **Table Management**: create, delete, enable, disable, truncate, describe
- **Column Families**: add, delete, modify with options (TTL, compression, bloom filter, etc.)
- **Namespaces**: create, delete, list
- **Snapshots**: create, delete, list, restore, clone
- **Regions**: split, merge, assign, unassign, compact, flush
- **Cluster**: status, balance, balancer control, list region servers

## Code Style

- Python 3.9+ with type hints
- Use `from __future__ import annotations` at top of files for forward references
- Follow existing patterns in the codebase
- Protobuf message types from `hbasedriver.protobuf_py.*`

## Adding New Admin Operations

1. Add protobuf imports in `master.py`
2. Implement method in `MasterConnection` class
3. Add response type mapping in `response.py`
4. Add high-level method in `client/admin.py`
5. Add tests in `test/test_admin_*.py`

## Debugging

```bash
# Check HBase Master Web UI
open http://localhost:16010

# Check ZooKeeper is responding
echo ruok | nc localhost 2181

# View region server hostnames registered in ZK
docker exec hbase-zk zkCli.sh get /hbase/meta-region-server 2>/dev/null | grep -A1 "PBUF"
```
