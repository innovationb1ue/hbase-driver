# hbase-driver

[![Tests](https://github.com/innovationb1ue/hbase-driver/actions/workflows/ci.yml/badge.svg)](https://github.com/innovationb1ue/hbase-driver/actions)
[![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)

Pure-Python native HBase client (no Thrift). This project implements core HBase regionserver and master RPCs so Python programs can perform table and metadata operations against an HBase cluster.

## Status

- Integration test status (local): **77 / 77 tests passing** (2026-02-21) using the custom 3-node Docker cluster.

## Quick Start

Get started with hbase-driver in just a few lines:

```python
from hbasedriver.client.client import Client
from hbasedriver.hbase_constants import HConstants
from hbasedriver.operations.put import Put
from hbasedriver.operations.get import Get
from hbasedriver.operations.scan import Scan

# Initialize client
config = {HConstants.ZOOKEEPER_QUORUM: "localhost:2181"}
client = Client(config)

# Put and Get data
with client.get_table("default", "mytable") as table:
    table.put(Put(b"row1").add_column(b"cf", b"col", b"value"))
    row = table.get(Get(b"row1"))
    print(row.get(b"cf", b"col"))  # b"value"

# Scan data
with client.get_table("default", "mytable") as table:
    with table.scan(Scan()) as scanner:
        for row in scanner:
            print(row.rowkey)
```

## Complete Example

For a comprehensive example covering all major features, see [complete_example.py](complete_example.py) which demonstrates:

- Basic CRUD operations (Put, Get, Delete, Scan)
- Advanced features (Batch, CheckAndPut, Increment)
- Filter usage for server-side filtering
- DDL operations (Create, Disable, Enable, Delete, Truncate)
- Connection management and resource cleanup
- Cache invalidation after table modifications

Run the example:
```bash
python3 complete_example.py
```

## Installation

```bash
pip install hbase-driver
```

Or for development:

```bash
git clone https://github.com/innovationb1ue/hbase-driver.git
cd hbase-driver
pip install -e .
```

## Connection Management

The hbase-driver provides context managers for automatic resource cleanup, similar to Java's try-with-resources:

```python
from hbasedriver.client.client import Client
from hbasedriver.hbase_constants import HConstants

# Using context manager - automatic cleanup
with Client({HConstants.ZOOKEEPER_QUORUM: "localhost:2181"}) as client:
    with client.get_table("default", "mytable") as table:
        # Do operations
        table.put(Put(b"row1").add_column(b"cf", b"col", b"value"))
    # Table automatically closed here
# Client automatically closed here

# Manual cleanup
client = Client({HConstants.ZOOKEEPER_QUORUM: "localhost:2181"})
try:
    table = client.get_table("default", "mytable")
    try:
        # Do operations
        table.put(Put(b"row1").add_column(b"cf", b"col", b"value"))
    finally:
        table.close()
finally:
    client.close()
```

### Resource Classes Supporting Context Managers

- `Client` - Main client connection
- `Table` - Table operations
- `Admin` - DDL operations
- `ResultScanner` - Scan results iteration

## Configuration

### Configuration Options

| Key | Default | Description |
|-----|---------|-------------|
| `hbase.zookeeper.quorum` | Required | ZooKeeper quorum addresses (comma-separated) |
| `zookeeper.znode.parent` | `/hbase` | ZooKeeper parent znode |
| `hbase.connection.pool.size` | 10 | Maximum connections per pool |
| `hbase.connection.idle.timeout` | 300 | Idle timeout in seconds |

### Using Configuration Constants

The driver provides named constants for configuration keys, compatible with HBase Java driver's HConstants:

```python
from hbasedriver.client.client import Client
from hbasedriver.hbase_constants import HConstants

config = {
    HConstants.ZOOKEEPER_QUORUM: "localhost:2181",
    HConstants.CONNECTION_POOL_SIZE: 20,
    HConstants.CONNECTION_IDLE_TIMEOUT: 600
}
client = Client(config)
```

### String Literals (Backward Compatible)

You can also use string literals directly:

```python
config = {
    "hbase.zookeeper.quorum": "localhost:2181",
    "hbase.connection.pool.size": 20
}
client = Client(config)
```

## API Reference

### Client

Main entry point for HBase operations.

```python
from hbasedriver.client.client import Client
from hbasedriver.hbase_constants import HConstants

client = Client({HConstants.ZOOKEEPER_QUORUM: "localhost:2181"})

# Get admin for DDL operations
admin = client.get_admin()

# Get table for data operations
table = client.get_table("default", "mytable")

# Close when done
client.close()
```

### Table

Perform data operations on a specific table.

```python
from hbasedriver.operations.put import Put
from hbasedriver.operations.get import Get
from hbasedriver.operations.delete import Delete
from hbasedriver.operations.scan import Scan

# Put data
table.put(Put(b"row1").add_column(b"cf", b"col", b"value"))

# Get data
row = table.get(Get(b"row1"))
if row:
    print(row.get(b"cf", b"col"))

# Delete data
table.delete(Delete(b"row1"))

# Scan data
with table.scan(Scan()) as scanner:
    for row in scanner:
        print(row.rowkey)
```

### Admin

Perform DDL operations.

```python
from hbasedriver.model import ColumnFamilyDescriptor
from hbasedriver.table_name import TableName

# Create table
cfd = ColumnFamilyDescriptor(b"cf")
admin.create_table(TableName.value_of(b"mytable"), [cfd])

# Disable table
admin.disable_table(TableName.value_of(b"mytable"))

# Enable table
admin.enable_table(TableName.value_of(b"mytable"))

# Delete table (must be disabled first)
admin.disable_table(TableName.value_of(b"mytable"))
admin.delete_table(TableName.value_of(b"mytable"))

# List tables
tables = admin.list_tables()
```

### Operations

#### Put

```python
from hbasedriver.operations.put import Put

# Simple put
put = Put(b"row1").add_column(b"cf", b"col", b"value")
table.put(put)

# Multiple columns
put = Put(b"row2") \
    .add_column(b"cf", b"name", b"Alice") \
    .add_column(b"cf", b"age", b"30") \
    .add_column(b"info", b"email", b"alice@example.com")
table.put(put)

# With timestamp
import time
ts = int(time.time() * 1000)
put = Put(b"row3").add_column(b"cf", b"col", b"value", ts=ts)
table.put(put)
```

#### Get

```python
from hbasedriver.operations.get import Get

# Get entire row
row = table.get(Get(b"row1"))

# Get specific column
row = table.get(Get(b"row1").add_column(b"cf", b"col"))

# Get multiple columns
row = table.get(Get(b"row1")
    .add_column(b"cf", b"name")
    .add_column(b"cf", b"age"))

# With time range
start_ts = int((time.time() - 86400) * 1000)  # 24 hours ago
end_ts = int(time.time() * 1000)
row = table.get(Get(b"row1").set_time_range(start_ts, end_ts))

# Check existence only
exists = table.get(Get(b"row1").set_check_existence_only(True)) is not None
```

#### Scan

```python
from hbasedriver.operations.scan import Scan

# Scan entire table
with table.scan(Scan()) as scanner:
    for row in scanner:
        print(row.rowkey)

# Scan with row key range
scan = Scan(start_row=b"row1", end_row=b"row9")
with table.scan(scan) as scanner:
    for row in scanner:
        print(row.rowkey)

# Scan with specific columns
scan = Scan().add_column(b"cf", b"col")
with table.scan(scan) as scanner:
    for row in scanner:
        print(row.get(b"cf", b"col"))

# Scan with limit
scan = Scan().set_limit(100)
with table.scan(scan) as scanner:
    for row in scanner:
        print(row.rowkey)

# Pagination
scan = Scan()
rows, resume_key = table.scan_page(scan, page_size=100)
while resume_key:
    scan = Scan(start_row=resume_key, include_start_row=False)
    rows, resume_key = table.scan_page(scan, page_size=100)
```

#### Batch Operations

```python
from hbasedriver.operations.batch import BatchGet, BatchPut

# Batch get
bg = BatchGet([b"row1", b"row2", b"row3"])
bg.add_column(b"cf", b"col1")
results = table.batch_get(bg)

# Batch put
bp = BatchPut()
bp.add_put(Put(b"row1").add_column(b"cf", b"col1", b"value1"))
bp.add_put(Put(b"row2").add_column(b"cf", b"col1", b"value2"))
results = table.batch_put(bp)

# Context manager for batch
with table.batch(batch_size=1000) as batch:
    batch.put(b"row1", {b"cf:col1": b"value1"})
    batch.put(b"row2", {b"cf:col1": b"value2"})
    batch.delete(b"row3")
# All operations executed when exiting context
```

#### Check and Put

```python
from hbasedriver.operations.increment import CheckAndPut

cap = CheckAndPut(b"row1")
cap.set_check(b"cf", b"lock", b"")  # Check if lock is empty
cap.set_put(Put(b"row1").add_column(b"cf", b"data", b"value"))
success = table.check_and_put(cap)
```

#### Increment

```python
from hbasedriver.operations.increment import Increment

inc = Increment(b"row1")
inc.add_column(b"cf", b"counter", 1)
new_value = table.increment(inc)
```

### Filters

The driver supports server-side filters:

```python
from hbasedriver.filter import PrefixFilter, RowFilter
from hbasedriver.filter.compare_filter import CompareOperator
from hbasedriver.filter.binary_comparator import BinaryComparator

# Prefix filter
scan = Scan().set_filter(PrefixFilter(b"abc"))

# Row filter with comparison
scan = Scan().set_filter(
    RowFilter(CompareOperator.EQUAL, BinaryComparator(b"row1"))
)
```

## Java Compatibility

This driver is designed to be familiar to HBase Java developers. Here's a quick comparison:

| Java Driver | Python Driver |
|-------------|---------------|
| `Connection connection = ConnectionFactory.createConnection(config)` | `client = Client(config)` |
| `Table table = connection.getTable(TableName.valueOf("mytable"))` | `table = client.get_table("default", "mytable")` |
| `table.put(new Put(Bytes.toBytes("row1"))...` | `table.put(Put(b"row1")...` |
| `Result result = table.get(new Get(Bytes.toBytes("row1"))...` | `row = table.get(Get(b"row1")...` |
| `try (ResultScanner scanner = table.scan(...))` | `with table.scan(...) as scanner:` |
| `try (Connection conn = ...)` | `with Client(config) as client:` |

### Configuration Constants

Java's `HConstants` are available as `hbase_constants.HConstants`:

| Java | Python |
|------|--------|
| `HConstants.ZOOKEEPER_QUORUM` | `HConstants.ZOOKEEPER_QUORUM` |
| `HConstants.ZOOKEEPER_ZNODE_PARENT` | `HConstants.ZOOKEEPER_ZNODE_PARENT` |
| (Custom connection pool size) | `HConstants.CONNECTION_POOL_SIZE` |
| (Custom idle timeout) | `HConstants.CONNECTION_IDLE_TIMEOUT` |

## Development

### Quickstart (3-node Docker dev environment)

Prerequisites: Docker and docker-compose installed.

1. Build, start the custom 3-node cluster and run the full test suite:

```bash
./scripts/run_tests_3node.sh
```

2. To run tests against an already-running cluster (fast):

```bash
./scripts/run_tests_3node.sh --no-start
```

3. Run a single test file or case:

```bash
./scripts/run_tests_3node.sh test/test_scan.py
./scripts/run_tests_3node.sh test/test_scan.py::test_scan
```

Legacy single-node dev workflow (still available):

```bash
./scripts/run_tests_docker.sh
```

See [TEST_GUIDE.md](docs/TEST_GUIDE.md) and [DEV_ENV.md](docs/DEV_ENV.md) for full documentation and troubleshooting steps.

## Documentation

- [API Reference](docs/api_reference.md) - Comprehensive API documentation
- [Advanced Usage](docs/advanced_usage.md) - Advanced features and patterns
- [Performance Guide](docs/performance_guide.md) - Performance tuning and optimization
- [Troubleshooting](docs/troubleshooting.md) - Common issues and solutions
- [Migration Guide](docs/migration_guide.md) - Migration from Java HBase, Happybase, and Thrift clients
- [Migration from happybase](docs/migration_from_happybase.md) - Guide for migrating from happybase/happybase-thrift
- [中文介绍](docs/中文介绍.md) - 中文文档介绍 | Chinese Introduction

## License

Apache License 2.0
