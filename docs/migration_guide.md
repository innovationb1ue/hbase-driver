# HBase Driver - Migration Guide

This guide helps you migrate from other HBase clients to hbase-driver.

## Table of Contents

- [Migrating from Java HBase](#migrating-from-java-hbase)
- [Migrating from Happybase](#migrating-from-happybase)
- [Migrating from Thrift Clients](#migrating-from-thrift-clients)

---

## Migrating from Java HBase

hbase-driver is designed to be familiar to HBase Java developers. The API follows similar patterns to the HBase Java driver.

### Quick Reference

| Java HBase | hbase-driver |
|-------------|---------------|
| Connection Setup | |
| `Connection connection = ConnectionFactory.createConnection(config)` | `client = Client(config)` |
| `Table table = connection.getTable(TableName.valueOf("mytable"))` | `table = client.get_table("default", "mytable")` |
| `table.put(new Put(Bytes.toBytes("row1"))...` | `table.put(Put(b"row1")...` |
| `Result result = table.get(new Get(Bytes.toBytes("row1"))...` | `row = table.get(Get(b"row1")...` |
| `try (ResultScanner scanner = table.scan(...))` | `with table.scan(...) as scanner:` |
| `try (Connection conn = ...)` | `with Client(config) as client:` |

### Configuration

```java
// Java HBase Configuration
Configuration config = HBaseConfiguration.create();
config.set("hbase.zookeeper.quorum", "localhost:2181");
config.set("hbase.connection.pool.size", "10");
Connection connection = ConnectionFactory.createConnection(config);
```

```python
# hbase-driver Configuration
from hbasedriver.hbase_constants import HConstants

config = {
    HConstants.ZOOKEEPER_QUORUM: "localhost:2181",
    HConstants.CONNECTION_POOL_SIZE: 10,
}
client = Client(config)
```

### Put Operation

```java
// Java HBase Put
Put put = new Put(Bytes.toBytes("row1"));
put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("col"), Bytes.toBytes("value"));
put.setTimestamp(System.currentTimeMillis());
table.put(put);
```

```python
# hbase-driver Put
from hbasedriver.operations.put import Put
import time

put = Put(b"row1").add_column(b"cf", b"col", b"value")
put.set_timestamp(int(time.time() * 1000))  # Milliseconds
table.put(put)
```

### Get Operation

```java
// Java HBase Get
Get get = new Get(Bytes.toBytes("row1"));
get.addFamily(Bytes.toBytes("cf"));
Result result = table.get(get);
byte[] value = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("col"));
```

```python
# hbase-driver Get
from hbasedriver.operations.get import Get

get = Get(b"row1").add_column(b"cf", b"col")
row = table.get(get)
value = row.get(b"cf", b"col")
if value:
    print(f"Value: {value}")
```

### Scan Operation

```java
// Java HBase Scan
Scan scan = new Scan();
scan.setStartRow(Bytes.toBytes("row1"));
scan.setStopRow(Bytes.toBytes("row9"));
scan.setFilter(new PrefixFilter(Bytes.toBytes("abc")));
ResultScanner scanner = table.getScanner(scan);
for (Result result : scanner) {
    byte[] row = result.getRow();
    // Process row
}
scanner.close();
```

```python
# hbase-driver Scan
from hbasedriver.operations.scan import Scan
from hbasedriver.filter import PrefixFilter

scan = Scan(start_row=b"row1", end_row=b"row9").set_filter(PrefixFilter(b"abc"))
with table.scan(scan) as scanner:
    for row in scanner:
        print(row.rowkey)
# Scanner automatically closed
```

### Key Differences

1. **Bytes Handling**: Java uses `byte[]`, Python uses `bytes`
   - Java: `Bytes.toBytes("string")`
   - Python: `b"string"` (bytes literal)

2. **Resource Management**: Java requires explicit `scanner.close()`, Python uses context manager `with`
   - Java: `scanner.close()` must be called
   - Python: `with table.scan(scan) as scanner:` auto-closes

3. **Timestamp Handling**: Java uses `long` (milliseconds), Python uses `int`
   - Java: `System.currentTimeMillis()` returns `long`
   - Python: `int(time.time() * 1000)` for milliseconds

4. **Comparison Operators**:
   - Java: `CompareOperator.EQUAL`, `CompareOperator.GREATER`
   - Python: Same enum values available in `hbasedriver.filter.compare_operator.CompareOperator`

---

## Migrating from Happybase

### Quick Reference

| Happybase | hbase-driver |
|-----------|---------------|
| `client.table("ns", "tb")` | `client.get_table("ns", "tb")` |
| `table.get(row_key)` | `table.get(Get(row_key))` |
| `table.scan()` | `table.scan(Scan())` |
| `table.put(row_key, data)` | `table.put(Put(row_key).add_column(...)` |

### Basic Operations

```python
# Happybase
from happybase import Client

client = Client("localhost:9090")
table = client.table("default", "mytable")

# Get
result = table.get("row1")
print(result)

# Put
table.put("row2", {"cf:col": "value"})

# Scan
for row in table.scan():
    print(row)
```

```python
# hbase-driver
from hbasedriver.client.client import Client
from hbasedriver.operations.put import Put
from hbasedriver.operations.get import Get
from hbasedriver.operations.scan import Scan

client = Client({"hbase.zookeeper.quorum": "localhost:2181"})
table = client.get_table("default", "mytable")

# Get
row = table.get(Get(b"row1"))
if row:
    print(row)

# Put
table.put(Put(b"row2").add_column(b"cf", b"col", b"value"))

# Scan
with table.scan(Scan()) as scanner:
    for row in scanner:
        print(row)
```

### Key Differences

1. **Connection Method**:
   - Happybase: Direct connection to RegionServer
   - hbase-driver: Connection via ZooKeeper for region discovery

2. **Get Return Type**:
   - Happybase: Returns dict or None
   - hbase-driver: Returns Row object or None (check with `if row:`)

3. **Put Data Format**:
   - Happybase: Uses dict for columns `{b"cf:col": "value"}`
   - hbase-driver: Uses builder pattern `Put(...).add_column(...)`

4. **Filter Support**:
   - Happybase: Limited filter support
   - hbase-driver: Full server-side filter support

---

## Migrating from Thrift Clients

### Quick Reference

| HBase Thrift | hbase-driver |
|---------------|---------------|
| `THBaseClient` | `Client` |
| `TTable` | `Table` |
| `TPut` | `Put` |
| `TGet` | `Get` |
| `TScan` | `Scan` |
| `Bytes.BytesWritable` | `bytes` |

### Connection Setup

```python
# HBase Thrift
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import THBaseService

transport = TSocket.TSocket("localhost", 9090)
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = THBaseService.Client(protocol)
transport.open()
```

```python
# hbase-driver (no Thrift dependency!)
from hbasedriver.client.client import Client
from hbasedriver.hbase_constants import HConstants

config = {HConstants.ZOOKEEPER_QUORUM: "localhost:2181"}
client = Client(config)
```

### Key Benefits of Migrating from Thrift

1. **No Thrift Dependency**: hbase-driver doesn't require Thrift
2. **Better Performance**: Direct RPC protocol is faster than Thrift
3. **Full Filter Support**: Complete server-side filter implementation
4. **Simpler Installation**: No Thrift compilation required

### Operation Mapping

```python
# Thrift TPut
tp = hbase.TPut()
tp.row = "row1"
tp.columnValues = {"cf:col": "value"}
client.mutateRow(tp)
```

```python
# hbase-driver Put
from hbasedriver.operations.put import Put

put = Put(b"row1").add_column(b"cf", b"col", b"value")
table.put(put)
```

---

## Common Patterns

### Handling Non-Existent Rows

```python
# Java/Happybase: Check if result is null
if result == null:
    print("Row not found")

# hbase-driver: Check if row is None
row = table.get(Get(b"row1"))
if row is None:
    print("Row not found")
else:
    # Process row
    pass
```

### Iterating Scan Results

```java
// Java HBase
ResultScanner scanner = table.getScanner(scan);
try {
    for (Result result : scanner) {
        // Process
    }
} finally {
    scanner.close();
}
```

```python
# hbase-driver - Context manager auto-closes
from hbasedriver.operations.scan import Scan

scan = Scan()
with table.scan(scan) as scanner:
    for row in scanner:
        print(row.rowkey)
# Scanner automatically closed
```

### Error Handling

```java
// Java HBase
try {
    table.put(put);
} catch (IOException e) {
    e.printStackTrace();
}
```

```python
# hbase-driver - Custom exceptions
from hbasedriver.exceptions import (
    TableNotFoundException,
    TableDisabledException,
    ConnectionException,
)

try:
    table.put(Put(b"row1").add_column(b"cf", b"col", b"value"))
except TableDisabledException as e:
    print(f"Cannot write to disabled table: {e}")
except TableNotFoundException as e:
    print(f"Table does not exist: {e}")
except ConnectionException as e:
    print(f"Connection failed: {e}")
```

---

## Configuration Constants

hbase-driver provides `HConstants` compatible with HBase Java driver:

| Java | Python |
|------|--------|
| `HConstants.ZOOKEEPER_QUORUM` | `HConstants.ZOOKEEPER_QUORUM` |
| `HConstants.ZOOKEEPER_ZNODE_PARENT` | `HConstants.ZOOKEEPER_ZNODE_PARENT` |
| `HBaseConfiguration.create()` | `Client(config)` with dict |

### Using String Literals (Backward Compatible)

```python
# Like Java HBase, you can use string literals:
config = {
    "hbase.zookeeper.quorum": "localhost:2181",
    "hbase.connection.pool.size": 10,
}
client = Client(config)
```

### Using Named Constants

```python
from hbasedriver.hbase_constants import HConstants

config = {
    HConstants.ZOOKEEPER_QUORUM: "localhost:2181",
    HConstants.CONNECTION_POOL_SIZE: 20,
    HConstants.CONNECTION_IDLE_TIMEOUT: 600,
}
client = Client(config)
```

---

## Advanced Features

### Filters

hbase-driver provides complete server-side filter support, similar to Java HBase:

```java
// Java HBase
Filter filter = new PageFilter(100);
Scan scan = new Scan();
scan.setFilter(filter);
```

```python
# hbase-driver
from hbasedriver.filter import PageFilter
from hbasedriver.operations.scan import Scan

scan = Scan().set_filter(PageFilter(100))
with table.scan(scan) as scanner:
    for row in scanner:
        print(row)
```

All Java HBase filters are available:
- `PrefixFilter` → `PrefixFilter`
- `RowFilter` → `RowFilter`
- `FamilyFilter` → `FamilyFilter`
- `QualifierFilter` → `QualifierFilter`
- `ValueFilter` → `ValueFilter`
- `PageFilter` → `PageFilter`
- `FilterList` → `FilterList`
- `SingleColumnValueFilter` → `SingleColumnValueFilter`
- `FirstKeyOnlyFilter` → `FirstKeyOnlyFilter`
- `MultipleColumnPrefixFilter` → `MultipleColumnPrefixFilter`
- `TimestampsFilter` → `TimestampsFilter`

### Batch Operations

```java
// Java HBase
Table table = connection.getTable(TableName.valueOf("mytable"));
List<Put> puts = new ArrayList<>();
puts.add(new Put(Bytes.toBytes("row1")));
puts.add(new Put(Bytes.toBytes("row2")));
Object[] results = table.batch(puts, new Object[]{});
```

```python
# hbase-driver
from hbasedriver.operations.batch import BatchPut

bp = BatchPut()
bp.add_put(Put(b"row1").add_column(b"cf", b"col", b"value1"))
bp.add_put(Put(b"row2").add_column(b"cf", b"col", b"value2"))
results = table.batch_put(bp)
```

---

## Testing Your Migration

### 1. Unit Tests

After migrating, write unit tests to verify behavior:

```python
# test_migration.py
import pytest
from hbasedriver.client.client import Client
from hbasedriver.operations.put import Put

def test_put():
    client = Client({"hbase.zookeeper.quorum": "localhost:2181"})
    table = client.get_table("default", "test_table")
    table.put(Put(b"test_row").add_column(b"cf", b"col", b"value"))
    row = table.get(Get(b"test_row"))
    assert row is not None
    assert row.get(b"cf", b"col") == b"value"
```

### 2. Integration Tests

Run integration tests against your HBase cluster:

```bash
# Using the 3-node Docker cluster
./scripts/run_tests_3node.sh

# Or against your own cluster
pytest test/ -k integration
```

### 3. Performance Comparison

Compare performance before and after migration:

```python
import time

# Measure put performance
start = time.time()
for i in range(1000):
    table.put(Put(f"row_{i}".encode()).add_column(b"cf", b"col", b"value"))
end = time.time()
print(f"1000 puts: {end - start:.2f}s")
```

---

## Troubleshooting

### Issue: Connection Timeout

**Symptom**: `ConnectionException` with timeout message.

**Solutions**:
1. Verify HBase cluster is running
2. Check network connectivity to ZooKeeper
3. Increase timeout: `HConstants.ZOOKEEPER_SESSION_TIMEOUT`
4. Check firewall settings

### Issue: Table Not Found

**Symptom**: `TableNotFoundException`.

**Solutions**:
1. Use `admin.create_table()` to create table first
2. Check table namespace and name match
3. Verify table exists: `admin.table_exists()`

### Issue: Table Disabled

**Symptom**: `TableDisabledException` when writing.

**Solutions**:
1. Enable table: `admin.enable_table(table_name)`
2. Check if table should be enabled

### Issue: Serialization Errors

**Symptom**: `SerializationException`.

**Solutions**:
1. Ensure protobuf compatibility
2. Check data types (use `bytes` for strings)
3. Verify filter serialization

---

## Getting Help

- **Documentation**: See [API Reference](api_reference.md) and [Advanced Usage](advanced_usage.md)
- **Issues**: Report bugs on [GitHub Issues](https://github.com/innovationb1ue/hbase-driver/issues)
- **Community**: Ask questions in [GitHub Discussions](https://github.com/innovationb1ue/hbase-driver/discussions)
