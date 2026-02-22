# Advanced Usage Guide

This guide covers advanced features and patterns for using python-hbase-driver.

## Table of Contents

- [Connection Management](#connection-management)
- [Advanced Scanning](#advanced-scanning)
- [Filtering](#filtering)
- [Batch Operations](#batch-operations)
- [Region Caching](#region-caching)
- [Error Handling](#error-handling)
- [Performance Tuning](#performance-tuning)

## Connection Management

### Connection Pooling

The `Table` class maintains a connection cache to RegionServers:

```python
from hbasedriver.client.client import Client

conf = {"hbase.zookeeper.quorum": "localhost"}
client = Client(conf)
table = client.get_table(b"default", b"my_table")

# Connections are cached automatically
# Reusing the same table object reuses connections
for i in range(1000):
    table.put(Put(f"row{i}".encode()))
```

### Connection Lifecycle

Connections are established on-demand and cached:

```python
# First operation establishes connection
table.put(Put(b"row1"))

# Subsequent operations reuse cached connection
table.put(Put(b"row2"))

# Connections persist until table object is garbage collected
```

## Advanced Scanning

### Server-Side Pagination

Use `next_batch()` for efficient pagination:

```python
from hbasedriver.operations.scan import Scan

scan = Scan()
scanner = table.get_scanner(scan)

batch_size = 100
while True:
    rows = scanner.next_batch(batch_size)
    if not rows:
        break
    process_rows(rows)

scanner.close()
```

### Stateless Pagination

For web APIs or stateless services:

```python
scan = Scan()
page_size = 20
resume_key = None

while True:
    if resume_key:
        scan = Scan(start_row=resume_key, include_start_row=False)

    rows, resume_key = table.scan_page(scan, page_size)

    if not rows:
        break

    send_to_client(rows, resume_key)
```

### Reversed Scans

Scan rows in reverse order:

```python
from hbasedriver.operations.scan import Scan

scan = Scan()
scan.set_reversed(True)
scan.set_start_row(b"zzz")  # Start from the end
scan.set_stop_row(b"aaa")   # Stop at the beginning
```

### Time Range Filtering

Filter results by timestamp:

```python
from hbasedriver.operations.get import Get

get = Get(b"row1")
get.set_time_range(start_ts=0, end_ts=1234567890)
row = table.get(get)
```

## Filtering

### Basic Row Filter

```python
from hbasedriver.filter.rowfilter import RowFilter
from hbasedriver.filter.comparer import CompareOperator

filter = RowFilter(CompareOperator.EQUAL, b"user:*")
scan.set_filter(filter)
```

### Using Filters with Scans

```python
from hbasedriver.operations.scan import Scan
from hbasedriver.filter.rowfilter import RowFilter
from hbasedriver.filter.comparer import CompareOperator

scan = Scan()
scan.add_column(b"cf", b"status")

# Only return rows where status is 'active'
filter = RowFilter(CompareOperator.EQUAL, b"active")
scan.set_filter(filter)

scanner = table.scan(scan)
for row in scanner:
    print(row.rowkey)
```

## Batch Operations

### Bulk Inserts

Perform multiple puts in a loop:

```python
from hbasedriver.operations.put import Put

# Prepare multiple puts
puts = []
for i in range(1000):
    put = Put(f"row{i}".encode())
    put.add_column(b"cf", b"data", f"value{i}".encode())
    puts.append(put)

# Execute puts
for put in puts:
    table.put(put)
```

### Bulk Gets

```python
from hbasedriver.operations.get import Get

rowkeys = [f"row{i}".encode() for i in range(1000)]
for rowkey in rowkeys:
    get = Get(rowkey)
    row = table.get(get)
    if row:
        process_row(row)
```

## Region Caching

### Understanding Region Caching

The client caches region locations to avoid meta lookups:

```python
# First lookup hits meta server
table.get(Get(b"row1"))

# Subsequent lookups use cached region
table.get(Get(b"row2"))  # Same region
table.get(Get(b"row3"))  # Same region
```

### Cache Invalidation

The cache automatically invalidates on region splits:

```python
# Initial cache
table.get(Get(b"row1"))

# After region split (handled automatically)
# Cache is refreshed on next lookup
table.get(Get(b"row1000"))
```

## Error Handling

### Handling Table Not Found

```python
from hbasedriver.exceptions.TableNotFoundException import TableNotFoundException

try:
    table = client.get_table(b"default", b"nonexistent")
    table.get(Get(b"row1"))
except TableNotFoundException as e:
    print(f"Table not found: {e}")
```

### Handling Connection Errors

```python
import time
from hbasedriver.exceptions.RemoteException import RemoteException

max_retries = 3
for attempt in range(max_retries):
    try:
        result = table.get(Get(b"row1"))
        break
    except RemoteException as e:
        if attempt < max_retries - 1:
            time.sleep(1)
            continue
        raise
```

### Handling Region Not Found

```python
try:
    table.get(Get(b"row1"))
except Exception as e:
    if "region not found" in str(e).lower():
        # Region may have moved; retry
        table.get(Get(b"row1"))
    else:
        raise
```

## Performance Tuning

### Scan Caching

Control the number of rows fetched per RPC:

```python
from hbasedriver.operations.scan import Scan

scan = Scan()
scan.set_limit(1000)  # Fetch up to 1000 rows per RPC

scanner = table.get_scanner(scan)
```

### Connection Reuse

Reuse table and client objects:

```python
# Good: Reuse objects
client = Client(conf)
table = client.get_table(b"default", b"my_table")

for i in range(10000):
    table.put(Put(f"row{i}".encode()))

# Avoid: Create new objects each time
for i in range(10000):
    client = Client(conf)  # Bad!
    table = client.get_table(b"default", b"my_table")
    table.put(Put(f"row{i}".encode()))
```

### Batch Size Optimization

Choose appropriate batch sizes for your use case:

```python
# Small batches for low latency
batch_size = 10

# Large batches for high throughput
batch_size = 1000

# Adjust based on row size
if row_size_mb > 1:
    batch_size = 10
else:
    batch_size = 1000
```

### Scan Parallelization

For large scans, process in parallel:

```python
from concurrent.futures import ThreadPoolExecutor

def scan_region(start, end):
    scan = Scan(start_row=start, stop_row=end)
    scanner = table.get_scanner(scan)
    return list(scanner)

# Split scan into ranges
ranges = [(b"a", b"m"), (b"m", b"z")]

with ThreadPoolExecutor(max_workers=2) as executor:
    results = executor.map(lambda r: scan_region(*r), ranges)
    for result in results:
        process_results(result)
```

## Monitoring and Debugging

### Logging

Enable debug logging:

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('pybase')

client = Client(conf)
```

### Region Location Debugging

```python
# Check which region serves a rowkey
region = table.locate_target_region(b"row1")
print(f"Region: {region.get_region_name()}")
print(f"Host: {region.host}")
print(f"Port: {region.port}")
```

## Best Practices

1. **Always close scanners** to free server resources
2. **Reuse client and table objects** for connection pooling
3. **Use appropriate batch sizes** for your workload
4. **Handle exceptions appropriately** with retries
5. **Monitor region splits** which may affect performance
6. **Use time range filtering** when querying historical data
7. **Prefer scans over multiple gets** for large result sets
