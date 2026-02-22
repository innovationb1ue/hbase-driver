# Performance Guide

This guide covers performance tuning and optimization strategies for python-hbase-driver.

## Table of Contents

- [Connection Management](#connection-management)
- [Scan Performance](#scan-performance)
- [Write Performance](#write-performance)
- [Read Performance](#read-performance)
- [Memory Management](#memory-management)
- [Benchmarking](#benchmarking)

## Connection Management

### Connection Pooling

The driver maintains a connection cache to RegionServers. Reusing connections is critical for performance.

**Good Practice:**
```python
from hbasedriver.client.client import Client

conf = {"hbase.zookeeper.quorum": "localhost"}
client = Client(conf)
table = client.get_table(b"default", b"my_table")

# Reuse the same table object
for i in range(10000):
    table.put(Put(f"row{i}".encode()))
```

**Avoid:**
```python
# Don't create new connections for each operation
for i in range(10000):
    client = Client(conf)  # Creates new connection each time
    table = client.get_table(b"default", b"my_table")
    table.put(Put(f"row{i}".encode()))
```

### Connection Lifecycle

- Connections are established on-demand
- Connections are cached per (host, port) tuple
- Connections persist until the table object is garbage collected
- Consider keeping a long-lived client/table object in your application

## Scan Performance

### Batch Size Tuning

The `scan.set_limit()` method controls the number of rows fetched per RPC.

**Small Batch (Low Latency):**
```python
scan = Scan()
scan.set_limit(10)  # Fetch 10 rows per RPC
```

**Large Batch (High Throughput):**
```python
scan = Scan()
scan.set_limit(1000)  # Fetch 1000 rows per RPC
```

**Recommendation:**
- Small rows (< 1KB): 500-1000 rows per batch
- Medium rows (1-10KB): 100-500 rows per batch
- Large rows (> 10KB): 10-100 rows per batch

### Scanner Caching

Server-side scanner caching reduces RPC overhead:

```python
scan = Scan()
scan.set_limit(500)  # Server caches 500 rows
scanner = table.get_scanner(scan)
```

### Filter Pushdown

Use filters to reduce data transfer:

```python
from hbasedriver.filter.rowfilter import RowFilter
from hbasedriver.filter.comparer import CompareOperator

scan = Scan()
scan.add_column(b"cf", b"status")

# Filter on server side
filter = RowFilter(CompareOperator.EQUAL, b"active")
scan.set_filter(filter)

scanner = table.scan(scan)
```

### Column Selection

Only request columns you need:

```python
# Good: Only request needed columns
scan = Scan()
scan.add_column(b"cf", b"col1")
scan.add_column(b"cf", b"col2")

# Avoid: Requesting all columns
scan = Scan()  # Returns all columns
```

### Parallel Scans

For large tables, split scans across regions:

```python
from concurrent.futures import ThreadPoolExecutor

def scan_range(start, end):
    scan = Scan(start_row=start, stop_row=end)
    scanner = table.get_scanner(scan)
    return list(scanner)

# Determine row key ranges based on region boundaries
ranges = [
    (b"", b"h"),
    (b"h", b"p"),
    (b"p", b"t"),
    (b"t", b"z"),
]

with ThreadPoolExecutor(max_workers=4) as executor:
    results = executor.map(lambda r: scan_range(*r), ranges)
    for result in results:
        process_results(result)
```

## Write Performance

### Batch Writes

Although batch operations are not yet implemented, you can still optimize by:

```python
# Prepare puts efficiently
puts = []
for i in range(1000):
    put = Put(f"row{i}".encode())
    put.add_column(b"cf", b"data", f"value{i}".encode())
    puts.append(put)

# Execute sequentially (connection is reused)
for put in puts:
    table.put(put)
```

### Write Buffering

For high-throughput writes, consider buffering:

```python
import time

write_buffer = []
buffer_size = 100

def flush_buffer():
    for put in write_buffer:
        table.put(put)
    write_buffer.clear()

for i in range(10000):
    put = Put(f"row{i}".encode())
    put.add_column(b"cf", b"data", f"value{i}".encode())
    write_buffer.append(put)

    if len(write_buffer) >= buffer_size:
        flush_buffer()

flush_buffer()
```

### Timestamp Management

Use consistent timestamps for related writes:

```python
import time

timestamp = int(time.time() * 1000)

put1 = Put(b"row1")
put1.add_column(b"cf", b"col1", b"value1", timestamp=timestamp)

put2 = Put(b"row2")
put2.add_column(b"cf", b"col2", b"value2", timestamp=timestamp)

table.put(put1)
table.put(put2)
```

## Read Performance

### Get vs Scan

Use `Get` for single-row lookups:

```python
# Fast: Single row
get = Get(b"row1")
row = table.get(get)
```

Use `Scan` for multi-row lookups:

```python
# Fast: Multiple rows
scan = Scan(start_row=b"row1", stop_row=b"row10")
scanner = table.get_scanner(scan)
rows = list(scanner)
```

Avoid multiple Gets when you can use a Scan:

```python
# Slow: Multiple RPCs
for i in range(10):
    get = Get(f"row{i}".encode())
    row = table.get(get)

# Fast: Single RPC
scan = Scan(start_row=b"row0", stop_row=b"row10")
scanner = table.get_scanner(scan)
rows = list(scanner)
```

### Version Control

Limit the number of versions returned:

```python
get = Get(b"row1")
get.set_max_versions(1)  # Only latest version
row = table.get(get)
```

### Time Range Filtering

Filter by timestamp to reduce data transfer:

```python
get = Get(b"row1")
get.set_time_range(start_ts=0, end_ts=1234567890)
row = table.get(get)
```

## Memory Management

### Streaming Large Results

For large result sets, use iteration instead of loading all into memory:

```python
# Good: Stream results
scanner = table.get_scanner(Scan())
for row in scanner:
    process_row(row)
scanner.close()

# Avoid: Load all into memory
scanner = table.get_scanner(Scan())
rows = list(scanner)  # Loads all rows into memory
scanner.close()
```

### Pagination

Use pagination for large result sets:

```python
scan = Scan()
page_size = 1000

while True:
    rows = scanner.next_batch(page_size)
    if not rows:
        break
    process_page(rows)
```

### Connection Cleanup

Ensure scanners are closed:

```python
scanner = table.get_scanner(Scan())
try:
    for row in scanner:
        process_row(row)
finally:
    scanner.close()  # Always close scanner
```

Or use context manager pattern:

```python
# Custom context manager
class ScannerContext:
    def __init__(self, table, scan):
        self.table = table
        self.scan = scan
        self.scanner = None

    def __enter__(self):
        self.scanner = self.table.get_scanner(self.scan)
        return self.scanner

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.scanner:
            self.scanner.close()

# Usage
with ScannerContext(table, Scan()) as scanner:
    for row in scanner:
        process_row(row)
```

## Benchmarking

### Measuring Throughput

```python
import time

def benchmark_puts(num_puts):
    start = time.time()

    for i in range(num_puts):
        put = Put(f"row{i}".encode())
        put.add_column(b"cf", b"data", f"value{i}".encode())
        table.put(put)

    end = time.time()
    duration = end - start
    throughput = num_puts / duration

    print(f"Put {num_puts} rows in {duration:.2f}s")
    print(f"Throughput: {throughput:.2f} ops/sec")

benchmark_puts(10000)
```

### Measuring Latency

```python
import time

def benchmark_get_latency(num_gets):
    latencies = []

    for i in range(num_gets):
        start = time.time()
        row = table.get(Get(f"row{i}".encode()))
        end = time.time()
        latencies.append((end - start) * 1000)  # ms

    avg_latency = sum(latencies) / len(latencies)
    p50_latency = sorted(latencies)[len(latencies) // 2]
    p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
    p99_latency = sorted(latencies)[int(len(latencies) * 0.99)]

    print(f"Average latency: {avg_latency:.2f}ms")
    print(f"P50 latency: {p50_latency:.2f}ms")
    print(f"P95 latency: {p95_latency:.2f}ms")
    print(f"P99 latency: {p99_latency:.2f}ms")

benchmark_get_latency(1000)
```

### Measuring Scan Performance

```python
def benchmark_scan(num_rows):
    start = time.time()

    scan = Scan()
    scanner = table.get_scanner(scan)

    rows = []
    for row in scanner:
        rows.append(row)
        if len(rows) >= num_rows:
            break

    end = time.time()
    duration = end - start
    throughput = len(rows) / duration

    scanner.close()

    print(f"Scanned {len(rows)} rows in {duration:.2f}s")
    print(f"Throughput: {throughput:.2f} rows/sec")

benchmark_scan(10000)
```

## Performance Tuning Checklist

- [ ] Reuse client and table objects
- [ ] Use appropriate batch sizes for scans
- [ ] Request only necessary columns
- [ ] Use filters to reduce data transfer
- [ ] Limit max versions when possible
- [ ] Use time range filtering for historical data
- [ ] Stream large results instead of loading all into memory
- [ ] Close scanners when done
- [ ] Profile and benchmark your specific use case
- [ ] Monitor HBase RegionServer metrics

## Common Performance Issues

### Issue: High Latency

**Symptoms:** Slow response times, high P95/P99 latency

**Solutions:**
- Check RegionServer load
- Reduce batch size
- Use filters to reduce data transfer
- Check network latency between client and HBase

### Issue: Low Throughput

**Symptoms:** Low operations per second

**Solutions:**
- Increase batch size for scans
- Use parallel scans for large tables
- Check RegionServer resources (CPU, memory, disk)
- Enable RegionServer scanner caching

### Issue: High Memory Usage

**Symptoms:** Out of memory errors, slow garbage collection

**Solutions:**
- Stream results instead of loading all into memory
- Use pagination
- Reduce batch size
- Close scanners promptly

### Issue: Connection Timeouts

**Symptoms:** Connection refused, timeout errors

**Solutions:**
- Check HBase cluster health
- Increase timeout values
- Check network connectivity
- Verify ZooKeeper configuration
