# Troubleshooting Guide

This guide helps you diagnose and resolve common issues with python-hbase-driver.

## Table of Contents

- [Connection Issues](#connection-issues)
- [Table Operations](#table-operations)
- [Scan Issues](#scan-issues)
- [Performance Issues](#performance-issues)
- [Testing Issues](#testing-issues)
- [Cluster Issues](#cluster-issues)

## Connection Issues

### Cannot Connect to ZooKeeper

**Error:**
```
Connection refused: Cannot connect to ZooKeeper
```

**Solutions:**

1. Check ZooKeeper is running:
```bash
# Check if ZooKeeper is accessible
echo stat | nc localhost 2181

# Or with docker
docker ps | grep zookeeper
```

2. Verify ZooKeeper configuration:
```python
conf = {
    "hbase.zookeeper.quorum": "localhost:2181"  # Check host:port
}
```

3. Check firewall rules:
```bash
# Test connectivity
telnet localhost 2181
```

### Cannot Connect to Master

**Error:**
```
Connection refused: Cannot connect to HBase Master
```

**Solutions:**

1. Verify Master is running:
```bash
# Check HBase Master status
docker ps | grep hmaster

# Or check HBase status
echo "status" | hbase shell
```

2. Check Master location:
```python
# Master location is discovered via ZooKeeper
# Ensure ZooKeeper is accessible
client = Client(conf)
print(f"Master: {client.master_host}:{client.master_port}")
```

3. Wait for Master initialization:
```bash
# HBase takes time to initialize
# Wait and retry
sleep 30
```

### Cannot Connect to RegionServer

**Error:**
```
Connection refused: Cannot connect to RegionServer
```

**Solutions:**

1. Check RegionServer status:
```bash
# Check RegionServer status
docker ps | grep regionserver
```

2. Verify RegionServer is reachable:
```bash
# Check network connectivity
telnet <regionserver-host> <regionserver-port>
```

3. Check RegionServer logs:
```bash
# View RegionServer logs
docker logs <regionserver-container>
```

## Table Operations

### Table Not Found

**Error:**
```
TableNotFoundException: Table not found
```

**Solutions:**

1. Check if table exists:
```python
from hbasedriver.client.admin import Admin

admin = client.get_admin()
if admin.table_exists(table_name):
    print("Table exists")
else:
    print("Table does not exist")
```

2. List all tables:
```python
tables = admin.list_tables()
print(f"Tables: {tables}")
```

3. Create table if needed:
```python
from hbasedriver.protobuf_py.HBase_pb2 import ColumnFamilySchema, ColumnFamilyDescriptor

cf = ColumnFamilyDescriptor(name=b"cf")
cfs = [ColumnFamilySchema(name=b"cf", attributes=cf.SerializeToString())]
admin.create_table(table_name, cfs)
```

### Table Not Enabled

**Error:**
```
TableNotEnabledException: Table is disabled
```

**Solutions:**

1. Check table state:
```python
admin.is_table_enabled(table_name)  # Returns True/False
```

2. Enable table:
```python
admin.enable_table(table_name)
```

3. Wait for table to be enabled:
```python
import time

admin.enable_table(table_name)
while not admin.is_table_enabled(table_name):
    time.sleep(1)
```

### Table Disable Timeout

**Error:**
```
TimeoutError: Timeout waiting for table to become DISABLED
```

**Solutions:**

1. Check region states:
```python
region_states = client.get_region_states(ns, tb)
print(f"Region states: {region_states}")
```

2. Increase timeout:
```python
admin.disable_table(table_name, timeout=120)  # 2 minutes
```

3. Check for long-running operations:
```bash
# Check HBase logs
docker logs <hmaster-container>
```

## Scan Issues

### Scanner Not Found

**Error:**
```
Scanner not found or expired
```

**Solutions:**

1. Always close scanners:
```python
scanner = table.get_scanner(Scan())
try:
    for row in scanner:
        process(row)
finally:
    scanner.close()
```

2. Use smaller batches:
```python
scan = Scan()
scan.set_limit(100)  # Reduce batch size
scanner = table.get_scanner(scan)
```

3. Check scanner lease timeout:
```python
# Scanner may time out if not used
# Keep iterating or close scanner promptly
```

### Scan Returns Empty Results

**Issue:** Scan runs but returns no rows

**Solutions:**

1. Verify table has data:
```python
# Check row count
scan = Scan()
scanner = table.get_scanner(scan)
count = sum(1 for _ in scanner)
scanner.close()
print(f"Row count: {count}")
```

2. Check scan range:
```python
scan = Scan()
scan.set_start_row(b"start")
scan.set_stop_row(b"end")
# Ensure range is correct
```

3. Check filters:
```python
scan = Scan()
filter = RowFilter(CompareOperator.EQUAL, b"pattern")
scan.set_filter(filter)
# Ensure filter matches existing data
```

### Slow Scans

**Issue:** Scans are taking too long

**Solutions:**

1. Increase batch size:
```python
scan = Scan()
scan.set_limit(1000)  # Fetch more rows per RPC
```

2. Use column filtering:
```python
scan = Scan()
scan.add_column(b"cf", b"col1")  # Only request needed columns
```

3. Use time range filtering:
```python
get = Get(b"row1")
get.set_time_range(start_ts=0, end_ts=1234567890)
```

## Performance Issues

### High Memory Usage

**Issue:** Client uses too much memory

**Solutions:**

1. Stream results instead of loading all:
```python
# Good: Stream
scanner = table.get_scanner(Scan())
for row in scanner:
    process(row)
scanner.close()

# Bad: Load all
scanner = table.get_scanner(Scan())
rows = list(scanner)  # Loads all into memory
```

2. Use pagination:
```python
scan = Scan()
page_size = 1000

while True:
    rows = scanner.next_batch(page_size)
    if not rows:
        break
    process_page(rows)
```

### Low Throughput

**Issue:** Too few operations per second

**Solutions:**

1. Reuse connections:
```python
# Good: Reuse client/table
client = Client(conf)
table = client.get_table(ns, tb)
for i in range(10000):
    table.put(put)

# Bad: Create new connections
for i in range(10000):
    client = Client(conf)  # New connection each time
    table = client.get_table(ns, tb)
    table.put(put)
```

2. Increase batch size:
```python
scan = Scan()
scan.set_limit(1000)  # Larger batches
```

3. Use parallel scans:
```python
from concurrent.futures import ThreadPoolExecutor

def scan_range(start, end):
    scan = Scan(start_row=start, stop_row=end)
    scanner = table.get_scanner(scan)
    return list(scanner)

ranges = [(b"a", b"m"), (b"m", b"z")]
with ThreadPoolExecutor(max_workers=2) as executor:
    results = executor.map(lambda r: scan_range(*r), ranges)
```

## Testing Issues

### Tests Fail on First Run

**Issue:** Integration tests fail initially but pass on retry

**Solutions:**

1. Wait for cluster to fully initialize:
```bash
# HBase takes time to start
sleep 30

# Check cluster status
docker ps
```

2. Check container logs:
```bash
# View HBase Master logs
docker logs hmaster

# View RegionServer logs
docker logs regionserver1
docker logs regionserver2
docker logs regionserver3
```

3. Run tests without starting cluster:
```bash
# If cluster is already running
./scripts/run_tests_3node.sh --no-start
```

### Tests Hang

**Issue:** Tests never complete

**Solutions:**

1. Check for infinite loops:
```bash
# Check test logs
docker logs test-container

# Check process status
docker ps
```

2. Increase timeout:
```python
# In test code
import time

timeout = 120  # 2 minutes
start = time.time()
while condition and (time.time() - start) < timeout:
    # wait
    time.sleep(1)
```

3. Check for blocked operations:
```bash
# Check HBase status
echo "status" | hbase shell

# Check region states
echo "list" | hbase shell
```

## Cluster Issues

### Master Not Initializing

**Issue:** HBase Master shows "initializing" status

**Solutions:**

1. Wait for initialization:
```python
import time

# Master can take 30-60 seconds to initialize
time.sleep(60)
```

2. Check ZooKeeper:
```bash
# Verify ZooKeeper is accessible
echo stat | nc localhost 2181
```

3. Check Master logs:
```bash
docker logs hmaster
```

### RegionServers Not Starting

**Issue:** RegionServers fail to start

**Solutions:**

1. Check RegionServer logs:
```bash
docker logs regionserver1
docker logs regionserver2
docker logs regionserver3
```

2. Check network connectivity:
```bash
# Test connectivity between containers
docker exec regionserver1 ping hmaster
```

3. Verify configuration:
```bash
# Check HBase configuration
docker exec hmaster cat /hbase/conf/hbase-site.xml
```

### Region Split Issues

**Issue:** Operations fail during region split

**Solutions:**

1. Wait for split to complete:
```python
import time

# Splits can take several seconds
time.sleep(10)
```

2. Retry failed operations:
```python
max_retries = 3
for attempt in range(max_retries):
    try:
        result = table.get(Get(b"row1"))
        break
    except Exception as e:
        if attempt < max_retries - 1:
            time.sleep(1)
            continue
        raise
```

3. Check region states:
```python
region_states = client.get_region_states(ns, tb)
print(f"Region states: {region_states}")
```

## Debugging

### Enable Debug Logging

```python
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger('pybase')
```

### Check Region Locations

```python
# Check which region serves a rowkey
region = table.locate_target_region(b"row1")
print(f"Region: {region.get_region_name()}")
print(f"Host: {region.host}")
print(f"Port: {region.port}")
print(f"Start key: {region.region_info.start_key}")
print(f"End key: {region.region_info.end_key}")
```

### Monitor Connections

```python
# Check cached regions
print(f"Cached regions: {len(table.regions)}")

# Check cached connections
print(f"Cached connections: {len(table.rs_conns)}")
```

## Getting Help

If you're still having issues:

1. Check the [GitHub Issues](https://github.com/innovationb1ue/hbase-driver/issues)
2. Review the [Test Guide](TEST_GUIDE.md)
3. Check HBase documentation: https://hbase.apache.org/book.html
4. Create a minimal reproducible example
5. Include error messages and logs in your issue report

## Common Error Messages

| Error Message | Cause | Solution |
|--------------|-------|----------|
| `Connection refused` | Server not running or wrong host/port | Check HBase cluster status |
| `TableNotFoundException` | Table doesn't exist | Create table or check table name |
| `TableNotEnabledException` | Table is disabled | Enable table |
| `Scanner not found` | Scanner expired | Close scanner promptly or reduce batch size |
| `Master is initializing` | Master still starting | Wait and retry |
| `Region not found` | Region moved or split | Retry operation |

## Additional Resources

- [API Reference](api_reference.md)
- [Advanced Usage](advanced_usage.md)
- [Performance Guide](performance_guide.md)
- [HBase Book](https://hbase.apache.org/book.html)
