# Migration Guide: From happybase/happybase-thrift

This guide helps you migrate from happybase or happybase-thrift to python-hbase-driver.

## Table of Contents

- [Key Differences](#key-differences)
- [Connection](#connection)
- [Table Operations](#table-operations)
- [CRUD Operations](#crud-operations)
- [Scanning](#scanning)
- [Batch Operations](#batch-operations)
- [Complete Example](#complete-example)

## Key Differences

| Feature | happybase | python-hbase-driver |
|---------|-----------|---------------------|
| Protocol | Thrift | Native HBase RPC |
| Dependencies | thrift, happybase | kazoo, protobuf |
| Connection Pool | Built-in | Manual (via object reuse) |
| Batch API | `table.batch()` | Loop (batch coming soon) |
| Admin API | `connection.tables()` | `admin.list_tables()` |
| Namespace Support | Limited | Full support |
| Type Hints | None | Full type hints |

## Connection

### happybase

```python
import happybase

connection = happybase.Connection(
    host='localhost',
    port=9090,
    autoconnect=True
)
```

### python-hbase-driver

```python
from hbasedriver.client.client import Client

conf = {
    "hbase.zookeeper.quorum": "localhost:2181"
}
client = Client(conf)
```

**Key Differences:**
- Uses ZooKeeper instead of direct HBase Thrift port
- No `autoconnect` parameter (connects on initialization)
- ZooKeeper port (2181) instead of Thrift port (9090)

## Table Operations

### Listing Tables

#### happybase

```python
tables = connection.tables()
print(tables)  # ['table1', 'table2']
```

#### python-hbase-driver

```python
from hbasedriver.client.admin import Admin

admin = client.get_admin()
tables = admin.list_tables()
print(tables)  # List of table descriptors
```

### Creating Tables

#### happybase

```python
connection.create_table(
    'my_table',
    {'cf1': dict(max_versions=10),
     'cf2': dict(max_versions=5)}
)
```

#### python-hbase-driver

```python
from hbasedriver.protobuf_py.HBase_pb2 import ColumnFamilySchema, ColumnFamilyDescriptor
from hbasedriver.common.table_name import TableName

# Create column family descriptors
cf1 = ColumnFamilyDescriptor(name=b"cf1")
cf1.max_versions = 10

cf2 = ColumnFamilyDescriptor(name=b"cf2")
cf2.max_versions = 5

# Create table schema
cfs = [
    ColumnFamilySchema(name=b"cf1", attributes=cf1.SerializeToString()),
    ColumnFamilySchema(name=b"cf2", attributes=cf2.SerializeToString())
]

tn = TableName.value_of(b"default", b"my_table")
admin.create_table(tn, cfs)
```

### Deleting Tables

#### happybase

```python
connection.disable_table('my_table')
connection.delete_table('my_table')
```

#### python-hbase-driver

```python
from hbasedriver.common.table_name import TableName

tn = TableName.value_of(b"default", b"my_table")
admin.disable_table(tn)
admin.delete_table(tn)
```

### Enabling/Disabling Tables

#### happybase

```python
connection.enable_table('my_table')
connection.disable_table('my_table')
```

#### python-hbase-driver

```python
admin.enable_table(tn)
admin.disable_table(tn)
```

### Table Existence Check

#### happybase

```python
if 'my_table' in connection.tables():
    print("Table exists")
```

#### python-hbase-driver

```python
if admin.table_exists(tn):
    print("Table exists")
```

## CRUD Operations

### Getting a Table Reference

#### happybase

```python
table = connection.table('my_table')
```

#### python-hbase-driver

```python
table = client.get_table(b"default", b"my_table")
```

### Put (Write)

#### happybase

```python
table.put(
    b'row1',
    {b'cf1:col1': b'value1',
     b'cf1:col2': b'value2'}
)
```

#### python-hbase-driver

```python
from hbasedriver.operations.put import Put

put = Put(b"row1")
put.add_column(b"cf1", b"col1", b"value1")
put.add_column(b"cf1", b"col2", b"value2")
table.put(put)
```

### Get (Read Single Row)

#### happybase

```python
row = table.row(b'row1')
print(row[b'cf1:col1'])  # b'value1'
```

#### python-hbase-driver

```python
from hbasedriver.operations.get import Get

get = Get(b"row1")
get.add_column(b"cf1", b"col1")
row = table.get(get)

if row:
    value = row.get(b"cf1", b"col1")
    print(value)  # b'value1'
```

### Get Multiple Rows

#### happybase

```python
rows = table.rows([b'row1', b'row2', b'row3'])
for key, data in rows:
    print(key, data)
```

#### python-hbase-driver

```python
from hbasedriver.operations.get import Get

rowkeys = [b"row1", b"row2", b"row3"]
for rowkey in rowkeys:
    get = Get(rowkey)
    row = table.get(get)
    if row:
        print(rowkey, row)
```

### Delete

#### happybase

```python
# Delete specific columns
table.delete(
    b'row1',
    columns=[b'cf1:col1', b'cf1:col2']
)

# Delete entire row
table.delete(b'row1')
```

#### python-hbase-driver

```python
from hbasedriver.operations.delete import Delete

# Delete specific columns
delete = Delete(b"row1")
delete.add_column(b"cf1", b"col1")
delete.add_column(b"cf1", b"col2")
table.delete(delete)

# Delete entire row
delete = Delete(b"row1")
delete.add_family(b"cf1")
table.delete(delete)

# Delete all columns in all families (entire row)
delete = Delete(b"row1")
table.delete(delete)
```

## Scanning

### Basic Scan

#### happybase

```python
for key, data in table.scan():
    print(key, data)
```

#### python-hbase-driver

```python
from hbasedriver.operations.scan import Scan

scan = Scan()
scanner = table.get_scanner(scan)

for row in scanner:
    print(row.rowkey, row)

scanner.close()
```

### Scan with Row Range

#### happybase

```python
for key, data in table.scan(row_start=b'row1', row_stop=b'row9'):
    print(key, data)
```

#### python-hbase-driver

```python
scan = Scan()
scan.set_start_row(b"row1")
scan.set_stop_row(b"row9")
scanner = table.get_scanner(scan)

for row in scanner:
    print(row.rowkey)

scanner.close()
```

### Scan with Column Filter

#### happybase

```python
for key, data in table.scan(columns=[b'cf1:col1']):
    print(key, data)
```

#### python-hbase-driver

```python
scan = Scan()
scan.add_column(b"cf1", b"col1")
scanner = table.get_scanner(scan)

for row in scanner:
    print(row.rowkey, row.get(b"cf1", b"col1"))

scanner.close()
```

### Scan with Limit

#### happybase

```python
for key, data in table.scan(limit=100):
    print(key, data)
```

#### python-hbase-driver

```python
scan = Scan()
scan.set_limit(100)
scanner = table.get_scanner(scan)

for row in scanner:
    print(row.rowkey)

scanner.close()
```

### Scan with Batch Size

#### happybase

```python
for key, data in table.scan(batch_size=100):
    print(key, data)
```

#### python-hbase-driver

```python
scan = Scan()
scan.set_limit(100)
scanner = table.get_scanner(scan)

while True:
    rows = scanner.next_batch(100)
    if not rows:
        break
    for row in rows:
        print(row.rowkey)

scanner.close()
```

### Reversed Scan

#### happybase

```python
for key, data in table.scan(reverse=True):
    print(key, data)
```

#### python-hbase-driver

```python
scan = Scan()
scan.set_reversed(True)
scanner = table.get_scanner(scan)

for row in scanner:
    print(row.rowkey)

scanner.close()
```

## Batch Operations

### Batch Put

#### happybase

```python
with table.batch() as b:
    b.put(b'row1', {b'cf1:col1': b'value1'})
    b.put(b'row2', {b'cf1:col1': b'value2'})
    b.put(b'row3', {b'cf1:col1': b'value3'})
```

#### python-hbase-driver

```python
# Batch operations coming soon
# For now, use a loop:
puts = []
for i in range(1, 4):
    put = Put(f"row{i}".encode())
    put.add_column(b"cf1", b"col1", f"value{i}".encode())
    puts.append(put)

# Execute puts (connections are reused)
for put in puts:
    table.put(put)
```

### Batch Delete

#### happybase

```python
with table.batch() as b:
    b.delete(b'row1')
    b.delete(b'row2')
    b.delete(b'row3')
```

#### python-hbase-driver

```python
# Batch operations coming soon
# For now, use a loop:
for rowkey in [b"row1", b"row2", b"row3"]:
    delete = Delete(rowkey)
    delete.add_family(b"cf1")
    table.delete(delete)
```

## Complete Example

### happybase

```python
import happybase

# Connect
connection = happybase.Connection('localhost')

# Create table
connection.create_table(
    'users',
    {'info': dict(max_versions=1),
     'data': dict(max_versions=1)}
)

# Get table
table = connection.table('users')

# Put data
table.put(b'user1', {
    b'info:name': b'Alice',
    b'info:email': b'alice@example.com',
    b'data:score': b'95'
})

# Get data
row = table.row(b'user1')
print(row[b'info:name'])  # b'Alice'

# Scan
for key, data in table.scan():
    print(key, data)

# Delete
table.delete(b'user1')

# Drop table
connection.disable_table('users')
connection.delete_table('users')
```

### python-hbase-driver

```python
from hbasedriver.client.client import Client
from hbasedriver.operations.put import Put
from hbasedriver.operations.get import Get
from hbasedriver.operations.delete import Delete
from hbasedriver.operations.scan import Scan
from hbasedriver.protobuf_py.HBase_pb2 import ColumnFamilySchema, ColumnFamilyDescriptor
from hbasedriver.common.table_name import TableName

# Connect
conf = {"hbase.zookeeper.quorum": "localhost:2181"}
client = Client(conf)

# Create table
cf_info = ColumnFamilyDescriptor(name=b"info")
cf_info.max_versions = 1

cf_data = ColumnFamilyDescriptor(name=b"data")
cf_data.max_versions = 1

cfs = [
    ColumnFamilySchema(name=b"info", attributes=cf_info.SerializeToString()),
    ColumnFamilySchema(name=b"data", attributes=cf_data.SerializeToString())
]

admin = client.get_admin()
tn = TableName.value_of(b"default", b"users")
admin.create_table(tn, cfs)

# Get table
table = client.get_table(b"default", b"users")

# Put data
put = Put(b"user1")
put.add_column(b"info", b"name", b"Alice")
put.add_column(b"info", b"email", b"alice@example.com")
put.add_column(b"data", b"score", b"95")
table.put(put)

# Get data
get = Get(b"user1")
row = table.get(get)
if row:
    print(row.get(b"info", b"name"))  # b'Alice'

# Scan
scan = Scan()
scanner = table.get_scanner(scan)
for row in scanner:
    print(row.rowkey, row)
scanner.close()

# Delete
delete = Delete(b"user1")
delete.add_family(b"info")
delete.add_family(b"data")
table.delete(delete)

# Drop table
admin.disable_table(tn)
admin.delete_table(tn)
```

## Common Migration Patterns

### Connection Context Manager

#### happybase

```python
with happybase.Connection() as connection:
    table = connection.table('my_table')
    # do work
```

#### python-hbase-driver

```python
# python-hbase-driver doesn't have a context manager
# But you can create your own:
class HBaseConnection:
    def __init__(self, conf):
        self.client = Client(conf)

    def __enter__(self):
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Optional cleanup
        pass

# Usage
with HBaseConnection(conf) as client:
    table = client.get_table(b"default", b"my_table")
    # do work
```

### Error Handling

#### happybase

```python
try:
    table.row(b'nonexistent')
except happybase.NoConnectionsAvailable:
    print("Connection error")
```

#### python-hbase-driver

```python
from hbasedriver.exceptions.TableNotFoundException import TableNotFoundException

try:
    table.get(Get(b"row1"))
except TableNotFoundException:
    print("Table not found")
except Exception as e:
    print(f"Error: {e}")
```

### Namespace Support

#### happybase

```python
# Limited namespace support
# Tables are typically in 'default' namespace
```

#### python-hbase-driver

```python
# Full namespace support
admin.create_namespace(b"my_namespace")

# Create table in namespace
tn = TableName.value_of(b"my_namespace", b"my_table")
admin.create_table(tn, cfs)

# Get table from namespace
table = client.get_table(b"my_namespace", b"my_table")
```

## Performance Considerations

1. **Connection Reuse**: python-hbase-driver doesn't have built-in connection pooling like happybase. Reuse client and table objects.

2. **Batch Operations**: happybase's `batch()` API is more efficient. python-hbase-driver will have batch operations in a future release.

3. **Memory Management**: Always close scanners in python-hbase-driver to free server resources.

4. **Protocol**: python-hbase-driver uses native HBase RPC, which is generally more efficient than Thrift.

## Unsupported Features

The following happybase features are not yet available in python-hbase-driver:

- `table.batch()` - Use loops for now (batch operations coming soon)
- `table.batch()` context manager - Coming soon
- `table.counter_*` - Counter operations not yet implemented
- `table.families()` - Use `admin.describe_table()` instead
- `connection.create_table()` prefixes - Use full TableName API
- Connection pooling - Manual (reuse objects)

## Additional Resources

- [API Reference](api_reference.md)
- [Advanced Usage](advanced_usage.md)
- [Performance Guide](performance_guide.md)
- [HBase Book](https://hbase.apache.org/book.html)
