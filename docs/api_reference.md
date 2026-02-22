# API Reference

This document provides a comprehensive reference for the python-hbase-driver API.

## Table of Contents

- [Client](#client)
- [Table](#table)
- [Admin](#admin)
- [Operations](#operations)
- [Filters](#filters)
- [Exceptions](#exceptions)

## Client

The `Client` class is the main entry point for interacting with HBase.

### Constructor

```python
from hbasedriver.client.client import Client

conf = {
    "hbase.zookeeper.quorum": "localhost"
}
client = Client(conf)
```

### Methods

#### `get_admin() -> Admin`

Returns an `Admin` instance for administrative operations.

```python
admin = client.get_admin()
```

#### `get_table(ns: bytes, tb: bytes) -> Table`

Returns a `Table` instance for data operations.

```python
table = client.get_table(b"default", b"my_table")
# or using string namespace
table = client.get_table(None, b"my_table")
```

#### `create_table(ns: bytes, tb: bytes, column_families: List[ColumnFamilyDescriptor], split_keys: List[bytes] = None)`

Creates a new table with the specified column families.

```python
from hbasedriver.protobuf_py.HBase_pb2 import ColumnFamilySchema, ColumnFamilyDescriptor

cf = ColumnFamilyDescriptor(name=b"cf")
cf.max_versions = 3
cfs = [ColumnFamilySchema(name=b"cf", attributes=cf.SerializeToString())]

client.create_table(b"default", b"my_table", cfs)
```

#### `delete_table(ns: bytes, tb: bytes)`

Deletes a table. The table must be disabled first.

```python
client.delete_table(b"default", b"my_table")
```

#### `disable_table(ns: bytes, tb: bytes)`

Disables a table.

```python
client.disable_table(b"default", b"my_table")
```

#### `enable_table(ns: bytes, tb: bytes)`

Enables a table.

```python
client.enable_table(b"default", b"my_table")
```

## Table

The `Table` class provides data operations for a specific table.

### Methods

#### `put(put: Put) -> bool`

Puts a row into the table.

```python
from hbasedriver.operations.put import Put

put = Put(b"row1")
put.add_column(b"cf", b"col1", b"value1")
table.put(put)
```

#### `get(get: Get) -> Row | None`

Gets a row from the table.

```python
from hbasedriver.operations.get import Get

get = Get(b"row1")
get.add_column(b"cf", b"col1")
row = table.get(get)
if row:
    value = row.get(b"cf", b"col1")
```

#### `delete(delete: Delete) -> bool`

Deletes a row or specific columns.

```python
from hbasedriver.operations.delete import Delete

delete = Delete(b"row1")
delete.add_column(b"cf", b"col1")
table.delete(delete)
```

#### `scan(scan: Scan) -> ResultScanner`

Creates a scanner for iterating over rows.

```python
from hbasedriver.operations.scan import Scan

scan = Scan()
scan.add_column(b"cf", b"col1")
scanner = table.scan(scan)

for row in scanner:
    print(row.rowkey)
    value = row.get(b"cf", b"col1")

scanner.close()
```

#### `get_scanner(scan: Scan) -> ResultScanner`

Creates a scanner with cluster-aware region crossing.

```python
scan = Scan(b"start_row", b"end_row")
scanner = table.get_scanner(scan)
```

#### `scan_page(scan: Scan, page_size: int) -> Tuple[List[Row], bytes | None]`

Performs stateless pagination, returning rows and a resume key.

```python
rows, resume_key = table.scan_page(Scan(), 10)
if resume_key:
    # Continue from where we left off
    next_scan = Scan(start_row=resume_key, include_start_row=False)
```

## Admin

The `Admin` class provides administrative operations for HBase.

### Methods

#### `create_table(table_name: TableName, column_families: List[ColumnFamilySchema], split_keys: List[bytes] = None)`

Creates a new table.

```python
from hbasedriver.common.table_name import TableName
from hbasedriver.protobuf_py.HBase_pb2 import ColumnFamilySchema, ColumnFamilyDescriptor

tn = TableName.value_of(b"default", b"my_table")
cf = ColumnFamilyDescriptor(name=b"cf")
cf.max_versions = 3
cfs = [ColumnFamilySchema(name=b"cf", attributes=cf.SerializeToString())]

admin.create_table(tn, cfs)
```

#### `delete_table(table_name: TableName)`

Deletes a table.

```python
admin.delete_table(tn)
```

#### `disable_table(table_name: TableName, timeout: int = 60)`

Disables a table.

```python
admin.disable_table(tn)
```

#### `enable_table(table_name: TableName, timeout: int = 60)`

Enables a table.

```python
admin.enable_table(tn)
```

#### `table_exists(table_name: TableName) -> bool`

Checks if a table exists.

```python
if admin.table_exists(tn):
    print("Table exists")
```

#### `list_tables(pattern: str = ".*", include_sys_tables: bool = False)`

Lists tables matching the pattern.

```python
tables = admin.list_tables()
```

#### `describe_table(table_name: TableName)`

Gets table descriptor.

```python
desc = admin.describe_table(tn)
```

#### `create_namespace(namespace: bytes | str)`

Creates a namespace.

```python
admin.create_namespace(b"my_namespace")
```

#### `delete_namespace(namespace: bytes | str)`

Deletes a namespace.

```python
admin.delete_namespace(b"my_namespace")
```

#### `list_namespaces() -> List[str]`

Lists all namespaces.

```python
namespaces = admin.list_namespaces()
```

#### `truncate_table(table_name: TableName, preserve_splits: bool = False, timeout: int = 60)`

Truncates a table (deletes all data).

```python
admin.truncate_table(tn)
```

## Operations

### Put

```python
from hbasedriver.operations.put import Put

put = Put(b"rowkey")
put.add_column(b"cf", b"qualifier", b"value", timestamp=1234567890)
```

### Get

```python
from hbasedriver.operations.get import Get

get = Get(b"rowkey")
get.add_column(b"cf", b"qualifier")
get.set_max_versions(3)
get.set_time_range(0, 1234567890)
```

### Scan

```python
from hbasedriver.operations.scan import Scan

scan = Scan()
scan.set_start_row(b"start")
scan.set_stop_row(b"stop")
scan.set_reversed(False)
scan.add_column(b"cf", b"qualifier")
scan.set_limit(100)
```

### Delete

```python
from hbasedriver.operations.delete import Delete

delete = Delete(b"rowkey")
delete.add_column(b"cf", b"qualifier")
delete.add_family(b"cf")
```

## Filters

### RowFilter

```python
from hbasedriver.filter.rowfilter import RowFilter
from hbasedriver.filter.comparer import CompareOperator

filter = RowFilter(CompareOperator.EQUAL, b"rowkey_pattern")
scan.set_filter(filter)
```

### CompareOperator

Available operators:
- `LESS`
- `LESS_OR_EQUAL`
- `EQUAL`
- `NOT_EQUAL`
- `GREATER_OR_EQUAL`
- `GREATER`
- `NO_OP`

## Exceptions

- `TableNotFoundException` - Table does not exist
- `IOException` - General I/O errors
- `RemoteException` - Remote HBase server errors

## Type Hints

The library includes type hints for better IDE support and static type checking. Enable type checking with mypy:

```bash
mypy src/hbasedriver/
```
