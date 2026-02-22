#!/usr/bin/env python3
"""
hbase-driver - Complete Usage Examples

This example demonstrates all major features of the hbase-driver:
- Basic CRUD operations (Put, Get, Delete, Scan)
- Advanced features (Batch, CheckAndPut, Increment)
- Filter usage for server-side filtering
- DDL operations (Create, Disable, Enable, Delete, Truncate)
- Connection management and resource cleanup
- Cache invalidation after table modifications

To run this example:
    python3 complete_example.py
"""

from hbasedriver.client.client import Client
from hbasedriver.hbase_constants import HConstants
from hbasedriver.operations.put import Put
from hbasedriver.operations.get import Get
from hbasedriver.operations.delete import Delete
from hbasedriver.operations.scan import Scan
from hbasedriver.operations.batch import BatchGet, BatchPut
from hbasedriver.operations.increment import Increment, CheckAndPut

# Filters
from hbasedriver.filter import (
    FilterList,
    FilterListOperator,
    RowFilter,
    PrefixFilter,
    FamilyFilter,
    QualifierFilter,
    ValueFilter,
    PageFilter,
    KeyOnlyFilter,
    ColumnPrefixFilter,
    SingleColumnValueFilter,
    FirstKeyOnlyFilter,
    MultipleColumnPrefixFilter,
    TimestampsFilter,
)
from hbasedriver.filter.compare_operator import CompareOperator
from hbasedriver.filter.binary_comparator import BinaryComparator
from hbasedriver.filter.binary_prefix_comparator import BinaryPrefixComparator

# Admin and DDL
from hbasedriver.client.admin import ColumnFamilyDescriptorBuilder
from hbasedriver.table_name import TableName


# ============================================================================
# Configuration
# ============================================================================

# Initialize client with ZooKeeper configuration
config = {
    HConstants.ZOOKEEPER_QUORUM: "localhost:2181",
    HConstants.CONNECTION_POOL_SIZE: 10,
    HConstants.CONNECTION_IDLE_TIMEOUT: 300,
}

print("=" * 60)
print("HBase Driver - Complete Usage Examples")
print("=" * 60)

# ============================================================================
# Initialize Client
# ============================================================================

print("\n[1] Initializing client...")
client = Client(config)
print(f"   Connected to ZooKeeper: {config[HConstants.ZOOKEEPER_QUORUM]}")

# ============================================================================
# DDL Operations - Create Table
# ============================================================================

print("\n[2] DDL Operations - Creating table...")

# Define table name
TABLE_NAME = TableName.value_of(b"default", b"example_table")

# Get admin for DDL operations
admin = client.get_admin()

# Clean up if table exists
if admin.table_exists(TABLE_NAME):
    print("   Deleting existing table...")
    try:
        admin.disable_table(TABLE_NAME)
    except Exception:
        pass
    admin.delete_table(TABLE_NAME)

# Create table with column families
print("   Creating table with column families...")
cf1 = ColumnFamilyDescriptorBuilder(b"cf1").build()
cf2 = ColumnFamilyDescriptorBuilder(b"cf2").build()
admin.create_table(TABLE_NAME, [cf1, cf2])
print(f"   Created table: {TABLE_NAME}")

# Get table for data operations
table = client.get_table(TABLE_NAME.ns, TABLE_NAME.tb)

# ============================================================================
# Basic CRUD Operations
# ============================================================================

print("\n[3] Basic CRUD Operations...")

# Put - Insert data
print("   Putting data...")
table.put(Put(b"row1").add_column(b"cf1", b"name", b"Alice"))
table.put(Put(b"row1").add_column(b"cf1", b"age", b"30"))
table.put(Put(b"row2").add_column(b"cf1", b"name", b"Bob"))
table.put(Put(b"row2").add_column(b"cf2", b"email", b"bob@example.com"))
table.put(Put(b"row3").add_column(b"cf1", b"name", b"Charlie"))
print("   Inserted 3 rows")

# Get - Retrieve data
print("   Getting data...")
row = table.get(Get(b"row1"))
if row:
    print(f"   row1: {row.get(b'cf1', b'name')} (age: {row.get(b'cf1', b'age')})")

# Get with multiple columns
row = table.get(Get(b"row2").add_column(b"cf1", b"name").add_column(b"cf2", b"email"))
if row:
    print(f"   row2: {row.get(b'cf1', b'name')} ({row.get(b'cf2', b'email')})")

# Scan - Read multiple rows
print("   Scanning data...")
with table.scan(Scan()) as scanner:
    scan_count = 0
    for row in scanner:
        scan_count += 1
        print(f"   - {row.rowkey}")
print(f"   Scanned {scan_count} rows")

# Delete - Remove data
print("   Deleting data...")
table.delete(Delete(b"row3"))
print("   Deleted row3")

# ============================================================================
# Scan with Filters
# ============================================================================

print("\n[4] Scan with Filters...")

# Prefix Filter - Only rows with row key prefix
print("   Using PrefixFilter...")
scan = Scan(start_row=b"row", end_row=b"rox").set_filter(PrefixFilter(b"row"))
with table.scan(scan) as scanner:
    print(f"   PrefixFilter 'row': {[r.rowkey for r in scanner]}")

# Page Filter - Limit number of rows returned
print("   Using PageFilter...")
scan = Scan(start_row=b"row", end_row=b"rox").set_filter(PageFilter(2))
with table.scan(scan) as scanner:
    print(f"   PageFilter(2): {[r.rowkey for r in scanner]}")

# Column Prefix Filter - Only columns with qualifier prefix
print("   Using ColumnPrefixFilter...")
table.put(Put(b"user1").add_column(b"cf1", b"name_title", b"Mr").add_column(b"cf1", b"name_first", b"John"))
scan = Scan(start_row=b"user", end_row=b"usf").set_filter(ColumnPrefixFilter(b"name_"))
with table.scan(scan) as scanner:
    for row in scanner:
        print(f"   ColumnPrefixFilter: {row.rowkey} -> {dict(row.kv.get(b'cf1', {}))}")

# FilterList - Combine multiple filters
print("   Using FilterList (AND)...")
scan = Scan(start_row=b"row", end_row=b"rox").set_filter(
    FilterList(
        [PrefixFilter(b"row"), PageFilter(2)],
        FilterListOperator.MUST_PASS_ALL  # AND logic
    )
)
with table.scan(scan) as scanner:
    print(f"   FilterList(AND): {[r.rowkey for r in scanner]}")

# FirstKeyOnlyFilter - Return only first column per row
print("   Using FirstKeyOnlyFilter...")
scan = Scan(start_row=b"row", end_row=b"rox").set_filter(FirstKeyOnlyFilter())
with table.scan(scan) as scanner:
    print(f"   FirstKeyOnlyFilter: {[(r.rowkey, len(list(r.kv.values())[0])) for r in scanner]}")

# ============================================================================
# Advanced Operations
# ============================================================================

print("\n[5] Advanced Operations...")

# Batch Put - Insert multiple rows efficiently
print("   Batch Put...")
bp = BatchPut()
bp.add_put(Put(b"batch1").add_column(b"cf1", b"data", b"value1"))
bp.add_put(Put(b"batch2").add_column(b"cf1", b"data", b"value2"))
bp.add_put(Put(b"batch3").add_column(b"cf1", b"data", b"value3"))
results = table.batch_put(bp)
print(f"   Batch put {len(results)} rows")

# Batch Get - Retrieve multiple rows efficiently
print("   Batch Get...")
bg = BatchGet([b"batch1", b"batch2", b"batch3"]).add_column(b"cf1", b"data")
results = table.batch_get(bg)
print(f"   Batch get {len(results)} rows: {[r.rowkey for r in results]}")

# Increment - Atomic counter
print("   Increment operation...")
table.put(Put(b"counter").add_column(b"cf1", b"count", b"0"))
inc = Increment(b"counter").add_column(b"cf1", b"count", 1)
new_value = table.increment(inc)
print(f"   Incremented counter to: {new_value}")

# CheckAndPut - Conditional update
print("   CheckAndPut operation...")
table.put(Put(b"lock").add_column(b"cf1", b"lock", b""))
cap = CheckAndPut(b"lock")
cap.set_check(b"cf1", b"lock", b"")  # Check if lock is empty
cap.set_put(Put(b"lock").add_column(b"cf1", b"lock", b"held"))
success = table.check_and_put(cap)
print(f"   CheckAndPut acquired lock: {success}")

# ============================================================================
# Truncate Table and Cache Invalidation
# ============================================================================

print("\n[6] Truncate Table and Cache Invalidation...")

# Truncate removes all data from the table
print("   Truncating table...")
admin.truncate_table(TABLE_NAME, preserve_splits=False)
print("   Table truncated")

# IMPORTANT: After truncating, invalidate the region cache
# The table's region cache is stale after truncate (table was deleted and recreated)
table.invalidate_cache()
print("   Region cache invalidated")

# Verify data is gone
row = table.get(Get(b"row1"))
print(f"   Data after truncate: {'exists' if row else 'gone'}")

# ============================================================================
# More Advanced Filters
# ============================================================================

print("\n[7] Advanced Filter Examples...")

# Re-populate table for advanced filter examples
print("   Re-populating table...")
table.put(Put(b"alice").add_column(b"cf1", b"status", b"active").add_column(b"cf1", b"score", b"95"))
table.put(Put(b"bob").add_column(b"cf1", b"status", b"inactive").add_column(b"cf1", b"score", b"87"))
table.put(Put(b"charlie").add_column(b"cf1", b"status", b"active").add_column(b"cf1", b"score", b"92"))

# SingleColumnValueFilter - Filter rows by column value
print("   Using SingleColumnValueFilter...")
scan = Scan().set_filter(
    SingleColumnValueFilter(
        column_family=b"cf1",
        column_qualifier=b"status",
        compare_operator=CompareOperator.EQUAL,
        comparator=BinaryComparator(b"active"),
        filter_if_missing=False
    )
)
with table.scan(scan) as scanner:
    print(f"   Status='active': {[r.rowkey for r in scanner]}")

# RowFilter with GREATER comparison
print("   Using RowFilter with GREATER...")
scan = Scan().set_filter(
    RowFilter(CompareOperator.GREATER, BinaryComparator(b"alice"))
)
with table.scan(scan) as scanner:
    print(f"   RowFilter > 'alice': {[r.rowkey for r in scanner]}")

# MultipleColumnPrefixFilter - Multiple column qualifiers
print("   Using MultipleColumnPrefixFilter...")
table.put(Put(b"test").add_column(b"cf1", b"name_first", b"John").add_column(b"cf1", b"name_last", b"Doe"))
scan = Scan(start_row=b"test", end_row=b"tesu").set_filter(
    MultipleColumnPrefixFilter([b"name_first", b"name_last"])
)
with table.scan(scan) as scanner:
    for row in scanner:
        print(f"   MultipleColumnPrefixFilter: {row.rowkey} -> {list(row.kv.get(b'cf1', {}).keys())}")

# ValueFilter - Filter by cell value
print("   Using ValueFilter...")
scan = Scan().set_filter(
    ValueFilter(CompareOperator.GREATER, BinaryComparator(b"90"))
)
with table.scan(scan) as scanner:
    for row in scanner:
        score = row.get(b'cf1', b'score')
        if score:
            print(f"   ValueFilter > 90: {row.rowkey} -> {score}")

# KeyOnlyFilter - Return only keys (no values)
print("   Using KeyOnlyFilter...")
scan = Scan(start_row=b"test", end_row=b"tesu").set_filter(KeyOnlyFilter())
with table.scan(scan) as scanner:
    print(f"   KeyOnlyFilter: {[r.rowkey for r in scanner]}")

# ============================================================================
# Cleanup
# ============================================================================

print("\n[8] Cleanup...")

# Disable and delete table
print("   Disabling and deleting table...")
admin.disable_table(TABLE_NAME)
admin.delete_table(TABLE_NAME)
print("   Table deleted")

# ============================================================================
# Close Resources
# ============================================================================

print("\n[9] Closing connections...")
client.close()
print("   Client closed")

print("\n" + "=" * 60)
print("Example completed successfully!")
print("=" * 60)
print("\nKey Takeaways:")
print("  - Use context managers (with statements) for automatic cleanup")
print("  - Call table.invalidate_cache() after truncate to clear stale region cache")
print("  - Use filters for server-side filtering to reduce data transfer")
print("  - Use batch operations for efficiency with multiple rows")
print("  - Use CheckAndPut for conditional updates (atomic operations)")
print("=" * 60)
