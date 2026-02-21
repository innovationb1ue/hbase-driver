"""
Simple large dataset scan test to debug scan issues.
"""

import pytest
import os
import time
from hbasedriver.client.client import Client
from hbasedriver.operations.column_family_builder import ColumnFamilyDescriptorBuilder
from hbasedriver.operations.put import Put
from hbasedriver.operations.scan import Scan
from hbasedriver.table_name import TableName

conf = {"hbase.zookeeper.quorum": os.getenv("HBASE_ZK", "127.0.0.1")}


def test_simple_large_scan():
    """Simple test: create table, insert rows, scan them."""
    client = Client(conf)
    admin = client.get_admin()
    
    table_name = TableName.value_of(b"", b"test_simple_large_scan")
    print(f"Table name created: ns={repr(table_name.ns)}, tb={repr(table_name.tb)}")
    print(f"Table name str: {table_name}")
    print(f"Table name __repr__: {repr(table_name)}")
    
    # Clean up
    if admin.table_exists(table_name):
        try:
            admin.disable_table(table_name)
        except:
            pass
        admin.delete_table(table_name)
    
    # Create table
    cf = ColumnFamilyDescriptorBuilder(b"cf").build()
    print(f"Creating table {table_name.ns}.{table_name.tb}...")
    admin.create_table(table_name, [cf])
    
    # Wait for meta with longer delay
    print("Waiting for table to propagate to meta...")
    for attempt in range(5):
        time.sleep(2)
        try:
            # Try to list tables to ensure meta is up to date
            tables = admin.list_table_names()
            print(f"  Tables in cluster: {len(tables)}")
            if table_name in tables:
                print(f"  ✓ Table found in meta after {(attempt+1)*2}s")
                break
            else:
                print(f"  ✗ Table not in meta yet ({attempt+1}/5)")
        except Exception as e:
            print(f"  ! Error checking meta: {e} ({attempt+1}/5)")
    
    # Get table reference
    table = client.get_table(table_name.ns, table_name.tb)
    print(f"Table reference created: {table}")
    
    # Insert 100 rows
    print("Inserting 100 rows...")
    for i in range(100):
        key = f"row_{i:05d}".encode()
        put = Put(key).add_column(b"cf", b"col", f"val_{i}".encode())
        table.put(put)
    print("Insert complete")
    
    # Verify table exists and can be fetched
    print("Verifying table with get...")
    from hbasedriver.operations.get import Get
    get_result = table.get(Get(b"row_00050"))
    print(f"Get result: {get_result}")
    assert get_result is not None
    
    # Try to scan (use explicit start_row to avoid meta lookup edge cases)
    print("Starting scan...")
    scan = Scan(b"row_00000").add_family(b"cf")
    count = 0
    try:
        for row in table.scan(scan):
            count += 1
            if count <= 3:
                print(f"  Row: {row.row}")
    except Exception as e:
        print(f"Scan failed: {e}")
        raise

    print(f"Scan complete: {count} rows")
    assert count == 100
    
    # Cleanup
    try:
        admin.disable_table(table_name)
    except:
        pass
    admin.delete_table(table_name)
