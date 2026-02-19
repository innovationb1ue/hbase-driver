import os
import time
import uuid

from hbasedriver.client.client import Client
from hbasedriver.operations.put import Put
from hbasedriver.operations.get import Get
from hbasedriver.operations.column_family_builder import ColumnFamilyDescriptorBuilder
from hbasedriver.table_name import TableName


conf = {"hbase.zookeeper.quorum": os.getenv("HBASE_ZK", "127.0.0.1")}


def test_truncate_table():
    client = Client(conf)
    admin = client.get_admin()

    ns = b""
    tbl = f"truncate_test_{uuid.uuid4().hex[:8]}"
    tn = TableName.value_of(ns, tbl.encode('utf-8'))

    # Ensure clean
    if admin.table_exists(tn):
        try:
            admin.disable_table(tn)
        except Exception:
            pass
        admin.delete_table(tn)

    # Create
    cf = ColumnFamilyDescriptorBuilder(b"cf").build()
    admin.create_table(tn, [cf])

    table = client.get_table(tn.ns, tn.tb)

    # Put some data
    table.put(Put(b'row1').add_column(b'cf', b'q', b'v1'))
    table.put(Put(b'row2').add_column(b'cf', b'q', b'v2'))

    # Verify data exists
    assert table.get(Get(b'row1')) is not None

    # Truncate
    admin.truncate_table(tn, preserve_splits=False)

    # Wait until data removed; tolerate transient NotServingRegionException while regions are reassigned
    end = time.time() + 60
    success = False
    while time.time() < end:
        try:
            if table.get(Get(b'row1')) is None:
                success = True
                break
        except Exception:
            # transient - region may be being reassigned after truncate
            pass
        time.sleep(1)

    assert success, "Timed out waiting for truncate to remove data"

    # Additional sanity: ensure table still exists
    assert admin.table_exists(tn)

    # Clean up
    try:
        admin.disable_table(tn)
    except Exception:
        pass
    admin.delete_table(tn)
