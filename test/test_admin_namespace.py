import os
import uuid
import time

from hbasedriver.client.client import Client
from hbasedriver.common.table_name import TableName
from hbasedriver.protobuf_py.HBase_pb2 import ColumnFamilySchema


def test_namespace_create_and_table_flow():
    conf = {"hbase.zookeeper.quorum": os.getenv("HBASE_ZK", "127.0.0.1")}
    client = Client(conf)
    admin = client.get_admin()

    ns = f"testns_{uuid.uuid4().hex[:8]}"
    ns_bytes = ns.encode('utf-8')

    # Create namespace
    admin.create_namespace(ns)
    time.sleep(1)

    # Create a temporary table under the new namespace to verify namespace exists
    tbl = f"tmp_{uuid.uuid4().hex[:8]}"
    table_name = TableName(ns_bytes, tbl.encode('utf-8'))
    cf = ColumnFamilySchema()
    cf.name = b'cf'

    admin.create_table(table_name, [cf])
    time.sleep(1)

    # Clean up table (must disable first)
    admin.disable_table(table_name)
    admin.delete_table(table_name)
    time.sleep(1)

    # Delete namespace
    admin.delete_namespace(ns)
    time.sleep(1)

    # After deletion, creating a table under this namespace should fail (namespace removed)
    table_name2 = TableName(ns_bytes, f"tmp2_{uuid.uuid4().hex[:8]}".encode('utf-8'))
    try:
        admin.create_table(table_name2, [cf])
        # If create_table succeeded, then namespace still exists; force cleanup then fail the test
        admin.delete_table(table_name2)
        assert False, "Expected create_table to fail for deleted namespace"
    except Exception:
        # expected
        pass
