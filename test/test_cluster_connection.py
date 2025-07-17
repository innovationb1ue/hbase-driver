import pytest

from hbasedriver.client import Client
from hbasedriver.client.ConnectionImplementation import ConnectionImplementation
from hbasedriver.client.cluster_connection import ClusterConnection
from hbasedriver.operations.column_family_builder import ColumnFamilyDescriptorBuilder
from hbasedriver.table_name import TableName

table_name = TableName.value_of(b"", b"test_cluster_connection_table")


@pytest.fixture(scope="module")
def table():
    client = Client(["127.0.0.1"])
    admin = client.get_admin()
    table_name = TableName.value_of(b"", b"test_cluster_connection_table")

    # Define column families
    cf1 = ColumnFamilyDescriptorBuilder(b"cf1").build()
    cf2 = ColumnFamilyDescriptorBuilder(b"cf2").build()
    column_families = [cf1, cf2]

    # Ensure a clean test table
    if admin.table_exists(table_name):
        try:
            admin.disable_table(table_name)
        except Exception:
            pass  # ignore if already disabled
        admin.delete_table(table_name)
    admin.create_table(table_name, column_families)
    return client.get_table(table_name.ns, table_name.tb)


@pytest.fixture(scope='module', autouse=True)
def cleanup():
    yield
    client = Client(["127.0.0.1"])
    admin = client.get_admin()
    table_name = TableName.value_of(b"", b"test_cluster_connection_table")

    # clean up by deleting the test table.
    if admin.table_exists(table_name):
        try:
            admin.disable_table(table_name)
        except Exception:
            pass  # ignore if already disabled
        admin.delete_table(table_name)


def test_zk_registry():
    conf = {"hbase.zookeeper.quorum": ["127.0.0.1"]}
    conn = ConnectionImplementation(conf)
    res = conn.locate_meta()
    print(res)


def test_cluster_locate_key_in_meta():
    conf = {"hbase.zookeeper.quorum": ["127.0.0.1"]}
    conn = ClusterConnection(conf)

    s = conn.locate_region_in_meta(table_name, b'row1')
    print(s)
