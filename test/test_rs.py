from hbasedriver.meta_server import MetaRsConnection
from hbasedriver.region import Region
from hbasedriver.regionserver import RsConnection
from hbasedriver.zk import locate_meta_region
import os
from hbasedriver.client.client import Client
from hbasedriver.table_name import TableName
from hbasedriver.operations.column_family_builder import ColumnFamilyDescriptorBuilder


def test_locate_region():
    host, port = locate_meta_region([os.getenv("HBASE_ZK", "127.0.0.1")])
    meta_rs = MetaRsConnection().connect(host, port)

    # Ensure the test table exists
    client = Client({"hbase.zookeeper.quorum": os.getenv("HBASE_ZK", "127.0.0.1")})
    admin = client.get_admin()
    table_name = TableName.value_of(b"", b"test_table")
    if not admin.table_exists(table_name):
        cf1 = ColumnFamilyDescriptorBuilder(b"cf1").build()
        cf2 = ColumnFamilyDescriptorBuilder(b"cf2").build()
        admin.create_table(table_name, [cf1, cf2])

    resp: Region = meta_rs.locate_region("", "test_table", "000")
    assert resp.host
    assert resp.port
