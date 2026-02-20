from hbasedriver.common.table_name import TableName
from hbasedriver.zk import locate_meta_region
from hbasedriver.zk_connection_registry import ZKConnectionRegistry


def test_locate_meta():
    print("start testing locate meta")
    import os
    host, port = locate_meta_region([os.getenv("HBASE_ZK", "127.0.0.1")])
    assert host
    assert port == 16020


def test_zk_registry():
    import os
    conf = {"hbase.zookeeper.quorum": os.getenv("HBASE_ZK", "127.0.0.1")}
    zk = ZKConnectionRegistry(conf)
    res = zk.get_meta_region_locations()
    assert res.locations[0].server_name.host
    assert res.locations[0].server_name.port == 16020
    assert res.locations[0].region_info.table_name == TableName.META_TABLE_NAME
    print(res)
