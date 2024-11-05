from hbasedriver.common.table_name import TableName
from src.hbasedriver.zk import locate_meta_region
from src.hbasedriver.zk_connection_registry import ZKConnectionRegistry


def test_locate_meta():
    print("start testing locate meta")
    host, port = locate_meta_region(["127.0.0.1"])
    assert host
    assert port == 16020


def test_zk_registry():
    conf = {"hbase.zookeeper.quorum": ["127.0.0.1"]}
    zk = ZKConnectionRegistry(conf)
    res = zk.get_meta_region_locations()
    assert res.locations[0].server_name.host == 'localhost'
    assert res.locations[0].server_name.port == 16020
    assert res.locations[0].region_info.table_name == TableName.META_TABLE_NAME
    print(res)
