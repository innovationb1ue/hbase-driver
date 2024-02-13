from src.hbasedriver.zk import locate_meta_region


def test_locate_meta():
    print("start testing locate meta")
    host, port = locate_meta_region(["127.0.0.1"])
    assert host
    assert port == 16020
