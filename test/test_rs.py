from src.hbasedriver.region_name import RegionName
from src.hbasedriver.regionserver import RsConnection
from src.hbasedriver.zk import locate_meta_region

host = "127.0.0.1"
port = 16020


def test_locate_region():
    host, port = locate_meta_region(["127.0.0.1"])
    rs = RsConnection()
    rs.connect(host, port)

    resp: RegionName = rs.locate_region("", "test_table", "000")
    assert resp.host
    assert resp.port
