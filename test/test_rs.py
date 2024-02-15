from src.hbasedriver.meta_server import MetaRsConnection
from src.hbasedriver.region import Region
from src.hbasedriver.regionserver import RsConnection
from src.hbasedriver.zk import locate_meta_region


def test_locate_region():
    host, port = locate_meta_region(["127.0.0.1"])
    meta_rs = MetaRsConnection().connect(host, port)

    resp: Region = meta_rs.locate_region("", "test_table", "000")
    assert resp.host
    assert resp.port
