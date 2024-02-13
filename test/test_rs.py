from src.hbasedriver.region_name import RegionName
from src.hbasedriver.regionserver import RsConnection
from src.hbasedriver.zk import locate_meta

host = "127.0.0.1"
port = 16020


def test_locate_region():
    host, port = locate_meta(["127.0.0.1"])
    rs = RsConnection()
    rs.connect(host, port)

    resp: RegionName = rs.locate_region("", "test_table", "000")
    assert resp.host
    assert resp.port


def test_put():
    host, port = locate_meta(["127.0.0.1"])
    rs = RsConnection()
    rs.connect(host, port)

    resp = rs.put("", "test_table", "row1", {"cf1": {"qf1": "123123"}})

    print(resp)


def test_get():
    host, port = locate_meta(["127.0.0.1"])
    rs = RsConnection()
    rs.connect(host, port)

    resp = rs.put("", "test_table", "row666", {"cf1": {"qf1": "123123"}})
    print(resp)

    row = rs.get("", "test_table", "row666", {"cf1": ["qf1"]})
    assert row.get('cf1', 'qf1') == b'123123'
    assert row.rowkey == b'row666'
