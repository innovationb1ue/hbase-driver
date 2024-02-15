from src.hbasedriver.client.client import Client
from src.hbasedriver.regionserver import RsConnection
from src.hbasedriver.zk import locate_meta_region


def test_put():
    client = Client(["127.0.0.1"])
    table = client.get_table("", "test_table")
    resp = table.put(b"row1", {"cf1": {"qf1": "123123"}})

    print(resp)


def test_get():
    client = Client(["127.0.0.1"])
    table = client.get_table("", "test_table")

    resp = table.put(b"row666", {"cf1": {"qf1": "123123"}})
    print(resp)

    row = table.get(b"row666", {"cf1": ["qf1"]})
    assert row.get(b'cf1', b'qf1') == b'123123'
    assert row.rowkey == b'row666'


def test_delete():
    client = Client(["127.0.0.1"])
    table = client.get_table("", "test_table")

    resp = table.put(b"row666", {"cf1": {"qf1": "123123"}})
    assert resp

    res = table.get(b"row666", {"cf1": []})
    assert res.get(b"cf1", b"qf1") == b"123123"

    processed = table.delete(b"row666", {"cf1": ["qf1"]})
    assert processed

    res_after_delete = table.get(b"row666", {"cf1": []})
    assert res_after_delete is None
