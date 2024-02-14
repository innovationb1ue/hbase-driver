from src.hbasedriver.client.client import Client
from src.hbasedriver.regionserver import RsConnection
from src.hbasedriver.zk import locate_meta_region


def test_put():
    client = Client(["127.0.0.1"])
    table = client.get_table("", "test_table")
    resp = table.put("row1", {"cf1": {"qf1": "123123"}})

    print(resp)


def test_get():
    client = Client(["127.0.0.1"])
    table = client.get_table("", "test_table")

    resp = table.put("row666", {"cf1": {"qf1": "123123"}})
    print(resp)

    row = table.get("row666", {"cf1": ["qf1"]})
    assert row.get('cf1', 'qf1') == b'123123'
    assert row.rowkey == b'row666'


def test_delete():
    client = Client(["127.0.0.1"])
    table = client.get_table("", "test_table")

    resp = table.put("row666", {"cf1": {"qf1": "123123"}})
    assert resp

    res = table.get("row666", {"cf1": []})
    assert res.get("cf1", "qf1") == b"123123"

    processed = table.delete("row666", {"cf1": ["qf1"]})
    assert processed

    res_after_delete = table.get("row666", {"cf1": []})
    assert res_after_delete is None
