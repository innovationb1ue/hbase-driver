from hbasedriver.client.client import Client
from hbasedriver.operations.delete import Delete
from hbasedriver.operations.get import Get
from hbasedriver.operations.put import Put


def test_put():
    client = Client(["127.0.0.1"])
    table = client.get_table("", "test_table")
    resp = table.put(Put(b"row1").add_column(b'cf1', b'qf1', b'123123'))

    print(resp)


def test_get():
    client = Client(["127.0.0.1"])
    table = client.get_table("", "test_table")

    resp = table.put(Put(b'row666').add_column(b'cf1', b'qf2', b'123123'))
    print(resp)

    row = table.get(Get(b'row666').add_family(b'cf1'))
    assert row.get(b'cf1', b'qf2') == b'123123'
    assert row.rowkey == b'row666'


def test_delete():
    client = Client(["127.0.0.1"])
    table = client.get_table("", "test_table")

    resp = table.put(Put(b"row666").add_column(b"cf1", b'qf1', b'123123'))
    assert resp

    res = table.get(Get(b"row666").add_family(b"cf1"))
    assert res.get(b"cf1", b"qf1") == b"123123"

    processed = table.delete(Delete(b"row666").add_family(b'cf1'))
    assert processed

    res_after_delete = table.get(Get(b"row666").add_family(b"cf1"))
    assert res_after_delete is None


def test_delete_version():
    client = Client(["127.0.0.1"])
    table = client.get_table("", "test_table")
    ts = 66669999
    resp = table.put(Put(b"row779").add_column(b"cf1", b'qf1', b'123123', ts=ts))
    assert resp

    res = table.get(Get(b"row779").add_family(b"cf1"))
    assert res.get(b"cf1", b"qf1") == b"123123"

    processed = table.delete(Delete(b"row779").add_family_version(b'cf1', ts=ts))
    assert processed

    res_after_delete = table.get(Get(b"row779").add_family(b"cf1"))
    assert res_after_delete is None
