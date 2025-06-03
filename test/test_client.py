import random

import pytest

from hbasedriver.client.client import Client
from hbasedriver.exceptions.RemoteException import TableExistsException
from hbasedriver.operations import Scan
from hbasedriver.operations.column_family_builder import ColumnFamilyDescriptorBuilder
from hbasedriver.operations.delete import Delete
from hbasedriver.operations.get import Get
from hbasedriver.operations.put import Put
from hbasedriver.table_name import TableName


@pytest.fixture
def table():
    client = Client(["127.0.0.1"])
    admin = client.get_admin()

    # Define column families
    cf1_builder = ColumnFamilyDescriptorBuilder(b"cf1")
    cf1_descriptor = cf1_builder.build()
    cf2_builder = ColumnFamilyDescriptorBuilder(b"cf2")
    cf2_descriptor = cf2_builder.build()
    column_families = [cf1_descriptor, cf2_descriptor]
    try:
        admin.table_exists(TableName.value_of(b'', b'test_table'))
        client.create_table(b"", b"test_table", column_families)
    except TableExistsException:
        pass

    return client.get_table(None, "test_table")


def test_put(table):
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
    # WARNING: in hbase, if we delete a specific version, we can not insert it again before a major compaction.
    # so this test might fail if you run it twice with the same ts and rowkey.
    client = Client(["127.0.0.1"])
    table = client.get_table("", "test_table")
    postfix = str(random.randint(10000, 20000)).encode('utf-8')
    rowkey = b"row" + postfix

    ts = 666700001
    resp = table.put(Put(rowkey).add_column(b"cf1", b'qf1', b'123123', ts=ts))
    assert resp

    res = table.get(Get(rowkey).add_family(b"cf1"))
    assert res.get(b"cf1", b"qf1") == b"123123"

    processed = table.delete(Delete(rowkey).add_family_version(b'cf1', ts=ts))
    assert processed

    res_after_delete = table.get(Get(rowkey).add_family(b"cf1"))
    assert res_after_delete is None


def test_delete_whole_row(table):
    rowkey = b"rowx889577"

    table.put(Put(rowkey).add_column(b"cf1", b'qf1', b'666'))
    table.put(Put(rowkey).add_column(b"cf1", b'qf2', b'777'))
    resp = table.put(Put(rowkey).add_column(b"cf1", b'qf3', b'888'))
    resp = table.put(Put(rowkey).add_column(b"cf2", b'qf3', b'888'))
    resp = table.put(Put(rowkey).add_column(b"cf2", b'qf4', b'888'))
    assert resp

    res = table.get(Get(rowkey).add_family(b"cf1"))
    assert res.get(b"cf1", b"qf1") == b"666"

    processed = table.delete(Delete(rowkey))
    assert processed

    res_after_delete = table.get(Get(rowkey).add_family(b"cf1").add_family(b"cf2"))
    assert res_after_delete is None
