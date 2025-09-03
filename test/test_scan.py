import random
import time

import pytest

from hbasedriver.client.client import Client
from hbasedriver.exceptions.RemoteException import TableExistsException
from hbasedriver.operations import Scan
from hbasedriver.operations.column_family_builder import ColumnFamilyDescriptorBuilder
from hbasedriver.operations.put import Put
from hbasedriver.operations.get import Get
from hbasedriver.operations.delete import Delete
from hbasedriver.table_name import TableName

conf = {"hbase.zookeeper.quorum": "127.0.0.1"}


@pytest.fixture(scope="module")
def admin():
    client = Client(conf)
    return client.get_admin()


@pytest.fixture(scope="module")
def client():
    client = Client(conf)
    return client


@pytest.fixture(scope="module")
def table_name() -> TableName:
    return TableName.value_of(b"", b"test_table")


@pytest.fixture(scope="module")
def column_families():
    cf1 = ColumnFamilyDescriptorBuilder(b"cf1").build()
    cf2 = ColumnFamilyDescriptorBuilder(b"cf2").build()
    return [cf1, cf2]


def create_default_test_table(admin):
    # Define column families
    cf1 = ColumnFamilyDescriptorBuilder(b"cf1").build()
    cf2 = ColumnFamilyDescriptorBuilder(b"cf2").build()
    column_families = [cf1, cf2]
    admin.create_table(TableName.value_of(b"", b"test_table"), column_families)


@pytest.fixture(scope="module")
def table():
    client = Client(conf)
    admin = client.get_admin()
    table_name = TableName.value_of(b"", b"test_table")

    # Ensure a clean test table
    if admin.table_exists(table_name):
        try:
            admin.disable_table(table_name)
        except Exception:
            pass  # ignore if already disabled
        admin.delete_table(table_name)

    create_default_test_table(admin)
    return client.get_table(table_name.ns, table_name.tb)


@pytest.fixture(scope='module', autouse=True)
def cleanup():
    yield
    client = Client(conf)
    admin = client.get_admin()
    table_name = TableName.value_of(b"", b"test_table")

    # clean up by deleting the test table.
    if admin.table_exists(table_name):
        try:
            admin.disable_table(table_name)
        except Exception:
            pass  # ignore if already disabled
        admin.delete_table(table_name)


def test_scan(client):
    table = client.get_table(b"", b"test_table")
    table.put(Put(b'scan_row_key1').add_column(b'cf1', b'qf1', b'666').add_column(b'cf2', b'qf16', b'6666').add_column(
        b'cf2', b'qf17777', b'666'))
    table.put(Put(b'scan_row_key2').add_column(b'cf1', b'qf1', b'666').add_column(b'cf2', b'qf16', b'6666').add_column(
        b'cf2', b'qf17777', b'666'))
    table.put(Put(b'scan_row_key3').add_column(b'cf1', b'qf1', b'666').add_column(b'cf2', b'qf16', b'6666').add_column(
        b'cf2', b'qf17777', b'666'))
    scan = Scan(b"scan_row_key1").add_family(b'cf1').add_family(b'cf2')
    resp = table.scan(scan)
    for i in resp:
        print(i)
