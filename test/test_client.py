import random
import time

import pytest

from hbasedriver.client.client import Client
from hbasedriver.exceptions.RemoteException import TableExistsException
from hbasedriver.operations.column_family_builder import ColumnFamilyDescriptorBuilder
from hbasedriver.operations.put import Put
from hbasedriver.operations.get import Get
from hbasedriver.operations.delete import Delete
from hbasedriver.table_name import TableName


@pytest.fixture(scope="module")
def admin():
    client = Client(["127.0.0.1"])
    return client.get_admin()


@pytest.fixture(scope="module")
def table_name() -> TableName:
    return TableName.value_of(b"", b"test_table")


@pytest.fixture(scope="module")
def column_families():
    cf1 = ColumnFamilyDescriptorBuilder(b"cf1").build()
    cf2 = ColumnFamilyDescriptorBuilder(b"cf2").build()
    return [cf1, cf2]


@pytest.fixture(scope="module")
def table():
    client = Client(["127.0.0.1"])
    admin = client.get_admin()
    table_name = TableName.value_of(b"", b"test_table")

    # Define column families
    cf1 = ColumnFamilyDescriptorBuilder(b"cf1").build()
    cf2 = ColumnFamilyDescriptorBuilder(b"cf2").build()
    column_families = [cf1, cf2]

    # Ensure a clean test table
    if admin.table_exists(table_name):
        try:
            admin.disable_table(table_name)
        except Exception:
            pass  # ignore if already disabled
        admin.delete_table(table_name)
    admin.create_table(table_name, column_families)
    return client.get_table(table_name.ns, table_name.tb)


@pytest.fixture(scope='module', autouse=True)
def cleanup():
    yield
    client = Client(["127.0.0.1"])
    admin = client.get_admin()
    table_name = TableName.value_of(b"", b"test_table")

    # clean up by deleting the test table.
    if admin.table_exists(table_name):
        try:
            admin.disable_table(table_name)
        except Exception:
            pass  # ignore if already disabled
        admin.delete_table(table_name)


def test_admin_create_and_check(admin):
    table_name = TableName.value_of(b"", b"test_admin_check")

    # Define column families
    cf1 = ColumnFamilyDescriptorBuilder(b"cf1").build()
    cf2 = ColumnFamilyDescriptorBuilder(b"cf2").build()
    column_families = [cf1, cf2]

    # Ensure clean state
    if admin.table_exists(table_name):
        try:
            admin.disable_table(table_name)
        except Exception:
            pass
        admin.delete_table(table_name)

    # Create fresh table
    admin.create_table(table_name, column_families)

    # Assert table exists
    assert admin.table_exists(table_name)

    # Describe and assert schema
    desc = admin.describe_table(table_name)
    assert len(desc.table_schema) == 1

    schema = desc.table_schema[0]
    cf_names = set(cf.name for cf in schema.column_families)

    assert cf_names == {b"cf1", b"cf2"}


def test_admin_disable_enable(admin, table_name):
    # Ensure table starts in enabled state
    if not admin.is_table_enabled(table_name):
        try:
            admin.enable_table(table_name)
        except Exception:
            pass  # It's possible the table was already in transition

    # Disable and verify
    admin.disable_table(table_name)
    assert admin.is_table_disabled(table_name)

    # Enable and verify
    admin.enable_table(table_name)
    assert admin.is_table_enabled(table_name)


def test_admin_delete(admin, table_name):
    if not admin.table_exists(table_name):
        admin.create_table(table_name)

    if not admin.is_table_disabled(table_name):
        admin.disable_table(table_name)

    admin.delete_table(table_name)
    assert not admin.table_exists(table_name)


def test_put_and_get(table):
    rowkey = b"row1"
    table.put(Put(rowkey).add_column(b"cf1", b"qf1", b"value1"))

    result = table.get(Get(rowkey).add_family(b"cf1"))
    assert result.rowkey == rowkey
    assert result.get(b"cf1", b"qf1") == b"value1"


def test_delete_column(table):
    rowkey = b"row2"
    table.put(Put(rowkey).add_column(b"cf1", b"qf2", b"val"))
    assert table.get(Get(rowkey).add_family(b"cf1")).get(b"cf1", b"qf2") == b"val"
    time.sleep(0.5)
    table.delete(Delete(rowkey).add_column(b"cf1", b"qf2"))
    assert table.get(Get(rowkey).add_family(b"cf1")) is None


def test_delete_entire_row(table):
    rowkey = b"row3"
    table.put(Put(rowkey).add_column(b"cf1", b"qf1", b"1"))
    table.put(Put(rowkey).add_column(b"cf1", b"qf2", b"2"))
    table.put(Put(rowkey).add_column(b"cf2", b"qf3", b"3"))

    assert table.get(Get(rowkey).add_family(b"cf1")) is not None
    table.delete(Delete(rowkey))
    assert table.get(Get(rowkey).add_family(b"cf1").add_family(b"cf2")) is None


def test_delete_specific_version(table):
    rowkey = b"row" + str(random.randint(10000, 99999)).encode()
    ts = 88888888

    table.put(Put(rowkey).add_column(b"cf1", b"qf1", b"v1", ts=ts))
    result = table.get(Get(rowkey).add_family(b"cf1"))
    assert result.get(b"cf1", b"qf1") == b"v1"

    table.delete(Delete(rowkey).add_family_version(b"cf1", ts))
    assert table.get(Get(rowkey).add_family(b"cf1")) is None
