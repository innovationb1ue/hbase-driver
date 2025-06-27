import random

import pytest

from hbasedriver.client.client import Client
from hbasedriver.exceptions.RemoteException import TableExistsException
from hbasedriver.filter.binary_comparator import BinaryComparator
from hbasedriver.filter.compare_operator import CompareOperator
from hbasedriver.filter.filter import Filter
from hbasedriver.filter.rowfilter import RowFilter
from hbasedriver.operations import Put
from hbasedriver.operations.column_family_builder import ColumnFamilyDescriptorBuilder
from hbasedriver.operations.scan import Scan

tb = b"test_scan_table"


@pytest.fixture
def table():
    client = Client(["127.0.0.1"])

    # Define column families
    cf1_builder = ColumnFamilyDescriptorBuilder(b"cf1")
    cf1_descriptor = cf1_builder.build()
    cf2_builder = ColumnFamilyDescriptorBuilder(b"cf2")
    cf2_descriptor = cf2_builder.build()
    column_families = [cf1_descriptor, cf2_descriptor]
    try:
        client.disable_table(b'', tb)
        client.delete_table(b'', tb)
    except Exception as e:
        pass

    try:
        client.create_table(None, tb, column_families)
    except TableExistsException:
        pass

    return client.get_table(None, "test_table")


@pytest.fixture(scope="module")
def cleanup():
    yield
    client = Client(["127.0.0.1"])
    client.disable_table(b"", tb)
    client.delete_table(b"", tb)


def test_scan():
    client = Client(["127.0.0.1"])
    table = client.get_table("", "test_table")
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


def test_scan_with_filter(table):
    client = Client(["127.0.0.1"])
    table = client.get_table("", tb)
    table.put(Put(b"row1").add_column(b'cf1', b'qf1', b'row1value'))
    scan = Scan()
    scan.set_filter(RowFilter(CompareOperator.EQUAL, BinaryComparator(b'row1')))
    res = table.scan(scan)
    for i in res:
        print(i)
