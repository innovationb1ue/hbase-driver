import time

import pytest

from hbasedriver.client import Client
from hbasedriver.exceptions.RemoteException import TableExistsException
from hbasedriver.operations.column_family_builder import ColumnFamilyDescriptorBuilder
from src.hbasedriver.master import MasterConnection

host = "127.0.0.1"
port = 16000
ms_test_table = b'test_table_master'
order_idx = 0

# tests in this file rely on the execution orders.

order_idx += 1

# before all test in this file: drop table first.
try:
    client = Client([host])
    client.disable_table(b'', ms_test_table)
    client.delete_table(b'', ms_test_table)
except:
    pass


@pytest.mark.order(order_idx)
def test_connect_master():
    client = MasterConnection()
    client.connect(host, port)


order_idx += 1


@pytest.mark.order(order_idx)
def test_create_table_with_attributes():
    client = Client([host])

    # Define column families
    cf1_builder = ColumnFamilyDescriptorBuilder(b"cf1")
    cf1_builder.set_compression_type(b"SNAPPY")
    cf1_builder.set_max_versions(5)
    cf1_builder.set_time_to_live(86400)
    cf1_builder.set_block_size(65536)
    cf1_descriptor = cf1_builder.build()

    cf2_builder = ColumnFamilyDescriptorBuilder(b"cf2")
    cf2_builder.set_compression_type(b"LZO")
    cf2_builder.set_max_versions(3)
    cf2_builder.set_time_to_live(3600)
    cf2_builder.set_block_size(32768)
    cf2_descriptor = cf2_builder.build()

    column_families = [cf1_descriptor, cf2_descriptor]

    try:
        client.create_table(b"", b"test_table_master", column_families, split_keys=[b"111111", b"222222", b"333333"])
    except TableExistsException:
        print("table already exist. ")
        pass
    time.sleep(3)


order_idx += 1


@pytest.mark.order(order_idx)
def test_describe_table():
    client = Client([host])
    resp = client.describe_table(b"", b"test_table_master")
    assert len(resp.table_schema) >= 1
    table_names = [i.table_name.qualifier for i in resp.table_schema]
    assert b'test_table_master' in table_names
    print(resp)


order_idx += 1


@pytest.mark.order(order_idx)
def test_disable_table():
    client = Client([host])
    client.disable_table(b'', b"test_table_master")
    client.enable_table(b'', b"test_table_master")
    client.disable_table(b'', b"test_table_master")
    time.sleep(2)


order_idx += 1


@pytest.mark.order(order_idx)
def test_delete_table():
    client = Client([host])
    client.delete_table(b'', b"test_table_master")
