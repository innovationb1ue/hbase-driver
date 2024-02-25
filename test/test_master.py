import time

import pytest

from hbasedriver.exceptions.RemoteException import TableExistsException
from hbasedriver.operations.column_family_builder import ColumnFamilyDescriptorBuilder
from src.hbasedriver.master import MasterConnection

host = "127.0.0.1"
port = 16000
ms_test_table = 'test_table_master'


# tests in this file rely on the execution orders.

@pytest.mark.order(1)
def test_connect_master():
    client = MasterConnection()
    client.connect(host, port)


@pytest.mark.order(2)
def test_create_table_with_attributes():
    client = MasterConnection()
    client.connect(host, port)

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
        pass
    time.sleep(3)


@pytest.mark.order(3)
def test_disable_table():
    client = MasterConnection().connect(host, port)
    client.disable_table("", "test_table_master")
    client.enable_table("", "test_table_master")
    client.disable_table("", "test_table_master")
    time.sleep(1)


@pytest.mark.order(4)
def test_delete_table():
    client = MasterConnection()
    client.connect(host, port)
    client.delete_table("", "test_table_master")
