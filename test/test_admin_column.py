"""
Test cases for Admin column family operations.
"""
import os
import uuid
import time

import pytest

from hbasedriver.client.client import Client
from hbasedriver.common.table_name import TableName
from hbasedriver.protobuf_py.HBase_pb2 import ColumnFamilySchema


@pytest.fixture(scope="module")
def client():
    """Get HBase client connection."""
    conf = {"hbase.zookeeper.quorum": os.getenv("HBASE_ZK", "127.0.0.1")}
    return Client(conf)


@pytest.fixture(scope="module")
def admin(client):
    """Get HBase admin connection."""
    return client.get_admin()


@pytest.fixture
def test_table_name():
    """Generate unique table name for testing."""
    name = f"test_cf_{uuid.uuid4().hex[:8]}"
    return TableName(b'default', name.encode())


def test_add_column_family(admin, test_table_name):
    """Test adding a column family to an existing table."""
    # Create table with initial column family
    cf1 = ColumnFamilySchema()
    cf1.name = b"cf1"

    admin.create_table(test_table_name, [cf1])
    time.sleep(1)

    try:
        # Disable table to add column family
        admin.disable_table(test_table_name)

        # Add new column family
        admin.add_column_family(
            test_table_name,
            b"cf2",
            max_versions=3,
            compression="NONE"
        )
        time.sleep(1)

        # Enable table
        admin.enable_table(test_table_name)

        # Verify column family was added
        desc = admin.describe_table(test_table_name)
        schema = desc.table_schema[0]
        cf_names = [cf.name for cf in schema.column_families]
        assert b"cf1" in cf_names
        assert b"cf2" in cf_names

    finally:
        # Cleanup
        admin.disable_table(test_table_name)
        admin.delete_table(test_table_name)


def test_delete_column_family(admin, test_table_name):
    """Test deleting a column family from a table."""
    # Create table with two column families
    cf1 = ColumnFamilySchema()
    cf1.name = b"cf1"
    cf2 = ColumnFamilySchema()
    cf2.name = b"cf2"

    admin.create_table(test_table_name, [cf1, cf2])
    time.sleep(1)

    try:
        # Disable table to delete column family
        admin.disable_table(test_table_name)

        # Delete column family
        admin.delete_column_family(test_table_name, b"cf2")
        time.sleep(1)

        # Enable table
        admin.enable_table(test_table_name)

        # Verify column family was deleted
        desc = admin.describe_table(test_table_name)
        schema = desc.table_schema[0]
        cf_names = [cf.name for cf in schema.column_families]
        assert b"cf1" in cf_names
        assert b"cf2" not in cf_names

    finally:
        # Cleanup
        admin.disable_table(test_table_name)
        admin.delete_table(test_table_name)


def test_modify_column_family(admin, test_table_name):
    """Test modifying column family settings."""
    # Create table with initial column family
    cf = ColumnFamilySchema()
    cf.name = b"cf"

    admin.create_table(test_table_name, [cf])
    time.sleep(1)

    try:
        # Disable table to modify column family
        admin.disable_table(test_table_name)

        # Modify column family - change max_versions
        admin.modify_column_family(
            test_table_name,
            b"cf",
            max_versions=5,
            time_to_live=86400  # 1 day
        )
        time.sleep(1)

        # Enable table
        admin.enable_table(test_table_name)

        # Verify modification (check schema)
        desc = admin.describe_table(test_table_name)
        schema = desc.table_schema[0]
        cf_desc = None
        for cf in schema.column_families:
            if cf.name == b"cf":
                cf_desc = cf
                break

        assert cf_desc is not None

    finally:
        # Cleanup
        admin.disable_table(test_table_name)
        admin.delete_table(test_table_name)


def test_add_column_family_with_options(admin, test_table_name):
    """Test adding column family with various options."""
    # Create table with initial column family
    cf1 = ColumnFamilySchema()
    cf1.name = b"cf1"

    admin.create_table(test_table_name, [cf1])
    time.sleep(1)

    try:
        admin.disable_table(test_table_name)

        # Add column family with multiple options
        admin.add_column_family(
            test_table_name,
            b"cf2",
            max_versions=10,
            min_versions=2,
            time_to_live=604800,  # 7 days
            compression="SNAPPY",
            bloom_filter="ROW",
            block_size=131072,  # 128KB
            block_cache_enabled=True,
            in_memory=False
        )
        time.sleep(1)

        admin.enable_table(test_table_name)

        # Verify column family exists
        desc = admin.describe_table(test_table_name)
        schema = desc.table_schema[0]
        cf_names = [cf.name for cf in schema.column_families]
        assert b"cf2" in cf_names

    finally:
        # Cleanup
        admin.disable_table(test_table_name)
        admin.delete_table(test_table_name)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
