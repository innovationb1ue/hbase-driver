"""
Test cases for Admin region operations.

Note: Region operations require region names which are obtained from
the meta table or region locator. These tests focus on the API structure.
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
    name = f"test_region_{uuid.uuid4().hex[:8]}"
    return TableName(b'default', name.encode())


class TestRegionOperations:
    """Test region-related admin operations."""

    def test_region_operations_require_region_info(self, admin, test_table_name):
        """Test that region operations require proper region info.

        Region operations like split and merge require RegionInfo objects
        which are typically obtained from the region locator or meta table.
        This test verifies the API structure.
        """
        # Create a table with pre-split regions
        cf = ColumnFamilySchema()
        cf.name = b"cf"

        split_keys = [b"a", b"b", b"c"]
        admin.create_table(test_table_name, [cf], split_keys=split_keys)
        time.sleep(1)

        try:
            # Verify table exists
            assert admin.table_exists(test_table_name)

            # Note: In a real scenario, you would:
            # 1. Get region info from meta table or region locator
            # 2. Call split_region(region_info, split_key)
            # 3. Call merge_regions(region1_spec, region2_spec)
            # 4. Call assign_region(region_spec) / unassign_region(region_spec)

            # For now, we just verify the methods exist
            assert hasattr(admin, 'split_region')
            assert hasattr(admin, 'merge_regions')
            assert hasattr(admin, 'assign_region')
            assert hasattr(admin, 'unassign_region')

        finally:
            admin.disable_table(test_table_name)
            admin.delete_table(test_table_name)


class TestRegionAssignments:
    """Test region assignment operations."""

    def test_assign_region_signature(self, admin):
        """Test assign_region method signature."""
        # Verify method exists and has correct signature
        import inspect
        sig = inspect.signature(admin.assign_region)
        params = list(sig.parameters.keys())
        assert "region_name" in params
        assert "override" in params

    def test_unassign_region_signature(self, admin):
        """Test unassign_region method signature."""
        import inspect
        sig = inspect.signature(admin.unassign_region)
        params = list(sig.parameters.keys())
        assert "region_name" in params
        assert "force" in params

    def test_merge_regions_signature(self, admin):
        """Test merge_regions method signature."""
        import inspect
        sig = inspect.signature(admin.merge_regions)
        params = list(sig.parameters.keys())
        assert "region1_name" in params
        assert "region2_name" in params
        assert "forcible" in params

    def test_split_region_signature(self, admin):
        """Test split_region method signature."""
        import inspect
        sig = inspect.signature(admin.split_region)
        params = list(sig.parameters.keys())
        assert "region_info" in params
        assert "split_key" in params


class TestPreSplitTable:
    """Test table creation with pre-split regions."""

    def test_create_table_with_splits(self, admin, test_table_name):
        """Test creating a table with pre-split regions."""
        cf = ColumnFamilySchema()
        cf.name = b"cf"

        # Create table with 4 split keys (results in 5 regions)
        split_keys = [b"aaa", b"bbb", b"ccc", b"ddd"]
        admin.create_table(test_table_name, [cf], split_keys=split_keys)
        time.sleep(1)

        try:
            # Verify table exists
            assert admin.table_exists(test_table_name)

            # Table should be enabled
            assert admin.is_table_enabled(test_table_name)

        finally:
            admin.disable_table(test_table_name)
            admin.delete_table(test_table_name)

    def test_create_table_with_binary_splits(self, admin, test_table_name):
        """Test creating a table with binary split keys."""
        cf = ColumnFamilySchema()
        cf.name = b"cf"

        # Create table with binary split keys
        split_keys = [b"\x00", b"\x40", b"\x80", b"\xc0"]
        admin.create_table(test_table_name, [cf], split_keys=split_keys)
        time.sleep(1)

        try:
            assert admin.table_exists(test_table_name)

        finally:
            admin.disable_table(test_table_name)
            admin.delete_table(test_table_name)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
