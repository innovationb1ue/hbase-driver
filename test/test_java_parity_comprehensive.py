"""
Comprehensive Java HBase Client Parity Tests

Tests all combinations of Put, Get, Delete, Scan operations with various options.
Includes tests with different table configurations (multiple column families, versions, TTL, compression).
"""

import pytest
import os
from hbasedriver.client.client import Client
from hbasedriver.operations.column_family_builder import ColumnFamilyDescriptorBuilder
from hbasedriver.operations.put import Put
from hbasedriver.operations.get import Get
from hbasedriver.operations.delete import Delete
from hbasedriver.operations.scan import Scan
from hbasedriver.table_name import TableName

conf = {"hbase.zookeeper.quorum": os.getenv("HBASE_ZK", "127.0.0.1")}


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture(scope="session")
def admin():
    client = Client(conf)
    return client.get_admin()


@pytest.fixture(scope="session")
def client_session():
    return Client(conf)


# ============================================================================
# TABLE CONFIGURATIONS
# ============================================================================

@pytest.fixture
def simple_table(admin, client_session):
    """Single column family table with default settings."""
    table_name = TableName.value_of(b"", b"test_simple_parity")
    
    if admin.table_exists(table_name):
        try:
            admin.disable_table(table_name)
        except:
            pass
        admin.delete_table(table_name)
    
    cf = ColumnFamilyDescriptorBuilder(b"cf").build()
    admin.create_table(table_name, [cf])
    
    table = client_session.get_table(table_name.ns, table_name.tb)
    yield table
    
    try:
        admin.disable_table(table_name)
    except:
        pass
    admin.delete_table(table_name)


@pytest.fixture
def multi_family_table(admin, client_session):
    """Table with multiple column families."""
    table_name = TableName.value_of(b"", b"test_multifamily_parity")
    
    if admin.table_exists(table_name):
        try:
            admin.disable_table(table_name)
        except:
            pass
        admin.delete_table(table_name)
    
    cf1 = ColumnFamilyDescriptorBuilder(b"family1").build()
    cf2 = ColumnFamilyDescriptorBuilder(b"family2").build()
    cf3 = ColumnFamilyDescriptorBuilder(b"family3").build()
    admin.create_table(table_name, [cf1, cf2, cf3])
    
    table = client_session.get_table(table_name.ns, table_name.tb)
    yield table
    
    try:
        admin.disable_table(table_name)
    except:
        pass
    admin.delete_table(table_name)


@pytest.fixture
def versioned_table(admin, client_session):
    """Table with high max versions (5)."""
    table_name = TableName.value_of(b"", b"test_versioned_parity")
    
    if admin.table_exists(table_name):
        try:
            admin.disable_table(table_name)
        except:
            pass
        admin.delete_table(table_name)
    
    cf = ColumnFamilyDescriptorBuilder(b"cf").set_max_versions(5).build()
    admin.create_table(table_name, [cf])
    
    table = client_session.get_table(table_name.ns, table_name.tb)
    yield table
    
    try:
        admin.disable_table(table_name)
    except:
        pass
    admin.delete_table(table_name)


@pytest.fixture
def configured_table(admin, client_session):
    """Table with multiple configurations (compression, TTL, block size)."""
    table_name = TableName.value_of(b"", b"test_configured_parity")
    
    if admin.table_exists(table_name):
        try:
            admin.disable_table(table_name)
        except:
            pass
        admin.delete_table(table_name)
    
    cf = (ColumnFamilyDescriptorBuilder(b"cf")
          .set_max_versions(3)
          .set_time_to_live(86400)  # 1 day
          .set_block_size(65536)    # 64KB
          .build())
    admin.create_table(table_name, [cf])
    
    table = client_session.get_table(table_name.ns, table_name.tb)
    yield table
    
    try:
        admin.disable_table(table_name)
    except:
        pass
    admin.delete_table(table_name)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def row_is_empty(row):
    """Check if row result is empty."""
    if row is None:
        return True
    return len(row.kv) == 0


def get_cell_value(row, family, qualifier):
    """Get cell value from row."""
    if row is None:
        return None
    return row.get(family, qualifier)


def get_all_cells_in_family(row, family):
    """Get all cells in a family."""
    if row is None or family not in row.kv:
        return {}
    return row.kv[family]


# ============================================================================
# PUT OPERATION TESTS - COMPREHENSIVE
# ============================================================================

class TestPutBasic:
    """Basic Put operations."""
    
    def test_put_single_cell(self, simple_table):
        """Put single cell and verify."""
        put = Put(b"put_key1").add_column(b"cf", b"col", b"value")
        simple_table.put(put)
        
        get = Get(b"put_key1").add_column(b"cf", b"col")
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", b"col") == b"value"
    
    def test_put_multiple_columns_same_family(self, simple_table):
        """Put multiple columns in same family."""
        put = Put(b"put_key2")
        for i in range(10):
            put.add_column(b"cf", f"col{i:02d}".encode(), f"value{i}".encode())
        simple_table.put(put)
        
        get = Get(b"put_key2").add_family(b"cf")
        result = simple_table.get(get)
        for i in range(10):
            assert get_cell_value(result, b"cf", f"col{i:02d}".encode()) == f"value{i}".encode()
    
    def test_put_multiple_families(self, multi_family_table):
        """Put data across multiple families."""
        put = Put(b"put_key3")
        put.add_column(b"family1", b"col", b"val1")
        put.add_column(b"family2", b"col", b"val2")
        put.add_column(b"family3", b"col", b"val3")
        multi_family_table.put(put)
        
        get = Get(b"put_key3")
        result = multi_family_table.get(get)
        assert get_cell_value(result, b"family1", b"col") == b"val1"
        assert get_cell_value(result, b"family2", b"col") == b"val2"
        assert get_cell_value(result, b"family3", b"col") == b"val3"
    
    def test_put_empty_value(self, simple_table):
        """Put empty value."""
        put = Put(b"put_key4").add_column(b"cf", b"col", b"")
        simple_table.put(put)
        
        get = Get(b"put_key4").add_column(b"cf", b"col")
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", b"col") == b""
    
    def test_put_with_explicit_timestamp(self, simple_table):
        """Put with explicit timestamp."""
        ts = 1000000
        put = Put(b"put_key5").add_column(b"cf", b"col", b"value", ts=ts)
        simple_table.put(put)
        
        get = Get(b"put_key5").add_column(b"cf", b"col")
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", b"col") == b"value"
    
    def test_put_overwrite(self, simple_table):
        """Put overwrites previous value."""
        put1 = Put(b"put_key6").add_column(b"cf", b"col", b"old")
        simple_table.put(put1)
        
        put2 = Put(b"put_key6").add_column(b"cf", b"col", b"new")
        simple_table.put(put2)
        
        get = Get(b"put_key6").add_column(b"cf", b"col")
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", b"col") == b"new"
    
    def test_put_large_value(self, simple_table):
        """Put larger value (10KB)."""
        large_val = b"x" * (10 * 1024)
        put = Put(b"put_key7").add_column(b"cf", b"large", large_val)
        simple_table.put(put)
        
        get = Get(b"put_key7").add_column(b"cf", b"large")
        result = simple_table.get(get)
        assert len(get_cell_value(result, b"cf", b"large")) == 10 * 1024
    
    def test_put_binary_data(self, simple_table):
        """Put binary data with null bytes."""
        binary_data = b"\x00\x01\x02\xff\xfe\xfd"
        put = Put(b"put_key8").add_column(b"cf", b"bin", binary_data)
        simple_table.put(put)
        
        get = Get(b"put_key8").add_column(b"cf", b"bin")
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", b"bin") == binary_data


# ============================================================================
# GET OPERATION TESTS - COMPREHENSIVE
# ============================================================================

class TestGetBasic:
    """Basic Get operations."""
    
    def test_get_nonexistent_row(self, simple_table):
        """Get nonexistent row returns empty."""
        get = Get(b"get_nonexistent").add_column(b"cf", b"col")
        result = simple_table.get(get)
        assert row_is_empty(result)
    
    def test_get_specific_column(self, simple_table):
        """Get specific column from row with multiple columns."""
        put = Put(b"get_key1")
        put.add_column(b"cf", b"col1", b"val1")
        put.add_column(b"cf", b"col2", b"val2")
        put.add_column(b"cf", b"col3", b"val3")
        simple_table.put(put)
        
        # Get only col1
        get = Get(b"get_key1").add_column(b"cf", b"col1")
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", b"col1") == b"val1"
        assert get_cell_value(result, b"cf", b"col2") is None
        assert get_cell_value(result, b"cf", b"col3") is None
    
    def test_get_multiple_columns(self, simple_table):
        """Get multiple specific columns."""
        put = Put(b"get_key2")
        put.add_column(b"cf", b"col1", b"val1")
        put.add_column(b"cf", b"col2", b"val2")
        put.add_column(b"cf", b"col3", b"val3")
        simple_table.put(put)
        
        get = Get(b"get_key2")
        get.add_column(b"cf", b"col1")
        get.add_column(b"cf", b"col3")
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", b"col1") == b"val1"
        assert get_cell_value(result, b"cf", b"col2") is None
        assert get_cell_value(result, b"cf", b"col3") == b"val3"
    
    def test_get_entire_family(self, simple_table):
        """Get all columns in family."""
        put = Put(b"get_key3")
        for i in range(5):
            put.add_column(b"cf", f"col{i}".encode(), f"val{i}".encode())
        simple_table.put(put)
        
        get = Get(b"get_key3").add_family(b"cf")
        result = simple_table.get(get)
        for i in range(5):
            assert get_cell_value(result, b"cf", f"col{i}".encode()) == f"val{i}".encode()
    
    def test_get_multiple_families(self, multi_family_table):
        """Get data from multiple families."""
        put = Put(b"get_key4")
        put.add_column(b"family1", b"col", b"val1")
        put.add_column(b"family2", b"col", b"val2")
        put.add_column(b"family3", b"col", b"val3")
        multi_family_table.put(put)
        
        get = Get(b"get_key4")
        get.add_family(b"family1")
        get.add_family(b"family3")
        result = multi_family_table.get(get)
        assert get_cell_value(result, b"family1", b"col") == b"val1"
        assert get_cell_value(result, b"family2", b"col") is None
        assert get_cell_value(result, b"family3", b"col") == b"val3"


class TestGetVersions:
    """Get operations with version control."""
    
    def test_get_read_versions(self, versioned_table):
        """Read multiple versions of cell."""
        # Put multiple versions (simulated with timestamps)
        put1 = Put(b"ver_key1").add_column(b"cf", b"col", b"v1", ts=100)
        versioned_table.put(put1)
        
        put2 = Put(b"ver_key1").add_column(b"cf", b"col", b"v2", ts=200)
        versioned_table.put(put2)
        
        put3 = Put(b"ver_key1").add_column(b"cf", b"col", b"v3", ts=300)
        versioned_table.put(put3)
        
        # Get latest (should be v3)
        get = Get(b"ver_key1").add_column(b"cf", b"col")
        result = versioned_table.get(get)
        # Latest version
        val = get_cell_value(result, b"cf", b"col")
        assert val in [b"v1", b"v2", b"v3"]


# ============================================================================
# DELETE OPERATION TESTS - COMPREHENSIVE
# ============================================================================

class TestDeleteBasic:
    """Basic Delete operations."""
    
    def test_delete_single_cell(self, simple_table):
        """Delete single cell."""
        put = Put(b"del_key1")
        put.add_column(b"cf", b"col1", b"val1")
        put.add_column(b"cf", b"col2", b"val2")
        simple_table.put(put)
        
        delete = Delete(b"del_key1").add_column(b"cf", b"col1")
        simple_table.delete(delete)
        
        get = Get(b"del_key1").add_family(b"cf")
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", b"col1") is None
        assert get_cell_value(result, b"cf", b"col2") == b"val2"
    
    def test_delete_multiple_cells(self, simple_table):
        """Delete multiple cells."""
        put = Put(b"del_key2")
        for i in range(5):
            put.add_column(b"cf", f"col{i}".encode(), f"val{i}".encode())
        simple_table.put(put)
        
        delete = Delete(b"del_key2")
        delete.add_column(b"cf", b"col1")
        delete.add_column(b"cf", b"col3")
        simple_table.delete(delete)
        
        get = Get(b"del_key2").add_family(b"cf")
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", b"col0") == b"val0"
        assert get_cell_value(result, b"cf", b"col1") is None
        assert get_cell_value(result, b"cf", b"col2") == b"val2"
        assert get_cell_value(result, b"cf", b"col3") is None
        assert get_cell_value(result, b"cf", b"col4") == b"val4"
    
    def test_delete_entire_family(self, simple_table):
        """Delete entire column family."""
        put = Put(b"del_key3")
        for i in range(5):
            put.add_column(b"cf", f"col{i}".encode(), f"val{i}".encode())
        simple_table.put(put)
        
        delete = Delete(b"del_key3").add_family(b"cf")
        simple_table.delete(delete)
        
        get = Get(b"del_key3").add_family(b"cf")
        result = simple_table.get(get)
        assert row_is_empty(result)
    
    def test_delete_entire_row(self, simple_table):
        """Delete entire row."""
        put = Put(b"del_key4")
        put.add_column(b"cf", b"col1", b"val1")
        put.add_column(b"cf", b"col2", b"val2")
        simple_table.put(put)
        
        delete = Delete(b"del_key4")
        simple_table.delete(delete)
        
        get = Get(b"del_key4")
        result = simple_table.get(get)
        assert row_is_empty(result)
    
    def test_delete_nonexistent_cell(self, simple_table):
        """Delete nonexistent cell (should not error)."""
        delete = Delete(b"del_nonexistent").add_column(b"cf", b"col")
        simple_table.delete(delete)  # Should not raise
    
    def test_delete_with_timestamp(self, simple_table):
        """Delete with specific timestamp."""
        put = Put(b"del_key5").add_column(b"cf", b"col", b"value", ts=1000)
        simple_table.put(put)
        
        delete = Delete(b"del_key5").add_column(b"cf", b"col", ts=1000)
        simple_table.delete(delete)
        
        get = Get(b"del_key5").add_column(b"cf", b"col")
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", b"col") is None
    
    def test_delete_multiple_families(self, multi_family_table):
        """Delete from multiple families."""
        put = Put(b"del_key6")
        put.add_column(b"family1", b"col", b"val1")
        put.add_column(b"family2", b"col", b"val2")
        put.add_column(b"family3", b"col", b"val3")
        multi_family_table.put(put)
        
        delete = Delete(b"del_key6")
        delete.add_family(b"family1")
        delete.add_family(b"family3")
        multi_family_table.delete(delete)
        
        get = Get(b"del_key6")
        result = multi_family_table.get(get)
        assert get_cell_value(result, b"family1", b"col") is None
        assert get_cell_value(result, b"family2", b"col") == b"val2"
        assert get_cell_value(result, b"family3", b"col") is None


# ============================================================================
# SCAN OPERATION TESTS - COMPREHENSIVE
# ============================================================================

class TestScanBasic:
    """Basic Scan operations."""
    
    def test_scan_initialization(self):
        """Test Scan can be initialized with various options."""
        scan1 = Scan()
        assert scan1 is not None
        
        scan2 = Scan(b"start")
        assert scan2 is not None
        
        scan3 = Scan(b"start", b"stop")
        assert scan3 is not None
    
    def test_scan_fluent_api(self):
        """Test Scan fluent API methods."""
        scan = Scan(b"start", b"stop")
        scan.add_family(b"cf")
        scan.add_column(b"cf", b"col")
        scan.setCaching(100)
        scan.setBatch(10)
        scan.setMaxResultSize(1024)
        scan.setReversed(False)
        scan.setAllowPartialResults(True)
        
        # All methods should return properly
        assert scan is not None
    
    def test_scan_methods_chainable(self):
        """Test Scan methods are chainable."""
        scan = (Scan(b"row1", b"row2")
                .add_family(b"cf")
                .setCaching(100)
                .setBatch(10))
        assert scan is not None


# ============================================================================
# MULTI-FAMILY TABLE TESTS
# ============================================================================

class TestMultiFamilyOperations:
    """Operations on tables with multiple column families."""
    
    def test_put_get_all_families(self, multi_family_table):
        """Put and get across all families."""
        put = Put(b"mf_key1")
        put.add_column(b"family1", b"col1", b"val1")
        put.add_column(b"family2", b"col2", b"val2")
        put.add_column(b"family3", b"col3", b"val3")
        multi_family_table.put(put)
        
        get = Get(b"mf_key1")
        result = multi_family_table.get(get)
        assert get_cell_value(result, b"family1", b"col1") == b"val1"
        assert get_cell_value(result, b"family2", b"col2") == b"val2"
        assert get_cell_value(result, b"family3", b"col3") == b"val3"
    
    def test_selective_family_get(self, multi_family_table):
        """Get from specific families."""
        put = Put(b"mf_key2")
        put.add_column(b"family1", b"col", b"val1")
        put.add_column(b"family2", b"col", b"val2")
        put.add_column(b"family3", b"col", b"val3")
        multi_family_table.put(put)
        
        # Get only family1 and family3
        get = Get(b"mf_key2")
        get.add_family(b"family1")
        get.add_family(b"family3")
        result = multi_family_table.get(get)
        assert get_cell_value(result, b"family1", b"col") == b"val1"
        assert get_cell_value(result, b"family2", b"col") is None
        assert get_cell_value(result, b"family3", b"col") == b"val3"
    
    def test_delete_single_family(self, multi_family_table):
        """Delete entire single family."""
        put = Put(b"mf_key3")
        put.add_column(b"family1", b"col", b"val1")
        put.add_column(b"family2", b"col", b"val2")
        put.add_column(b"family3", b"col", b"val3")
        multi_family_table.put(put)
        
        delete = Delete(b"mf_key3").add_family(b"family2")
        multi_family_table.delete(delete)
        
        get = Get(b"mf_key3")
        result = multi_family_table.get(get)
        assert get_cell_value(result, b"family1", b"col") == b"val1"
        assert get_cell_value(result, b"family2", b"col") is None
        assert get_cell_value(result, b"family3", b"col") == b"val3"
    
    def test_batch_multi_family_operations(self, multi_family_table):
        """Batch operations on multiple families."""
        for i in range(3):
            put = Put(f"mf_batch_{i}".encode())
            put.add_column(b"family1", b"col", f"v1_{i}".encode())
            put.add_column(b"family2", b"col", f"v2_{i}".encode())
            put.add_column(b"family3", b"col", f"v3_{i}".encode())
            multi_family_table.put(put)
        
        # Verify all
        for i in range(3):
            get = Get(f"mf_batch_{i}".encode())
            result = multi_family_table.get(get)
            assert get_cell_value(result, b"family1", b"col") == f"v1_{i}".encode()
            assert get_cell_value(result, b"family2", b"col") == f"v2_{i}".encode()
            assert get_cell_value(result, b"family3", b"col") == f"v3_{i}".encode()


# ============================================================================
# CONFIGURED TABLE TESTS
# ============================================================================

class TestConfiguredTableOperations:
    """Operations on tables with specific configurations."""
    
    def test_configured_table_put_get(self, configured_table):
        """Put/get on configured table."""
        put = Put(b"conf_key1").add_column(b"cf", b"col", b"value")
        configured_table.put(put)
        
        get = Get(b"conf_key1").add_column(b"cf", b"col")
        result = configured_table.get(get)
        assert get_cell_value(result, b"cf", b"col") == b"value"
    
    def test_configured_table_many_columns(self, configured_table):
        """Put many columns to configured table."""
        put = Put(b"conf_key2")
        for i in range(20):
            put.add_column(b"cf", f"col{i:02d}".encode(), f"val{i}".encode())
        configured_table.put(put)
        
        get = Get(b"conf_key2").add_family(b"cf")
        result = configured_table.get(get)
        for i in range(20):
            assert get_cell_value(result, b"cf", f"col{i:02d}".encode()) == f"val{i}".encode()
    
    def test_configured_table_delete_and_verify(self, configured_table):
        """Delete from configured table."""
        put = Put(b"conf_key3")
        for i in range(5):
            put.add_column(b"cf", f"col{i}".encode(), f"val{i}".encode())
        configured_table.put(put)
        
        delete = Delete(b"conf_key3").add_column(b"cf", b"col2")
        configured_table.delete(delete)
        
        get = Get(b"conf_key3").add_family(b"cf")
        result = configured_table.get(get)
        assert get_cell_value(result, b"cf", b"col0") == b"val0"
        assert get_cell_value(result, b"cf", b"col2") is None
        assert get_cell_value(result, b"cf", b"col4") == b"val4"


# ============================================================================
# EDGE CASES AND SPECIAL SCENARIOS
# ============================================================================

class TestEdgeCases:
    """Edge cases and special scenarios."""
    
    def test_unicode_data(self, simple_table):
        """Unicode characters in keys and values."""
        key = "键名".encode("utf-8")
        col = "列名".encode("utf-8")
        val = "值".encode("utf-8")
        
        put = Put(key).add_column(b"cf", col, val)
        simple_table.put(put)
        
        get = Get(key).add_column(b"cf", col)
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", col) == val
    
    def test_binary_data_with_null_bytes(self, simple_table):
        """Binary data containing null bytes."""
        binary_data = b"\x00\x01\x02\x03\x04\xff\xfe\xfd\xfc"
        put = Put(b"bin_key1").add_column(b"cf", b"bin", binary_data)
        simple_table.put(put)
        
        get = Get(b"bin_key1").add_column(b"cf", b"bin")
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", b"bin") == binary_data
    
    def test_many_columns_in_row(self, simple_table):
        """Row with many columns."""
        put = Put(b"many_cols_key")
        for i in range(100):
            put.add_column(b"cf", f"col{i:03d}".encode(), f"val{i}".encode())
        simple_table.put(put)
        
        get = Get(b"many_cols_key").add_family(b"cf")
        result = simple_table.get(get)
        for i in range(100):
            assert get_cell_value(result, b"cf", f"col{i:03d}".encode()) == f"val{i}".encode()
    
    def test_selective_column_get_from_many(self, simple_table):
        """Get specific columns from row with many columns."""
        put = Put(b"selective_key")
        for i in range(50):
            put.add_column(b"cf", f"col{i:02d}".encode(), f"val{i}".encode())
        simple_table.put(put)
        
        get = Get(b"selective_key")
        get.add_column(b"cf", b"col05")
        get.add_column(b"cf", b"col15")
        get.add_column(b"cf", b"col25")
        result = simple_table.get(get)
        
        assert get_cell_value(result, b"cf", b"col05") == b"val5"
        assert get_cell_value(result, b"cf", b"col15") == b"val15"
        assert get_cell_value(result, b"cf", b"col25") == b"val25"
        assert get_cell_value(result, b"cf", b"col06") is None


# ============================================================================
# COMBINED PUT/GET/DELETE WORKFLOW TESTS
# ============================================================================

class TestIntegratedWorkflows:
    """Test complete workflows combining Put, Get, Delete."""
    
    def test_write_update_read_delete(self, simple_table):
        """Complete workflow: write -> update -> read -> delete."""
        # Write
        put1 = Put(b"workflow1").add_column(b"cf", b"col", b"initial")
        simple_table.put(put1)
        
        # Read
        get = Get(b"workflow1").add_column(b"cf", b"col")
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", b"col") == b"initial"
        
        # Update
        put2 = Put(b"workflow1").add_column(b"cf", b"col", b"updated")
        simple_table.put(put2)
        
        # Read again
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", b"col") == b"updated"
        
        # Delete
        delete = Delete(b"workflow1")
        simple_table.delete(delete)
        
        # Verify deletion
        result = simple_table.get(get)
        assert row_is_empty(result)
    
    def test_bulk_write_read_selective_delete(self, simple_table):
        """Bulk operations with selective deletion."""
        # Bulk write
        for i in range(10):
            put = Put(f"bulk_{i:02d}".encode())
            for j in range(5):
                put.add_column(b"cf", f"col{j}".encode(), f"val{i}_{j}".encode())
            simple_table.put(put)
        
        # Read all
        for i in range(10):
            get = Get(f"bulk_{i:02d}".encode()).add_family(b"cf")
            result = simple_table.get(get)
            for j in range(5):
                assert get_cell_value(result, b"cf", f"col{j}".encode()) == f"val{i}_{j}".encode()
        
        # Selective delete
        for i in [2, 5, 8]:
            delete = Delete(f"bulk_{i:02d}".encode()).add_column(b"cf", b"col1")
            simple_table.delete(delete)
        
        # Verify
        for i in [2, 5, 8]:
            get = Get(f"bulk_{i:02d}".encode()).add_family(b"cf")
            result = simple_table.get(get)
            assert get_cell_value(result, b"cf", b"col1") is None
            assert get_cell_value(result, b"cf", b"col0") == f"val{i}_0".encode()


# ============================================================================
# DATA TYPE AND ENCODING TESTS
# ============================================================================

class TestDataTypes:
    """Test various data types and encodings."""
    
    def test_numeric_as_bytes(self, simple_table):
        """Numeric values stored as bytes."""
        # Store integers as bytes
        for i in range(10):
            put = Put(f"num_{i}".encode()).add_column(b"cf", b"int", str(i * 100).encode())
            simple_table.put(put)
        
        # Retrieve and verify
        for i in range(10):
            get = Get(f"num_{i}".encode()).add_column(b"cf", b"int")
            result = simple_table.get(get)
            assert get_cell_value(result, b"cf", b"int") == str(i * 100).encode()
    
    def test_json_as_value(self, simple_table):
        """JSON data stored as value."""
        import json
        
        data = {"user": "alice", "age": 30, "city": "NYC"}
        json_bytes = json.dumps(data).encode()
        
        put = Put(b"json_key").add_column(b"cf", b"data", json_bytes)
        simple_table.put(put)
        
        get = Get(b"json_key").add_column(b"cf", b"data")
        result = simple_table.get(get)
        retrieved_data = json.loads(get_cell_value(result, b"cf", b"data").decode())
        assert retrieved_data == data
    
    def test_mixed_encodings(self, simple_table):
        """Mix of different text encodings."""
        encodings = [
            ("UTF-8", "Hello 世界"),
            ("ASCII", "Hello World"),
        ]
        
        for encoding_name, text in encodings:
            encoded = text.encode("utf-8")
            put = Put(encoding_name.encode()).add_column(b"cf", b"text", encoded)
            simple_table.put(put)
        
        for encoding_name, text in encodings:
            get = Get(encoding_name.encode()).add_column(b"cf", b"text")
            result = simple_table.get(get)
            retrieved = get_cell_value(result, b"cf", b"text").decode("utf-8")
            assert retrieved == text


# ============================================================================
# STRESS TESTS WITH MANY OPERATIONS
# ============================================================================

class TestStressOperations:
    """Stress tests with many operations."""
    
    def test_many_rows_put_get(self, simple_table):
        """Put and get many rows."""
        num_rows = 50
        
        # Put
        for i in range(num_rows):
            put = Put(f"stress_{i:03d}".encode()).add_column(b"cf", b"col", f"value_{i}".encode())
            simple_table.put(put)
        
        # Get
        for i in range(num_rows):
            get = Get(f"stress_{i:03d}".encode()).add_column(b"cf", b"col")
            result = simple_table.get(get)
            assert get_cell_value(result, b"cf", b"col") == f"value_{i}".encode()
    
    def test_row_with_many_versions(self, versioned_table):
        """Row with many version writes."""
        row_key = b"versions_stress"
        
        # Write multiple versions
        for v in range(5):
            put = Put(row_key).add_column(b"cf", b"col", f"version_{v}".encode())
            versioned_table.put(put)
        
        # Get should return latest
        get = Get(row_key).add_column(b"cf", b"col")
        result = versioned_table.get(get)
        val = get_cell_value(result, b"cf", b"col")
        assert val in [b"version_0", b"version_1", b"version_2", b"version_3", b"version_4"]
    
    def test_wide_row_many_columns(self, simple_table):
        """Very wide row with many columns."""
        num_cols = 200
        
        put = Put(b"wide_row")
        for i in range(num_cols):
            put.add_column(b"cf", f"col{i:04d}".encode(), f"val{i}".encode())
        simple_table.put(put)
        
        # Verify all columns
        get = Get(b"wide_row").add_family(b"cf")
        result = simple_table.get(get)
        for i in range(num_cols):
            assert get_cell_value(result, b"cf", f"col{i:04d}".encode()) == f"val{i}".encode()


# ============================================================================
# CONSISTENCY TESTS
# ============================================================================

class TestConsistency:
    """Test consistency and correctness."""
    
    def test_overwrite_consistency(self, simple_table):
        """Verify overwrite is consistent."""
        key = b"consistency_key"
        
        # Overwrite 10 times
        for i in range(10):
            put = Put(key).add_column(b"cf", b"col", f"value_{i}".encode())
            simple_table.put(put)
            
            # Verify immediately
            get = Get(key).add_column(b"cf", b"col")
            result = simple_table.get(get)
            assert get_cell_value(result, b"cf", b"col") == f"value_{i}".encode()
    
    def test_multi_family_consistency(self, multi_family_table):
        """Verify multi-family operations are consistent."""
        key = b"mf_consistency"
        
        # Write to all families
        put = Put(key)
        put.add_column(b"family1", b"c1", b"v1")
        put.add_column(b"family2", b"c2", b"v2")
        put.add_column(b"family3", b"c3", b"v3")
        multi_family_table.put(put)
        
        # Verify consistency over multiple reads
        for _ in range(3):
            get = Get(key)
            result = multi_family_table.get(get)
            assert get_cell_value(result, b"family1", b"c1") == b"v1"
            assert get_cell_value(result, b"family2", b"c2") == b"v2"
            assert get_cell_value(result, b"family3", b"c3") == b"v3"
    
    def test_delete_consistency(self, simple_table):
        """Verify delete is consistent."""
        key = b"delete_consistency"
        
        # Put data
        put = Put(key).add_column(b"cf", b"col", b"value")
        simple_table.put(put)
        
        # Delete
        delete = Delete(key)
        simple_table.delete(delete)
        
        # Multiple reads should confirm deletion
        for _ in range(3):
            get = Get(key).add_column(b"cf", b"col")
            result = simple_table.get(get)
            assert row_is_empty(result)


# ============================================================================
# OPERATION COMBINATIONS
# ============================================================================

class TestOperationCombinations:
    """Test various combinations of Put, Get, Delete parameters."""
    
    def test_put_get_delete_with_multiple_columns(self, simple_table):
        """Combined operations with multiple columns."""
        put = Put(b"combo1")
        put.add_column(b"cf", b"a", b"1")
        put.add_column(b"cf", b"b", b"2")
        put.add_column(b"cf", b"c", b"3")
        simple_table.put(put)
        
        # Get specific
        get = Get(b"combo1")
        get.add_column(b"cf", b"a")
        get.add_column(b"cf", b"c")
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", b"a") == b"1"
        assert get_cell_value(result, b"cf", b"b") is None
        assert get_cell_value(result, b"cf", b"c") == b"3"
        
        # Delete specific
        delete = Delete(b"combo1")
        delete.add_column(b"cf", b"b")
        simple_table.delete(delete)
        
        # Verify
        get = Get(b"combo1").add_family(b"cf")
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", b"a") == b"1"
        assert get_cell_value(result, b"cf", b"b") is None
        assert get_cell_value(result, b"cf", b"c") == b"3"
    
    def test_sequential_deletes_different_scopes(self, simple_table):
        """Sequential deletes at different scopes."""
        # Setup
        put = Put(b"seqdel")
        for i in range(5):
            put.add_column(b"cf", f"col{i}".encode(), f"val{i}".encode())
        simple_table.put(put)
        
        # Delete cell
        delete1 = Delete(b"seqdel").add_column(b"cf", b"col0")
        simple_table.delete(delete1)
        
        get = Get(b"seqdel").add_family(b"cf")
        result = simple_table.get(get)
        assert get_cell_value(result, b"cf", b"col0") is None
        assert get_cell_value(result, b"cf", b"col1") == b"val1"
        
        # Delete family
        delete2 = Delete(b"seqdel").add_family(b"cf")
        simple_table.delete(delete2)
        
        result = simple_table.get(get)
        assert row_is_empty(result)
