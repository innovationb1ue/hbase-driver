"""
Comprehensive integration tests for HBase filters.

Tests all implemented filters against a real 3-node HBase cluster:
- FilterList (AND/OR combinations)
- RowFilter
- FamilyFilter
- QualifierFilter
- ValueFilter
- PrefixFilter
- PageFilter
- KeyOnlyFilter
- ColumnPrefixFilter
- SingleColumnValueFilter
- FirstKeyOnlyFilter
- MultipleColumnPrefixFilter
- TimestampsFilter

IMPORTANT: These tests require a running HBase cluster. Do NOT run directly
with pytest. Instead, use the provided test script:

    ./scripts/run_tests_3node.sh test/test_filter.py

This script ensures the Docker containers are running and properly configured
before executing the tests.
"""
import pytest
import time

from hbasedriver.client.client import Client
from hbasedriver.operations import Scan
from hbasedriver.operations.put import Put
from hbasedriver.operations.get import Get
from hbasedriver.protobuf_py.HBase_pb2 import ColumnFamilySchema
from hbasedriver.common.table_name import TableName

# Import all filters
from hbasedriver.filter import (
    FilterList,
    RowFilter,
    FamilyFilter,
    QualifierFilter,
    ValueFilter,
    PrefixFilter,
    PageFilter,
    KeyOnlyFilter,
    ColumnPrefixFilter,
    SingleColumnValueFilter,
    FirstKeyOnlyFilter,
    MultipleColumnPrefixFilter,
    TimestampsFilter,
    CompareOperator,
    BinaryComparator,
    BinaryPrefixComparator,
)

# ZooKeeper address for the 3-node cluster
ZK_ADDR = "hbase-zk:2181"
TABLE_NAME = TableName.value_of(b"default", b"test_filter")


@pytest.fixture(scope="module")
def filter_table():
    """Create and populate a test table for filter testing."""
    import sys
    print("\n[filter_table fixture] Starting setup...", file=sys.stderr, flush=True)

    conf = {"hbase.zookeeper.quorum": ZK_ADDR}
    client = Client(conf)
    admin = client.get_admin()

    # Clean up if exists
    print("[filter_table fixture] Checking if table exists...", file=sys.stderr, flush=True)
    try:
        if admin.table_exists(TABLE_NAME):
            print(f"[filter_table fixture] Table {TABLE_NAME} exists, deleting...", file=sys.stderr, flush=True)
            try:
                admin.disable_table(TABLE_NAME)
            except Exception:
                pass
            admin.delete_table(TABLE_NAME)
            time.sleep(1)
    except Exception as e:
        print(f"[filter_table fixture] Cleanup exception: {e}", file=sys.stderr, flush=True)

    # Create table with multiple column families
    print("[filter_table fixture] Creating table...", file=sys.stderr, flush=True)
    cf1 = ColumnFamilySchema()
    cf1.name = b"cf1"
    cf2 = ColumnFamilySchema()
    cf2.name = b"cf2"
    cf3 = ColumnFamilySchema()
    cf3.name = b"data"
    admin.create_table(TABLE_NAME, [cf1, cf2, cf3])

    # Verify table exists
    print("[filter_table fixture] Verifying table creation...", file=sys.stderr, flush=True)
    assert admin.table_exists(TABLE_NAME), f"Table {TABLE_NAME} should exist!"
    print(f"[filter_table fixture] Table {TABLE_NAME} exists!", file=sys.stderr, flush=True)

    # Wait for table to be ready
    time.sleep(3)

    table = client.get_table(TABLE_NAME.ns, TABLE_NAME.tb)

    # Insert comprehensive test data
    print("[filter_table fixture] Inserting test data...", file=sys.stderr, flush=True)
    test_data = [
        # Rows with prefix 'row_'
        (b'row_001', b'cf1', b'col_a', b'value_a1'),
        (b'row_001', b'cf1', b'col_b', b'value_b1'),
        (b'row_001', b'cf2', b'col_a', b'value_a2'),
        (b'row_001', b'cf2', b'col_x', b'value_x1'),
        (b'row_001', b'data', b'info', b'data_info_1'),

        (b'row_002', b'cf1', b'col_a', b'value_a1'),
        (b'row_002', b'cf1', b'col_c', b'value_c1'),
        (b'row_002', b'cf2', b'col_b', b'value_b2'),
        (b'row_002', b'data', b'info', b'data_info_2'),

        (b'row_003', b'cf1', b'col_a', b'value_a1'),
        (b'row_003', b'cf1', b'col_b', b'value_b1'),
        (b'row_003', b'data', b'meta', b'meta_data'),

        # Rows with prefix 'user_'
        (b'user_alice', b'cf1', b'name', b'Alice'),
        (b'user_alice', b'cf1', b'age', b'30'),
        (b'user_alice', b'data', b'email', b'alice@example.com'),

        (b'user_bob', b'cf1', b'name', b'Bob'),
        (b'user_bob', b'cf1', b'age', b'25'),
        (b'user_bob', b'cf2', b'name', b'Bob Backup'),

        (b'user_charlie', b'cf1', b'name', b'Charlie'),
        (b'user_charlie', b'data', b'status', b'active'),

        # Rows with prefix 'prod_'
        (b'prod_001', b'cf1', b'name', b'Product A'),
        (b'prod_001', b'data', b'price', b'99.99'),
        (b'prod_001', b'data', b'stock', b'100'),

        (b'prod_002', b'cf1', b'name', b'Product B'),
        (b'prod_002', b'data', b'price', b'149.99'),

        # Other rows
        (b'other_row', b'cf2', b'misc', b'misc_value'),
        (b'zzz_last', b'cf1', b'col_a', b'last_value'),
    ]

    for rowkey, family, qualifier, value in test_data:
        table.put(Put(rowkey).add_column(family, qualifier, value))

    print("[filter_table fixture] Data inserted!", file=sys.stderr, flush=True)

    time.sleep(1)  # Wait for data to be written

    print("[filter_table fixture] Yielding table...", file=sys.stderr, flush=True)
    yield table

    # Cleanup
    print("[filter_table fixture] Cleaning up...", file=sys.stderr, flush=True)
    try:
        admin.disable_table(TABLE_NAME)
        admin.delete_table(TABLE_NAME)
    except Exception:
        pass


# ============================================================================
# PrefixFilter Tests
# ============================================================================

class TestPrefixFilter:
    """Test PrefixFilter with various prefixes."""

    def test_prefix_filter_row(self, filter_table):
        """Test filtering rows by prefix 'row_'."""
        # Provide start_row to help with region lookup
        scan = Scan(start_row=b'row_').set_filter(PrefixFilter(b'row_')).add_family(b'cf1').add_family(b'cf2').add_family(b'data')
        results = list(filter_table.scan(scan))

        row_keys = {r.rowkey for r in results}
        assert b'row_001' in row_keys
        assert b'row_002' in row_keys
        assert b'row_003' in row_keys
        assert b'user_alice' not in row_keys
        assert b'prod_001' not in row_keys

    def test_prefix_filter_user(self, filter_table):
        """Test filtering rows by prefix 'user_'."""
        scan = Scan(start_row=b'user_').set_filter(PrefixFilter(b'user_')).add_family(b'cf1').add_family(b'cf2').add_family(b'data')
        results = list(filter_table.scan(scan))

        row_keys = {r.rowkey for r in results}
        assert b'user_alice' in row_keys
        assert b'user_bob' in row_keys
        assert b'user_charlie' in row_keys
        assert b'row_001' not in row_keys
        assert b'prod_001' not in row_keys

    def test_prefix_filter_prod(self, filter_table):
        """Test filtering rows by prefix 'prod_'."""
        scan = Scan(start_row=b'prod_').set_filter(PrefixFilter(b'prod_')).add_family(b'cf1').add_family(b'data')
        results = list(filter_table.scan(scan))

        row_keys = {r.rowkey for r in results}
        assert b'prod_001' in row_keys
        assert b'prod_002' in row_keys
        assert len(row_keys) == 2

    def test_prefix_filter_no_match(self, filter_table):
        """Test prefix filter with no matching rows."""
        scan = Scan(start_row=b'nonexistent_').set_filter(PrefixFilter(b'nonexistent_')).add_family(b'cf1')
        results = list(filter_table.scan(scan))
        assert len(results) == 0

    def test_prefix_filter_with_column_family(self, filter_table):
        """Test prefix filter combined with column family restriction."""
        scan = Scan(start_row=b'row_').set_filter(PrefixFilter(b'row_')).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        # All results should be from cf1 family only
        for result in results:
            # Row.kv is a dict of {family: {qualifier: value}}
            families = set(result.kv.keys())
            assert families == {b'cf1'}


# ============================================================================
# PageFilter Tests
# ============================================================================

class TestPageFilter:
    """Test PageFilter for pagination."""

    def test_page_filter_limit_2(self, filter_table):
        """Test page filter with limit 2."""
        scan = Scan(start_row=b'prod_').set_filter(PageFilter(2)).add_family(b'cf1').add_family(b'data')
        results = list(filter_table.scan(scan))
        assert len(results) <= 2

    def test_page_filter_limit_5(self, filter_table):
        """Test page filter with limit 5."""
        scan = Scan(start_row=b'prod_').set_filter(PageFilter(5)).add_family(b'cf1').add_family(b'data')
        results = list(filter_table.scan(scan))
        assert len(results) <= 5
        assert len(results) > 0

    def test_page_filter_limit_100(self, filter_table):
        """Test page filter with high limit."""
        scan = Scan(start_row=b'other_row').set_filter(PageFilter(100)).add_family(b'cf1').add_family(b'cf2')
        results = list(filter_table.scan(scan))
        # Should return at least 1 row
        assert len(results) >= 1


# ============================================================================
# QualifierFilter Tests
# ============================================================================

class TestQualifierFilter:
    """Test QualifierFilter for filtering by column qualifier."""

    def test_qualifier_filter_equal(self, filter_table):
        """Test qualifier filter with EQUAL operator."""
        scan = Scan(start_row=b'row_').set_filter(
            QualifierFilter(CompareOperator.EQUAL, BinaryComparator(b'col_a'))
        ).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        for result in results:
            if result.kv:
                qualifiers = set()
                for family_data in result.kv.values():
                    qualifiers.update(family_data.keys())
                assert b'col_a' in qualifiers

    def test_qualifier_filter_prefix(self, filter_table):
        """Test qualifier filter with BinaryPrefixComparator."""
        scan = Scan(start_row=b'row_').set_filter(
            QualifierFilter(CompareOperator.EQUAL, BinaryPrefixComparator(b'col_'))
        ).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        for result in results:
            for family_data in result.kv.values():
                for qualifier in family_data.keys():
                    assert qualifier.startswith(b'col_')


# ============================================================================
# ValueFilter Tests
# ============================================================================

class TestValueFilter:
    """Test ValueFilter for filtering by cell value."""

    def test_value_filter_equal(self, filter_table):
        """Test value filter with EQUAL operator."""
        scan = Scan(start_row=b'row_').set_filter(
            ValueFilter(CompareOperator.EQUAL, BinaryComparator(b'value_a1'))
        ).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        for result in results:
            if result.kv:
                values = set()
                for family_data in result.kv.values():
                    values.update(family_data.values())
                assert b'value_a1' in values

    def test_value_filter_prefix(self, filter_table):
        """Test value filter with BinaryPrefixComparator."""
        scan = Scan(start_row=b'row_').set_filter(
            ValueFilter(CompareOperator.EQUAL, BinaryPrefixComparator(b'value_'))
        ).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        for result in results:
            for family_data in result.kv.values():
                for value in family_data.values():
                    assert value.startswith(b'value_')


# ============================================================================
# FamilyFilter Tests
# ============================================================================

class TestFamilyFilter:
    """Test FamilyFilter for filtering by column family."""

    def test_family_filter_cf1(self, filter_table):
        """Test family filter for cf1."""
        scan = Scan(start_row=b'row_').set_filter(
            FamilyFilter(CompareOperator.EQUAL, BinaryComparator(b'cf1'))
        ).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        for result in results:
            families = set(result.kv.keys())
            assert families == {b'cf1'}

    def test_family_filter_cf2(self, filter_table):
        """Test family filter for cf2."""
        scan = Scan(start_row=b'row_').set_filter(
            FamilyFilter(CompareOperator.EQUAL, BinaryComparator(b'cf2'))
        ).add_family(b'cf2')
        results = list(filter_table.scan(scan))

        for result in results:
            families = set(result.kv.keys())
            assert families == {b'cf2'}

    def test_family_filter_data(self, filter_table):
        """Test family filter for data family."""
        scan = Scan(start_row=b'row_').set_filter(
            FamilyFilter(CompareOperator.EQUAL, BinaryComparator(b'data'))
        ).add_family(b'data')
        results = list(filter_table.scan(scan))

        for result in results:
            families = set(result.kv.keys())
            assert families == {b'data'}


# ============================================================================
# ColumnPrefixFilter Tests
# ============================================================================

class TestColumnPrefixFilter:
    """Test ColumnPrefixFilter for filtering columns by qualifier prefix."""

    def test_column_prefix_filter_col(self, filter_table):
        """Test column prefix filter for 'col_' prefix."""
        scan = Scan(start_row=b'row_').set_filter(
            ColumnPrefixFilter(b'col_')
        ).add_family(b'cf1').add_family(b'cf2')
        results = list(filter_table.scan(scan))

        for result in results:
            for family_data in result.kv.values():
                for qualifier in family_data.keys():
                    assert qualifier.startswith(b'col_')

    def test_column_prefix_filter_name(self, filter_table):
        """Test column prefix filter for 'name' prefix."""
        scan = Scan(start_row=b'user_').set_filter(
            ColumnPrefixFilter(b'name')
        ).add_family(b'cf1').add_family(b'cf2')
        results = list(filter_table.scan(scan))

        for result in results:
            for family_data in result.kv.values():
                for qualifier in family_data.keys():
                    assert qualifier.startswith(b'name')


# ============================================================================
# RowFilter Tests
# ============================================================================

class TestRowFilter:
    """Test RowFilter for filtering by row key comparison."""

    def test_row_filter_equal(self, filter_table):
        """Test row filter with EQUAL operator."""
        scan = Scan(start_row=b'row_001').set_filter(
            RowFilter(CompareOperator.EQUAL, BinaryComparator(b'row_001'))
        ).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        row_keys = {r.rowkey for r in results}
        assert row_keys == {b'row_001'}

    def test_row_filter_greater_than(self, filter_table):
        """Test row filter with GREATER operator."""
        scan = Scan(start_row=b'user_bob').set_filter(
            RowFilter(CompareOperator.GREATER, BinaryComparator(b'user_bob'))
        ).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        row_keys = {r.rowkey for r in results}
        for rk in row_keys:
            assert rk > b'user_bob'

    def test_row_filter_greater_or_equal(self, filter_table):
        """Test row filter with GREATER_OR_EQUAL operator."""
        scan = Scan(start_row=b'user_bob').set_filter(
            RowFilter(CompareOperator.GREATER_OR_EQUAL, BinaryComparator(b'user_bob'))
        ).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        row_keys = {r.rowkey for r in results}
        for rk in row_keys:
            assert rk >= b'user_bob'
        assert b'user_bob' in row_keys


# ============================================================================
# KeyOnlyFilter Tests
# ============================================================================

class TestKeyOnlyFilter:
    """Test KeyOnlyFilter for returning only row keys."""

    def test_key_only_filter_empty_value(self, filter_table):
        """Test key only filter with empty values."""
        scan = Scan(start_row=b'row_').set_filter(
            KeyOnlyFilter(len_as_val=False)
        ).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        for result in results:
            for family_data in result.kv.values():
                for value in family_data.values():
                    assert value == b''

    def test_key_only_filter_len_as_val(self, filter_table):
        """Test key only filter with length as value."""
        scan = Scan(start_row=b'row_').set_filter(
            KeyOnlyFilter(len_as_val=True)
        ).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        for result in results:
            for family_data in result.kv.values():
                for value in family_data.values():
                    # HBase uses 4-byte or 8-byte integers for length
                    # depending on configuration
                    assert len(value) in (4, 8)
                    if len(value) == 8:
                        original_len = int.from_bytes(value, byteorder='big')
                    else:
                        original_len = int.from_bytes(value, byteorder='big')
                    assert original_len >= 0


# ============================================================================
# FilterList Tests
# ============================================================================

class TestFilterList:
    """Test FilterList for combining filters."""

    def test_filter_list_must_pass_all(self, filter_table):
        """Test FilterList with MUST_PASS_ALL (AND logic)."""
        prefix_filter = PrefixFilter(b'row_')
        qualifier_filter = QualifierFilter(
            CompareOperator.EQUAL,
            BinaryComparator(b'col_a')
        )

        filter_list = FilterList(
            operator=FilterList.Operator.MUST_PASS_ALL,
            filters=[prefix_filter, qualifier_filter]
        )

        scan = Scan(start_row=b'row_').set_filter(filter_list).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        for result in results:
            assert result.rowkey.startswith(b'row_')

    def test_filter_list_must_pass_one(self, filter_table):
        """Test FilterList with MUST_PASS_ONE (OR logic)."""
        filter1 = PrefixFilter(b'row_')
        filter2 = PrefixFilter(b'user_')

        filter_list = FilterList(
            operator=FilterList.Operator.MUST_PASS_ONE,
            filters=[filter1, filter2]
        )

        scan = Scan(start_row=b'row_').set_filter(filter_list).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        row_keys = {r.rowkey for r in results}
        for rk in row_keys:
            assert rk.startswith(b'row_') or rk.startswith(b'user_')

    def test_filter_list_prefix_and_page(self, filter_table):
        """Test combining PrefixFilter with PageFilter."""
        filter_list = FilterList(
            operator=FilterList.Operator.MUST_PASS_ALL,
            filters=[
                PrefixFilter(b'user_'),
                PageFilter(2)
            ]
        )

        scan = Scan(start_row=b'user_').set_filter(filter_list).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        assert len(results) <= 2
        for result in results:
            assert result.rowkey.startswith(b'user_')

    def test_filter_list_value_and_qualifier(self, filter_table):
        """Test combining ValueFilter with QualifierFilter."""
        filter_list = FilterList(
            operator=FilterList.Operator.MUST_PASS_ALL,
            filters=[
                ValueFilter(CompareOperator.EQUAL, BinaryComparator(b'value_a1')),
                QualifierFilter(CompareOperator.EQUAL, BinaryComparator(b'col_a'))
            ]
        )

        scan = Scan(start_row=b'row_').set_filter(filter_list).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        for result in results:
            if result.kv:
                # Check that we have col_a with value_a1
                for family_data in result.kv.values():
                    if b'col_a' in family_data:
                        assert family_data[b'col_a'] == b'value_a1'


# ============================================================================
# Complex Filter Combinations Tests
# ============================================================================

class TestComplexFilterCombinations:
    """Test complex filter combinations."""

    def test_nested_filter_lists(self, filter_table):
        """Test nested FilterList structures."""
        inner_list = FilterList(
            operator=FilterList.Operator.MUST_PASS_ONE,
            filters=[
                PrefixFilter(b'row_'),
                PrefixFilter(b'user_')
            ]
        )

        outer_list = FilterList(
            operator=FilterList.Operator.MUST_PASS_ALL,
            filters=[
                inner_list,
                FamilyFilter(CompareOperator.EQUAL, BinaryComparator(b'cf1'))
            ]
        )

        scan = Scan(start_row=b'row_').set_filter(outer_list).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        for result in results:
            assert result.rowkey.startswith(b'row_') or result.rowkey.startswith(b'user_')
            families = set(result.kv.keys())
            assert families == {b'cf1'}

    def test_row_range_with_filters(self, filter_table):
        """Test row range scan combined with filters."""
        scan = Scan(start_row=b'row_001')
        scan.set_filter(
            ValueFilter(CompareOperator.EQUAL, BinaryPrefixComparator(b'value_'))
        ).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        for result in results:
            assert result.rowkey >= b'row_001'
            for family_data in result.kv.values():
                for value in family_data.values():
                    assert value.startswith(b'value_')

    def test_column_prefix_with_page_filter(self, filter_table):
        """Test ColumnPrefixFilter with PageFilter."""
        filter_list = FilterList(
            operator=FilterList.Operator.MUST_PASS_ALL,
            filters=[
                ColumnPrefixFilter(b'col_'),
                PageFilter(3)
            ]
        )

        scan = Scan(start_row=b'row_').set_filter(filter_list).add_family(b'cf1').add_family(b'cf2')
        results = list(filter_table.scan(scan))

        assert len(results) <= 3
        for result in results:
            for family_data in result.kv.values():
                for qualifier in family_data.keys():
                    assert qualifier.startswith(b'col_')


# ============================================================================
# Edge Cases Tests
# ============================================================================

class TestFilterEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_page_filter_size_1(self, filter_table):
        """Test PageFilter with size 1."""
        scan = Scan(start_row=b'row_').set_filter(PageFilter(1)).add_family(b'cf1')
        results = list(filter_table.scan(scan))
        assert len(results) == 1

    def test_key_only_filter_preserves_metadata(self, filter_table):
        """Test that KeyOnlyFilter preserves row key and other metadata."""
        scan = Scan(start_row=b'row_').set_filter(KeyOnlyFilter(len_as_val=False)).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        for result in results:
            assert result.rowkey is not None
            assert result.kv is not None
            # Check that we have data in cf1 family
            assert b'cf1' in result.kv

    def test_row_filter_not_equal(self, filter_table):
        """Test RowFilter with NOT_EQUAL operator."""
        scan = Scan(start_row=b'row_').set_filter(
            RowFilter(CompareOperator.NOT_EQUAL, BinaryComparator(b'row_001'))
        ).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        row_keys = {r.rowkey for r in results}
        assert b'row_001' not in row_keys


# ============================================================================
# SingleColumnValueFilter Tests
# ============================================================================

class TestSingleColumnValueFilter:
    """Test SingleColumnValueFilter for filtering rows by column value."""

    def test_single_column_value_equal(self, filter_table):
        """Test filtering rows by column value equality."""
        filter_ = SingleColumnValueFilter(
            column_family=b'cf1',
            column_qualifier=b'name',
            compare_operator=CompareOperator.EQUAL,
            comparator=BinaryComparator(b'Alice')
        )
        scan = Scan(start_row=b'user_').set_filter(filter_).add_family(b'cf1').add_family(b'cf2').add_family(b'data')
        results = list(filter_table.scan(scan))

        row_keys = {r.rowkey for r in results}
        assert b'user_alice' in row_keys
        assert b'user_bob' not in row_keys
        assert b'user_charlie' not in row_keys

    def test_single_column_value_not_equal(self, filter_table):
        """Test filtering rows by column value not equal."""
        filter_ = SingleColumnValueFilter(
            column_family=b'cf1',
            column_qualifier=b'name',
            compare_operator=CompareOperator.NOT_EQUAL,
            comparator=BinaryComparator(b'Bob')
        )
        scan = Scan(start_row=b'user_').set_filter(filter_).add_family(b'cf1').add_family(b'cf2').add_family(b'data')
        results = list(filter_table.scan(scan))

        row_keys = {r.rowkey for r in results}
        assert b'user_bob' not in row_keys
        assert b'user_alice' in row_keys or b'user_charlie' in row_keys

    def test_single_column_value_greater(self, filter_table):
        """Test filtering rows by column value greater than."""
        # Filter rows where age > 25
        filter_ = SingleColumnValueFilter(
            column_family=b'cf1',
            column_qualifier=b'age',
            compare_operator=CompareOperator.GREATER,
            comparator=BinaryComparator(b'25')
        )
        scan = Scan(start_row=b'user_').set_filter(filter_).add_family(b'cf1').add_family(b'cf2').add_family(b'data')
        results = list(filter_table.scan(scan))

        row_keys = {r.rowkey for r in results}
        assert b'user_bob' not in row_keys  # age 25
        assert b'user_alice' in row_keys  # age 30

    def test_single_column_value_greater_or_equal(self, filter_table):
        """Test filtering rows by column value greater or equal."""
        filter_ = SingleColumnValueFilter(
            column_family=b'cf1',
            column_qualifier=b'age',
            compare_operator=CompareOperator.GREATER_OR_EQUAL,
            comparator=BinaryComparator(b'25')
        )
        scan = Scan(start_row=b'user_').set_filter(filter_).add_family(b'cf1').add_family(b'cf2').add_family(b'data')
        results = list(filter_table.scan(scan))

        row_keys = {r.rowkey for r in results}
        # Both Alice (30) and Bob (25) should match
        assert b'user_bob' in row_keys
        assert b'user_alice' in row_keys

    def test_single_column_value_less(self, filter_table):
        """Test filtering rows by column value less than."""
        filter_ = SingleColumnValueFilter(
            column_family=b'cf1',
            column_qualifier=b'age',
            compare_operator=CompareOperator.LESS,
            comparator=BinaryComparator(b'30')
        )
        scan = Scan(start_row=b'user_').set_filter(filter_).add_family(b'cf1').add_family(b'cf2').add_family(b'data')
        results = list(filter_table.scan(scan))

        row_keys = {r.rowkey for r in results}
        assert b'user_alice' not in row_keys  # age 30
        assert b'user_bob' in row_keys  # age 25

    def test_single_column_value_less_or_equal(self, filter_table):
        """Test filtering rows by column value less or equal."""
        filter_ = SingleColumnValueFilter(
            column_family=b'cf1',
            column_qualifier=b'age',
            compare_operator=CompareOperator.LESS_OR_EQUAL,
            comparator=BinaryComparator(b'30')
        )
        scan = Scan(start_row=b'user_').set_filter(filter_).add_family(b'cf1').add_family(b'cf2').add_family(b'data')
        results = list(filter_table.scan(scan))

        row_keys = {r.rowkey for r in results}
        assert b'user_bob' in row_keys
        assert b'user_alice' in row_keys

    def test_single_column_value_filter_if_missing(self, filter_table):
        """Test filter_if_missing flag behavior."""
        # Rows that don't have 'age' column should be filtered out
        filter_ = SingleColumnValueFilter(
            column_family=b'cf1',
            column_qualifier=b'age',
            compare_operator=CompareOperator.EQUAL,
            comparator=BinaryComparator(b'25'),
            filter_if_missing=True
        )
        scan = Scan(start_row=b'user_').set_filter(filter_).add_family(b'cf1').add_family(b'cf2').add_family(b'data')
        results = list(filter_table.scan(scan))

        row_keys = {r.rowkey for r in results}
        # Only Bob (has age=25) should match, Charlie has no age column
        assert b'user_bob' in row_keys
        assert b'user_charlie' not in row_keys


# ============================================================================
# FirstKeyOnlyFilter Tests
# ============================================================================

class TestFirstKeyOnlyFilter:
    """Test FirstKeyOnlyFilter for returning only first column."""

    def test_first_key_only(self, filter_table):
        """Test returning only first column."""
        filter_ = FirstKeyOnlyFilter(len_as_val=False)
        scan = Scan(start_row=b'row_').set_filter(filter_).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        for result in results:
            # Each row should have only one column from cf1
            column_count = sum(len(family_data) for family_data in result.kv.values())
            assert column_count == 1, f"Expected 1 column, got {column_count} in row {result.rowkey}"

    def test_first_key_only_preserves_row_key(self, filter_table):
        """Test that FirstKeyOnlyFilter preserves row key."""
        filter_ = FirstKeyOnlyFilter(len_as_val=False)
        # Use end_row to limit scan to user_ prefixed rows only
        scan = Scan(start_row=b'user_', end_row=b'usf').set_filter(filter_).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        for result in results:
            assert result.rowkey is not None
            assert result.rowkey.startswith(b'user_')

    def test_first_key_only_with_len_as_val(self, filter_table):
        """Test FirstKeyOnlyFilter with len_as_val=True.

        Note: len_as_val is not serialized in the protobuf protocol,
        so values are returned as-is by the HBase server.
        """
        filter_ = FirstKeyOnlyFilter(len_as_val=True)
        scan = Scan(start_row=b'row_', end_row=b'ros').set_filter(filter_).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        # Since len_as_val is not supported by the protobuf, values are returned as-is
        # The filter still only returns the first column of each row
        for result in results:
            assert result.rowkey is not None
            column_count = sum(len(family_data) for family_data in result.kv.values())
            assert column_count == 1  # Only first column returned

    def test_first_key_only_empty_value(self, filter_table):
        """Test FirstKeyOnlyFilter with empty value (len_as_val=False)."""
        filter_ = FirstKeyOnlyFilter(len_as_val=False)
        scan = Scan(start_row=b'row_', end_row=b'ros').set_filter(filter_).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        for result in results:
            for family_data in result.kv.values():
                for value in family_data.values():
                    # With len_as_val=False, values should be unchanged (not empty)
                    # The filter only returns the first column, doesn't transform values
                    assert value is not None


# ============================================================================
# MultipleColumnPrefixFilter Tests
# ============================================================================

class TestMultipleColumnPrefixFilter:
    """Test MultipleColumnPrefixFilter for filtering by multiple prefixes."""

    def test_multiple_column_prefix(self, filter_table):
        """Test filtering by multiple column prefixes."""
        filter_ = MultipleColumnPrefixFilter([b'col_', b'name_'])
        scan = Scan(start_row=b'user_').set_filter(filter_).add_family(b'cf1').add_family(b'cf2')
        results = list(filter_table.scan(scan))

        for result in results:
            for family_data in result.kv.values():
                for qualifier in family_data.keys():
                    assert qualifier.startswith(b'col_') or qualifier.startswith(b'name_'), \
                        f"Qualifier {qualifier} doesn't match any prefix"

    def test_multiple_column_prefix_single_prefix(self, filter_table):
        """Test MultipleColumnPrefixFilter with single prefix."""
        filter_ = MultipleColumnPrefixFilter([b'name'])
        scan = Scan(start_row=b'user_').set_filter(filter_).add_family(b'cf1').add_family(b'cf2')
        results = list(filter_table.scan(scan))

        for result in results:
            for family_data in result.kv.values():
                for qualifier in family_data.keys():
                    assert qualifier.startswith(b'name'), \
                        f"Qualifier {qualifier} doesn't start with 'name'"

    def test_multiple_column_prefix_many_prefixes(self, filter_table):
        """Test MultipleColumnPrefixFilter with many prefixes."""
        filter_ = MultipleColumnPrefixFilter([b'col_', b'name', b'info', b'price', b'stock'])
        scan = Scan(start_row=b'prod_').set_filter(filter_).add_family(b'cf1').add_family(b'data')
        results = list(filter_table.scan(scan))

        valid_prefixes = [b'col_', b'name', b'info', b'price', b'stock']
        for result in results:
            for family_data in result.kv.values():
                for qualifier in family_data.keys():
                    matches_any = any(qualifier.startswith(prefix) for prefix in valid_prefixes)
                    assert matches_any, \
                        f"Qualifier {qualifier} doesn't match any of {valid_prefixes}"

    def test_multiple_column_prefix_no_match(self, filter_table):
        """Test MultipleColumnPrefixFilter with no matching prefixes."""
        filter_ = MultipleColumnPrefixFilter([b'nonexistent_'])
        scan = Scan(start_row=b'row_').set_filter(filter_).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        # Should still return rows, but with empty results (no matching columns)
        # or return rows with 0 columns depending on HBase behavior
        for result in results:
            column_count = sum(len(family_data) for family_data in result.kv.values())
            # No columns should match the prefix
            assert column_count == 0, f"Expected 0 columns, got {column_count}"


# ============================================================================
# TimestampsFilter Tests
# ============================================================================

class TestTimestampsFilter:
    """Test TimestampsFilter for filtering by specific timestamps."""

    def test_timestamps_filter_single(self, filter_table):
        """Test filtering by single timestamp."""
        # Insert data with specific timestamp
        table = filter_table
        test_row = b'ts_test_001'
        test_ts = 1234567890

        # Insert data with known timestamp
        put = Put(test_row)
        put.add_column(b'cf1', b'col_a', b'value_with_ts', test_ts)
        table.put(put)

        # Filter by that timestamp
        filter_ = TimestampsFilter([test_ts])
        scan = Scan(start_row=b'ts_test_').set_filter(filter_).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        # Clean up the test row
        table.delete(put)

        # Verify we got results
        assert len(results) > 0

    def test_timestamps_filter_multiple(self, filter_table):
        """Test filtering by multiple timestamps."""
        # Insert data with multiple timestamps
        table = filter_table
        test_row = b'ts_test_002'
        ts1 = 1234567890
        ts2 = 1234567891

        # Insert data with different timestamps
        put1 = Put(test_row)
        put1.add_column(b'cf1', b'col_a', b'value_ts1', ts1)
        table.put(put1)

        put2 = Put(test_row)
        put2.add_column(b'cf1', b'col_b', b'value_ts2', ts2)
        table.put(put2)

        # Filter by both timestamps
        filter_ = TimestampsFilter([ts1, ts2])
        scan = Scan(start_row=b'ts_test_').set_filter(filter_).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        # Clean up the test row
        table.delete(put1)
        table.delete(put2)

        # Verify we got results
        assert len(results) > 0

    def test_timestamps_filter_empty_list(self, filter_table):
        """Test TimestampsFilter with empty timestamp list."""
        filter_ = TimestampsFilter([])
        scan = Scan(start_row=b'row_').set_filter(filter_).add_family(b'cf1')
        results = list(filter_table.scan(scan))

        # With no timestamps, should return no cells (or rows with empty data)
        for result in results:
            column_count = sum(len(family_data) for family_data in result.kv.values())
            assert column_count == 0

    def test_timestamps_filter_with_can_hint(self, filter_table):
        """Test TimestampsFilter with can_hint flag."""
        filter_ = TimestampsFilter([1234567890], can_hint=True)
        scan = Scan(start_row=b'row_').set_filter(filter_).add_family(b'cf1')

        # Just verify the filter can be created and used
        results = list(filter_table.scan(scan))
        assert isinstance(results, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
