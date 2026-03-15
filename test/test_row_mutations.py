"""
Tests for RowMutations - atomic multi-mutation operations on a single row.
"""
import pytest

from hbasedriver.client.client import Client
from hbasedriver.operations import Put, Delete, Increment, Append, RowMutations
from hbasedriver.operations.get import Get
from hbasedriver.table_name import TableName
from hbasedriver.protobuf_py.HBase_pb2 import ColumnFamilySchema


@pytest.fixture(scope="class")
def client():
    """Create a client connected to the test cluster."""
    c = Client({"hbase.zookeeper.quorum": "hbase-zk:2181"})
    yield c
    c.close()


@pytest.fixture(scope="class")
def test_table(client):
    """Create a test table for mutations tests."""
    admin = client.get_admin()
    tn = TableName.value_of(b"default", b"test_row_mutations")

    # Clean up if exists
    try:
        admin.disable_table(tn)
        admin.delete_table(tn)
    except Exception:
        pass

    # Create table with column families
    cf1 = ColumnFamilySchema()
    cf1.name = b"cf1"
    cf2 = ColumnFamilySchema()
    cf2.name = b"cf2"
    admin.create_table(tn, [cf1, cf2])

    yield tn

    # Cleanup
    try:
        admin.disable_table(tn)
        admin.delete_table(tn)
    except Exception:
        pass


class TestRowMutations:
    """Tests for atomic RowMutations operations."""

    def test_put_and_delete_atomic(self, client, test_table):
        """Test atomic put followed by delete in same operation."""
        with client.get_table(test_table.ns, test_table.tb) as table:
            rowkey = b"row_mut_1"

            # First, put some data that we'll delete
            table.put(Put(rowkey).add_column(b"cf1", b"to_delete", b"delete_me"))
            table.put(Put(rowkey).add_column(b"cf1", b"to_keep", b"keep_me"))

            # Verify initial state
            row = table.get(Get(rowkey).add_column(b"cf1", b"to_delete"))
            assert row is not None
            assert row.get(b"cf1", b"to_delete") == b"delete_me"

            # Atomically put new value and delete old value
            rm = RowMutations(rowkey)
            rm.add(Put(rowkey).add_column(b"cf1", b"new_col", b"new_value"))
            rm.add(Delete(rowkey).add_column(b"cf1", b"to_delete"))

            success = table.mutate_row(rm)
            assert success is True

            # Verify results
            row = table.get(Get(rowkey))
            assert row.get(b"cf1", b"to_delete") is None  # deleted
            assert row.get(b"cf1", b"to_keep") == b"keep_me"  # unchanged
            assert row.get(b"cf1", b"new_col") == b"new_value"  # added

    def test_put_and_increment_atomic(self, client, test_table):
        """Test atomic put and increment combination."""
        with client.get_table(test_table.ns, test_table.tb) as table:
            rowkey = b"row_mut_2"

            # Put initial counter
            table.put(Put(rowkey).add_column(b"cf1", b"counter", (1).to_bytes(8, byteorder='big', signed=True)))

            # Atomically put a value and increment counter
            rm = RowMutations(rowkey)
            rm.add(Put(rowkey).add_column(b"cf1", b"status", b"active"))
            rm.add(Increment(rowkey).add_column(b"cf1", b"counter", 5))

            success = table.mutate_row(rm)
            assert success is True

            # Verify results
            row = table.get(Get(rowkey))
            assert row.get(b"cf1", b"status") == b"active"
            counter_val = int.from_bytes(row.get(b"cf1", b"counter"), byteorder='big', signed=True)
            assert counter_val == 6

    def test_put_and_append_atomic(self, client, test_table):
        """Test atomic put and append combination."""
        with client.get_table(test_table.ns, test_table.tb) as table:
            rowkey = b"row_mut_3"

            # Put initial value
            table.put(Put(rowkey).add_column(b"cf1", b"tags", b"tag1"))

            # Atomically put new value and append to existing
            rm = RowMutations(rowkey)
            rm.add(Put(rowkey).add_column(b"cf1", b"new_field", b"value"))
            rm.add(Append(rowkey).add_column(b"cf1", b"tags", b",tag2"))

            success = table.mutate_row(rm)
            assert success is True

            # Verify results
            row = table.get(Get(rowkey))
            assert row.get(b"cf1", b"new_field") == b"value"
            assert row.get(b"cf1", b"tags") == b"tag1,tag2"

    def test_multiple_puts_atomic(self, client, test_table):
        """Test multiple puts in single atomic operation."""
        with client.get_table(test_table.ns, test_table.tb) as table:
            rowkey = b"row_mut_4"

            # Atomically put multiple values
            rm = RowMutations(rowkey)
            rm.add(Put(rowkey).add_column(b"cf1", b"col1", b"value1"))
            rm.add(Put(rowkey).add_column(b"cf1", b"col2", b"value2"))
            rm.add(Put(rowkey).add_column(b"cf2", b"col3", b"value3"))

            success = table.mutate_row(rm)
            assert success is True

            # Verify all values were put
            row = table.get(Get(rowkey))
            assert row.get(b"cf1", b"col1") == b"value1"
            assert row.get(b"cf1", b"col2") == b"value2"
            assert row.get(b"cf2", b"col3") == b"value3"

    def test_empty_mutations_error(self, client, test_table):
        """Test that empty RowMutations raises error."""
        with client.get_table(test_table.ns, test_table.tb) as table:
            rm = RowMutations(b"row_mut_5")

            with pytest.raises(ValueError, match="must contain at least one mutation"):
                table.mutate_row(rm)

    def test_unsupported_mutation_type_error(self, client, test_table):
        """Test that unsupported mutation type raises error."""
        with client.get_table(test_table.ns, test_table.tb) as table:
            rm = RowMutations(b"row_mut_6")
            # Add an unsupported type by directly modifying the internal list
            rm._mutations.append("invalid_type")

            with pytest.raises(ValueError, match="Unsupported mutation type"):
                table.mutate_row(rm)


class TestExistsOperations:
    """Tests for exists() and exists_all() operations."""

    def test_exists_row_present(self, client, test_table):
        """Test exists returns True for existing row."""
        with client.get_table(test_table.ns, test_table.tb) as table:
            rowkey = b"exists_row_1"

            # Put data
            table.put(Put(rowkey).add_column(b"cf1", b"col", b"value"))

            # Check existence
            exists = table.exists(Get(rowkey))
            assert exists is True

    def test_exists_row_missing(self, client, test_table):
        """Test exists returns False for missing row."""
        with client.get_table(test_table.ns, test_table.tb) as table:
            rowkey = b"exists_row_nonexistent"

            # Check existence without putting data
            exists = table.exists(Get(rowkey))
            assert exists is False

    def test_exists_column_present(self, client, test_table):
        """Test exists returns True for existing column."""
        with client.get_table(test_table.ns, test_table.tb) as table:
            rowkey = b"exists_row_2"

            # Put data
            table.put(Put(rowkey).add_column(b"cf1", b"col1", b"value1"))
            table.put(Put(rowkey).add_column(b"cf1", b"col2", b"value2"))

            # Check specific column exists
            exists = table.exists(Get(rowkey).add_column(b"cf1", b"col1"))
            assert exists is True

    def test_exists_column_missing(self, client, test_table):
        """Test exists returns False for missing column."""
        with client.get_table(test_table.ns, test_table.tb) as table:
            rowkey = b"exists_row_3"

            # Put data in one column
            table.put(Put(rowkey).add_column(b"cf1", b"col1", b"value1"))

            # Check different column exists
            exists = table.exists(Get(rowkey).add_column(b"cf1", b"col2"))
            assert exists is False

    def test_exists_all_mixed(self, client, test_table):
        """Test exists_all with mix of existing and missing rows."""
        with client.get_table(test_table.ns, test_table.tb) as table:
            # Put data in some rows
            table.put(Put(b"exists_all_1").add_column(b"cf1", b"col", b"value"))
            table.put(Put(b"exists_all_2").add_column(b"cf1", b"col", b"value"))
            # exists_all_3 not created

            # Check all rows
            gets = [Get(b"exists_all_1"), Get(b"exists_all_2"), Get(b"exists_all_3")]
            results = table.exists_all(gets)

            assert results[b"exists_all_1"] is True
            assert results[b"exists_all_2"] is True
            assert results[b"exists_all_3"] is False

    def test_exists_after_delete(self, client, test_table):
        """Test exists returns False after row is deleted."""
        with client.get_table(test_table.ns, test_table.tb) as table:
            rowkey = b"exists_row_4"

            # Put and verify exists
            table.put(Put(rowkey).add_column(b"cf1", b"col", b"value"))
            assert table.exists(Get(rowkey)) is True

            # Delete row
            table.delete(Delete(rowkey))

            # Verify no longer exists
            assert table.exists(Get(rowkey)) is False
