"""
Test atomic operations functionality.

Tests for server-side atomic operations: CheckAndPut, CheckAndDelete,
Increment, and Append.
"""
import pytest

from hbasedriver.client.client import Client
from hbasedriver.operations.put import Put
from hbasedriver.operations.get import Get
from hbasedriver.operations.delete import Delete
from hbasedriver.operations.increment import Increment, CheckAndPut
from hbasedriver.operations.append import Append
from hbasedriver.protobuf_py.HBase_pb2 import ColumnFamilySchema
from hbasedriver.common.table_name import TableName


def setup_module():
    """Setup test table."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    admin = client.get_admin()

    # Create table if not exists
    tn = TableName.value_of(b"default", b"test_atomic")
    try:
        admin.disable_table(tn)
        admin.delete_table(tn)
    except Exception:
        pass

    cf = ColumnFamilySchema()
    cf.name = b"cf"
    cfs = [cf]
    admin.create_table(tn, cfs)


def teardown_module():
    """Cleanup test table."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    admin = client.get_admin()

    tn = TableName.value_of(b"default", b"test_atomic")
    try:
        admin.disable_table(tn)
        admin.delete_table(tn)
    except Exception:
        pass


class TestCheckAndPut:
    """Tests for server-side atomic check-and-put operation."""

    def test_check_and_put_value_matches(self):
        """Test check-and-put when value matches."""
        conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
        client = Client(conf)
        table = client.get_table(b"default", b"test_atomic")

        # Set initial value
        table.put(Put(b"cap_row1").add_column(b"cf", b"lock", b"unlocked"))

        # Check-and-put: lock is "unlocked", set data
        cap = CheckAndPut(b"cap_row1")
        cap.set_check(b"cf", b"lock", b"unlocked")
        cap.set_put(Put(b"cap_row1").add_column(b"cf", b"data", b"locked_data"))

        success = table.check_and_put(cap)
        assert success is True

        # Verify data was set
        row = table.get(Get(b"cap_row1").add_column(b"cf", b"data"))
        assert row is not None
        assert row.get(b"cf", b"data") == b"locked_data"

    def test_check_and_put_value_mismatch(self):
        """Test check-and-put when value doesn't match."""
        conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
        client = Client(conf)
        table = client.get_table(b"default", b"test_atomic")

        # Set initial value
        table.put(Put(b"cap_row2").add_column(b"cf", b"lock", b"locked"))

        # Check-and-put: expecting "unlocked" but it's "locked"
        cap = CheckAndPut(b"cap_row2")
        cap.set_check(b"cf", b"lock", b"unlocked")
        cap.set_put(Put(b"cap_row2").add_column(b"cf", b"data", b"should_not_set"))

        success = table.check_and_put(cap)
        assert success is False

        # Verify data was NOT set
        row = table.get(Get(b"cap_row2").add_column(b"cf", b"data"))
        assert row is None or row.get(b"cf", b"data") is None

    def test_check_and_put_nonexistent_column(self):
        """Test check-and-put checking for non-existence."""
        conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
        client = Client(conf)
        table = client.get_table(b"default", b"test_atomic")

        # Check-and-put: column doesn't exist (None), set value
        cap = CheckAndPut(b"cap_row3")
        cap.set_check(b"cf", b"nonexistent", None)  # Check for non-existence
        cap.set_put(Put(b"cap_row3").add_column(b"cf", b"data", b"created"))

        success = table.check_and_put(cap)
        assert success is True

        # Verify data was set
        row = table.get(Get(b"cap_row3").add_column(b"cf", b"data"))
        assert row is not None
        assert row.get(b"cf", b"data") == b"created"


class TestCheckAndDelete:
    """Tests for server-side atomic check-and-delete operation."""

    def test_check_and_delete_value_matches(self):
        """Test check-and-delete when value matches."""
        conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
        client = Client(conf)
        table = client.get_table(b"default", b"test_atomic")

        # Set up test data
        table.put(Put(b"cad_row1").add_column(b"cf", b"lock", b"locked"))
        table.put(Put(b"cad_row1").add_column(b"cf", b"data", b"to_delete"))

        # Check-and-delete: lock is "locked", delete data
        success = table.check_and_delete(
            b"cad_row1",
            b"cf",
            b"lock",
            b"locked",
            Delete(b"cad_row1").add_column(b"cf", b"data")
        )
        assert success is True

        # Verify data was deleted
        row = table.get(Get(b"cad_row1").add_column(b"cf", b"data"))
        assert row is None or row.get(b"cf", b"data") is None

    def test_check_and_delete_value_mismatch(self):
        """Test check-and-delete when value doesn't match."""
        conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
        client = Client(conf)
        table = client.get_table(b"default", b"test_atomic")

        # Set up test data
        table.put(Put(b"cad_row2").add_column(b"cf", b"lock", b"unlocked"))
        table.put(Put(b"cad_row2").add_column(b"cf", b"data", b"should_remain"))

        # Check-and-delete: expecting "locked" but it's "unlocked"
        success = table.check_and_delete(
            b"cad_row2",
            b"cf",
            b"lock",
            b"locked",
            Delete(b"cad_row2").add_column(b"cf", b"data")
        )
        assert success is False

        # Verify data was NOT deleted
        row = table.get(Get(b"cad_row2").add_column(b"cf", b"data"))
        assert row is not None
        assert row.get(b"cf", b"data") == b"should_remain"


class TestAtomicIncrement:
    """Tests for server-side atomic increment operation."""

    def test_increment_new_counter(self):
        """Test increment on a new counter."""
        conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
        client = Client(conf)
        table = client.get_table(b"default", b"test_atomic")

        inc = Increment(b"inc_row1")
        inc.add_column(b"cf", b"counter", 10)

        new_value = table.increment(inc)
        assert new_value == 10

    def test_increment_existing_counter(self):
        """Test increment on existing counter."""
        conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
        client = Client(conf)
        table = client.get_table(b"default", b"test_atomic")

        # Set initial value using increment
        inc_init = Increment(b"inc_row2")
        inc_init.add_column(b"cf", b"counter", 100)
        table.increment(inc_init)

        # Increment by 50
        inc = Increment(b"inc_row2")
        inc.add_column(b"cf", b"counter", 50)

        new_value = table.increment(inc)
        assert new_value == 150

    def test_increment_negative(self):
        """Test increment with negative value (decrement)."""
        conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
        client = Client(conf)
        table = client.get_table(b"default", b"test_atomic")

        # Set initial value
        inc_init = Increment(b"inc_row3")
        inc_init.add_column(b"cf", b"counter", 100)
        table.increment(inc_init)

        # Decrement by 30
        inc = Increment(b"inc_row3")
        inc.add_column(b"cf", b"counter", -30)

        new_value = table.increment(inc)
        assert new_value == 70

    def test_increment_multiple_columns(self):
        """Test increment on multiple columns."""
        conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
        client = Client(conf)
        table = client.get_table(b"default", b"test_atomic")

        inc = Increment(b"inc_row4")
        inc.add_column(b"cf", b"counter1", 5)
        inc.add_column(b"cf", b"counter2", 10)
        inc.add_column(b"cf", b"counter3", 15)

        new_value = table.increment(inc)
        # Returns first counter value
        assert new_value == 5

        # Verify all counters
        row = table.get(
            Get(b"inc_row4")
            .add_column(b"cf", b"counter1")
            .add_column(b"cf", b"counter2")
            .add_column(b"cf", b"counter3")
        )
        assert row is not None
        c1 = row.get(b"cf", b"counter1")
        c2 = row.get(b"cf", b"counter2")
        c3 = row.get(b"cf", b"counter3")
        assert int.from_bytes(c1, byteorder='big', signed=True) == 5
        assert int.from_bytes(c2, byteorder='big', signed=True) == 10
        assert int.from_bytes(c3, byteorder='big', signed=True) == 15


class TestAppend:
    """Tests for server-side atomic append operation."""

    def test_append_to_existing(self):
        """Test append to existing value."""
        conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
        client = Client(conf)
        table = client.get_table(b"default", b"test_atomic")

        # Set initial value
        table.put(Put(b"app_row1").add_column(b"cf", b"text", b"Hello"))

        # Append to value
        append = Append(b"app_row1")
        append.add_column(b"cf", b"text", b" World")

        result = table.append(append)
        assert result is not None
        assert result.get(b"cf", b"text") == b"Hello World"

    def test_append_to_nonexistent(self):
        """Test append to non-existent column (becomes initial value)."""
        conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
        client = Client(conf)
        table = client.get_table(b"default", b"test_atomic")

        append = Append(b"app_row2")
        append.add_column(b"cf", b"text", b"Initial")

        result = table.append(append)
        assert result is not None
        assert result.get(b"cf", b"text") == b"Initial"

    def test_append_multiple_columns(self):
        """Test append to multiple columns."""
        conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
        client = Client(conf)
        table = client.get_table(b"default", b"test_atomic")

        # Set initial values
        table.put(Put(b"app_row3").add_column(b"cf", b"tags", b"tag1"))
        table.put(Put(b"app_row3").add_column(b"cf", b"suffix", b"start"))

        # Append to multiple columns
        append = Append(b"app_row3")
        append.add_column(b"cf", b"tags", b",tag2,tag3")
        append.add_column(b"cf", b"suffix", b"_end")

        result = table.append(append)
        assert result is not None
        assert result.get(b"cf", b"tags") == b"tag1,tag2,tag3"
        assert result.get(b"cf", b"suffix") == b"start_end"

    def test_append_no_return(self):
        """Test append without returning results."""
        conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
        client = Client(conf)
        table = client.get_table(b"default", b"test_atomic")

        table.put(Put(b"app_row4").add_column(b"cf", b"text", b"Before"))

        append = Append(b"app_row4")
        append.add_column(b"cf", b"text", b"After")
        append.set_return_results(False)

        result = table.append(append)
        # Result should be None when return_results is False
        # (depending on HBase server behavior, might return empty result)

        # Verify the append actually happened
        row = table.get(Get(b"app_row4").add_column(b"cf", b"text"))
        assert row is not None
        assert row.get(b"cf", b"text") == b"BeforeAfter"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
