"""
Test BufferedMutator functionality.

Tests for BufferedMutator class including basic operations, buffer management,
flush behavior, and exception handling.
"""
import pytest

from hbasedriver.client.client import Client
from hbasedriver.client.buffered_mutator import (
    BufferedMutator,
    BufferedMutatorParams,
    ExceptionListener
)
from hbasedriver.operations.put import Put
from hbasedriver.operations.get import Get
from hbasedriver.operations.delete import Delete
from hbasedriver.protobuf_py.HBase_pb2 import ColumnFamilySchema
from hbasedriver.common.table_name import TableName


def setup_module():
    """Setup test table."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    admin = client.get_admin()

    # Create table if not exists
    tn = TableName.value_of(b"default", b"test_buffered_mutator")
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

    tn = TableName.value_of(b"default", b"test_buffered_mutator")
    try:
        admin.disable_table(tn)
        admin.delete_table(tn)
    except Exception:
        pass


def test_buffered_mutator_context_manager():
    """Test BufferedMutator with context manager."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)

    # Use context manager
    with client.get_buffered_mutator(b"default", b"test_buffered_mutator") as mutator:
        mutator.mutate(Put(b"row1").add_column(b"cf", b"col", b"value1"))
        mutator.mutate(Put(b"row2").add_column(b"cf", b"col", b"value2"))

    # Verify data was flushed on close
    table = client.get_table(b"default", b"test_buffered_mutator")

    get1 = Get(b"row1")
    get1.add_column(b"cf", b"col")
    row1 = table.get(get1)
    assert row1 is not None
    assert row1.get(b"cf", b"col") == b"value1"

    get2 = Get(b"row2")
    get2.add_column(b"cf", b"col")
    row2 = table.get(get2)
    assert row2 is not None
    assert row2.get(b"cf", b"col") == b"value2"


def test_buffered_mutator_explicit_flush():
    """Test explicit flush."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)

    mutator = client.get_buffered_mutator(b"default", b"test_buffered_mutator")

    try:
        mutator.mutate(Put(b"row3").add_column(b"cf", b"col", b"value3"))

        # Data should not be visible yet
        table = client.get_table(b"default", b"test_buffered_mutator")
        get3 = Get(b"row3")
        get3.add_column(b"cf", b"col")
        row3_before = table.get(get3)
        assert row3_before is None

        # Flush
        mutator.flush()

        # Now data should be visible
        row3_after = table.get(get3)
        assert row3_after is not None
        assert row3_after.get(b"cf", b"col") == b"value3"

    finally:
        mutator.close()


def test_buffered_mutator_delete():
    """Test BufferedMutator with Delete operations."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)

    # First insert some data
    table = client.get_table(b"default", b"test_buffered_mutator")
    table.put(Put(b"row_delete_test").add_column(b"cf", b"col", b"value"))

    # Verify it exists
    get = Get(b"row_delete_test")
    get.add_column(b"cf", b"col")
    assert table.get(get) is not None

    # Delete via BufferedMutator
    with client.get_buffered_mutator(b"default", b"test_buffered_mutator") as mutator:
        mutator.mutate(Delete(b"row_delete_test").add_column(b"cf", b"col"))

    # Verify deletion
    assert table.get(get) is None


def test_buffered_mutator_mutate_all():
    """Test mutate_all method."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)

    mutations = [
        Put(b"row_mutate_1").add_column(b"cf", b"col", b"value1"),
        Put(b"row_mutate_2").add_column(b"cf", b"col", b"value2"),
        Put(b"row_mutate_3").add_column(b"cf", b"col", b"value3"),
    ]

    with client.get_buffered_mutator(b"default", b"test_buffered_mutator") as mutator:
        mutator.mutate_all(mutations)

    # Verify all rows
    table = client.get_table(b"default", b"test_buffered_mutator")
    for i in range(1, 4):
        get = Get(f"row_mutate_{i}".encode())
        get.add_column(b"cf", b"col")
        row = table.get(get)
        assert row is not None
        assert row.get(b"cf", b"col") == f"value{i}".encode()


def test_buffered_mutator_buffer_size():
    """Test buffer size configuration."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)

    params = BufferedMutatorParams()
    params.write_buffer_size = 64 * 1024  # 64KB

    mutator = client.get_buffered_mutator(
        b"default", b"test_buffered_mutator", params
    )

    try:
        assert mutator.get_write_buffer_size() == 64 * 1024

        # Change buffer size
        mutator.set_write_buffer_size(128 * 1024)
        assert mutator.get_write_buffer_size() == 128 * 1024

    finally:
        mutator.close()


def test_buffered_mutator_buffer_size_flush():
    """Test auto-flush when buffer size is reached."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)

    # Use minimum buffer size (64KB) to trigger auto-flush with enough data
    params = BufferedMutatorParams()
    params.write_buffer_size = 64 * 1024  # 64KB minimum
    params.write_buffer_periodic_flush = 0  # Disable periodic flush

    mutator = client.get_buffered_mutator(
        b"default", b"test_buffered_mutator", params
    )

    table = client.get_table(b"default", b"test_buffered_mutator")

    try:
        # Write enough data to trigger buffer flush (~100KB total)
        # Each row is about 60 bytes (rowkey + family + qualifier + large value)
        for i in range(2000):
            large_value = b"x" * 50  # 50 bytes each
            mutator.mutate(
                Put(f"row_size_{i}".encode()).add_column(b"cf", b"col", large_value)
            )

        # After writing ~120KB, buffer should have flushed at least once
        # Check that at least some data is visible
        count = 0
        for i in range(0, 2000, 100):  # Sample every 100th row
            get = Get(f"row_size_{i}".encode())
            get.add_column(b"cf", b"col")
            if table.get(get) is not None:
                count += 1

        # At least some rows should have been flushed
        assert count > 0

    finally:
        mutator.close()


def test_buffered_mutator_get_name():
    """Test get_name method."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)

    mutator = client.get_buffered_mutator(b"default", b"test_buffered_mutator")

    try:
        assert mutator.get_name() == b"test_buffered_mutator"
    finally:
        mutator.close()


def test_buffered_mutator_current_buffer_size():
    """Test current buffer size tracking."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)

    params = BufferedMutatorParams()
    params.write_buffer_size = 2 * 1024 * 1024  # 2MB - won't auto-flush
    params.write_buffer_periodic_flush = 0  # Disable periodic flush

    mutator = client.get_buffered_mutator(
        b"default", b"test_buffered_mutator", params
    )

    try:
        assert mutator.get_current_buffer_size() == 0
        assert mutator.get_current_buffer_size_mutations() == 0

        # Add a mutation
        mutator.mutate(Put(b"row_buf_test").add_column(b"cf", b"col", b"value"))

        # Buffer should have some data
        assert mutator.get_current_buffer_size() > 0
        assert mutator.get_current_buffer_size_mutations() == 1

        # Flush
        mutator.flush()

        # Buffer should be empty
        assert mutator.get_current_buffer_size() == 0
        assert mutator.get_current_buffer_size_mutations() == 0

    finally:
        mutator.close()


def test_buffered_mutator_closed_error():
    """Test that operations fail after close."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)

    mutator = client.get_buffered_mutator(b"default", b"test_buffered_mutator")
    mutator.close()

    # Should raise error after close
    with pytest.raises(RuntimeError, match="BufferedMutator is closed"):
        mutator.mutate(Put(b"row_closed").add_column(b"cf", b"col", b"value"))

    with pytest.raises(RuntimeError, match="BufferedMutator is closed"):
        mutator.flush()


def test_buffered_mutator_invalid_mutation():
    """Test that invalid mutation types are rejected."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)

    mutator = client.get_buffered_mutator(b"default", b"test_buffered_mutator")

    try:
        # Should raise ValueError for invalid mutation type
        with pytest.raises(ValueError, match="Unsupported mutation type"):
            mutator.mutate("not a mutation")  # type: ignore
    finally:
        mutator.close()


def test_buffered_mutator_bulk_write():
    """Test bulk write with many mutations."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)

    # Write 1000 rows
    with client.get_buffered_mutator(b"default", b"test_buffered_mutator") as mutator:
        for i in range(1000):
            mutator.mutate(
                Put(f"bulk_row_{i:04d}".encode()).add_column(
                    b"cf", b"col", f"value_{i}".encode()
                )
            )

    # Verify all rows
    table = client.get_table(b"default", b"test_buffered_mutator")
    count = 0
    for i in range(1000):
        get = Get(f"bulk_row_{i:04d}".encode())
        get.add_column(b"cf", b"col")
        row = table.get(get)
        if row is not None:
            assert row.get(b"cf", b"col") == f"value_{i}".encode()
            count += 1

    assert count == 1000


class MockExceptionListener(ExceptionListener):
    """Mock exception listener that records exceptions."""

    def __init__(self):
        self.exceptions: list[tuple] = []
        self.should_continue = True

    def on_exception(self, _mutation, exception) -> bool:
        self.exceptions.append((_mutation, exception))
        return self.should_continue


def test_buffered_mutator_exception_listener():
    """Test exception listener callback."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)

    # Create listener
    listener = MockExceptionListener()

    params = BufferedMutatorParams()
    params.write_buffer_size = 2 * 1024 * 1024  # Large buffer
    params.write_buffer_periodic_flush = 0  # Disable periodic flush

    mutator = client.get_buffered_mutator(
        b"default", b"test_buffered_mutator", params
    )
    mutator.set_exception_listener(listener)

    try:
        # Add valid mutations
        mutator.mutate(Put(b"row_listener_1").add_column(b"cf", b"col", b"value1"))

        # Flush - should succeed
        mutator.flush()

        assert len(listener.exceptions) == 0

    finally:
        mutator.close()


def test_buffered_mutator_params():
    """Test BufferedMutatorParams configuration."""
    params = BufferedMutatorParams()

    # Check defaults
    assert params.write_buffer_size == 2 * 1024 * 1024  # 2MB
    assert params.write_buffer_periodic_flush == 3000  # 3 seconds
    assert params.max_key_value_size == -1  # No limit

    # Modify
    params.write_buffer_size = 4 * 1024 * 1024  # 4MB
    params.write_buffer_periodic_flush = 5000  # 5 seconds
    params.max_key_value_size = 1024 * 1024  # 1MB

    assert params.write_buffer_size == 4 * 1024 * 1024
    assert params.write_buffer_periodic_flush == 5000
    assert params.max_key_value_size == 1024 * 1024


def test_buffered_mutator_min_buffer_size():
    """Test minimum buffer size enforcement."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)

    # Try to set buffer size below minimum
    params = BufferedMutatorParams()
    params.write_buffer_size = 1024  # Below minimum (64KB)

    mutator = client.get_buffered_mutator(
        b"default", b"test_buffered_mutator", params
    )

    try:
        # Should be at least minimum
        assert mutator.get_write_buffer_size() >= BufferedMutator.MIN_WRITE_BUFFER_SIZE

        # Try to set below minimum via setter
        with pytest.raises(ValueError, match="below minimum"):
            mutator.set_write_buffer_size(1024)

    finally:
        mutator.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
