"""
Test batch operations functionality.

Tests for Batch, BatchGet, BatchPut, CheckAndPut, and Increment operations.
"""
import pytest

from hbasedriver.client.client import Client
from hbasedriver.operations.put import Put
from hbasedriver.operations.get import Get
from hbasedriver.operations.delete import Delete
from hbasedriver.operations.scan import Scan
from hbasedriver.operations.batch import Batch, BatchGet, BatchPut
from hbasedriver.operations.increment import Increment, CheckAndPut
from hbasedriver.protobuf_py.HBase_pb2 import ColumnFamilySchema
from hbasedriver.common.table_name import TableName


def setup_module():
    """Setup test table."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    admin = client.get_admin()

    # Create table if not exists
    tn = TableName.value_of(b"default", b"test_batch")
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

    tn = TableName.value_of(b"default", b"test_batch")
    try:
        admin.disable_table(tn)
        admin.delete_table(tn)
    except Exception:
        pass


def test_batch_put_simple():
    """Test simple batch put operations."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    table = client.get_table(b"default", b"test_batch")

    # Use batch to put multiple rows
    with table.batch() as b:
        b.put(b"row1", {b"cf:col1": b"value1"})
        b.put(b"row2", {b"cf:col1": b"value2"})
        b.put(b"row3", {b"cf:col1": b"value3"})

    # Verify all rows were inserted
    get1 = Get(b"row1")
    get1.add_column(b"cf", b"col1")
    row1 = table.get(get1)
    assert row1 is not None
    assert row1.get(b"cf", b"col1") == b"value1"

    get2 = Get(b"row2")
    get2.add_column(b"cf", b"col1")
    row2 = table.get(get2)
    assert row2 is not None
    assert row2.get(b"cf", b"col1") == b"value2"

    get3 = Get(b"row3")
    get3.add_column(b"cf", b"col1")
    row3 = table.get(get3)
    assert row3 is not None
    assert row3.get(b"cf", b"col1") == b"value3"


def test_batch_put_with_multiple_columns():
    """Test batch put with multiple columns per row."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    table = client.get_table(b"default", b"test_batch")

    # Use batch with multiple columns
    with table.batch() as b:
        b.put(b"row4", {
            b"cf:col1": b"value1",
            b"cf:col2": b"value2",
            b"cf:col3": b"value3"
        })

    # Verify
    get = Get(b"row4")
    get.add_column(b"cf", b"col1")
    get.add_column(b"cf", b"col2")
    get.add_column(b"cf", b"col3")
    row = table.get(get)
    assert row is not None
    assert row.get(b"cf", b"col1") == b"value1"
    assert row.get(b"cf", b"col2") == b"value2"
    assert row.get(b"cf", b"col3") == b"value3"


def test_batch_put_with_family_dict():
    """Test batch put using family dictionary format."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    table = client.get_table(b"default", b"test_batch")

    # Use batch with family dict format
    with table.batch() as b:
        b.put(b"row5", {
            b"cf": {
                b"col1": b"value1",
                b"col2": b"value2"
            }
        })

    # Verify
    get = Get(b"row5")
    get.add_column(b"cf", b"col1")
    get.add_column(b"cf", b"col2")
    row = table.get(get)
    assert row is not None
    assert row.get(b"cf", b"col1") == b"value1"
    assert row.get(b"cf", b"col2") == b"value2"


def test_batch_delete():
    """Test batch delete operations."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    table = client.get_table(b"default", b"test_batch")

    # First insert some rows
    with table.batch() as b:
        b.put(b"row10", {b"cf:col1": b"value1"})
        b.put(b"row11", {b"cf:col1": b"value1"})
        b.put(b"row12", {b"cf:col1": b"value1"})

    # Verify they exist
    get10 = Get(b"row10")
    get10.add_column(b"cf", b"col1")
    assert table.get(get10) is not None

    # Delete rows using batch
    with table.batch() as b:
        b.delete(b"row10")
        b.delete(b"row11")

    # Verify deletions
    assert table.get(get10) is None

    get11 = Get(b"row11")
    get11.add_column(b"cf", b"col1")
    assert table.get(get11) is None

    # Verify row12 still exists
    get12 = Get(b"row12")
    get12.add_column(b"cf", b"col1")
    assert table.get(get12) is not None


def test_batch_delete_columns():
    """Test batch delete specific columns."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    table = client.get_table(b"default", b"test_batch")

    # Insert row with multiple columns
    with table.batch() as b:
        b.put(b"row20", {
            b"cf:col1": b"value1",
            b"cf:col2": b"value2",
            b"cf:col3": b"value3"
        })

    # Delete specific columns
    with table.batch() as b:
        b.delete(b"row20", [b"cf:col1", b"cf:col2"])

    # Verify col1 and col2 are gone, col3 remains
    get = Get(b"row20")
    get.add_column(b"cf", b"col1")
    get.add_column(b"cf", b"col2")
    get.add_column(b"cf", b"col3")
    row = table.get(get)

    assert row is not None
    assert row.get(b"cf", b"col1") is None
    assert row.get(b"cf", b"col2") is None
    assert row.get(b"cf", b"col3") == b"value3"


def test_batch_auto_flush():
    """Test batch auto-flush when batch size is reached."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    table = client.get_table(b"default", b"test_batch")

    # Use small batch size to test auto-flush
    batch_size = 3
    with table.batch(batch_size=batch_size) as b:
        for i in range(10):
            b.put(f"row_auto_{i}".encode(), {b"cf:col1": f"value{i}".encode()})

    # Verify all rows were inserted
    count = 0
    for i in range(10):
        get = Get(f"row_auto_{i}".encode())
        get.add_column(b"cf", b"col1")
        row = table.get(get)
        if row is not None:
            count += 1

    assert count == 10


def test_batch_get():
    """Test batch get operations."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    table = client.get_table(b"default", b"test_batch")

    # Insert some rows
    with table.batch() as b:
        b.put(b"row30", {b"cf:col1": b"value30"})
        b.put(b"row31", {b"cf:col1": b"value31"})
        b.put(b"row32", {b"cf:col1": b"value32"})

    # Batch get
    bg = BatchGet([b"row30", b"row31", b"row32", b"row_nonexistent"])
    bg.add_column(b"cf", b"col1")

    results = table.batch_get(bg)

    assert len(results) == 4
    assert results[b"row30"] is not None
    assert results[b"row30"].get(b"cf", b"col1") == b"value30"
    assert results[b"row31"] is not None
    assert results[b"row31"].get(b"cf", b"col1") == b"value31"
    assert results[b"row32"] is not None
    assert results[b"row32"].get(b"cf", b"col1") == b"value32"
    assert results[b"row_nonexistent"] is None


def test_batch_put_class():
    """Test BatchPut class."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    table = client.get_table(b"default", b"test_batch")

    # Create BatchPut
    bp = BatchPut()
    bp.add_put(Put(b"row40").add_column(b"cf", b"col1", b"value40"))
    bp.add_put(Put(b"row41").add_column(b"cf", b"col1", b"value41"))

    # Execute
    results = table.batch_put(bp)
    assert all(results)  # All should succeed

    # Verify
    get40 = Get(b"row40")
    get40.add_column(b"cf", b"col1")
    row40 = table.get(get40)
    assert row40 is not None
    assert row40.get(b"cf", b"col1") == b"value40"


def test_check_and_put_success():
    """Test CheckAndPut when check passes."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    table = client.get_table(b"default", b"test_batch")

    # Insert initial value
    put = Put(b"row50")
    put.add_column(b"cf", b"lock", b"")
    table.put(put)

    # CheckAndPut - lock is empty, should succeed
    cap = CheckAndPut(b"row50")
    cap.set_check(b"cf", b"lock", b"")
    cap.set_put(Put(b"row50").add_column(b"cf", b"data", b"locked"))

    result = table.check_and_put(cap)
    assert result is True

    # Verify value was set
    get = Get(b"row50")
    get.add_column(b"cf", b"data")
    row = table.get(get)
    assert row is not None
    assert row.get(b"cf", b"data") == b"locked"


def test_check_and_put_failure():
    """Test CheckAndPut when check fails."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    table = client.get_table(b"default", b"test_batch")

    # Insert initial value
    put = Put(b"row51")
    put.add_column(b"cf", b"lock", b"locked")
    table.put(put)

    # CheckAndPut - lock is not empty, should fail
    cap = CheckAndPut(b"row51")
    cap.set_check(b"cf", b"lock", b"")
    cap.set_put(Put(b"row51").add_column(b"cf", b"data", b"should_not_set"))

    result = table.check_and_put(cap)
    assert result is False

    # Verify value was NOT set
    get = Get(b"row51")
    get.add_column(b"cf", b"data")
    row = table.get(get)
    assert row is None or row.get(b"cf", b"data") is None


def test_increment_new():
    """Test increment on new column."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    table = client.get_table(b"default", b"test_batch")

    # Increment non-existent counter
    inc = Increment(b"row60")
    inc.add_column(b"cf", b"counter", 5)

    new_value = table.increment(inc)
    assert new_value == 5

    # Verify
    get = Get(b"row60")
    get.add_column(b"cf", b"counter")
    row = table.get(get)
    assert row is not None
    assert row.get(b"cf", b"counter") == b"5"


def test_increment_existing():
    """Test increment on existing counter."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    table = client.get_table(b"default", b"test_batch")

    # Set initial value
    put = Put(b"row61")
    put.add_column(b"cf", b"counter", b"10")
    table.put(put)

    # Increment
    inc = Increment(b"row61")
    inc.add_column(b"cf", b"counter", 7)

    new_value = table.increment(inc)
    assert new_value == 17

    # Verify
    get = Get(b"row61")
    get.add_column(b"cf", b"counter")
    row = table.get(get)
    assert row is not None
    assert row.get(b"cf", b"counter") == b"17"


def test_increment_multiple():
    """Test increment with multiple columns."""
    conf = {"hbase.zookeeper.quorum": "hbase-zk:2181"}
    client = Client(conf)
    table = client.get_table(b"default", b"test_batch")

    # Increment multiple counters
    inc = Increment(b"row62")
    inc.add_column(b"cf", b"counter1", 5)
    inc.add_column(b"cf", b"counter2", 10)

    new_value = table.increment(inc)
    assert new_value == 10  # Returns last incremented value

    # Verify
    get = Get(b"row62")
    get.add_column(b"cf", b"counter1")
    get.add_column(b"cf", b"counter2")
    row = table.get(get)
    assert row is not None
    assert row.get(b"cf", b"counter1") == b"5"
    assert row.get(b"cf", b"counter2") == b"10"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
