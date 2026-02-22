"""
Batch operations for bulk data operations.

This module provides BatchPut and BatchGet classes for efficient bulk
operations, as well as a Batch context manager for transaction-like behavior.
"""
from typing import TYPE_CHECKING

from hbasedriver.operations.delete import Delete
from hbasedriver.operations.get import Get
from hbasedriver.operations.put import Put

if TYPE_CHECKING:
    from hbasedriver.client.table import Table


class Batch:
    """
    Batch context manager for grouping multiple operations.

    Operations are collected and can be executed together for better performance.

    Example:
        with table.batch() as b:
            b.put(b'row1', {b'cf:col1': b'value1'})
            b.put(b'row2', {b'cf:col1': b'value2'})
            b.delete(b'row3')
        # All operations are executed when exiting the context
    """

    def __init__(self, table: 'Table', batch_size: int = 1000):
        """
        Initialize a Batch object.

        Args:
            table: The Table object to perform operations on
            batch_size: Maximum number of operations to buffer before auto-flush
        """
        self.table = table
        self.batch_size = batch_size
        self._puts: list[Put] = []
        self._deletes: list[Delete] = []

    def put(self, rowkey: bytes, data: dict[bytes, bytes]):
        """
        Add a put operation to the batch.

        Args:
            rowkey: The row key
            data: Dictionary of {column_family:qualifier: value} or {column: value}
                    Can be in format {b'cf:col': b'value'} or {b'cf:col': b'value'}
                    For multiple values: {b'cf': {b'col1': b'val1', b'col2': b'val2'}}
        """
        put = Put(rowkey)

        for key, value in data.items():
            if b':' in key:
                # Format: b'cf:qualifier' -> b'value'
                family, qualifier = key.split(b':', 1)
                put.add_column(family, qualifier, value)
            else:
                # Format: b'family' -> {b'qualifier': b'value'}
                family = key
                for qualifier, val in value.items():
                    put.add_column(family, qualifier, val)

        self._puts.append(put)

        # Auto-flush if batch size reached
        if len(self._puts) >= self.batch_size:
            self._flush_puts()

    def delete(self, rowkey: bytes, columns: list[bytes] | None = None):
        """
        Add a delete operation to the batch.

        Args:
            rowkey: The row key to delete
            columns: Optional list of columns to delete. If None, deletes entire row.
                     Format: [b'cf:qualifier', ...]
        """
        delete = Delete(rowkey)

        if columns:
            for column in columns:
                if b':' in column:
                    family, qualifier = column.split(b':', 1)
                    delete.add_column(family, qualifier)
                else:
                    delete.add_family(column)
        else:
            # Delete all families in the row
            delete = Delete(rowkey)
            # Will delete entire row when executed

        self._deletes.append(delete)

        # Auto-flush if batch size reached
        if len(self._deletes) >= self.batch_size:
            self._flush_deletes()

    def _flush_puts(self):
        """Execute all pending put operations."""
        for put in self._puts:
            self.table.put(put)
        self._puts.clear()

    def _flush_deletes(self):
        """Execute all pending delete operations."""
        for delete in self._deletes:
            self.table.delete(delete)
        self._deletes.clear()

    def __enter__(self):
        """Enter the batch context."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the batch context and flush all operations."""
        if exc_type is None:
            # No exception, flush all operations
            self._flush_puts()
            self._flush_deletes()
        else:
            # Exception occurred, clear pending operations
            self._puts.clear()
            self._deletes.clear()
        return False


class BatchGet:
    """
    Bulk get operations for retrieving multiple rows efficiently.

    Example:
        bg = BatchGet([b'row1', b'row2', b'row3'])
        bg.add_column(b'cf', b'col1')
        bg.set_max_versions(1)

        results = table.batch_get(bg)
        for rowkey, row in results.items():
            print(rowkey, row)
    """

    def __init__(self, rowkeys: list[bytes]):
        """
        Initialize a BatchGet operation.

        Args:
            rowkeys: List of row keys to retrieve
        """
        self.rowkeys = rowkeys
        self.family_columns: dict[bytes, list[bytes]] = {}
        self.max_versions: int = 1
        self.time_ranges: tuple[int, int] = (0, 0x7fffffffffffffff)
        self.check_existence_only: bool = False

    def add_column(self, family: bytes, qualifier: bytes | None = None):
        """
        Add a column to fetch.

        Args:
            family: Column family
            qualifier: Optional column qualifier. If None, fetches all qualifiers in family.
        """
        if qualifier is None:
            self.family_columns[family] = []
        else:
            if family not in self.family_columns:
                self.family_columns[family] = []
            self.family_columns[family].append(qualifier)

    def add_family(self, family: bytes):
        """
        Add a column family to fetch.

        Args:
            family: Column family
        """
        self.family_columns[family] = []

    def set_max_versions(self, max_versions: int):
        """
        Set the maximum number of versions to return.

        Args:
            max_versions: Maximum number of versions
        """
        self.max_versions = max_versions
        return self

    def set_time_range(self, start_ts: int, end_ts: int):
        """
        Set the time range for the get.

        Args:
            start_ts: Start timestamp (inclusive), 0 means beginning of time
            end_ts: End timestamp (exclusive), max value means end of time
        """
        self.time_ranges = (start_ts, end_ts)
        return self

    def set_check_existence_only(self, check_only: bool):
        """
        Set whether to only check for existence without fetching data.

        Args:
            check_only: If True, only checks if row exists
        """
        self.check_existence_only = check_only
        return self


class BatchPut:
    """
    Bulk put operations for inserting multiple rows efficiently.

    Example:
        bp = BatchPut()
        bp.add_put(Put(b'row1').add_column(b'cf', b'col1', b'value1'))
        bp.add_put(Put(b'row2').add_column(b'cf', b'col1', b'value2'))

        table.batch_put(bp)
    """

    def __init__(self):
        """Initialize a BatchPut operation."""
        self.puts: list[Put] = []

    def add_put(self, put: Put):
        """
        Add a put operation to the batch.

        Args:
            put: Put operation to add
        """
        self.puts.append(put)
        return self

    def add_puts(self, puts: list[Put]):
        """
        Add multiple put operations to the batch.

        Args:
            puts: List of Put operations to add
        """
        self.puts.extend(puts)
        return self
