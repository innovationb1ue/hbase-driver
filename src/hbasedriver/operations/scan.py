from collections import defaultdict
from typing import Optional

from hbasedriver.filter.filter import Filter
from hbasedriver.protobuf_py.Client_pb2 import Scan as PbScan, Column as PbColumn
from hbasedriver.protobuf_py.Filter_pb2 import Filter as PbFilter


class Scan:
    """Represents a Scan operation for retrieving multiple rows from HBase.

    The Scan class is used to retrieve data from multiple rows in HBase.
    Scans support filtering by row key range, column families/columns,
    time ranges, and server-side filters.

    Example:
        >>> from hbasedriver.operations.scan import Scan
        >>> # Scan entire table
        >>> scan = Scan()
        >>> with table.scan(scan) as scanner:
        ...     for row in scanner:
        ...         print(row)
        >>> # Scan with row key range
        >>> scan = Scan(start_row=b"row1", end_row=b"row9")
        >>> with table.scan(scan) as scanner:
        ...     for row in scanner:
        ...         print(row)
        >>> # Scan with specific columns
        >>> scan = Scan().add_column(b"cf", b"col")
        >>> with table.scan(scan) as scanner:
        ...     for row in scanner:
        ...         print(row)
        >>> # Scan with filter
        >>> from hbasedriver.filter import PrefixFilter
        >>> scan = Scan().set_filter(PrefixFilter(b"abc"))
        >>> with table.scan(scan) as scanner:
        ...     for row in scanner:
        ...         print(row)
        >>> # Pagination
        >>> scan = Scan()
        >>> rows, resume_key = table.scan_page(scan, page_size=100)
        >>> while resume_key:
        ...     scan = Scan(start_row=resume_key, include_start_row=False)
        ...     rows, resume_key = table.scan_page(scan, page_size=100)

    Attributes:
        MAX_ROWKEY_LENGTH: Maximum allowed row key length (32767 bytes)
    """

    MAX_ROWKEY_LENGTH = 32767

    def __init__(self, start_row: bytes = b'', end_row: bytes = b''):
        """Initialize a Scan operation with optional row key range.

        Args:
            start_row: Starting row key (inclusive by default)
            end_row: Ending row key (inclusive by default)

        Note:
            To get exclusive start row, use with_start_row(row, inclusive=False)
            To get exclusive end row, use with_end_row(row, inclusive=False)
        """
        self.start_row = start_row
        self.start_row_inclusive = True
        self.end_row = end_row
        self.end_row_inclusive = True
        self.min_stamp: int = 0
        self.max_stamp: int = 0x7fffffffffffffff
        self.family_map: dict[bytes, list] = defaultdict(list)
        self.limit: int = 20
        self.filter: Optional[Filter] = None

        # Java-parity scan options
        self.caching: int = 20
        self.batch_size: int = 0
        self.max_result_size: int = 0
        self.reversed: bool = False
        self.read_type = None
        self.need_cursor_result: bool = False
        self.allow_partial_results: bool = False
        self.small: bool = False
        self.mvcc_read_point: int = 0
        self.consistency = 0

    def add_family(self, family: bytes):
        """Add all columns from the specified column family.

        Args:
            family: Column family name (bytes)

        Returns:
            self (for method chaining)

        Example:
            >>> scan = Scan().add_family(b"cf")
        """
        self.family_map[family] = []
        return self

    def set_limit(self, limit: int):
        """Set the maximum number of rows to return from this scan.

        Args:
            limit: Maximum number of rows to return

        Returns:
            self (for method chaining)

        Example:
            >>> scan = Scan().set_limit(100)
        """
        self.limit = limit
        return self

    def add_column(self, family: bytes, qualifier: bytes):
        """Add a specific column to this scan.

        Args:
            family: Column family name (bytes)
            qualifier: Column qualifier name (bytes)

        Returns:
            self (for method chaining)

        Example:
            >>> scan = Scan().add_column(b"cf", b"col")
        """
        self.family_map[family].append(qualifier)
        return self

    def set_time_range(self, min_stamp: int, max_stamp: int):
        """Set the time range for this scan operation.

        Args:
            min_stamp: Minimum timestamp (milliseconds since epoch)
            max_stamp: Maximum timestamp (milliseconds since epoch)

        Returns:
            self (for method chaining)

        Example:
            >>> import time
            >>> start = int((time.time() - 86400) * 1000)
            >>> end = int(time.time() * 1000)
            >>> scan = Scan().set_time_range(start, end)
        """
        self.min_stamp = min_stamp
        self.max_stamp = max_stamp
        return self

    def with_start_row(self, start_row: bytes, inclusive: bool = True):
        """Set the start row for this scan.

        Args:
            start_row: Starting row key (bytes)
            inclusive: Whether the start row is included (default: True)

        Returns:
            self (for method chaining)

        Raises:
            ValueError: If row key length exceeds MAX_ROWKEY_LENGTH

        Example:
            >>> scan = Scan().with_start_row(b"row1")
            >>> scan = Scan().with_start_row(b"row1", inclusive=False)
        """
        if len(start_row) > Scan.MAX_ROWKEY_LENGTH:
            raise ValueError("rowkey length must be smaller than {}".format(Scan.MAX_ROWKEY_LENGTH))
        self.start_row = start_row
        self.start_row_inclusive = inclusive
        return self

    def with_end_row(self, end_row: bytes, inclusive: bool = True):
        """Set the end row for this scan.

        Args:
            end_row: Ending row key (bytes)
            inclusive: Whether the end row is included (default: True)

        Returns:
            self (for method chaining)

        Raises:
            ValueError: If row key length exceeds MAX_ROWKEY_LENGTH

        Example:
            >>> scan = Scan().with_end_row(b"row9")
            >>> scan = Scan().with_end_row(b"row9", inclusive=False)
        """
        if len(end_row) > Scan.MAX_ROWKEY_LENGTH:
            raise ValueError("rowkey length must be smaller than {}".format(Scan.MAX_ROWKEY_LENGTH))
        self.end_row = end_row
        self.end_row_inclusive = inclusive
        return self

    def set_filter(self, filter_in: Filter):
        """Set a server-side filter for this scan operation.

        Args:
            filter_in: Filter to apply server-side

        Returns:
            self (for method chaining)

        Example:
            >>> from hbasedriver.filter import PrefixFilter
            >>> scan = Scan().set_filter(PrefixFilter(b"abc"))
        """
        self.filter = filter_in
        return self

    def setCaching(self, caching: int):
        """Set the number of rows to cache on the client side.

        This is the Java HBase API name. See also `setBatch()`.

        Args:
            caching: Number of rows to cache

        Returns:
            self (for method chaining)
        """
        self.caching = caching
        return self

    def setBatch(self, batch_size: int):
        """Set the number of columns to fetch in each RPC.

        This is useful for wide rows with many columns.

        Args:
            batch_size: Number of columns per batch

        Returns:
            self (for method chaining)

        Example:
            >>> scan = Scan().setBatch(100)
        """
        self.batch_size = batch_size
        return self

    def setMaxResultSize(self, size: int):
        """Set the maximum result size in bytes.

        Args:
            size: Maximum result size in bytes

        Returns:
            self (for method chaining)
        """
        self.max_result_size = size
        return self

    def setReversed(self, reversed_: bool):
        """Set whether to scan in reverse order.

        Args:
            reversed_: True for reverse scan, False for normal scan

        Returns:
            self (for method chaining)

        Example:
            >>> scan = Scan().setReversed(True)
        """
        self.reversed = reversed_
        return self

    def setReadType(self, read_type):
        """Set the read type for this scan.

        Args:
            read_type: Read type (e.g., ReadType.DEFAULT, ReadType.STREAM)

        Returns:
            self (for method chaining)
        """
        self.read_type = read_type
        return self

    def setNeedCursorResult(self, val: bool):
        """Set whether cursor results are needed.

        Args:
            val: True if cursor results needed

        Returns:
            self (for method chaining)
        """
        self.need_cursor_result = val
        return self

    def setAllowPartialResults(self, val: bool):
        """Set whether to allow partial results.

        Args:
            val: True to allow partial results

        Returns:
            self (for method chaining)
        """
        self.allow_partial_results = val
        return self

    def setSmall(self, val: bool):
        """Set whether this is a small scan.

        Args:
            val: True for small scan

        Returns:
            self (for method chaining)
        """
        self.small = val
        return self

    def setConsistency(self, consistency):
        """Set the consistency level for this scan.

        Args:
            consistency: Consistency level

        Returns:
            self (for method chaining)
        """
        self.consistency = consistency
        return self

    def to_protobuf(self):
        """Create and return a protobuf Scan message populated from this Scan."""
        pb_scan = PbScan()
        pb_scan.include_start_row = self.start_row_inclusive
        pb_scan.start_row = self.start_row
        pb_scan.stop_row = self.end_row
        pb_scan.include_stop_row = self.end_row_inclusive

        if self.filter is not None:
            # protobuf Filter can be constructed from name + serialized bytes
            pb_filter = PbFilter()
            pb_filter.name = self.filter.get_name()
            pb_filter.serialized_filter = self.filter.to_byte_array()
            pb_scan.filter.CopyFrom(pb_filter)

        if self.batch_size:
            pb_scan.batch_size = self.batch_size
        if self.max_result_size:
            pb_scan.max_result_size = self.max_result_size
        if self.caching:
            pb_scan.caching = self.caching

        pb_scan.reversed = self.reversed
        if self.read_type is not None:
            pb_scan.readType = self.read_type

        pb_scan.need_cursor_result = self.need_cursor_result
        pb_scan.allow_partial_results = self.allow_partial_results
        pb_scan.small = self.small

        if self.mvcc_read_point:
            pb_scan.mvcc_read_point = self.mvcc_read_point
        if self.consistency is not None:
            pb_scan.consistency = self.consistency

        for family, qfs in self.family_map.items():
            if len(qfs) == 0:
                pb_scan.column.append(PbColumn(family=family))
            else:
                for qf in qfs:
                    pb_scan.column.append(PbColumn(family=family, qualifier=qf))

        return pb_scan
