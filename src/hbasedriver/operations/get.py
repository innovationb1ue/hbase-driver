from collections import defaultdict

from hbasedriver.filter.filter import Filter


class Get:
    """Represents a Get operation for retrieving a single row from HBase.

    The Get class is used to retrieve data from a single row in HBase.
    You can specify which column families and columns to retrieve,
    filter by time range, and apply server-side filters.

    Example:
        >>> from hbasedriver.operations.get import Get
        >>> # Get entire row
        >>> get = Get(b"row1")
        >>> row = table.get(get)
        >>> # Get specific column
        >>> get = Get(b"row1").add_column(b"cf", b"col")
        >>> row = table.get(get)
        >>> # Get with multiple columns
        >>> get = Get(b"row2")
        >>>     .add_column(b"cf", b"name")
        >>>     .add_column(b"cf", b"age")
        >>> row = table.get(get)
        >>> # Get with time range
        >>> import time
        >>> start_ts = int((time.time() - 86400) * 1000)  # 24 hours ago
        >>> end_ts = int(time.time() * 1000)
        >>> get = Get(b"row3").set_time_range(start_ts, end_ts)
        >>> row = table.get(get)

    Attributes:
        rowkey: The row key to retrieve
        family_columns: Dictionary mapping column families to qualifiers
        time_ranges: Tuple of (min_timestamp, max_timestamp)
        max_versions: Maximum number of versions to retrieve
        check_existence_only: If True, only check existence without retrieving data
        filter: Optional filter to apply server-side
    """

    def __init__(self, rowkey: bytes):
        """Initialize a Get operation for the given row key.

        Args:
            rowkey: The row key (as bytes) to retrieve
        """
        self.rowkey = rowkey
        self.family_columns: dict[bytes, list[bytes]] = defaultdict(list)
        self.time_ranges = (0, 0x7fffffffffffffff)
        self.max_versions = 1
        self.check_existence_only = False
        self.filter: Filter | None = None

    def add_family(self, family: bytes):
        """Add all columns from a column family to this get operation.

        Args:
            family: Column family name (bytes)

        Returns:
            self (for method chaining)

        Example:
            >>> get = Get(b"row1").add_family(b"cf")
        """
        self.family_columns[family] = []
        return self

    def add_column(self, family: bytes, qualifier: bytes):
        """Add a specific column to this get operation.

        Args:
            family: Column family name (bytes)
            qualifier: Column qualifier name (bytes)

        Returns:
            self (for method chaining)

        Example:
            >>> get = Get(b"row1").add_column(b"cf", b"col")
        """
        self.family_columns[family].append(qualifier)
        return self

    def set_time_range(self, min_ts, max_ts):
        """Set the time range for this get operation.

        Args:
            min_ts: Minimum timestamp (milliseconds since epoch)
            max_ts: Maximum timestamp (milliseconds since epoch)

        Returns:
            self (for method chaining)
        """
        self.time_ranges = (min_ts, max_ts)
        return self

    def set_time_stamp(self, ts):
        """Set a specific timestamp for this get operation.

        Only cells with the exact specified timestamp will be returned.

        Args:
            ts: Timestamp in milliseconds since epoch

        Returns:
            self (for method chaining)
        """
        self.time_ranges = (ts, ts + 1)
        return self

    def read_versions(self, versions):
        """Set the maximum number of versions to retrieve.

        Args:
            versions: Maximum number of versions to retrieve (must be positive)

        Raises:
            Exception: If versions is not positive

        Example:
            >>> get = Get(b"row1").read_versions(3)
        """
        if versions <= 0:
            raise Exception("versions must be positive")
        self.max_versions = versions

    def set_filter(self, filter_in: Filter):
        """Set a server-side filter for this get operation.

        Args:
            filter_in: Filter to apply server-side
        """
        self.filter = filter_in

    def set_check_existence_only(self, check_existence_only: bool):
        """Set whether to only check row existence without retrieving data.

        When set to True, the operation only verifies if the row exists
        without returning any data. This is more efficient than retrieving
        all data when you only need to know if a row exists.

        Args:
            check_existence_only: If True, only check existence

        Returns:
            self (for method chaining)

        Example:
            >>> get = Get(b"row1").set_check_existence_only(True)
            >>> row = table.get(get)  # Returns Row if exists, None otherwise
        """
        self.check_existence_only = check_existence_only
        return self
