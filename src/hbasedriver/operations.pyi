"""
Type stubs for hbasedriver.operations module.

This file provides type hints for IDE autocomplete without importing
the actual implementations at runtime.
"""

from typing import TYPE_CHECKING, Any, List, Optional

if TYPE_CHECKING:
    from hbasedriver.model import Cell, CellType

    class Put:
        """Put operation for inserting or updating a row in HBase.

        Use builder methods to add columns and set attributes.

        Example:
            >>> put = Put(b"row1").add_column(b"cf", b"col", b"value")
            >>> put.set_timestamp(1234567890)
            >>> table.put(put)
            >>>
        """

        rowkey: bytes
        mutations: list
        attributes: dict

        def __init__(self, rowkey: bytes, **attributes) -> None: ...

        def add_column(self, family: bytes, qualifier: bytes, value: bytes, timestamp: int = None) -> Put: ...

        def set_timestamp(self, timestamp: int) -> Put: ...

        def set_durability(self, durability: Any) -> Put: ...

        def __str__(self) -> str: ...

    class Get:
        """Get operation for retrieving a row from HBase.

        Example:
            >>> get = Get(b"row1").add_column(b"cf", b"col")
            >>> get.set_time_range(start_ts, end_ts)
            >>> get.set_check_existence_only(True)
            >>> row = table.get(get)
            >>>
        """

        rowkey: bytes
        mutations: list
        max_versions: int
        time_range: Optional[tuple[int, int]]
        check_existence_only: bool
        attributes: dict

        def __init__(self, rowkey: bytes, **attributes) -> None: ...

        def add_column(self, family: bytes, qualifier: bytes) -> Get: ...

        def set_max_versions(self, max_versions: int) -> Get: ...

        def set_time_range(self, start_ts: int, end_ts: int) -> Get: ...

        def set_check_existence_only(self, check_only: bool) -> Get: ...

        def __str__(self) -> str: ...

    class Delete:
        """Delete operation for removing a row or specific columns from HBase.

        Example:
            >>> delete = Delete(b"row1")
            >>> delete.add_column(b"cf", b"col")
            >>> table.delete(delete)
            >>>
        """

        rowkey: bytes
        mutations: list
        attributes: dict

        def __init__(self, rowkey: bytes, **attributes) -> None: ...

        def add_column(self, family: bytes, qualifier: bytes) -> Delete: ...

        def __str__(self) -> str: ...

    class Scan:
        """Scan operation for reading multiple rows from HBase.

        Supports start row, stop row, row limit, caching, and filtering.

        Example:
            >>> scan = Scan(start_row=b"row1", end_row=b"row9")
            >>> scan.add_column(b"cf", b"col")
            >>> scan.set_limit(100)
            >>> scan.set_filter(PrefixFilter(b"abc"))
            >>> with table.scan(scan) as scanner:
            ...     for row in scanner:
            ...         print(row)
            >>>
        """

        start_row: bytes
        end_row: bytes
        end_row_inclusive: bool
        max_versions: int
        batch_size: int
        columns: list[tuple[bytes, bytes]]
        cache_blocks: bool
        reversed: bool
        limit: int
        small: bool
        allow_partial_results: bool
        consistency: Any
        filter: Any
        read_all_versions: bool
        attributes: dict

        def __init__(self, **attributes) -> None: ...

        def set_start_row(self, row: bytes) -> Scan: ...

        def set_stop_row(self, row: bytes) -> Scan: ...

        def add_column(self, family: bytes, qualifier: bytes) -> Scan: ...

        def set_limit(self, limit: int) -> Scan: ...

        def set_filter(self, filter: Any) -> Scan: ...

        def __str__(self) -> str: ...

    class BatchGet:
        """Batch get operation for retrieving multiple rows efficiently.

        Example:
            >>> bg = BatchGet([b"row1", b"row2", b"row3"])
            >>> bg.add_column(b"cf", b"col")
            >>> results = table.batch_get(bg)
            >>>
        """

        rowkeys: list[bytes]
        mutations: list

        def __init__(self, rowkeys: list[bytes], **attributes) -> None: ...

        def add_column(self, family: bytes, qualifier: bytes) -> BatchGet: ...

        def __str__(self) -> str: ...

    class BatchPut:
        """Batch put operation for inserting multiple rows efficiently.

        Example:
            >>> with table.batch(batch_size=100) as batch:
            ...     batch.put(b"row1", {b"cf:col": "value1"})
            ...     batch.put(b"row2", {b"cf:col": "value2"})
            >>>
        """

        batch: list[tuple[bytes, dict[bytes, Any]]]
        attributes: dict

        def __init__(self, **attributes) -> None: ...

        def add_put(self, rowkey: bytes, data: dict[bytes, Any]) -> BatchPut: ...

        def __str__(self) -> str: ...

    class Increment:
        """Increment operation for atomically incrementing a counter value.

        Example:
            >>> inc = Increment(b"counter").add_column(b"cf", b"count", 1)
            >>> new_value = table.increment(inc)
            >>> print(f"New value: {new_value}")
            >>>
        """

        rowkey: bytes
        mutations: list
        attributes: dict

        def __init__(self, rowkey: bytes, **attributes) -> None: ...

        def add_column(self, family: bytes, qualifier: bytes, amount: int) -> Increment: ...

        def __str__(self) -> str: ...

    class CheckAndPut:
        """Check and Put operation for conditional atomic updates.

        Checks if a column has a specific value, and if so, puts a new value.
        Similar to CAS (Compare-And-Swap) operation.

        Example:
            >>> cap = CheckAndPut(b"lock")
            >>> cap.set_check(b"cf", b"lock", b"")  # Check if empty
            >>> cap.set_put(Put(b"lock").add_column(b"cf", b"lock", b"held"))
            >>> success = table.check_and_put(cap)
            >>> if success:
            ...     print("Lock acquired!")
            >>>
        """

        rowkey: bytes
        mutations: list
        attributes: dict

        def __init__(self, rowkey: bytes, **attributes) -> None: ...

        def set_check(self, family: bytes, qualifier: bytes, value: bytes) -> CheckAndPut: ...

        def set_put(self, put: Put) -> CheckAndPut: ...

        def __str__(self) -> str: ...

else:
    # Runtime type stubs - minimal imports to avoid import errors
    Put = object  # type: ignore
    Get = object  # type: ignore
    Delete = object  # type: ignore
    Scan = object  # type: ignore
    BatchGet = object  # type: ignore
    BatchPut = object  # type: ignore
    Increment = object  # type: ignore
    CheckAndPut = object  # type: ignore
    Cell = object  # type: ignore
    CellType = object  # type: ignore
