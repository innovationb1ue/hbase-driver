"""
Type stubs for hbasedriver.model module.

This file provides type hints for IDE autocomplete without importing
the actual implementations at runtime.
"""

from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:

    class CellType:
        """Cell type enumeration.

        Values:
            - PUT: Cell was inserted/updated
            - DELETE: Cell was deleted
        """

        PUT: int
        DELETE: int

    class Cell:
        """Represents a single cell in HBase.

        A cell contains a value with its metadata (family, qualifier,
        timestamp, and type).

        Example:
            >>> cell = Cell(
            ...     rowkey=b"row1",
            ...     family=b"cf",
            ...     qualifier=b"col",
            ...     value=b"value",
            ...     ts=1234567890,
            ...     cell_type=CellType.PUT
            ... )
            >>>
        """

        rowkey: bytes
        family: bytes
        qualifier: bytes
        value: bytes
        ts: int
        type: CellType

        def __init__(
            self,
            rowkey: bytes,
            family: bytes,
            qualifier: bytes,
            value: bytes,
            ts: int = None,
            type: CellType = CellType.PUT
        ) -> None: ...

    class Row:
        """Represents a row in HBase.

        A row contains multiple cells from different column families.
        Provides dict-like access for cell values.

        Example:
            >>> row = Row(b"row1", {b"cf:col": Cell(...)})
            >>> value = row.get(b"cf", b"col")
            >>> print(f"Row: {row.rowkey}, Value: {value.value}")
            >>>
        """

        rowkey: bytes
        kv: Dict[bytes, Dict[bytes, Cell]]

        def __init__(self, rowkey: bytes, kv: Dict[bytes, Dict[bytes, Cell]]) -> None: ...

        def get(self, family: bytes, qualifier: bytes) -> Any: ...

        def __contains__(self, item: Any) -> bool: ...

        def __iter__(self) -> Row: ...

        def __str__(self) -> str: ...

else:
    # Runtime type stubs - minimal imports to avoid import errors
    CellType = object  # type: ignore
    Cell = object  # type: ignore
    Row = object  # type: ignore
