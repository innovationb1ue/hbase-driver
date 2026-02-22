from collections import defaultdict

from hbasedriver.model import Cell, CellType


class Put:
    """Represents a Put operation for inserting/updating a row in HBase.

    The Put class is used to build a mutation operation that inserts or updates
    one or more columns in a single row. This is the HBase equivalent of an
    INSERT or UPDATE statement in SQL.

    Example:
        >>> from hbasedriver.operations.put import Put
        >>> # Simple put with one column
        >>> put = Put(b"row1").add_column(b"cf", b"col", b"value")
        >>> table.put(put)
        >>> # Put with multiple columns
        >>> put = Put(b"row2")
        >>>     .add_column(b"cf", b"name", b"Alice")
        >>>     .add_column(b"cf", b"age", b"30")
        >>>     .add_column(b"info", b"email", b"alice@example.com")
        >>> table.put(put)
        >>> # Put with explicit timestamp
        >>> import time
        >>> ts = int(time.time() * 1000)
        >>> put = Put(b"row3").add_column(b"cf", b"col", b"value", ts=ts)
        >>> table.put(put)

    Attributes:
        rowkey: The row key for this put operation
        family_cells: Dictionary mapping column families to list of Cells
    """

    def __init__(self, rowkey: bytes):
        """Initialize a Put operation for the given row key.

        Args:
            rowkey: The row key (as bytes) for this put operation
        """
        self.rowkey = rowkey
        self.family_cells: dict[bytes, list[Cell]] = defaultdict(list)

    def add_column(self, family: bytes, qualifier: bytes, value: bytes, ts: int = None):
        """Add a column to this put operation.

        Args:
            family: Column family name (bytes)
            qualifier: Column qualifier name (bytes)
            value: Column value (bytes)
            ts: Optional timestamp (milliseconds since epoch). If not provided,
                the server will assign a timestamp.

        Returns:
            self (for method chaining)

        Raises:
            ValueError: If timestamp is negative

        Example:
            >>> put = Put(b"row1").add_column(b"cf", b"col", b"value")
        """
        if ts is not None and ts < 0:
            raise ValueError("Timestamp cannot be negative. ts={}".format(ts))
        self.family_cells[family].append(Cell(self.rowkey, family, qualifier, value, ts, CellType.PUT))
        return self
