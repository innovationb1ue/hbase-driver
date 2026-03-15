"""Append operation for atomic appends to column values.

This module provides the Append class for atomically appending data to
existing column values in HBase.
"""
from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

from hbasedriver.model import Cell, CellType

if TYPE_CHECKING:
    pass


class Append:
    """Append operation for atomically appending to column values.

    This operation atomically appends data to the end of existing column values.
    If the column does not exist, the append value becomes the initial value.

    This is a server-side atomic operation (equivalent to Java HBase's Append).

    Example:
        >>> # Append to existing values
        >>> append = Append(b'row1')
        >>> append.add_column(b'cf', b'suffix_col', b'_suffix')
        >>> append.add_column(b'cf', b'tags', b',new_tag')
        >>>
        >>> result = table.append(append)
        >>> # result contains the new values after append
        >>> new_value = result.get(b'cf', b'suffix_col')

    Attributes:
        rowkey: The row key for this append operation
        family_cells: Dictionary mapping column families to list of Cells
        return_results: Whether to return the new values after append
    """

    def __init__(self, rowkey: bytes):
        """Initialize an Append operation.

        Args:
            rowkey: The row key (as bytes) for this append operation
        """
        self.rowkey = rowkey
        self.family_cells: dict[bytes, list[Cell]] = defaultdict(list)
        self.return_results: bool = True

    def add_column(
        self,
        family: bytes,
        qualifier: bytes,
        value: bytes,
        ts: int | None = None
    ) -> 'Append':
        """Add a column to append to.

        Args:
            family: Column family name (bytes)
            qualifier: Column qualifier name (bytes)
            value: Value to append (bytes)
            ts: Optional timestamp. If not provided, server assigns one.

        Returns:
            self (for method chaining)

        Example:
            >>> append = Append(b'row1')
            >>> append.add_column(b'cf', b'col', b'_suffix')
        """
        self.family_cells[family].append(
            Cell(self.rowkey, family, qualifier, value, ts, CellType.PUT)
        )
        return self

    def set_return_results(self, return_results: bool) -> 'Append':
        """Set whether to return the new values after append.

        Args:
            return_results: If True, returns the new values after append

        Returns:
            self (for method chaining)
        """
        self.return_results = return_results
        return self

    def get_family_cells(self) -> dict[bytes, list[Cell]]:
        """Get the family cells dictionary.

        Returns:
            Dictionary mapping column families to list of Cells
        """
        return self.family_cells
