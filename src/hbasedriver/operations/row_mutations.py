"""RowMutations operation for atomic multi-mutation on a single row.

This module provides RowMutations class for performing multiple mutations
(Put, Delete, Increment, Append) atomically on a single row.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from hbasedriver.operations.put import Put
from hbasedriver.operations.delete import Delete
from hbasedriver.operations.increment import Increment
from hbasedriver.operations.append import Append

if TYPE_CHECKING:
    pass


class RowMutations:
    """Atomic multi-mutation operation on a single row.

    This operation performs multiple mutations (Put, Delete, Increment, Append)
    atomically on a single row. All mutations are applied together or none are.

    This is equivalent to Java HBase's RowMutations class.

    Example:
        >>> from hbasedriver.operations import RowMutations, Put, Delete
        >>>
        >>> # Create RowMutations
        >>> rm = RowMutations(b'row1')
        >>> rm.add(Put(b'row1').add_column(b'cf', b'col1', b'value1'))
        >>> rm.add(Delete(b'row1').add_column(b'cf', b'col2'))
        >>>
        >>> # Execute atomically
        >>> table.mutate_row(rm)

    Attributes:
        rowkey: The row key for all mutations
        mutations: List of mutation operations
    """

    def __init__(self, rowkey: bytes):
        """Initialize RowMutations for the given row.

        Args:
            rowkey: The row key (as bytes) for all mutations
        """
        self.rowkey = rowkey
        self._mutations: list[Put | Delete | Increment | Append] = []

    def add(self, mutation: Put | Delete | Increment | Append) -> 'RowMutations':
        """Add a mutation to the atomic operation.

        Args:
            mutation: A Put, Delete, Increment, or Append operation

        Returns:
            self (for method chaining)

        Raises:
            ValueError: If mutation rowkey doesn't match RowMutations rowkey
            ValueError: If mutation type is not supported
        """
        if mutation.rowkey != self.rowkey:
            raise ValueError(
                f"Mutation rowkey {mutation.rowkey!r} doesn't match "
                f"RowMutations rowkey {self.rowkey!r}"
            )
        if not isinstance(mutation, (Put, Delete, Increment, Append)):
            raise ValueError(
                f"Unsupported mutation type: {type(mutation).__name__}. "
                "Only Put, Delete, Increment, and Append are supported."
            )
        self._mutations.append(mutation)
        return self

    def add_put(self, put: Put) -> 'RowMutations':
        """Add a Put operation.

        Args:
            put: Put operation to add

        Returns:
            self (for method chaining)
        """
        return self.add(put)

    def add_delete(self, delete: Delete) -> 'RowMutations':
        """Add a Delete operation.

        Args:
            delete: Delete operation to add

        Returns:
            self (for method chaining)
        """
        return self.add(delete)

    def add_increment(self, increment: Increment) -> 'RowMutations':
        """Add an Increment operation.

        Args:
            increment: Increment operation to add

        Returns:
            self (for method chaining)
        """
        return self.add(increment)

    def add_append(self, append: Append) -> 'RowMutations':
        """Add an Append operation.

        Args:
            append: Append operation to add

        Returns:
            self (for method chaining)
        """
        return self.add(append)

    def get_mutations(self) -> list[Put | Delete | Increment | Append]:
        """Get all mutations.

        Returns:
            List of mutation operations
        """
        return self._mutations.copy()

    def __len__(self) -> int:
        """Return the number of mutations."""
        return len(self._mutations)

    def __bool__(self) -> bool:
        """Return True if there are any mutations."""
        return len(self._mutations) > 0
