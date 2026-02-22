"""
Increment operations for counter functionality.

This module provides Increment class for atomically incrementing counter values.
"""
from typing import TYPE_CHECKING

from hbasedriver.model import Cell, CellType
from hbasedriver.operations import Get

if TYPE_CHECKING:
    pass


class Increment:
    """
    Increment operation for atomically incrementing a counter value.

    This operation atomically increments a column value by a specified amount.
    If the column does not exist, it is created with the increment value.

    Example:
        inc = Increment(b'row1')
        inc.add_column(b'cf', b'counter', 1)

        new_value = table.increment(inc)
        print(f"New counter value: {new_value}")
    """

    def __init__(self, rowkey: bytes):
        """
        Initialize an Increment operation.

        Args:
            rowkey: The row key to increment
        """
        self.rowkey = rowkey
        self.family_cells: dict[bytes, list[Cell]] = {}
        self.return_results: bool = True

    def add_column(self, family: bytes, qualifier: bytes, amount: int = 1):
        """
        Add a column to increment.

        Args:
            family: Column family
            qualifier: Column qualifier
            amount: Amount to increment by (default: 1)
        """
        if family not in self.family_cells:
            self.family_cells[family] = []

        # Store increment amount in value field as bytes (using PUT type)
        self.family_cells[family].append(
            Cell(self.rowkey, family, qualifier, str(amount).encode(), None, CellType.PUT)
        )
        return self

    def set_return_results(self, return_results: bool):
        """
        Set whether to return the incremented values.

        Args:
            return_results: If True, returns the new values after increment
        """
        self.return_results = return_results
        return self

    def to_get(self) -> Get:
        """
        Convert increment to a Get operation for reading current value.

        This is used when increment RPC is not available.
        The client will read-modify-write manually.

        Returns:
            Get operation to read current value
        """
        get = Get(self.rowkey)
        for family, cells in self.family_cells.items():
            for cell in cells:
                get.add_column(family, cell.qualifier)
        return get


class CheckAndPut:
    """
    Check-and-put operation for conditional updates.

    This operation checks if a column has a specific value before performing a put.
    If the check passes, the put is performed atomically.

    Example:
        cap = CheckAndPut(b'row1')
        cap.set_check(b'cf', b'lock', b'')
        cap.set_put(Put(b'row1').add_column(b'cf', b'data', b'value'))

        success = table.check_and_put(cap)
        if success:
            print("Put succeeded")
        else:
            print("Check failed")
    """

    def __init__(self, rowkey: bytes):
        """
        Initialize a CheckAndPut operation.

        Args:
            rowkey: The row key to check and put
        """
        self.rowkey = rowkey
        self.check_family: bytes | None = None
        self.check_qualifier: bytes | None = None
        self.check_value: bytes | None = None
        self.put_operation = None
        self.compare_op = "EQUAL"  # Can be EQUAL, NOT_EQUAL, etc.

    def set_check(self, family: bytes, qualifier: bytes, value: bytes | None = None):
        """
        Set the column to check.

        Args:
            family: Column family to check
            qualifier: Column qualifier to check
            value: Expected value. If None, checks for non-existence
        """
        self.check_family = family
        self.check_qualifier = qualifier
        self.check_value = value
        return self

    def set_put(self, put_operation):
        """
        Set the put operation to execute if check passes.

        Args:
            put_operation: Put operation to execute
        """
        self.put_operation = put_operation
        return self

    def set_compare_operator(self, op: str):
        """
        Set the comparison operator.

        Args:
            op: Comparison operator ('EQUAL', 'NOT_EQUAL', etc.)
        """
        self.compare_op = op
        return self

    def validate(self) -> bool:
        """
        Validate the check-and-put operation.

        Returns:
            True if operation is valid, False otherwise
        """
        if self.check_family is None or self.check_qualifier is None:
            return False
        if self.put_operation is None:
            return False
        return True
