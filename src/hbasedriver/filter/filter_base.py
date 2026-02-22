from typing import List

from hbasedriver.filter.filter import Filter, ReturnCode
from hbasedriver.model import Cell


class FilterBase(Filter):
    """
    Abstract base class for Filter implementations.

    Provides default implementations for most Filter methods to reduce boilerplate.
    Subclasses only need to implement the methods relevant to their filtering logic.
    """

    def __init__(self):
        """Initialize filter with serialization cache."""
        self._serialized_cache: bytes | None = None

    def reset(self) -> None:
        """Reset filter state between rows. Override if filter maintains row state."""
        pass

    # Snake_case implementations (from Filter abstract class)
    def filter_all_remaining(self) -> bool:
        """Determine if scan should terminate. Override to implement early termination."""
        return self.filterAllRemaining()

    def filter_row_key(self, cell: Cell) -> bool:
        """Filter based on row key. Override to implement row key filtering."""
        return self.filterRowKey(cell)

    def filter_cell(self, cell: Cell) -> ReturnCode:
        """Filter a cell. Override to implement cell filtering."""
        return ReturnCode.INCLUDE

    def transform_cell(self, cell: Cell) -> Cell:
        """Transform a cell. Override to modify cells after filtering."""
        return self.transformCell(cell)

    def filter_row_cells(self, cells: List[Cell]) -> None:
        """Modify final cell list. Override to post-process cells."""
        self.filterRowCells(cells)

    def has_filter_row(self) -> bool:
        """Check if filter uses filterRow. Override if filterRow is meaningful."""
        return self.hasFilterRow()

    def filter_row(self) -> bool:
        """Final row veto. Override to drop entire rows."""
        return self.filterRow()

    def get_next_cell_hint(self, cell: Cell) -> Cell:
        """Get next cell hint. Override for SEEK_NEXT_USING_HINT."""
        return self.getNextCellHint(cell)

    def is_family_essential(self, name: bytes) -> bool:
        """Check if family is essential. Override to optimize column family access."""
        return self.isFamilyEssential(name)

    def to_byte_array(self) -> bytes:
        """Serialize filter with caching to avoid repeated serialization overhead."""
        if self._serialized_cache is None:
            self._serialized_cache = self.toByteArray()
        return self._serialized_cache

    def are_serialized_fields_equal(self, other: Filter) -> bool:
        """Check serialized equality. Override for proper comparison."""
        return self.areSerializedFieldsEqual(other)

    # CamelCase methods (can be overridden by subclasses)
    def filterAllRemaining(self) -> bool:
        """Default: don't terminate early."""
        return False

    def filterRowKey(self, cell: Cell) -> bool:
        """Default: don't filter by row key."""
        if self.filterAllRemaining():
            return True
        return False

    def transformCell(self, v: Cell) -> Cell:
        """Default: no transformation."""
        return v

    def filterRowCells(self, ignored: List[Cell]) -> None:
        """Default: no modification."""
        pass

    def hasFilterRow(self) -> bool:
        """Default: filterRow not used."""
        return False

    def filterRow(self) -> bool:
        """Default: don't drop row."""
        return False

    def getNextCellHint(self, currentCell: Cell) -> Cell:
        """Default: no hint."""
        return None

    def isFamilyEssential(self, name: bytes) -> bool:
        """Default: all families essential."""
        return True

    @staticmethod
    def createFilterFromArguments(filterArguments: List[bytes]) -> Filter:
        """Create filter from parsed arguments. Override for string parsing support."""
        raise NotImplementedError("This method has not been implemented")

    def __str__(self) -> str:
        return self.__class__.__name__

    def toByteArray(self) -> bytes:
        """Default: empty serialization. Override for proper serialization."""
        return bytes()

    def areSerializedFieldsEqual(self, other: Filter) -> bool:
        """Default: assume equal. Override for proper comparison."""
        return True
