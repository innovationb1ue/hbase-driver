"""
PageFilter - Filter to limit number of rows per page.

This filter is used to limit the number of rows returned per page.
It should be used together with proper row key management for pagination.
"""
from typing import List

from hbasedriver.filter.filter import ReturnCode, Filter
from hbasedriver.filter.filter_base import FilterBase
from hbasedriver.model import Cell
import hbasedriver.protobuf_py.Filter_pb2 as FilterProtos


class PageFilter(FilterBase):
    """
    This filter is used to limit the number of rows returned.

    When scan reaches the page size, all remaining rows are filtered out.
    """

    def __init__(self, page_size: int):
        """
        Initialize PageFilter.

        :param page_size: Maximum number of rows to return.
        """
        super().__init__()
        if page_size <= 0:
            raise ValueError("Page size must be positive")
        self.page_size = page_size
        self._rows_accepted = 0

    def get_name(self) -> str:
        return "org.apache.hadoop.hbase.filter.PageFilter"

    def get_page_size(self) -> int:
        """Get the page size."""
        return self.page_size

    def reset(self) -> None:
        """Reset counter for new scan."""
        # Don't reset rows_accepted - we want to keep counting across rows
        pass

    # Implement Filter abstract methods (snake_case)
    def filter_all_remaining(self) -> bool:
        """Check if we've reached the page size."""
        return self._rows_accepted >= self.page_size

    def filter_row_key(self, cell: Cell) -> bool:
        """Filter based on row key - count rows."""
        if self._rows_accepted >= self.page_size:
            return True
        # Increment counter for this row
        self._rows_accepted += 1
        return False

    def filter_cell(self, cell: Cell) -> ReturnCode:
        """Filter cell - include if we haven't reached page size."""
        if self._rows_accepted > self.page_size:
            return ReturnCode.NEXT_ROW
        return ReturnCode.INCLUDE

    def transform_cell(self, cell: Cell) -> Cell:
        """Transform cell - no transformation for PageFilter."""
        return cell

    def filter_row_cells(self, cells: List[Cell]) -> None:
        """Direct modification of cell list - not used."""
        pass

    def has_filter_row(self) -> bool:
        """Check if filter has filter row functionality."""
        return False

    def filter_row(self) -> bool:
        """Filter out rows after page size is reached."""
        return self._rows_accepted > self.page_size

    def get_next_cell_hint(self, cell: Cell) -> Cell:
        """Get next cell hint - not used."""
        return None

    def is_family_essential(self, name: bytes) -> bool:
        """Check if family is essential."""
        return True

    def to_byte_array(self) -> bytes:
        """Serialize the PageFilter to bytes."""
        builder = FilterProtos.PageFilter()
        builder.page_size = self.page_size
        return builder.SerializeToString()

    @staticmethod
    def parse_from(pb_bytes: bytes) -> 'PageFilter':
        """Deserialize PageFilter from bytes."""
        proto = FilterProtos.PageFilter()
        proto.MergeFromString(pb_bytes)
        return PageFilter(proto.page_size)

    def are_serialized_fields_equal(self, other: Filter) -> bool:
        """Check if serialized fields are equal."""
        if not isinstance(other, PageFilter):
            return False
        return self.page_size == other.page_size

    def __str__(self) -> str:
        return f"PageFilter({self.page_size})"
