"""
ColumnPrefixFilter - Filter columns based on qualifier prefix.

This filter is used to filter based on column qualifier prefix.
It only returns columns whose qualifiers start with the specified prefix.
"""
from typing import List

from hbasedriver.filter.filter import ReturnCode, Filter
from hbasedriver.filter.filter_base import FilterBase
from hbasedriver.model import Cell
import hbasedriver.protobuf_py.Filter_pb2 as FilterProtos


class ColumnPrefixFilter(FilterBase):
    """
    This filter is used to filter based on column qualifier prefix.

    Pass columns that have same prefix as the specified prefix.
    """

    def __init__(self, prefix: bytes):
        """
        Initialize ColumnPrefixFilter.

        :param prefix: The prefix bytes to match column qualifiers against.
        """
        super().__init__()
        self.prefix = prefix

    def get_name(self) -> str:
        return "org.apache.hadoop.hbase.filter.ColumnPrefixFilter"

    def get_prefix(self) -> bytes:
        """Get the prefix."""
        return self.prefix

    def reset(self) -> None:
        """Reset filter state - not used for ColumnPrefixFilter."""
        pass

    # Implement Filter abstract methods (snake_case)
    def filter_all_remaining(self) -> bool:
        """Check if all remaining should be filtered out."""
        return False

    def filter_row_key(self, cell: Cell) -> bool:
        """ColumnPrefixFilter doesn't filter by row key."""
        return False

    def filter_cell(self, cell: Cell) -> ReturnCode:
        """Filter based on column qualifier prefix."""
        qualifier = cell.qualifier if cell.qualifier else b''

        if not qualifier.startswith(self.prefix):
            return ReturnCode.SKIP

        return ReturnCode.INCLUDE

    def transform_cell(self, cell: Cell) -> Cell:
        """Transform cell - no transformation for ColumnPrefixFilter."""
        return cell

    def filter_row_cells(self, cells: List[Cell]) -> None:
        """Direct modification of cell list - not used."""
        pass

    def has_filter_row(self) -> bool:
        """Check if filter has filter row functionality."""
        return False

    def filter_row(self) -> bool:
        """ColumnPrefixFilter doesn't filter entire rows."""
        return False

    def get_next_cell_hint(self, cell: Cell) -> Cell:
        """Get next cell hint - not used."""
        return None

    def is_family_essential(self, name: bytes) -> bool:
        """Check if family is essential."""
        return True

    def to_byte_array(self) -> bytes:
        """Serialize the ColumnPrefixFilter to bytes."""
        builder = FilterProtos.ColumnPrefixFilter()
        builder.prefix = self.prefix
        return builder.SerializeToString()

    @staticmethod
    def parse_from(pb_bytes: bytes) -> 'ColumnPrefixFilter':
        """Deserialize ColumnPrefixFilter from bytes."""
        proto = FilterProtos.ColumnPrefixFilter()
        proto.MergeFromString(pb_bytes)
        return ColumnPrefixFilter(proto.prefix)

    def are_serialized_fields_equal(self, other: Filter) -> bool:
        """Check if serialized fields are equal."""
        if not isinstance(other, ColumnPrefixFilter):
            return False
        return self.prefix == other.prefix

    def __str__(self) -> str:
        return f"ColumnPrefixFilter({self.prefix!r})"
