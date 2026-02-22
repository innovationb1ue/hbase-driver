"""
MultipleColumnPrefixFilter - Filter columns by multiple prefixes.

Similar to ColumnPrefixFilter but accepts a list of prefixes.
"""
from typing import List

from hbasedriver.filter.filter import ReturnCode, Filter
from hbasedriver.filter.filter_base import FilterBase
from hbasedriver.model import Cell
import hbasedriver.protobuf_py.Filter_pb2 as FilterProtos


class MultipleColumnPrefixFilter(FilterBase):
    """
    Filter columns by multiple prefixes.

    Returns only columns whose qualifiers start with any of the
    specified prefixes. This is useful when you want to include
    columns matching multiple patterns.
    """

    def __init__(self, sorted_prefixes: List[bytes]):
        """
        Initialize MultipleColumnPrefixFilter.

        Args:
            sorted_prefixes: List of column qualifier prefixes to match
        """
        super().__init__()
        self.sorted_prefixes = sorted(sorted_prefixes)

    def get_name(self) -> str:
        return "org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter"

    def get_sorted_prefixes(self) -> List[bytes]:
        """Get the sorted prefixes."""
        return self.sorted_prefixes

    def reset(self) -> None:
        """Reset filter state - not used for MultipleColumnPrefixFilter."""
        pass

    # Implement Filter abstract methods (snake_case)
    def filter_all_remaining(self) -> bool:
        """Check if all remaining should be filtered out."""
        return False

    def filter_row_key(self, cell: Cell) -> bool:
        """MultipleColumnPrefixFilter doesn't filter by row key."""
        return False

    def filter_cell(self, cell: Cell) -> ReturnCode:
        """Filter cell based on column qualifier prefix."""
        qualifier = cell.qualifier if cell.qualifier else b''

        for prefix in self.sorted_prefixes:
            if qualifier.startswith(prefix):
                return ReturnCode.INCLUDE
        return ReturnCode.SKIP

    def transform_cell(self, cell: Cell) -> Cell:
        """No transformation."""
        return cell

    def filter_row_cells(self, cells: List[Cell]) -> None:
        """Remove cells not matching any prefix."""
        to_remove = []
        for cell in cells:
            matches = False
            qualifier = cell.qualifier if cell.qualifier else b''
            for prefix in self.sorted_prefixes:
                if qualifier.startswith(prefix):
                    matches = True
                    break
            if not matches:
                to_remove.append(cell)

        for cell in to_remove:
            cells.remove(cell)

    def has_filter_row(self) -> bool:
        """Check if filter has filter row functionality."""
        return False

    def filter_row(self) -> bool:
        """MultipleColumnPrefixFilter doesn't filter entire rows."""
        return False

    def get_next_cell_hint(self, cell: Cell) -> Cell:
        """No hint."""
        return None

    def is_family_essential(self, name: bytes) -> bool:
        """Any family could contain matching columns."""
        return True

    def to_byte_array(self) -> bytes:
        """Serialize to protobuf."""
        builder = FilterProtos.MultipleColumnPrefixFilter()
        for prefix in self.sorted_prefixes:
            builder.sorted_prefixes.append(prefix)
        return builder.SerializeToString()

    @staticmethod
    def parse_from(pb_bytes: bytes) -> 'MultipleColumnPrefixFilter':
        """Deserialize from protobuf."""
        proto = FilterProtos.MultipleColumnPrefixFilter()
        proto.MergeFromString(pb_bytes)
        return MultipleColumnPrefixFilter(list(proto.sorted_prefixes))

    def are_serialized_fields_equal(self, other: Filter) -> bool:
        """Check if serialized fields are equal."""
        if not isinstance(other, MultipleColumnPrefixFilter):
            return False
        return self.sorted_prefixes == other.sorted_prefixes

    def __str__(self) -> str:
        return f"MultipleColumnPrefixFilter({len(self.sorted_prefixes)} prefixes)"
