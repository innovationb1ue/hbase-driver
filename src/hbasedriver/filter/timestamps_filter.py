"""
TimestampsFilter - Filter cells by specific timestamps.

This filter only returns cells whose timestamps match any of
the specified timestamps.
"""
from typing import List

from hbasedriver.filter.filter import ReturnCode, Filter
from hbasedriver.filter.filter_base import FilterBase
from hbasedriver.model import Cell
import hbasedriver.protobuf_py.Filter_pb2 as FilterProtos


class TimestampsFilter(FilterBase):
    """
    Filter cells by specific timestamps.

    Only returns cells with timestamps that exactly match any of
    the provided timestamp values.
    """

    def __init__(self, timestamps: List[int], can_hint: bool = True):
        """
        Initialize TimestampsFilter.

        Args:
            timestamps: List of timestamps (milliseconds since epoch) to include
            can_hint: Whether to provide optimization hints
        """
        super().__init__()
        self.timestamps = set(timestamps)
        self.can_hint = can_hint

    def get_name(self) -> str:
        return "org.apache.hadoop.hbase.filter.TimestampsFilter"

    def get_timestamps(self) -> List[int]:
        """Get the list of timestamps."""
        return list(self.timestamps)

    def get_can_hint(self) -> bool:
        """Get the can_hint flag."""
        return self.can_hint

    def reset(self) -> None:
        """Reset filter state - not used for TimestampsFilter."""
        pass

    # Implement Filter abstract methods (snake_case)
    def filter_all_remaining(self) -> bool:
        """Check if all remaining should be filtered out."""
        return False

    def filter_row_key(self, cell: Cell) -> bool:
        """TimestampsFilter doesn't filter by row key."""
        return False

    def filter_cell(self, cell: Cell) -> ReturnCode:
        """Filter cell based on timestamp."""
        if cell.ts is None:
            # Cells without timestamp are included by default
            return ReturnCode.INCLUDE

        if cell.ts in self.timestamps:
            return ReturnCode.INCLUDE
        return ReturnCode.SKIP

    def transform_cell(self, cell: Cell) -> Cell:
        """No transformation."""
        return cell

    def filter_row_cells(self, cells: List[Cell]) -> None:
        """Remove cells with non-matching timestamps."""
        to_remove = []
        for cell in cells:
            if cell.ts is not None and cell.ts not in self.timestamps:
                to_remove.append(cell)

        for cell in to_remove:
            cells.remove(cell)

    def has_filter_row(self) -> bool:
        """Check if filter has filter row functionality."""
        return False

    def filter_row(self) -> bool:
        """TimestampsFilter doesn't filter entire rows."""
        return False

    def get_next_cell_hint(self, cell: Cell) -> Cell:
        """No hint."""
        return None

    def is_family_essential(self, name: bytes) -> bool:
        """Any family could contain matching cells."""
        return True

    def to_byte_array(self) -> bytes:
        """Serialize to protobuf."""
        builder = FilterProtos.TimestampsFilter()
        for ts in sorted(self.timestamps):
            builder.timestamps.append(ts)
        builder.can_hint = self.can_hint
        return builder.SerializeToString()

    @staticmethod
    def parse_from(pb_bytes: bytes) -> 'TimestampsFilter':
        """Deserialize from protobuf."""
        proto = FilterProtos.TimestampsFilter()
        proto.MergeFromString(pb_bytes)
        return TimestampsFilter(list(proto.timestamps), proto.can_hint)

    def are_serialized_fields_equal(self, other: Filter) -> bool:
        """Check if serialized fields are equal."""
        if not isinstance(other, TimestampsFilter):
            return False
        return self.timestamps == other.timestamps and self.can_hint == other.can_hint

    def __str__(self) -> str:
        return f"TimestampsFilter({len(self.timestamps)} timestamps)"
