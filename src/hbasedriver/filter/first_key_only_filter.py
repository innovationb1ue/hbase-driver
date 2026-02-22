"""
FirstKeyOnlyFilter - Return only the first column of each row.

This filter optimizes row count queries by returning only the first
column of each row, reducing data transfer.
"""
from typing import List

from hbasedriver.filter.filter import ReturnCode, Filter
from hbasedriver.filter.filter_base import FilterBase
from hbasedriver.model import Cell
import hbasedriver.protobuf_py.Filter_pb2 as FilterProtos


class FirstKeyOnlyFilter(FilterBase):
    """
    Return only the first column of each row.

    This is different from KeyOnlyFilter:
    - KeyOnlyFilter returns keys with empty values for ALL columns
    - FirstKeyOnlyFilter returns ONLY the first column
    """

    def __init__(self, len_as_val: bool = False):
        """
        Initialize FirstKeyOnlyFilter.

        Args:
            len_as_val: If True, value contains length as 8-byte integer
        """
        super().__init__()
        self.len_as_val = len_as_val
        self._first_column_seen = False

    def get_name(self) -> str:
        return "org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter"

    def get_len_as_val(self) -> bool:
        """Get whether to use length as value."""
        return self.len_as_val

    def reset(self) -> None:
        """Reset for new row."""
        self._first_column_seen = False

    # Implement Filter abstract methods (snake_case)
    def filter_all_remaining(self) -> bool:
        """Check if all remaining should be filtered out."""
        return False

    def filter_row_key(self, cell: Cell) -> bool:
        """FirstKeyOnlyFilter doesn't filter by row key."""
        return False

    def filter_cell(self, cell: Cell) -> ReturnCode:
        """Include first cell, skip others."""
        if not self._first_column_seen:
            self._first_column_seen = True
            return ReturnCode.INCLUDE
        return ReturnCode.SKIP

    def transform_cell(self, cell: Cell) -> Cell:
        """Transform cell if len_as_val is True."""
        if self.len_as_val:
            # Replace value with its length as 8-byte big-endian integer
            original_len = len(cell.value) if cell.value else 0
            new_value = original_len.to_bytes(8, byteorder='big')
        else:
            # Keep original value (only returning first column)
            new_value = cell.value

        return Cell(
            rowkey=cell.rowkey,
            family=cell.family,
            qualifier=cell.qualifier,
            value=new_value,
            ts=cell.ts,
            cell_type=cell.type
        )

    def filter_row_cells(self, cells: List[Cell]) -> None:
        """Remove all but first cell."""
        if len(cells) > 1:
            del cells[1:]

    def has_filter_row(self) -> bool:
        """Check if filter has filter row functionality."""
        return False

    def filter_row(self) -> bool:
        """FirstKeyOnlyFilter doesn't filter entire rows."""
        return False

    def get_next_cell_hint(self, cell: Cell) -> Cell:
        """No hint."""
        return None

    def is_family_essential(self, name: bytes) -> bool:
        """Any family could contain first column."""
        return True

    def to_byte_array(self) -> bytes:
        """Serialize to protobuf."""
        # Note: The FirstKeyOnlyFilter protobuf message has no fields in the current
        # proto definition, so we can't serialize the len_as_val flag.
        # The flag is still used in the filter_cell and transform_cell methods
        # for client-side processing.
        builder = FilterProtos.FirstKeyOnlyFilter()
        return builder.SerializeToString()

    @staticmethod
    def parse_from(pb_bytes: bytes) -> 'FirstKeyOnlyFilter':
        """Deserialize from protobuf."""
        proto = FilterProtos.FirstKeyOnlyFilter()
        proto.MergeFromString(pb_bytes)
        # Note: FirstKeyOnlyFilter protobuf has no fields, so we always return default
        # len_as_val=False. The len_as_val flag is used only for client-side processing.
        return FirstKeyOnlyFilter(len_as_val=False)

    def are_serialized_fields_equal(self, other: Filter) -> bool:
        """Check if serialized fields are equal."""
        if not isinstance(other, FirstKeyOnlyFilter):
            return False
        return self.len_as_val == other.len_as_val

    def __str__(self) -> str:
        return f"FirstKeyOnlyFilter(len_as_val={self.len_as_val})"
