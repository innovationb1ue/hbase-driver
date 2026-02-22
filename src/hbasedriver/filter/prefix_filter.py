"""
PrefixFilter - Filter rows based on row key prefix.

This filter is used to filter based on row key prefix. It only includes rows
whose row keys start with the specified prefix.
"""
from typing import List

from hbasedriver.filter.filter import ReturnCode, Filter
from hbasedriver.filter.filter_base import FilterBase
from hbasedriver.model import Cell
import hbasedriver.protobuf_py.Filter_pb2 as FilterProtos


class PrefixFilter(FilterBase):
    """
    This filter is used to filter based on row key prefix.

    Pass rows that have same prefix as the specified prefix.
    """

    def __init__(self, prefix: bytes):
        """
        Initialize PrefixFilter.

        :param prefix: The prefix bytes to match row keys against.
        """
        super().__init__()
        self.prefix = prefix
        self._passed_prefix = False
        self._filter_out_row = False

    def get_name(self) -> str:
        return "org.apache.hadoop.hbase.filter.PrefixFilter"

    def get_prefix(self) -> bytes:
        """Get the prefix."""
        return self.prefix

    def reset(self) -> None:
        """Reset filter state for new row."""
        self._filter_out_row = False

    # Implement Filter abstract methods (snake_case)
    def filter_all_remaining(self) -> bool:
        """If we've passed the prefix range, filter all remaining."""
        return self._passed_prefix

    def filter_row_key(self, cell: Cell) -> bool:
        """Filter based on row key prefix."""
        row_key = cell.rowkey if cell.rowkey else b''

        # Check if row key starts with prefix
        if not row_key.startswith(self.prefix):
            # Check if we've passed the prefix in lexicographical order
            if len(row_key) > 0 and len(self.prefix) > 0:
                # If row_key is greater than prefix and doesn't start with it,
                # we've passed all matching rows
                if row_key > self.prefix:
                    self._passed_prefix = True
            self._filter_out_row = True
            return True

        return False

    def filter_cell(self, cell: Cell) -> ReturnCode:
        """Filter cell - include if row matches prefix."""
        if self._filter_out_row:
            return ReturnCode.NEXT_ROW
        return ReturnCode.INCLUDE

    def transform_cell(self, cell: Cell) -> Cell:
        """Transform cell - no transformation for PrefixFilter."""
        return cell

    def filter_row_cells(self, cells: List[Cell]) -> None:
        """Direct modification of cell list - not used."""
        pass

    def has_filter_row(self) -> bool:
        """Check if filter has filter row functionality."""
        return True

    def filter_row(self) -> bool:
        """Filter out rows that don't match."""
        return self._filter_out_row

    def get_next_cell_hint(self, cell: Cell) -> Cell:
        """Get next cell hint - not used."""
        return None

    def is_family_essential(self, name: bytes) -> bool:
        """Check if family is essential."""
        return True

    def to_byte_array(self) -> bytes:
        """Serialize the PrefixFilter to bytes."""
        builder = FilterProtos.PrefixFilter()
        builder.prefix = self.prefix
        return builder.SerializeToString()

    @staticmethod
    def parse_from(pb_bytes: bytes) -> 'PrefixFilter':
        """Deserialize PrefixFilter from bytes."""
        proto = FilterProtos.PrefixFilter()
        proto.MergeFromString(pb_bytes)
        return PrefixFilter(proto.prefix)

    def are_serialized_fields_equal(self, other: Filter) -> bool:
        """Check if serialized fields are equal."""
        if not isinstance(other, PrefixFilter):
            return False
        return self.prefix == other.prefix

    def __str__(self) -> str:
        return f"PrefixFilter({self.prefix!r})"
