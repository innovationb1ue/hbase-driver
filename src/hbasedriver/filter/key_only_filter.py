"""
KeyOnlyFilter - Filter that returns only row keys, no values.

This filter is used to return only the key portion of each Key-Value.
The values can optionally be replaced with an empty byte array.
"""
from typing import List

from hbasedriver.filter.filter import ReturnCode, Filter
from hbasedriver.filter.filter_base import FilterBase
from hbasedriver.model import Cell, CellType
import hbasedriver.protobuf_py.Filter_pb2 as FilterProtos


class KeyOnlyFilter(FilterBase):
    """
    This filter is used to return only the key portion of each Key-Value.

    Primarily used for row-existence checks or counting rows where
    values are not needed.
    """

    def __init__(self, len_as_val: bool = False):
        """
        Initialize KeyOnlyFilter.

        :param len_as_val: If True, the value is set to be the length of the
                          original value as an 8-byte big-endian integer.
                          If False, the value is set to an empty byte array.
        """
        super().__init__()
        self.len_as_val = len_as_val

    def get_name(self) -> str:
        return "org.apache.hadoop.hbase.filter.KeyOnlyFilter"

    def get_len_as_val(self) -> bool:
        """Get whether to use length as value."""
        return self.len_as_val

    def reset(self) -> None:
        """Reset filter state - not used for KeyOnlyFilter."""
        pass

    # Implement Filter abstract methods (snake_case)
    def filter_all_remaining(self) -> bool:
        """Check if all remaining should be filtered out."""
        return False

    def filter_row_key(self, cell: Cell) -> bool:
        """KeyOnlyFilter doesn't filter by row key."""
        return False

    def filter_cell(self, cell: Cell) -> ReturnCode:
        """Include all cells - transformation happens in transform_cell."""
        return ReturnCode.INCLUDE

    def transform_cell(self, cell: Cell) -> Cell:
        """Transform cell to remove or replace value."""
        if self.len_as_val:
            # Replace value with its length as 8-byte big-endian integer
            original_len = len(cell.value) if cell.value else 0
            new_value = original_len.to_bytes(8, byteorder='big')
        else:
            # Replace value with empty byte array
            new_value = b''

        # Create a new cell with the transformed value
        return Cell(
            rowkey=cell.rowkey,
            family=cell.family,
            qualifier=cell.qualifier,
            value=new_value,
            ts=cell.ts,
            cell_type=cell.type
        )

    def filter_row_cells(self, cells: List[Cell]) -> None:
        """Direct modification of cell list - not used."""
        pass

    def has_filter_row(self) -> bool:
        """Check if filter has filter row functionality."""
        return False

    def filter_row(self) -> bool:
        """KeyOnlyFilter doesn't filter entire rows."""
        return False

    def get_next_cell_hint(self, cell: Cell) -> Cell:
        """Get next cell hint - not used."""
        return None

    def is_family_essential(self, name: bytes) -> bool:
        """Check if family is essential."""
        return True

    def to_byte_array(self) -> bytes:
        """Serialize the KeyOnlyFilter to bytes."""
        builder = FilterProtos.KeyOnlyFilter()
        builder.len_as_val = self.len_as_val
        return builder.SerializeToString()

    @staticmethod
    def parse_from(pb_bytes: bytes) -> 'KeyOnlyFilter':
        """Deserialize KeyOnlyFilter from bytes."""
        proto = FilterProtos.KeyOnlyFilter()
        proto.MergeFromString(pb_bytes)
        return KeyOnlyFilter(proto.len_as_val)

    def are_serialized_fields_equal(self, other: Filter) -> bool:
        """Check if serialized fields are equal."""
        if not isinstance(other, KeyOnlyFilter):
            return False
        return self.len_as_val == other.len_as_val

    def __str__(self) -> str:
        return f"KeyOnlyFilter(len_as_val={self.len_as_val})"
