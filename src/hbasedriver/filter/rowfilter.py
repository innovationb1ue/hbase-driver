from abc import ABC, abstractmethod
from enum import Enum
from typing import List
import io

from hbasedriver.filter.byte_array_comparable import ByteArrayComparable
from hbasedriver.filter.compare_operator import CompareOperator
from hbasedriver.filter.filter import ReturnCode


class RowFilter(CompareFilter):
    def __init__(self, op: CompareOperator, row_comparator: ByteArrayComparable):
        super().__init__(op, row_comparator)
        self.filter_out_row = False

    def reset(self):
        self.filter_out_row = False

    def filter_cell(self, cell) -> ReturnCode:
        if self.filter_out_row:
            return ReturnCode.NEXT_ROW
        return ReturnCode.INCLUDE

    def filter_row_key(self, first_row_cell) -> bool:
        if self.compare_row(self.get_compare_operator(), self.comparator, first_row_cell):
            self.filter_out_row = True
        return self.filter_out_row

    def filter_row(self) -> bool:
        return self.filter_out_row

    def to_byte_array(self) -> bytes:
        builder = FilterProtos.RowFilter.newBuilder()
        builder.setCompareFilter(self.convert())
        return builder.build().to_byte_array()

    @staticmethod
    def parse_from(pb_bytes: bytes):
        proto = FilterProtos.RowFilter.parse_from(pb_bytes)
        value_compare_op = CompareOperator.Value(proto.getCompareFilter().getCompareOp().name)
        value_comparator = None
        if proto.getCompareFilter().hasComparator():
            value_comparator = ProtobufUtil.to_comparator(proto.getCompareFilter().getComparator())
        return RowFilter(value_compare_op, value_comparator)

    def are_serialized_fields_equal(self, other) -> bool:
        if isinstance(other, RowFilter):
            return super().are_serialized_fields_equal(other)
        return False

    def equals(self, obj) -> bool:
        return isinstance(obj, Filter) and self.are_serialized_fields_equal(obj)

    def hash_code(self) -> int:
        return super().hash_code()

    @classmethod
    def create_filter_from_arguments(cls, filter_arguments: List[bytes]) -> 'RowFilter':
        arguments = CompareFilter.extract_arguments(filter_arguments)
        compare_op = CompareOperator(arguments[0])
        comparator = ByteArrayComparable(arguments[1])
        return RowFilter(compare_op, comparator)
