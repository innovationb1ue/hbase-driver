from abc import ABC, abstractmethod
from enum import Enum
from typing import List
import io

from hbasedriver.filter.binary_comparator import BinaryComparator
from hbasedriver.filter.byte_array_comparable import ByteArrayComparable
from hbasedriver.filter.compare_filter import CompareFilter
from hbasedriver.filter.compare_operator import CompareOperator
from hbasedriver.filter.filter import ReturnCode, Filter
import hbasedriver.protobuf_py.Filter_pb2 as FilterProtos


# /**
#  * This filter is used to filter based on the key. It takes an operator (equal, greater, not equal,
#  * etc) and a byte [] comparator for the row, and column qualifier portions of a key.
#  * <p>
#  * This filter can be wrapped with {@link WhileMatchFilter} to add more control.
#  * <p>
#  * Multiple filters can be combined using {@link FilterList}.
#  * <p>
#  * If an already known row range needs to be scanned, use
#  * {@link org.apache.hadoop.hbase.CellScanner} start and stop rows directly rather than a filter.
#  */
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
        builder = FilterProtos.RowFilter()
        builder.compare_filter = self.convert()
        return builder.SerializeToString()

    @staticmethod
    def parse_from(pb_bytes: bytes):
        proto = FilterProtos.RowFilter()
        proto.MergeFromString(pb_bytes)
        value_compare_op = CompareOperator(proto.compare_filter.compare_op.Value())
        value_comparator = None
        if proto.compare_filter.HasField("compare_filter"):
            java_class_name = proto.compare_filter.comparator.name
            if java_class_name == "org.apache.hadoop.hbase.filter.BinaryComparator":
                value_comparator = BinaryComparator(proto.compare_filter.comparator.serialized_comparator)
            else:
                raise NotImplementedError("comparator {} is not supported yet. ", java_class_name)
        return RowFilter(value_compare_op, value_comparator)

    def are_serialized_fields_equal(self, other) -> bool:
        if isinstance(other, RowFilter):
            return super().are_serialized_fields_equal(other)
        return False

    def equals(self, obj) -> bool:
        return isinstance(obj, Filter) and self.are_serialized_fields_equal(obj)

    def hash_code(self) -> int:
        # not sure whether we need this in python.
        return -1

    @classmethod
    def create_filter_from_arguments(cls, filter_arguments: List[bytes]) -> 'RowFilter':
        arguments = CompareFilter.extract_arguments(filter_arguments)
        compare_op = CompareOperator(arguments[0])
        comparator = ByteArrayComparable(arguments[1])
        return RowFilter(compare_op, comparator)
