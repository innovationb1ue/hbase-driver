from abc import abstractmethod
from typing import List, Union

from hbasedriver.filter.byte_array_comparable import ByteArrayComparable
from hbasedriver.filter.compare_operator import CompareOperator
from hbasedriver.filter.filter_base import FilterBase
from hbasedriver.filter.parse_constant import ParseConstants
from hbasedriver.filter.parse_filter import create_compare_operator
from hbasedriver.filter.binary_comparator import BinaryComparator
from hbasedriver.filter.binary_prefix_comparator import BinaryPrefixComparator
from hbasedriver.model import Cell
from hbasedriver.protobuf_py.Filter_pb2 import CompareFilter as CompareFilterProto
from hbasedriver.protobuf_py.Comparator_pb2 import Comparator as ComparatorProto
from hbasedriver.protobuf_py.HBase_pb2 import CompareType


class CompareFilter(FilterBase):
    def __init__(self, op: CompareOperator, comparator: ByteArrayComparable):
        super().__init__()
        self.op: CompareOperator = op
        self.comparator: ByteArrayComparable = comparator

    def getCompareOperator(self) -> CompareOperator:
        return self.op

    def getComparator(self) -> ByteArrayComparable:
        return self.comparator

    @abstractmethod
    def filterRowKey(self, cell: Cell) -> bool:
        pass

    @abstractmethod
    def compareRow(self, op: CompareOperator, comparator: ByteArrayComparable, cell: Cell) -> bool:
        if op == CompareOperator.NO_OP:
            return True
        pass

    @abstractmethod
    def compareFamily(self, op: CompareOperator, comparator: ByteArrayComparable, cell: Cell) -> bool:
        pass

    @abstractmethod
    def compareQualifier(self, op: CompareOperator, comparator: ByteArrayComparable, cell: Cell) -> bool:
        pass

    @abstractmethod
    def compareValue(self, op: CompareOperator, comparator: ByteArrayComparable, cell: Cell) -> bool:
        pass

    @staticmethod
    def compare(op: CompareOperator, compareResult: int) -> bool:
        pass

    @staticmethod
    def extract_arguments(filterArguments: List[bytes]) -> (CompareOperator, ByteArrayComparable):
        assert len(filterArguments) == 2
        args1_b: bytes = filterArguments[0]
        compare_operator = create_compare_operator(args1_b)
        args2_b: bytes = filterArguments[1]
        comparator_type, comparator_value = args2_b.split(b':')
        if comparator_type == ParseConstants.binaryType:
            comparable = BinaryComparator(comparator_value)
        elif comparator_type == ParseConstants.binaryPrefixType:
            comparable = BinaryPrefixComparator(comparator_value)
        else:
            raise NotImplementedError("comparator {} not implemented yet", comparator_type)

        return compare_operator, comparable

        # get pb instance of this filter

    # return a pb version of the instance.
    def convert(self) -> CompareFilterProto:
        t = CompareFilterProto()
        t.compare_op = CompareType.Value(self.op.name)

        ctor_proto = ComparatorProto()
        # set this name to java class name.
        ctor_proto.name = self.comparator.get_name()
        ctor_proto.serialized_comparator = self.comparator.to_byte_array()
        t.comparator = ctor_proto
        return t

    # this should return **JAVA** class path !!!!
    # since server rely on this to instantiate your filter.
    @abstractmethod
    def get_name(self):
        pass

    def areSerializedFieldsEqual(self, other: 'Filter') -> bool:
        pass

    def __str__(self) -> str:
        pass

    def __eq__(self, obj: object) -> bool:
        pass

    def __hash__(self) -> int:
        pass
