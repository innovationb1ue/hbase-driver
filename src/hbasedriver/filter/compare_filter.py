from abc import abstractmethod
from typing import List, Union

from hbasedriver.filter.byte_array_comparable import ByteArrayComparable
from hbasedriver.filter.compare_operator import CompareOperator
from hbasedriver.filter.filter_base import FilterBase
from hbasedriver.model import Cell
from hbasedriver.protobuf_py.Filter_pb2 import CompareFilter as CompareFilterProto
from hbasedriver.protobuf_py.Comparator_pb2 import Comparator as ComparatorProto


class CompareFilter(FilterBase):
    def __init__(self, op: CompareOperator, comparator: ByteArrayComparable):
        super().__init__()
        self.op = op
        self.comparator = comparator

    def getCompareOperator(self) -> CompareOperator:
        return self.op

    def getComparator(self) -> ByteArrayComparable:
        return self.comparator

    @abstractmethod
    def filterRowKey(self, cell: Cell) -> bool:
        pass

    @abstractmethod
    def compareRow(self, op: CompareOperator, comparator: ByteArrayComparable, cell: Cell) -> bool:
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
    def extractArguments(filterArguments: List[bytes]) -> List[Union[CompareOperator, ByteArrayComparable]]:
        pass

    # get pb instance of this filter
    def convert(self) -> CompareFilterProto:
        t = CompareFilterProto()
        ctor_proto = ComparatorProto()
        ctor_proto.name = self.get_name()
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
