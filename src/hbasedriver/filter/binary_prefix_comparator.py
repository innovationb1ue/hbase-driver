from hbasedriver.filter import comparer
from hbasedriver.filter.byte_array_comparable import ByteArrayComparable
from hbasedriver.filter.comparer import Comparer
from hbasedriver.protobuf_py.Comparator_pb2 import BinaryPrefixComparator as BinaryPrefixComparatorProto
from hbasedriver.util.protobuf_util import ProtobufUtil


class BinaryPrefixComparator(ByteArrayComparable):
    def __init__(self, value):
        super().__init__(value)

    def compare_to(self, value: bytes, offset: int = 0, length: int = None) -> int:
        return Comparer.compare_to(self.value, 0, len(self.value), value, offset, min(len(self.value), length))

    def compare_to_with_buffer(self, value: bytes, offset: int, length: int) -> int:
        if len(self.value) <= length:
            length = len(self.value)
        return Comparer.compare_to(self.value, 0, len(self.value), value, offset, length)

    def to_byte_array(self) -> bytes:
        comparator_proto = BinaryPrefixComparatorProto()
        comparator_proto.comparable.CopyFrom(ProtobufUtil.to_byte_array_comparable(self.value))
        return comparator_proto.SerializeToString()

    @staticmethod
    def parse_from(pb_bytes: bytes) -> 'BinaryPrefixComparator':
        proto = BinaryPrefixComparatorProto()
        proto.ParseFromString(pb_bytes)
        return BinaryPrefixComparator(proto.comparable.value)

    def are_serialized_fields_equal(self, other: 'BinaryPrefixComparator') -> bool:
        if other == self:
            return True
        if not isinstance(other, BinaryPrefixComparator):
            return False
        return super().are_serialized_fields_equal(other)
