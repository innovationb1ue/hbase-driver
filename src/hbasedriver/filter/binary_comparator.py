from hbasedriver.filter.byte_array_comparable import ByteArrayComparable
from hbasedriver.filter.comparer import Comparer
from hbasedriver.protobuf_py.Comparator_pb2 import BinaryComparator as BinaryComparatorProto
from hbasedriver.protobuf_py.Comparator_pb2 import ByteArrayComparable as ByteArrayComparableProto


class BinaryComparator(ByteArrayComparable):
    def __init__(self, value):
        super().__init__(value)

    def compare_to(self, value: bytes, offset: int = 0, length: int = None) -> int:
        return Comparer.get_instance().compare_to(self.value, 0, len(self.value), value, offset, length)

    def compare_to_with_buffer(self, value: bytes, offset, length):
        return Comparer.get_instance().compare_to(self.value, 0, len(self.value), value, offset, length)

    def to_byte_array(self):
        comparator_proto = BinaryComparatorProto()
        comparator_proto.comparable = ByteArrayComparableProto(self.value)
        return comparator_proto

    @staticmethod
    def parse_from(pb_bytes):
        proto = BinaryComparatorProto()
        proto.MergeFrom(pb_bytes)
        return BinaryComparator(proto.comparable.value)

    def are_serialized_fields_equal(self, other):
        if other == self:
            return True
        if not isinstance(other, BinaryComparator):
            return False
        return super().are_serialized_fields_equal(other)
