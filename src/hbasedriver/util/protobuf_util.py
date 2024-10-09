from hbasedriver.protobuf_py.Comparator_pb2 import ByteArrayComparable


class ProtobufUtil:
    @staticmethod
    def to_byte_array_comparable(value: bytes):
        byte_array_comparable = ByteArrayComparable()
        byte_array_comparable.value = value
        return byte_array_comparable
