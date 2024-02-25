from typing import Iterable, Dict

from hbasedriver.protobuf_py.HBase_pb2 import ColumnFamilySchema, BytesBytesPair, NameStringPair


class ColumnFamilyDescriptorBuilder:
    def __init__(self, name: bytes):
        self.name = name
        self.attributes: list[BytesBytesPair] = []
        self.configuration: Iterable[NameStringPair] = {}

    def _add_attribute_pair(self, key: bytes, value: bytes) -> None:
        self.attributes.append(BytesBytesPair(first=key, second=value))

    def set_compression_type(self, compression_type: bytes) -> 'ColumnFamilyDescriptorBuilder':
        self._add_attribute_pair(b"compression_type", compression_type)
        return self

    def set_max_versions(self, max_versions: int) -> 'ColumnFamilyDescriptorBuilder':
        self._add_attribute_pair(b"max_versions", str(max_versions).encode('utf-8'))
        return self

    def set_time_to_live(self, time_to_live: int) -> 'ColumnFamilyDescriptorBuilder':
        self._add_attribute_pair(b"time_to_live", str(time_to_live).encode('utf-8'))
        return self

    def set_block_size(self, block_size: int) -> 'ColumnFamilyDescriptorBuilder':
        self._add_attribute_pair(b"block_size", str(block_size).encode('utf-8'))
        return self

    def build(self) -> 'ColumnFamilySchema':
        return ColumnFamilySchema(name=self.name, attributes=self.attributes, configuration=self.configuration)
