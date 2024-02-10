import HBase_pb2 as _HBase_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CompactionEventTracker(_message.Message):
    __slots__ = ("compacted_store_file",)
    COMPACTED_STORE_FILE_FIELD_NUMBER: _ClassVar[int]
    compacted_store_file: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, compacted_store_file: _Optional[_Iterable[bytes]] = ...) -> None: ...

class FileInfoProto(_message.Message):
    __slots__ = ("map_entry",)
    MAP_ENTRY_FIELD_NUMBER: _ClassVar[int]
    map_entry: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.BytesBytesPair]
    def __init__(self, map_entry: _Optional[_Iterable[_Union[_HBase_pb2.BytesBytesPair, _Mapping]]] = ...) -> None: ...

class FileTrailerProto(_message.Message):
    __slots__ = ("file_info_offset", "load_on_open_data_offset", "uncompressed_data_index_size", "total_uncompressed_bytes", "data_index_count", "meta_index_count", "entry_count", "num_data_index_levels", "first_data_block_offset", "last_data_block_offset", "comparator_class_name", "compression_codec", "encryption_key")
    FILE_INFO_OFFSET_FIELD_NUMBER: _ClassVar[int]
    LOAD_ON_OPEN_DATA_OFFSET_FIELD_NUMBER: _ClassVar[int]
    UNCOMPRESSED_DATA_INDEX_SIZE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_UNCOMPRESSED_BYTES_FIELD_NUMBER: _ClassVar[int]
    DATA_INDEX_COUNT_FIELD_NUMBER: _ClassVar[int]
    META_INDEX_COUNT_FIELD_NUMBER: _ClassVar[int]
    ENTRY_COUNT_FIELD_NUMBER: _ClassVar[int]
    NUM_DATA_INDEX_LEVELS_FIELD_NUMBER: _ClassVar[int]
    FIRST_DATA_BLOCK_OFFSET_FIELD_NUMBER: _ClassVar[int]
    LAST_DATA_BLOCK_OFFSET_FIELD_NUMBER: _ClassVar[int]
    COMPARATOR_CLASS_NAME_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_CODEC_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTION_KEY_FIELD_NUMBER: _ClassVar[int]
    file_info_offset: int
    load_on_open_data_offset: int
    uncompressed_data_index_size: int
    total_uncompressed_bytes: int
    data_index_count: int
    meta_index_count: int
    entry_count: int
    num_data_index_levels: int
    first_data_block_offset: int
    last_data_block_offset: int
    comparator_class_name: str
    compression_codec: int
    encryption_key: bytes
    def __init__(self, file_info_offset: _Optional[int] = ..., load_on_open_data_offset: _Optional[int] = ..., uncompressed_data_index_size: _Optional[int] = ..., total_uncompressed_bytes: _Optional[int] = ..., data_index_count: _Optional[int] = ..., meta_index_count: _Optional[int] = ..., entry_count: _Optional[int] = ..., num_data_index_levels: _Optional[int] = ..., first_data_block_offset: _Optional[int] = ..., last_data_block_offset: _Optional[int] = ..., comparator_class_name: _Optional[str] = ..., compression_codec: _Optional[int] = ..., encryption_key: _Optional[bytes] = ...) -> None: ...
