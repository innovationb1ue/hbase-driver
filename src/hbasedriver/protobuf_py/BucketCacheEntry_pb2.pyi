from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BlockType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    data: _ClassVar[BlockType]
    encoded_data: _ClassVar[BlockType]
    leaf_index: _ClassVar[BlockType]
    bloom_chunk: _ClassVar[BlockType]
    meta: _ClassVar[BlockType]
    intermediate_index: _ClassVar[BlockType]
    root_index: _ClassVar[BlockType]
    file_info: _ClassVar[BlockType]
    general_bloom_meta: _ClassVar[BlockType]
    delete_family_bloom_meta: _ClassVar[BlockType]
    trailer: _ClassVar[BlockType]
    index_v1: _ClassVar[BlockType]

class BlockPriority(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    single: _ClassVar[BlockPriority]
    multi: _ClassVar[BlockPriority]
    memory: _ClassVar[BlockPriority]
data: BlockType
encoded_data: BlockType
leaf_index: BlockType
bloom_chunk: BlockType
meta: BlockType
intermediate_index: BlockType
root_index: BlockType
file_info: BlockType
general_bloom_meta: BlockType
delete_family_bloom_meta: BlockType
trailer: BlockType
index_v1: BlockType
single: BlockPriority
multi: BlockPriority
memory: BlockPriority

class BucketCacheEntry(_message.Message):
    __slots__ = ("cache_capacity", "io_class", "map_class", "deserializers", "backing_map", "checksum")
    class DeserializersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: str
        def __init__(self, key: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...
    CACHE_CAPACITY_FIELD_NUMBER: _ClassVar[int]
    IO_CLASS_FIELD_NUMBER: _ClassVar[int]
    MAP_CLASS_FIELD_NUMBER: _ClassVar[int]
    DESERIALIZERS_FIELD_NUMBER: _ClassVar[int]
    BACKING_MAP_FIELD_NUMBER: _ClassVar[int]
    CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    cache_capacity: int
    io_class: str
    map_class: str
    deserializers: _containers.ScalarMap[int, str]
    backing_map: BackingMap
    checksum: bytes
    def __init__(self, cache_capacity: _Optional[int] = ..., io_class: _Optional[str] = ..., map_class: _Optional[str] = ..., deserializers: _Optional[_Mapping[int, str]] = ..., backing_map: _Optional[_Union[BackingMap, _Mapping]] = ..., checksum: _Optional[bytes] = ...) -> None: ...

class BackingMap(_message.Message):
    __slots__ = ("entry",)
    ENTRY_FIELD_NUMBER: _ClassVar[int]
    entry: _containers.RepeatedCompositeFieldContainer[BackingMapEntry]
    def __init__(self, entry: _Optional[_Iterable[_Union[BackingMapEntry, _Mapping]]] = ...) -> None: ...

class BackingMapEntry(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: BlockCacheKey
    value: BucketEntry
    def __init__(self, key: _Optional[_Union[BlockCacheKey, _Mapping]] = ..., value: _Optional[_Union[BucketEntry, _Mapping]] = ...) -> None: ...

class BlockCacheKey(_message.Message):
    __slots__ = ("hfilename", "offset", "block_type", "primary_replica_block")
    HFILENAME_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    BLOCK_TYPE_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_REPLICA_BLOCK_FIELD_NUMBER: _ClassVar[int]
    hfilename: str
    offset: int
    block_type: BlockType
    primary_replica_block: bool
    def __init__(self, hfilename: _Optional[str] = ..., offset: _Optional[int] = ..., block_type: _Optional[_Union[BlockType, str]] = ..., primary_replica_block: bool = ...) -> None: ...

class BucketEntry(_message.Message):
    __slots__ = ("offset", "length", "access_counter", "deserialiser_index", "priority")
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    ACCESS_COUNTER_FIELD_NUMBER: _ClassVar[int]
    DESERIALISER_INDEX_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    offset: int
    length: int
    access_counter: int
    deserialiser_index: int
    priority: BlockPriority
    def __init__(self, offset: _Optional[int] = ..., length: _Optional[int] = ..., access_counter: _Optional[int] = ..., deserialiser_index: _Optional[int] = ..., priority: _Optional[_Union[BlockPriority, str]] = ...) -> None: ...
