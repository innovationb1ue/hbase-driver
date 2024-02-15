import HBase_pb2 as _HBase_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ScopeType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REPLICATION_SCOPE_LOCAL: _ClassVar[ScopeType]
    REPLICATION_SCOPE_GLOBAL: _ClassVar[ScopeType]
    REPLICATION_SCOPE_SERIAL: _ClassVar[ScopeType]
REPLICATION_SCOPE_LOCAL: ScopeType
REPLICATION_SCOPE_GLOBAL: ScopeType
REPLICATION_SCOPE_SERIAL: ScopeType

class WALHeader(_message.Message):
    __slots__ = ("has_compression", "encryption_key", "has_tag_compression", "writer_cls_name", "cell_codec_cls_name", "has_value_compression", "value_compression_algorithm")
    HAS_COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTION_KEY_FIELD_NUMBER: _ClassVar[int]
    HAS_TAG_COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    WRITER_CLS_NAME_FIELD_NUMBER: _ClassVar[int]
    CELL_CODEC_CLS_NAME_FIELD_NUMBER: _ClassVar[int]
    HAS_VALUE_COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    VALUE_COMPRESSION_ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    has_compression: bool
    encryption_key: bytes
    has_tag_compression: bool
    writer_cls_name: str
    cell_codec_cls_name: str
    has_value_compression: bool
    value_compression_algorithm: int
    def __init__(self, has_compression: bool = ..., encryption_key: _Optional[bytes] = ..., has_tag_compression: bool = ..., writer_cls_name: _Optional[str] = ..., cell_codec_cls_name: _Optional[str] = ..., has_value_compression: bool = ..., value_compression_algorithm: _Optional[int] = ...) -> None: ...

class WALKey(_message.Message):
    __slots__ = ("encoded_region_name", "table_name", "log_sequence_number", "write_time", "cluster_id", "scopes", "following_kv_count", "cluster_ids", "nonceGroup", "nonce", "orig_sequence_number", "extended_attributes")
    ENCODED_REGION_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    LOG_SEQUENCE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    WRITE_TIME_FIELD_NUMBER: _ClassVar[int]
    CLUSTER_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    FOLLOWING_KV_COUNT_FIELD_NUMBER: _ClassVar[int]
    CLUSTER_IDS_FIELD_NUMBER: _ClassVar[int]
    NONCEGROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    ORIG_SEQUENCE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    EXTENDED_ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    encoded_region_name: bytes
    table_name: bytes
    log_sequence_number: int
    write_time: int
    cluster_id: _HBase_pb2.UUID
    scopes: _containers.RepeatedCompositeFieldContainer[FamilyScope]
    following_kv_count: int
    cluster_ids: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.UUID]
    nonceGroup: int
    nonce: int
    orig_sequence_number: int
    extended_attributes: _containers.RepeatedCompositeFieldContainer[Attribute]
    def __init__(self, encoded_region_name: _Optional[bytes] = ..., table_name: _Optional[bytes] = ..., log_sequence_number: _Optional[int] = ..., write_time: _Optional[int] = ..., cluster_id: _Optional[_Union[_HBase_pb2.UUID, _Mapping]] = ..., scopes: _Optional[_Iterable[_Union[FamilyScope, _Mapping]]] = ..., following_kv_count: _Optional[int] = ..., cluster_ids: _Optional[_Iterable[_Union[_HBase_pb2.UUID, _Mapping]]] = ..., nonceGroup: _Optional[int] = ..., nonce: _Optional[int] = ..., orig_sequence_number: _Optional[int] = ..., extended_attributes: _Optional[_Iterable[_Union[Attribute, _Mapping]]] = ...) -> None: ...

class Attribute(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: bytes
    def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...

class FamilyScope(_message.Message):
    __slots__ = ("family", "scope_type")
    FAMILY_FIELD_NUMBER: _ClassVar[int]
    SCOPE_TYPE_FIELD_NUMBER: _ClassVar[int]
    family: bytes
    scope_type: ScopeType
    def __init__(self, family: _Optional[bytes] = ..., scope_type: _Optional[_Union[ScopeType, str]] = ...) -> None: ...

class CompactionDescriptor(_message.Message):
    __slots__ = ("table_name", "encoded_region_name", "family_name", "compaction_input", "compaction_output", "store_home_dir", "region_name")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ENCODED_REGION_NAME_FIELD_NUMBER: _ClassVar[int]
    FAMILY_NAME_FIELD_NUMBER: _ClassVar[int]
    COMPACTION_INPUT_FIELD_NUMBER: _ClassVar[int]
    COMPACTION_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    STORE_HOME_DIR_FIELD_NUMBER: _ClassVar[int]
    REGION_NAME_FIELD_NUMBER: _ClassVar[int]
    table_name: bytes
    encoded_region_name: bytes
    family_name: bytes
    compaction_input: _containers.RepeatedScalarFieldContainer[str]
    compaction_output: _containers.RepeatedScalarFieldContainer[str]
    store_home_dir: str
    region_name: bytes
    def __init__(self, table_name: _Optional[bytes] = ..., encoded_region_name: _Optional[bytes] = ..., family_name: _Optional[bytes] = ..., compaction_input: _Optional[_Iterable[str]] = ..., compaction_output: _Optional[_Iterable[str]] = ..., store_home_dir: _Optional[str] = ..., region_name: _Optional[bytes] = ...) -> None: ...

class FlushDescriptor(_message.Message):
    __slots__ = ("action", "table_name", "encoded_region_name", "flush_sequence_number", "store_flushes", "region_name")
    class FlushAction(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        START_FLUSH: _ClassVar[FlushDescriptor.FlushAction]
        COMMIT_FLUSH: _ClassVar[FlushDescriptor.FlushAction]
        ABORT_FLUSH: _ClassVar[FlushDescriptor.FlushAction]
        CANNOT_FLUSH: _ClassVar[FlushDescriptor.FlushAction]
    START_FLUSH: FlushDescriptor.FlushAction
    COMMIT_FLUSH: FlushDescriptor.FlushAction
    ABORT_FLUSH: FlushDescriptor.FlushAction
    CANNOT_FLUSH: FlushDescriptor.FlushAction
    class StoreFlushDescriptor(_message.Message):
        __slots__ = ("family_name", "store_home_dir", "flush_output")
        FAMILY_NAME_FIELD_NUMBER: _ClassVar[int]
        STORE_HOME_DIR_FIELD_NUMBER: _ClassVar[int]
        FLUSH_OUTPUT_FIELD_NUMBER: _ClassVar[int]
        family_name: bytes
        store_home_dir: str
        flush_output: _containers.RepeatedScalarFieldContainer[str]
        def __init__(self, family_name: _Optional[bytes] = ..., store_home_dir: _Optional[str] = ..., flush_output: _Optional[_Iterable[str]] = ...) -> None: ...
    ACTION_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ENCODED_REGION_NAME_FIELD_NUMBER: _ClassVar[int]
    FLUSH_SEQUENCE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    STORE_FLUSHES_FIELD_NUMBER: _ClassVar[int]
    REGION_NAME_FIELD_NUMBER: _ClassVar[int]
    action: FlushDescriptor.FlushAction
    table_name: bytes
    encoded_region_name: bytes
    flush_sequence_number: int
    store_flushes: _containers.RepeatedCompositeFieldContainer[FlushDescriptor.StoreFlushDescriptor]
    region_name: bytes
    def __init__(self, action: _Optional[_Union[FlushDescriptor.FlushAction, str]] = ..., table_name: _Optional[bytes] = ..., encoded_region_name: _Optional[bytes] = ..., flush_sequence_number: _Optional[int] = ..., store_flushes: _Optional[_Iterable[_Union[FlushDescriptor.StoreFlushDescriptor, _Mapping]]] = ..., region_name: _Optional[bytes] = ...) -> None: ...

class StoreDescriptor(_message.Message):
    __slots__ = ("family_name", "store_home_dir", "store_file", "store_file_size_bytes")
    FAMILY_NAME_FIELD_NUMBER: _ClassVar[int]
    STORE_HOME_DIR_FIELD_NUMBER: _ClassVar[int]
    STORE_FILE_FIELD_NUMBER: _ClassVar[int]
    STORE_FILE_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    family_name: bytes
    store_home_dir: str
    store_file: _containers.RepeatedScalarFieldContainer[str]
    store_file_size_bytes: int
    def __init__(self, family_name: _Optional[bytes] = ..., store_home_dir: _Optional[str] = ..., store_file: _Optional[_Iterable[str]] = ..., store_file_size_bytes: _Optional[int] = ...) -> None: ...

class BulkLoadDescriptor(_message.Message):
    __slots__ = ("table_name", "encoded_region_name", "stores", "bulkload_seq_num", "cluster_ids", "replicate")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ENCODED_REGION_NAME_FIELD_NUMBER: _ClassVar[int]
    STORES_FIELD_NUMBER: _ClassVar[int]
    BULKLOAD_SEQ_NUM_FIELD_NUMBER: _ClassVar[int]
    CLUSTER_IDS_FIELD_NUMBER: _ClassVar[int]
    REPLICATE_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    encoded_region_name: bytes
    stores: _containers.RepeatedCompositeFieldContainer[StoreDescriptor]
    bulkload_seq_num: int
    cluster_ids: _containers.RepeatedScalarFieldContainer[str]
    replicate: bool
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., encoded_region_name: _Optional[bytes] = ..., stores: _Optional[_Iterable[_Union[StoreDescriptor, _Mapping]]] = ..., bulkload_seq_num: _Optional[int] = ..., cluster_ids: _Optional[_Iterable[str]] = ..., replicate: bool = ...) -> None: ...

class RegionEventDescriptor(_message.Message):
    __slots__ = ("event_type", "table_name", "encoded_region_name", "log_sequence_number", "stores", "server", "region_name")
    class EventType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        REGION_OPEN: _ClassVar[RegionEventDescriptor.EventType]
        REGION_CLOSE: _ClassVar[RegionEventDescriptor.EventType]
    REGION_OPEN: RegionEventDescriptor.EventType
    REGION_CLOSE: RegionEventDescriptor.EventType
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ENCODED_REGION_NAME_FIELD_NUMBER: _ClassVar[int]
    LOG_SEQUENCE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    STORES_FIELD_NUMBER: _ClassVar[int]
    SERVER_FIELD_NUMBER: _ClassVar[int]
    REGION_NAME_FIELD_NUMBER: _ClassVar[int]
    event_type: RegionEventDescriptor.EventType
    table_name: bytes
    encoded_region_name: bytes
    log_sequence_number: int
    stores: _containers.RepeatedCompositeFieldContainer[StoreDescriptor]
    server: _HBase_pb2.ServerName
    region_name: bytes
    def __init__(self, event_type: _Optional[_Union[RegionEventDescriptor.EventType, str]] = ..., table_name: _Optional[bytes] = ..., encoded_region_name: _Optional[bytes] = ..., log_sequence_number: _Optional[int] = ..., stores: _Optional[_Iterable[_Union[StoreDescriptor, _Mapping]]] = ..., server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., region_name: _Optional[bytes] = ...) -> None: ...

class WALTrailer(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
