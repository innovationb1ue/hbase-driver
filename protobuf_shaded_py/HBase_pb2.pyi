from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CompareType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LESS: _ClassVar[CompareType]
    LESS_OR_EQUAL: _ClassVar[CompareType]
    EQUAL: _ClassVar[CompareType]
    NOT_EQUAL: _ClassVar[CompareType]
    GREATER_OR_EQUAL: _ClassVar[CompareType]
    GREATER: _ClassVar[CompareType]
    NO_OP: _ClassVar[CompareType]

class TimeUnit(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NANOSECONDS: _ClassVar[TimeUnit]
    MICROSECONDS: _ClassVar[TimeUnit]
    MILLISECONDS: _ClassVar[TimeUnit]
    SECONDS: _ClassVar[TimeUnit]
    MINUTES: _ClassVar[TimeUnit]
    HOURS: _ClassVar[TimeUnit]
    DAYS: _ClassVar[TimeUnit]
LESS: CompareType
LESS_OR_EQUAL: CompareType
EQUAL: CompareType
NOT_EQUAL: CompareType
GREATER_OR_EQUAL: CompareType
GREATER: CompareType
NO_OP: CompareType
NANOSECONDS: TimeUnit
MICROSECONDS: TimeUnit
MILLISECONDS: TimeUnit
SECONDS: TimeUnit
MINUTES: TimeUnit
HOURS: TimeUnit
DAYS: TimeUnit

class TableName(_message.Message):
    __slots__ = ("namespace", "qualifier")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    QUALIFIER_FIELD_NUMBER: _ClassVar[int]
    namespace: bytes
    qualifier: bytes
    def __init__(self, namespace: _Optional[bytes] = ..., qualifier: _Optional[bytes] = ...) -> None: ...

class TableSchema(_message.Message):
    __slots__ = ("table_name", "attributes", "column_families", "configuration")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    COLUMN_FAMILIES_FIELD_NUMBER: _ClassVar[int]
    CONFIGURATION_FIELD_NUMBER: _ClassVar[int]
    table_name: TableName
    attributes: _containers.RepeatedCompositeFieldContainer[BytesBytesPair]
    column_families: _containers.RepeatedCompositeFieldContainer[ColumnFamilySchema]
    configuration: _containers.RepeatedCompositeFieldContainer[NameStringPair]
    def __init__(self, table_name: _Optional[_Union[TableName, _Mapping]] = ..., attributes: _Optional[_Iterable[_Union[BytesBytesPair, _Mapping]]] = ..., column_families: _Optional[_Iterable[_Union[ColumnFamilySchema, _Mapping]]] = ..., configuration: _Optional[_Iterable[_Union[NameStringPair, _Mapping]]] = ...) -> None: ...

class TableState(_message.Message):
    __slots__ = ("state",)
    class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        ENABLED: _ClassVar[TableState.State]
        DISABLED: _ClassVar[TableState.State]
        DISABLING: _ClassVar[TableState.State]
        ENABLING: _ClassVar[TableState.State]
    ENABLED: TableState.State
    DISABLED: TableState.State
    DISABLING: TableState.State
    ENABLING: TableState.State
    STATE_FIELD_NUMBER: _ClassVar[int]
    state: TableState.State
    def __init__(self, state: _Optional[_Union[TableState.State, str]] = ...) -> None: ...

class ColumnFamilySchema(_message.Message):
    __slots__ = ("name", "attributes", "configuration")
    NAME_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    CONFIGURATION_FIELD_NUMBER: _ClassVar[int]
    name: bytes
    attributes: _containers.RepeatedCompositeFieldContainer[BytesBytesPair]
    configuration: _containers.RepeatedCompositeFieldContainer[NameStringPair]
    def __init__(self, name: _Optional[bytes] = ..., attributes: _Optional[_Iterable[_Union[BytesBytesPair, _Mapping]]] = ..., configuration: _Optional[_Iterable[_Union[NameStringPair, _Mapping]]] = ...) -> None: ...

class RegionInfo(_message.Message):
    __slots__ = ("region_id", "table_name", "start_key", "end_key", "offline", "split", "replica_id")
    REGION_ID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    START_KEY_FIELD_NUMBER: _ClassVar[int]
    END_KEY_FIELD_NUMBER: _ClassVar[int]
    OFFLINE_FIELD_NUMBER: _ClassVar[int]
    SPLIT_FIELD_NUMBER: _ClassVar[int]
    REPLICA_ID_FIELD_NUMBER: _ClassVar[int]
    region_id: int
    table_name: TableName
    start_key: bytes
    end_key: bytes
    offline: bool
    split: bool
    replica_id: int
    def __init__(self, region_id: _Optional[int] = ..., table_name: _Optional[_Union[TableName, _Mapping]] = ..., start_key: _Optional[bytes] = ..., end_key: _Optional[bytes] = ..., offline: bool = ..., split: bool = ..., replica_id: _Optional[int] = ...) -> None: ...

class FavoredNodes(_message.Message):
    __slots__ = ("favored_node",)
    FAVORED_NODE_FIELD_NUMBER: _ClassVar[int]
    favored_node: _containers.RepeatedCompositeFieldContainer[ServerName]
    def __init__(self, favored_node: _Optional[_Iterable[_Union[ServerName, _Mapping]]] = ...) -> None: ...

class RegionSpecifier(_message.Message):
    __slots__ = ("type", "value")
    class RegionSpecifierType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        REGION_NAME: _ClassVar[RegionSpecifier.RegionSpecifierType]
        ENCODED_REGION_NAME: _ClassVar[RegionSpecifier.RegionSpecifierType]
    REGION_NAME: RegionSpecifier.RegionSpecifierType
    ENCODED_REGION_NAME: RegionSpecifier.RegionSpecifierType
    TYPE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    type: RegionSpecifier.RegionSpecifierType
    value: bytes
    def __init__(self, type: _Optional[_Union[RegionSpecifier.RegionSpecifierType, str]] = ..., value: _Optional[bytes] = ...) -> None: ...

class TimeRange(_message.Message):
    __slots__ = ("to",)
    FROM_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    to: int
    def __init__(self, to: _Optional[int] = ..., **kwargs) -> None: ...

class TimeRangeTracker(_message.Message):
    __slots__ = ("to",)
    FROM_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    to: int
    def __init__(self, to: _Optional[int] = ..., **kwargs) -> None: ...

class ColumnFamilyTimeRange(_message.Message):
    __slots__ = ("column_family", "time_range")
    COLUMN_FAMILY_FIELD_NUMBER: _ClassVar[int]
    TIME_RANGE_FIELD_NUMBER: _ClassVar[int]
    column_family: bytes
    time_range: TimeRange
    def __init__(self, column_family: _Optional[bytes] = ..., time_range: _Optional[_Union[TimeRange, _Mapping]] = ...) -> None: ...

class ServerName(_message.Message):
    __slots__ = ("host_name", "port", "start_code")
    HOST_NAME_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    START_CODE_FIELD_NUMBER: _ClassVar[int]
    host_name: str
    port: int
    start_code: int
    def __init__(self, host_name: _Optional[str] = ..., port: _Optional[int] = ..., start_code: _Optional[int] = ...) -> None: ...

class Coprocessor(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class NameStringPair(_message.Message):
    __slots__ = ("name", "value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    value: str
    def __init__(self, name: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class NameBytesPair(_message.Message):
    __slots__ = ("name", "value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    value: bytes
    def __init__(self, name: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...

class BytesBytesPair(_message.Message):
    __slots__ = ("first", "second")
    FIRST_FIELD_NUMBER: _ClassVar[int]
    SECOND_FIELD_NUMBER: _ClassVar[int]
    first: bytes
    second: bytes
    def __init__(self, first: _Optional[bytes] = ..., second: _Optional[bytes] = ...) -> None: ...

class NameInt64Pair(_message.Message):
    __slots__ = ("name", "value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    value: int
    def __init__(self, name: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...

class ProcedureDescription(_message.Message):
    __slots__ = ("signature", "instance", "creation_time", "configuration")
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_FIELD_NUMBER: _ClassVar[int]
    CREATION_TIME_FIELD_NUMBER: _ClassVar[int]
    CONFIGURATION_FIELD_NUMBER: _ClassVar[int]
    signature: str
    instance: str
    creation_time: int
    configuration: _containers.RepeatedCompositeFieldContainer[NameStringPair]
    def __init__(self, signature: _Optional[str] = ..., instance: _Optional[str] = ..., creation_time: _Optional[int] = ..., configuration: _Optional[_Iterable[_Union[NameStringPair, _Mapping]]] = ...) -> None: ...

class EmptyMsg(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class LongMsg(_message.Message):
    __slots__ = ("long_msg",)
    LONG_MSG_FIELD_NUMBER: _ClassVar[int]
    long_msg: int
    def __init__(self, long_msg: _Optional[int] = ...) -> None: ...

class DoubleMsg(_message.Message):
    __slots__ = ("double_msg",)
    DOUBLE_MSG_FIELD_NUMBER: _ClassVar[int]
    double_msg: float
    def __init__(self, double_msg: _Optional[float] = ...) -> None: ...

class BigDecimalMsg(_message.Message):
    __slots__ = ("bigdecimal_msg",)
    BIGDECIMAL_MSG_FIELD_NUMBER: _ClassVar[int]
    bigdecimal_msg: bytes
    def __init__(self, bigdecimal_msg: _Optional[bytes] = ...) -> None: ...

class UUID(_message.Message):
    __slots__ = ("least_sig_bits", "most_sig_bits")
    LEAST_SIG_BITS_FIELD_NUMBER: _ClassVar[int]
    MOST_SIG_BITS_FIELD_NUMBER: _ClassVar[int]
    least_sig_bits: int
    most_sig_bits: int
    def __init__(self, least_sig_bits: _Optional[int] = ..., most_sig_bits: _Optional[int] = ...) -> None: ...

class NamespaceDescriptor(_message.Message):
    __slots__ = ("name", "configuration")
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONFIGURATION_FIELD_NUMBER: _ClassVar[int]
    name: bytes
    configuration: _containers.RepeatedCompositeFieldContainer[NameStringPair]
    def __init__(self, name: _Optional[bytes] = ..., configuration: _Optional[_Iterable[_Union[NameStringPair, _Mapping]]] = ...) -> None: ...

class VersionInfo(_message.Message):
    __slots__ = ("version", "url", "revision", "user", "date", "src_checksum", "version_major", "version_minor")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    URL_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    DATE_FIELD_NUMBER: _ClassVar[int]
    SRC_CHECKSUM_FIELD_NUMBER: _ClassVar[int]
    VERSION_MAJOR_FIELD_NUMBER: _ClassVar[int]
    VERSION_MINOR_FIELD_NUMBER: _ClassVar[int]
    version: str
    url: str
    revision: str
    user: str
    date: str
    src_checksum: str
    version_major: int
    version_minor: int
    def __init__(self, version: _Optional[str] = ..., url: _Optional[str] = ..., revision: _Optional[str] = ..., user: _Optional[str] = ..., date: _Optional[str] = ..., src_checksum: _Optional[str] = ..., version_major: _Optional[int] = ..., version_minor: _Optional[int] = ...) -> None: ...

class RegionServerInfo(_message.Message):
    __slots__ = ("infoPort", "version_info")
    INFOPORT_FIELD_NUMBER: _ClassVar[int]
    VERSION_INFO_FIELD_NUMBER: _ClassVar[int]
    infoPort: int
    version_info: VersionInfo
    def __init__(self, infoPort: _Optional[int] = ..., version_info: _Optional[_Union[VersionInfo, _Mapping]] = ...) -> None: ...

class RegionExceptionMessage(_message.Message):
    __slots__ = ("region", "exception")
    REGION_FIELD_NUMBER: _ClassVar[int]
    EXCEPTION_FIELD_NUMBER: _ClassVar[int]
    region: RegionSpecifier
    exception: NameBytesPair
    def __init__(self, region: _Optional[_Union[RegionSpecifier, _Mapping]] = ..., exception: _Optional[_Union[NameBytesPair, _Mapping]] = ...) -> None: ...

class CacheEvictionStats(_message.Message):
    __slots__ = ("evicted_blocks", "bytes_evicted", "max_cache_size", "exception")
    EVICTED_BLOCKS_FIELD_NUMBER: _ClassVar[int]
    BYTES_EVICTED_FIELD_NUMBER: _ClassVar[int]
    MAX_CACHE_SIZE_FIELD_NUMBER: _ClassVar[int]
    EXCEPTION_FIELD_NUMBER: _ClassVar[int]
    evicted_blocks: int
    bytes_evicted: int
    max_cache_size: int
    exception: _containers.RepeatedCompositeFieldContainer[RegionExceptionMessage]
    def __init__(self, evicted_blocks: _Optional[int] = ..., bytes_evicted: _Optional[int] = ..., max_cache_size: _Optional[int] = ..., exception: _Optional[_Iterable[_Union[RegionExceptionMessage, _Mapping]]] = ...) -> None: ...

class RegionLocation(_message.Message):
    __slots__ = ("region_info", "server_name", "seq_num")
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    SERVER_NAME_FIELD_NUMBER: _ClassVar[int]
    SEQ_NUM_FIELD_NUMBER: _ClassVar[int]
    region_info: RegionInfo
    server_name: ServerName
    seq_num: int
    def __init__(self, region_info: _Optional[_Union[RegionInfo, _Mapping]] = ..., server_name: _Optional[_Union[ServerName, _Mapping]] = ..., seq_num: _Optional[int] = ...) -> None: ...

class LogRequest(_message.Message):
    __slots__ = ("log_class_name", "log_message")
    LOG_CLASS_NAME_FIELD_NUMBER: _ClassVar[int]
    LOG_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    log_class_name: str
    log_message: bytes
    def __init__(self, log_class_name: _Optional[str] = ..., log_message: _Optional[bytes] = ...) -> None: ...

class LogEntry(_message.Message):
    __slots__ = ("log_class_name", "log_message")
    LOG_CLASS_NAME_FIELD_NUMBER: _ClassVar[int]
    LOG_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    log_class_name: str
    log_message: bytes
    def __init__(self, log_class_name: _Optional[str] = ..., log_message: _Optional[bytes] = ...) -> None: ...
