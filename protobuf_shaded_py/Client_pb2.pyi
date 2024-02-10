import HBase_pb2 as _HBase_pb2
import Filter_pb2 as _Filter_pb2
import Cell_pb2 as _Cell_pb2
import Comparator_pb2 as _Comparator_pb2
import MapReduce_pb2 as _MapReduce_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Consistency(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STRONG: _ClassVar[Consistency]
    TIMELINE: _ClassVar[Consistency]
STRONG: Consistency
TIMELINE: Consistency

class Authorizations(_message.Message):
    __slots__ = ("label",)
    LABEL_FIELD_NUMBER: _ClassVar[int]
    label: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, label: _Optional[_Iterable[str]] = ...) -> None: ...

class CellVisibility(_message.Message):
    __slots__ = ("expression",)
    EXPRESSION_FIELD_NUMBER: _ClassVar[int]
    expression: str
    def __init__(self, expression: _Optional[str] = ...) -> None: ...

class Column(_message.Message):
    __slots__ = ("family", "qualifier")
    FAMILY_FIELD_NUMBER: _ClassVar[int]
    QUALIFIER_FIELD_NUMBER: _ClassVar[int]
    family: bytes
    qualifier: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, family: _Optional[bytes] = ..., qualifier: _Optional[_Iterable[bytes]] = ...) -> None: ...

class Get(_message.Message):
    __slots__ = ("row", "column", "attribute", "filter", "time_range", "max_versions", "cache_blocks", "store_limit", "store_offset", "existence_only", "closest_row_before", "consistency", "cf_time_range", "load_column_families_on_demand")
    ROW_FIELD_NUMBER: _ClassVar[int]
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTE_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    TIME_RANGE_FIELD_NUMBER: _ClassVar[int]
    MAX_VERSIONS_FIELD_NUMBER: _ClassVar[int]
    CACHE_BLOCKS_FIELD_NUMBER: _ClassVar[int]
    STORE_LIMIT_FIELD_NUMBER: _ClassVar[int]
    STORE_OFFSET_FIELD_NUMBER: _ClassVar[int]
    EXISTENCE_ONLY_FIELD_NUMBER: _ClassVar[int]
    CLOSEST_ROW_BEFORE_FIELD_NUMBER: _ClassVar[int]
    CONSISTENCY_FIELD_NUMBER: _ClassVar[int]
    CF_TIME_RANGE_FIELD_NUMBER: _ClassVar[int]
    LOAD_COLUMN_FAMILIES_ON_DEMAND_FIELD_NUMBER: _ClassVar[int]
    row: bytes
    column: _containers.RepeatedCompositeFieldContainer[Column]
    attribute: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.NameBytesPair]
    filter: _Filter_pb2.Filter
    time_range: _HBase_pb2.TimeRange
    max_versions: int
    cache_blocks: bool
    store_limit: int
    store_offset: int
    existence_only: bool
    closest_row_before: bool
    consistency: Consistency
    cf_time_range: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.ColumnFamilyTimeRange]
    load_column_families_on_demand: bool
    def __init__(self, row: _Optional[bytes] = ..., column: _Optional[_Iterable[_Union[Column, _Mapping]]] = ..., attribute: _Optional[_Iterable[_Union[_HBase_pb2.NameBytesPair, _Mapping]]] = ..., filter: _Optional[_Union[_Filter_pb2.Filter, _Mapping]] = ..., time_range: _Optional[_Union[_HBase_pb2.TimeRange, _Mapping]] = ..., max_versions: _Optional[int] = ..., cache_blocks: bool = ..., store_limit: _Optional[int] = ..., store_offset: _Optional[int] = ..., existence_only: bool = ..., closest_row_before: bool = ..., consistency: _Optional[_Union[Consistency, str]] = ..., cf_time_range: _Optional[_Iterable[_Union[_HBase_pb2.ColumnFamilyTimeRange, _Mapping]]] = ..., load_column_families_on_demand: bool = ...) -> None: ...

class Result(_message.Message):
    __slots__ = ("cell", "associated_cell_count", "exists", "stale", "partial")
    CELL_FIELD_NUMBER: _ClassVar[int]
    ASSOCIATED_CELL_COUNT_FIELD_NUMBER: _ClassVar[int]
    EXISTS_FIELD_NUMBER: _ClassVar[int]
    STALE_FIELD_NUMBER: _ClassVar[int]
    PARTIAL_FIELD_NUMBER: _ClassVar[int]
    cell: _containers.RepeatedCompositeFieldContainer[_Cell_pb2.Cell]
    associated_cell_count: int
    exists: bool
    stale: bool
    partial: bool
    def __init__(self, cell: _Optional[_Iterable[_Union[_Cell_pb2.Cell, _Mapping]]] = ..., associated_cell_count: _Optional[int] = ..., exists: bool = ..., stale: bool = ..., partial: bool = ...) -> None: ...

class GetRequest(_message.Message):
    __slots__ = ("region", "get")
    REGION_FIELD_NUMBER: _ClassVar[int]
    GET_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    get: Get
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., get: _Optional[_Union[Get, _Mapping]] = ...) -> None: ...

class GetResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: Result
    def __init__(self, result: _Optional[_Union[Result, _Mapping]] = ...) -> None: ...

class Condition(_message.Message):
    __slots__ = ("row", "family", "qualifier", "compare_type", "comparator", "time_range", "filter")
    ROW_FIELD_NUMBER: _ClassVar[int]
    FAMILY_FIELD_NUMBER: _ClassVar[int]
    QUALIFIER_FIELD_NUMBER: _ClassVar[int]
    COMPARE_TYPE_FIELD_NUMBER: _ClassVar[int]
    COMPARATOR_FIELD_NUMBER: _ClassVar[int]
    TIME_RANGE_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    row: bytes
    family: bytes
    qualifier: bytes
    compare_type: _HBase_pb2.CompareType
    comparator: _Comparator_pb2.Comparator
    time_range: _HBase_pb2.TimeRange
    filter: _Filter_pb2.Filter
    def __init__(self, row: _Optional[bytes] = ..., family: _Optional[bytes] = ..., qualifier: _Optional[bytes] = ..., compare_type: _Optional[_Union[_HBase_pb2.CompareType, str]] = ..., comparator: _Optional[_Union[_Comparator_pb2.Comparator, _Mapping]] = ..., time_range: _Optional[_Union[_HBase_pb2.TimeRange, _Mapping]] = ..., filter: _Optional[_Union[_Filter_pb2.Filter, _Mapping]] = ...) -> None: ...

class MutationProto(_message.Message):
    __slots__ = ("row", "mutate_type", "column_value", "timestamp", "attribute", "durability", "time_range", "associated_cell_count", "nonce")
    class Durability(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        USE_DEFAULT: _ClassVar[MutationProto.Durability]
        SKIP_WAL: _ClassVar[MutationProto.Durability]
        ASYNC_WAL: _ClassVar[MutationProto.Durability]
        SYNC_WAL: _ClassVar[MutationProto.Durability]
        FSYNC_WAL: _ClassVar[MutationProto.Durability]
    USE_DEFAULT: MutationProto.Durability
    SKIP_WAL: MutationProto.Durability
    ASYNC_WAL: MutationProto.Durability
    SYNC_WAL: MutationProto.Durability
    FSYNC_WAL: MutationProto.Durability
    class MutationType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        APPEND: _ClassVar[MutationProto.MutationType]
        INCREMENT: _ClassVar[MutationProto.MutationType]
        PUT: _ClassVar[MutationProto.MutationType]
        DELETE: _ClassVar[MutationProto.MutationType]
    APPEND: MutationProto.MutationType
    INCREMENT: MutationProto.MutationType
    PUT: MutationProto.MutationType
    DELETE: MutationProto.MutationType
    class DeleteType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        DELETE_ONE_VERSION: _ClassVar[MutationProto.DeleteType]
        DELETE_MULTIPLE_VERSIONS: _ClassVar[MutationProto.DeleteType]
        DELETE_FAMILY: _ClassVar[MutationProto.DeleteType]
        DELETE_FAMILY_VERSION: _ClassVar[MutationProto.DeleteType]
    DELETE_ONE_VERSION: MutationProto.DeleteType
    DELETE_MULTIPLE_VERSIONS: MutationProto.DeleteType
    DELETE_FAMILY: MutationProto.DeleteType
    DELETE_FAMILY_VERSION: MutationProto.DeleteType
    class ColumnValue(_message.Message):
        __slots__ = ("family", "qualifier_value")
        class QualifierValue(_message.Message):
            __slots__ = ("qualifier", "value", "timestamp", "delete_type", "tags")
            QUALIFIER_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
            DELETE_TYPE_FIELD_NUMBER: _ClassVar[int]
            TAGS_FIELD_NUMBER: _ClassVar[int]
            qualifier: bytes
            value: bytes
            timestamp: int
            delete_type: MutationProto.DeleteType
            tags: bytes
            def __init__(self, qualifier: _Optional[bytes] = ..., value: _Optional[bytes] = ..., timestamp: _Optional[int] = ..., delete_type: _Optional[_Union[MutationProto.DeleteType, str]] = ..., tags: _Optional[bytes] = ...) -> None: ...
        FAMILY_FIELD_NUMBER: _ClassVar[int]
        QUALIFIER_VALUE_FIELD_NUMBER: _ClassVar[int]
        family: bytes
        qualifier_value: _containers.RepeatedCompositeFieldContainer[MutationProto.ColumnValue.QualifierValue]
        def __init__(self, family: _Optional[bytes] = ..., qualifier_value: _Optional[_Iterable[_Union[MutationProto.ColumnValue.QualifierValue, _Mapping]]] = ...) -> None: ...
    ROW_FIELD_NUMBER: _ClassVar[int]
    MUTATE_TYPE_FIELD_NUMBER: _ClassVar[int]
    COLUMN_VALUE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTE_FIELD_NUMBER: _ClassVar[int]
    DURABILITY_FIELD_NUMBER: _ClassVar[int]
    TIME_RANGE_FIELD_NUMBER: _ClassVar[int]
    ASSOCIATED_CELL_COUNT_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    row: bytes
    mutate_type: MutationProto.MutationType
    column_value: _containers.RepeatedCompositeFieldContainer[MutationProto.ColumnValue]
    timestamp: int
    attribute: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.NameBytesPair]
    durability: MutationProto.Durability
    time_range: _HBase_pb2.TimeRange
    associated_cell_count: int
    nonce: int
    def __init__(self, row: _Optional[bytes] = ..., mutate_type: _Optional[_Union[MutationProto.MutationType, str]] = ..., column_value: _Optional[_Iterable[_Union[MutationProto.ColumnValue, _Mapping]]] = ..., timestamp: _Optional[int] = ..., attribute: _Optional[_Iterable[_Union[_HBase_pb2.NameBytesPair, _Mapping]]] = ..., durability: _Optional[_Union[MutationProto.Durability, str]] = ..., time_range: _Optional[_Union[_HBase_pb2.TimeRange, _Mapping]] = ..., associated_cell_count: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class MutateRequest(_message.Message):
    __slots__ = ("region", "mutation", "condition", "nonce_group")
    REGION_FIELD_NUMBER: _ClassVar[int]
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    mutation: MutationProto
    condition: Condition
    nonce_group: int
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., mutation: _Optional[_Union[MutationProto, _Mapping]] = ..., condition: _Optional[_Union[Condition, _Mapping]] = ..., nonce_group: _Optional[int] = ...) -> None: ...

class MutateResponse(_message.Message):
    __slots__ = ("result", "processed")
    RESULT_FIELD_NUMBER: _ClassVar[int]
    PROCESSED_FIELD_NUMBER: _ClassVar[int]
    result: Result
    processed: bool
    def __init__(self, result: _Optional[_Union[Result, _Mapping]] = ..., processed: bool = ...) -> None: ...

class Scan(_message.Message):
    __slots__ = ("column", "attribute", "start_row", "stop_row", "filter", "time_range", "max_versions", "cache_blocks", "batch_size", "max_result_size", "store_limit", "store_offset", "load_column_families_on_demand", "small", "reversed", "consistency", "caching", "allow_partial_results", "cf_time_range", "mvcc_read_point", "include_start_row", "include_stop_row", "readType", "need_cursor_result")
    class ReadType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        DEFAULT: _ClassVar[Scan.ReadType]
        STREAM: _ClassVar[Scan.ReadType]
        PREAD: _ClassVar[Scan.ReadType]
    DEFAULT: Scan.ReadType
    STREAM: Scan.ReadType
    PREAD: Scan.ReadType
    COLUMN_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTE_FIELD_NUMBER: _ClassVar[int]
    START_ROW_FIELD_NUMBER: _ClassVar[int]
    STOP_ROW_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    TIME_RANGE_FIELD_NUMBER: _ClassVar[int]
    MAX_VERSIONS_FIELD_NUMBER: _ClassVar[int]
    CACHE_BLOCKS_FIELD_NUMBER: _ClassVar[int]
    BATCH_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAX_RESULT_SIZE_FIELD_NUMBER: _ClassVar[int]
    STORE_LIMIT_FIELD_NUMBER: _ClassVar[int]
    STORE_OFFSET_FIELD_NUMBER: _ClassVar[int]
    LOAD_COLUMN_FAMILIES_ON_DEMAND_FIELD_NUMBER: _ClassVar[int]
    SMALL_FIELD_NUMBER: _ClassVar[int]
    REVERSED_FIELD_NUMBER: _ClassVar[int]
    CONSISTENCY_FIELD_NUMBER: _ClassVar[int]
    CACHING_FIELD_NUMBER: _ClassVar[int]
    ALLOW_PARTIAL_RESULTS_FIELD_NUMBER: _ClassVar[int]
    CF_TIME_RANGE_FIELD_NUMBER: _ClassVar[int]
    MVCC_READ_POINT_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_START_ROW_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_STOP_ROW_FIELD_NUMBER: _ClassVar[int]
    READTYPE_FIELD_NUMBER: _ClassVar[int]
    NEED_CURSOR_RESULT_FIELD_NUMBER: _ClassVar[int]
    column: _containers.RepeatedCompositeFieldContainer[Column]
    attribute: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.NameBytesPair]
    start_row: bytes
    stop_row: bytes
    filter: _Filter_pb2.Filter
    time_range: _HBase_pb2.TimeRange
    max_versions: int
    cache_blocks: bool
    batch_size: int
    max_result_size: int
    store_limit: int
    store_offset: int
    load_column_families_on_demand: bool
    small: bool
    reversed: bool
    consistency: Consistency
    caching: int
    allow_partial_results: bool
    cf_time_range: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.ColumnFamilyTimeRange]
    mvcc_read_point: int
    include_start_row: bool
    include_stop_row: bool
    readType: Scan.ReadType
    need_cursor_result: bool
    def __init__(self, column: _Optional[_Iterable[_Union[Column, _Mapping]]] = ..., attribute: _Optional[_Iterable[_Union[_HBase_pb2.NameBytesPair, _Mapping]]] = ..., start_row: _Optional[bytes] = ..., stop_row: _Optional[bytes] = ..., filter: _Optional[_Union[_Filter_pb2.Filter, _Mapping]] = ..., time_range: _Optional[_Union[_HBase_pb2.TimeRange, _Mapping]] = ..., max_versions: _Optional[int] = ..., cache_blocks: bool = ..., batch_size: _Optional[int] = ..., max_result_size: _Optional[int] = ..., store_limit: _Optional[int] = ..., store_offset: _Optional[int] = ..., load_column_families_on_demand: bool = ..., small: bool = ..., reversed: bool = ..., consistency: _Optional[_Union[Consistency, str]] = ..., caching: _Optional[int] = ..., allow_partial_results: bool = ..., cf_time_range: _Optional[_Iterable[_Union[_HBase_pb2.ColumnFamilyTimeRange, _Mapping]]] = ..., mvcc_read_point: _Optional[int] = ..., include_start_row: bool = ..., include_stop_row: bool = ..., readType: _Optional[_Union[Scan.ReadType, str]] = ..., need_cursor_result: bool = ...) -> None: ...

class ScanRequest(_message.Message):
    __slots__ = ("region", "scan", "scanner_id", "number_of_rows", "close_scanner", "next_call_seq", "client_handles_partials", "client_handles_heartbeats", "track_scan_metrics", "renew", "limit_of_rows")
    REGION_FIELD_NUMBER: _ClassVar[int]
    SCAN_FIELD_NUMBER: _ClassVar[int]
    SCANNER_ID_FIELD_NUMBER: _ClassVar[int]
    NUMBER_OF_ROWS_FIELD_NUMBER: _ClassVar[int]
    CLOSE_SCANNER_FIELD_NUMBER: _ClassVar[int]
    NEXT_CALL_SEQ_FIELD_NUMBER: _ClassVar[int]
    CLIENT_HANDLES_PARTIALS_FIELD_NUMBER: _ClassVar[int]
    CLIENT_HANDLES_HEARTBEATS_FIELD_NUMBER: _ClassVar[int]
    TRACK_SCAN_METRICS_FIELD_NUMBER: _ClassVar[int]
    RENEW_FIELD_NUMBER: _ClassVar[int]
    LIMIT_OF_ROWS_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    scan: Scan
    scanner_id: int
    number_of_rows: int
    close_scanner: bool
    next_call_seq: int
    client_handles_partials: bool
    client_handles_heartbeats: bool
    track_scan_metrics: bool
    renew: bool
    limit_of_rows: int
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., scan: _Optional[_Union[Scan, _Mapping]] = ..., scanner_id: _Optional[int] = ..., number_of_rows: _Optional[int] = ..., close_scanner: bool = ..., next_call_seq: _Optional[int] = ..., client_handles_partials: bool = ..., client_handles_heartbeats: bool = ..., track_scan_metrics: bool = ..., renew: bool = ..., limit_of_rows: _Optional[int] = ...) -> None: ...

class Cursor(_message.Message):
    __slots__ = ("row",)
    ROW_FIELD_NUMBER: _ClassVar[int]
    row: bytes
    def __init__(self, row: _Optional[bytes] = ...) -> None: ...

class ScanResponse(_message.Message):
    __slots__ = ("cells_per_result", "scanner_id", "more_results", "ttl", "results", "stale", "partial_flag_per_result", "more_results_in_region", "heartbeat_message", "scan_metrics", "mvcc_read_point", "cursor")
    CELLS_PER_RESULT_FIELD_NUMBER: _ClassVar[int]
    SCANNER_ID_FIELD_NUMBER: _ClassVar[int]
    MORE_RESULTS_FIELD_NUMBER: _ClassVar[int]
    TTL_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    STALE_FIELD_NUMBER: _ClassVar[int]
    PARTIAL_FLAG_PER_RESULT_FIELD_NUMBER: _ClassVar[int]
    MORE_RESULTS_IN_REGION_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    SCAN_METRICS_FIELD_NUMBER: _ClassVar[int]
    MVCC_READ_POINT_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    cells_per_result: _containers.RepeatedScalarFieldContainer[int]
    scanner_id: int
    more_results: bool
    ttl: int
    results: _containers.RepeatedCompositeFieldContainer[Result]
    stale: bool
    partial_flag_per_result: _containers.RepeatedScalarFieldContainer[bool]
    more_results_in_region: bool
    heartbeat_message: bool
    scan_metrics: _MapReduce_pb2.ScanMetrics
    mvcc_read_point: int
    cursor: Cursor
    def __init__(self, cells_per_result: _Optional[_Iterable[int]] = ..., scanner_id: _Optional[int] = ..., more_results: bool = ..., ttl: _Optional[int] = ..., results: _Optional[_Iterable[_Union[Result, _Mapping]]] = ..., stale: bool = ..., partial_flag_per_result: _Optional[_Iterable[bool]] = ..., more_results_in_region: bool = ..., heartbeat_message: bool = ..., scan_metrics: _Optional[_Union[_MapReduce_pb2.ScanMetrics, _Mapping]] = ..., mvcc_read_point: _Optional[int] = ..., cursor: _Optional[_Union[Cursor, _Mapping]] = ...) -> None: ...

class BulkLoadHFileRequest(_message.Message):
    __slots__ = ("region", "family_path", "assign_seq_num", "fs_token", "bulk_token", "copy_file", "cluster_ids", "replicate")
    class FamilyPath(_message.Message):
        __slots__ = ("family", "path")
        FAMILY_FIELD_NUMBER: _ClassVar[int]
        PATH_FIELD_NUMBER: _ClassVar[int]
        family: bytes
        path: str
        def __init__(self, family: _Optional[bytes] = ..., path: _Optional[str] = ...) -> None: ...
    REGION_FIELD_NUMBER: _ClassVar[int]
    FAMILY_PATH_FIELD_NUMBER: _ClassVar[int]
    ASSIGN_SEQ_NUM_FIELD_NUMBER: _ClassVar[int]
    FS_TOKEN_FIELD_NUMBER: _ClassVar[int]
    BULK_TOKEN_FIELD_NUMBER: _ClassVar[int]
    COPY_FILE_FIELD_NUMBER: _ClassVar[int]
    CLUSTER_IDS_FIELD_NUMBER: _ClassVar[int]
    REPLICATE_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    family_path: _containers.RepeatedCompositeFieldContainer[BulkLoadHFileRequest.FamilyPath]
    assign_seq_num: bool
    fs_token: DelegationToken
    bulk_token: str
    copy_file: bool
    cluster_ids: _containers.RepeatedScalarFieldContainer[str]
    replicate: bool
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., family_path: _Optional[_Iterable[_Union[BulkLoadHFileRequest.FamilyPath, _Mapping]]] = ..., assign_seq_num: bool = ..., fs_token: _Optional[_Union[DelegationToken, _Mapping]] = ..., bulk_token: _Optional[str] = ..., copy_file: bool = ..., cluster_ids: _Optional[_Iterable[str]] = ..., replicate: bool = ...) -> None: ...

class BulkLoadHFileResponse(_message.Message):
    __slots__ = ("loaded",)
    LOADED_FIELD_NUMBER: _ClassVar[int]
    loaded: bool
    def __init__(self, loaded: bool = ...) -> None: ...

class DelegationToken(_message.Message):
    __slots__ = ("identifier", "password", "kind", "service")
    IDENTIFIER_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    identifier: bytes
    password: bytes
    kind: str
    service: str
    def __init__(self, identifier: _Optional[bytes] = ..., password: _Optional[bytes] = ..., kind: _Optional[str] = ..., service: _Optional[str] = ...) -> None: ...

class PrepareBulkLoadRequest(_message.Message):
    __slots__ = ("table_name", "region")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    region: _HBase_pb2.RegionSpecifier
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ...) -> None: ...

class PrepareBulkLoadResponse(_message.Message):
    __slots__ = ("bulk_token",)
    BULK_TOKEN_FIELD_NUMBER: _ClassVar[int]
    bulk_token: str
    def __init__(self, bulk_token: _Optional[str] = ...) -> None: ...

class CleanupBulkLoadRequest(_message.Message):
    __slots__ = ("bulk_token", "region")
    BULK_TOKEN_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    bulk_token: str
    region: _HBase_pb2.RegionSpecifier
    def __init__(self, bulk_token: _Optional[str] = ..., region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ...) -> None: ...

class CleanupBulkLoadResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CoprocessorServiceCall(_message.Message):
    __slots__ = ("row", "service_name", "method_name", "request")
    ROW_FIELD_NUMBER: _ClassVar[int]
    SERVICE_NAME_FIELD_NUMBER: _ClassVar[int]
    METHOD_NAME_FIELD_NUMBER: _ClassVar[int]
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    row: bytes
    service_name: str
    method_name: str
    request: bytes
    def __init__(self, row: _Optional[bytes] = ..., service_name: _Optional[str] = ..., method_name: _Optional[str] = ..., request: _Optional[bytes] = ...) -> None: ...

class CoprocessorServiceResult(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: _HBase_pb2.NameBytesPair
    def __init__(self, value: _Optional[_Union[_HBase_pb2.NameBytesPair, _Mapping]] = ...) -> None: ...

class CoprocessorServiceRequest(_message.Message):
    __slots__ = ("region", "call")
    REGION_FIELD_NUMBER: _ClassVar[int]
    CALL_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    call: CoprocessorServiceCall
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., call: _Optional[_Union[CoprocessorServiceCall, _Mapping]] = ...) -> None: ...

class CoprocessorServiceResponse(_message.Message):
    __slots__ = ("region", "value")
    REGION_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    value: _HBase_pb2.NameBytesPair
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., value: _Optional[_Union[_HBase_pb2.NameBytesPair, _Mapping]] = ...) -> None: ...

class Action(_message.Message):
    __slots__ = ("index", "mutation", "get", "service_call")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    MUTATION_FIELD_NUMBER: _ClassVar[int]
    GET_FIELD_NUMBER: _ClassVar[int]
    SERVICE_CALL_FIELD_NUMBER: _ClassVar[int]
    index: int
    mutation: MutationProto
    get: Get
    service_call: CoprocessorServiceCall
    def __init__(self, index: _Optional[int] = ..., mutation: _Optional[_Union[MutationProto, _Mapping]] = ..., get: _Optional[_Union[Get, _Mapping]] = ..., service_call: _Optional[_Union[CoprocessorServiceCall, _Mapping]] = ...) -> None: ...

class RegionAction(_message.Message):
    __slots__ = ("region", "atomic", "action", "condition")
    REGION_FIELD_NUMBER: _ClassVar[int]
    ATOMIC_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    atomic: bool
    action: _containers.RepeatedCompositeFieldContainer[Action]
    condition: Condition
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., atomic: bool = ..., action: _Optional[_Iterable[_Union[Action, _Mapping]]] = ..., condition: _Optional[_Union[Condition, _Mapping]] = ...) -> None: ...

class RegionLoadStats(_message.Message):
    __slots__ = ("memStoreLoad", "heapOccupancy", "compactionPressure")
    MEMSTORELOAD_FIELD_NUMBER: _ClassVar[int]
    HEAPOCCUPANCY_FIELD_NUMBER: _ClassVar[int]
    COMPACTIONPRESSURE_FIELD_NUMBER: _ClassVar[int]
    memStoreLoad: int
    heapOccupancy: int
    compactionPressure: int
    def __init__(self, memStoreLoad: _Optional[int] = ..., heapOccupancy: _Optional[int] = ..., compactionPressure: _Optional[int] = ...) -> None: ...

class MultiRegionLoadStats(_message.Message):
    __slots__ = ("region", "stat")
    REGION_FIELD_NUMBER: _ClassVar[int]
    STAT_FIELD_NUMBER: _ClassVar[int]
    region: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionSpecifier]
    stat: _containers.RepeatedCompositeFieldContainer[RegionLoadStats]
    def __init__(self, region: _Optional[_Iterable[_Union[_HBase_pb2.RegionSpecifier, _Mapping]]] = ..., stat: _Optional[_Iterable[_Union[RegionLoadStats, _Mapping]]] = ...) -> None: ...

class ResultOrException(_message.Message):
    __slots__ = ("index", "result", "exception", "service_result", "loadStats")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    EXCEPTION_FIELD_NUMBER: _ClassVar[int]
    SERVICE_RESULT_FIELD_NUMBER: _ClassVar[int]
    LOADSTATS_FIELD_NUMBER: _ClassVar[int]
    index: int
    result: Result
    exception: _HBase_pb2.NameBytesPair
    service_result: CoprocessorServiceResult
    loadStats: RegionLoadStats
    def __init__(self, index: _Optional[int] = ..., result: _Optional[_Union[Result, _Mapping]] = ..., exception: _Optional[_Union[_HBase_pb2.NameBytesPair, _Mapping]] = ..., service_result: _Optional[_Union[CoprocessorServiceResult, _Mapping]] = ..., loadStats: _Optional[_Union[RegionLoadStats, _Mapping]] = ...) -> None: ...

class RegionActionResult(_message.Message):
    __slots__ = ("resultOrException", "exception", "processed")
    RESULTOREXCEPTION_FIELD_NUMBER: _ClassVar[int]
    EXCEPTION_FIELD_NUMBER: _ClassVar[int]
    PROCESSED_FIELD_NUMBER: _ClassVar[int]
    resultOrException: _containers.RepeatedCompositeFieldContainer[ResultOrException]
    exception: _HBase_pb2.NameBytesPair
    processed: bool
    def __init__(self, resultOrException: _Optional[_Iterable[_Union[ResultOrException, _Mapping]]] = ..., exception: _Optional[_Union[_HBase_pb2.NameBytesPair, _Mapping]] = ..., processed: bool = ...) -> None: ...

class MultiRequest(_message.Message):
    __slots__ = ("regionAction", "nonceGroup", "condition")
    REGIONACTION_FIELD_NUMBER: _ClassVar[int]
    NONCEGROUP_FIELD_NUMBER: _ClassVar[int]
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    regionAction: _containers.RepeatedCompositeFieldContainer[RegionAction]
    nonceGroup: int
    condition: Condition
    def __init__(self, regionAction: _Optional[_Iterable[_Union[RegionAction, _Mapping]]] = ..., nonceGroup: _Optional[int] = ..., condition: _Optional[_Union[Condition, _Mapping]] = ...) -> None: ...

class MultiResponse(_message.Message):
    __slots__ = ("regionActionResult", "processed", "regionStatistics")
    REGIONACTIONRESULT_FIELD_NUMBER: _ClassVar[int]
    PROCESSED_FIELD_NUMBER: _ClassVar[int]
    REGIONSTATISTICS_FIELD_NUMBER: _ClassVar[int]
    regionActionResult: _containers.RepeatedCompositeFieldContainer[RegionActionResult]
    processed: bool
    regionStatistics: MultiRegionLoadStats
    def __init__(self, regionActionResult: _Optional[_Iterable[_Union[RegionActionResult, _Mapping]]] = ..., processed: bool = ..., regionStatistics: _Optional[_Union[MultiRegionLoadStats, _Mapping]] = ...) -> None: ...
