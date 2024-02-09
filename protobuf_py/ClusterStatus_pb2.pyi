import HBase_pb2 as _HBase_pb2
import ClusterId_pb2 as _ClusterId_pb2
import FS_pb2 as _FS_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Option(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    HBASE_VERSION: _ClassVar[Option]
    CLUSTER_ID: _ClassVar[Option]
    LIVE_SERVERS: _ClassVar[Option]
    DEAD_SERVERS: _ClassVar[Option]
    MASTER: _ClassVar[Option]
    BACKUP_MASTERS: _ClassVar[Option]
    MASTER_COPROCESSORS: _ClassVar[Option]
    REGIONS_IN_TRANSITION: _ClassVar[Option]
    BALANCER_ON: _ClassVar[Option]
HBASE_VERSION: Option
CLUSTER_ID: Option
LIVE_SERVERS: Option
DEAD_SERVERS: Option
MASTER: Option
BACKUP_MASTERS: Option
MASTER_COPROCESSORS: Option
REGIONS_IN_TRANSITION: Option
BALANCER_ON: Option

class RegionState(_message.Message):
    __slots__ = ("region_info", "state", "stamp")
    class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        OFFLINE: _ClassVar[RegionState.State]
        PENDING_OPEN: _ClassVar[RegionState.State]
        OPENING: _ClassVar[RegionState.State]
        OPEN: _ClassVar[RegionState.State]
        PENDING_CLOSE: _ClassVar[RegionState.State]
        CLOSING: _ClassVar[RegionState.State]
        CLOSED: _ClassVar[RegionState.State]
        SPLITTING: _ClassVar[RegionState.State]
        SPLIT: _ClassVar[RegionState.State]
        FAILED_OPEN: _ClassVar[RegionState.State]
        FAILED_CLOSE: _ClassVar[RegionState.State]
        MERGING: _ClassVar[RegionState.State]
        MERGED: _ClassVar[RegionState.State]
        SPLITTING_NEW: _ClassVar[RegionState.State]
        MERGING_NEW: _ClassVar[RegionState.State]
    OFFLINE: RegionState.State
    PENDING_OPEN: RegionState.State
    OPENING: RegionState.State
    OPEN: RegionState.State
    PENDING_CLOSE: RegionState.State
    CLOSING: RegionState.State
    CLOSED: RegionState.State
    SPLITTING: RegionState.State
    SPLIT: RegionState.State
    FAILED_OPEN: RegionState.State
    FAILED_CLOSE: RegionState.State
    MERGING: RegionState.State
    MERGED: RegionState.State
    SPLITTING_NEW: RegionState.State
    MERGING_NEW: RegionState.State
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    STAMP_FIELD_NUMBER: _ClassVar[int]
    region_info: _HBase_pb2.RegionInfo
    state: RegionState.State
    stamp: int
    def __init__(self, region_info: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ..., state: _Optional[_Union[RegionState.State, str]] = ..., stamp: _Optional[int] = ...) -> None: ...

class RegionInTransition(_message.Message):
    __slots__ = ("spec", "region_state")
    SPEC_FIELD_NUMBER: _ClassVar[int]
    REGION_STATE_FIELD_NUMBER: _ClassVar[int]
    spec: _HBase_pb2.RegionSpecifier
    region_state: RegionState
    def __init__(self, spec: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., region_state: _Optional[_Union[RegionState, _Mapping]] = ...) -> None: ...

class StoreSequenceId(_message.Message):
    __slots__ = ("family_name", "sequence_id")
    FAMILY_NAME_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_ID_FIELD_NUMBER: _ClassVar[int]
    family_name: bytes
    sequence_id: int
    def __init__(self, family_name: _Optional[bytes] = ..., sequence_id: _Optional[int] = ...) -> None: ...

class RegionStoreSequenceIds(_message.Message):
    __slots__ = ("last_flushed_sequence_id", "store_sequence_id")
    LAST_FLUSHED_SEQUENCE_ID_FIELD_NUMBER: _ClassVar[int]
    STORE_SEQUENCE_ID_FIELD_NUMBER: _ClassVar[int]
    last_flushed_sequence_id: int
    store_sequence_id: _containers.RepeatedCompositeFieldContainer[StoreSequenceId]
    def __init__(self, last_flushed_sequence_id: _Optional[int] = ..., store_sequence_id: _Optional[_Iterable[_Union[StoreSequenceId, _Mapping]]] = ...) -> None: ...

class RegionLoad(_message.Message):
    __slots__ = ("region_specifier", "stores", "storefiles", "store_uncompressed_size_MB", "storefile_size_MB", "memstore_size_MB", "storefile_index_size_KB", "read_requests_count", "write_requests_count", "total_compacting_KVs", "current_compacted_KVs", "root_index_size_KB", "total_static_index_size_KB", "total_static_bloom_size_KB", "complete_sequence_id", "data_locality", "last_major_compaction_ts", "store_complete_sequence_id", "filtered_read_requests_count", "store_ref_count", "max_compacted_store_file_ref_count")
    REGION_SPECIFIER_FIELD_NUMBER: _ClassVar[int]
    STORES_FIELD_NUMBER: _ClassVar[int]
    STOREFILES_FIELD_NUMBER: _ClassVar[int]
    STORE_UNCOMPRESSED_SIZE_MB_FIELD_NUMBER: _ClassVar[int]
    STOREFILE_SIZE_MB_FIELD_NUMBER: _ClassVar[int]
    MEMSTORE_SIZE_MB_FIELD_NUMBER: _ClassVar[int]
    STOREFILE_INDEX_SIZE_KB_FIELD_NUMBER: _ClassVar[int]
    READ_REQUESTS_COUNT_FIELD_NUMBER: _ClassVar[int]
    WRITE_REQUESTS_COUNT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COMPACTING_KVS_FIELD_NUMBER: _ClassVar[int]
    CURRENT_COMPACTED_KVS_FIELD_NUMBER: _ClassVar[int]
    ROOT_INDEX_SIZE_KB_FIELD_NUMBER: _ClassVar[int]
    TOTAL_STATIC_INDEX_SIZE_KB_FIELD_NUMBER: _ClassVar[int]
    TOTAL_STATIC_BLOOM_SIZE_KB_FIELD_NUMBER: _ClassVar[int]
    COMPLETE_SEQUENCE_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_LOCALITY_FIELD_NUMBER: _ClassVar[int]
    LAST_MAJOR_COMPACTION_TS_FIELD_NUMBER: _ClassVar[int]
    STORE_COMPLETE_SEQUENCE_ID_FIELD_NUMBER: _ClassVar[int]
    FILTERED_READ_REQUESTS_COUNT_FIELD_NUMBER: _ClassVar[int]
    STORE_REF_COUNT_FIELD_NUMBER: _ClassVar[int]
    MAX_COMPACTED_STORE_FILE_REF_COUNT_FIELD_NUMBER: _ClassVar[int]
    region_specifier: _HBase_pb2.RegionSpecifier
    stores: int
    storefiles: int
    store_uncompressed_size_MB: int
    storefile_size_MB: int
    memstore_size_MB: int
    storefile_index_size_KB: int
    read_requests_count: int
    write_requests_count: int
    total_compacting_KVs: int
    current_compacted_KVs: int
    root_index_size_KB: int
    total_static_index_size_KB: int
    total_static_bloom_size_KB: int
    complete_sequence_id: int
    data_locality: float
    last_major_compaction_ts: int
    store_complete_sequence_id: _containers.RepeatedCompositeFieldContainer[StoreSequenceId]
    filtered_read_requests_count: int
    store_ref_count: int
    max_compacted_store_file_ref_count: int
    def __init__(self, region_specifier: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., stores: _Optional[int] = ..., storefiles: _Optional[int] = ..., store_uncompressed_size_MB: _Optional[int] = ..., storefile_size_MB: _Optional[int] = ..., memstore_size_MB: _Optional[int] = ..., storefile_index_size_KB: _Optional[int] = ..., read_requests_count: _Optional[int] = ..., write_requests_count: _Optional[int] = ..., total_compacting_KVs: _Optional[int] = ..., current_compacted_KVs: _Optional[int] = ..., root_index_size_KB: _Optional[int] = ..., total_static_index_size_KB: _Optional[int] = ..., total_static_bloom_size_KB: _Optional[int] = ..., complete_sequence_id: _Optional[int] = ..., data_locality: _Optional[float] = ..., last_major_compaction_ts: _Optional[int] = ..., store_complete_sequence_id: _Optional[_Iterable[_Union[StoreSequenceId, _Mapping]]] = ..., filtered_read_requests_count: _Optional[int] = ..., store_ref_count: _Optional[int] = ..., max_compacted_store_file_ref_count: _Optional[int] = ...) -> None: ...

class UserLoad(_message.Message):
    __slots__ = ("userName", "clientMetrics")
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    CLIENTMETRICS_FIELD_NUMBER: _ClassVar[int]
    userName: str
    clientMetrics: _containers.RepeatedCompositeFieldContainer[ClientMetrics]
    def __init__(self, userName: _Optional[str] = ..., clientMetrics: _Optional[_Iterable[_Union[ClientMetrics, _Mapping]]] = ...) -> None: ...

class ClientMetrics(_message.Message):
    __slots__ = ("hostName", "read_requests_count", "write_requests_count", "filtered_requests_count")
    HOSTNAME_FIELD_NUMBER: _ClassVar[int]
    READ_REQUESTS_COUNT_FIELD_NUMBER: _ClassVar[int]
    WRITE_REQUESTS_COUNT_FIELD_NUMBER: _ClassVar[int]
    FILTERED_REQUESTS_COUNT_FIELD_NUMBER: _ClassVar[int]
    hostName: str
    read_requests_count: int
    write_requests_count: int
    filtered_requests_count: int
    def __init__(self, hostName: _Optional[str] = ..., read_requests_count: _Optional[int] = ..., write_requests_count: _Optional[int] = ..., filtered_requests_count: _Optional[int] = ...) -> None: ...

class ReplicationLoadSink(_message.Message):
    __slots__ = ("ageOfLastAppliedOp", "timeStampsOfLastAppliedOp")
    AGEOFLASTAPPLIEDOP_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMPSOFLASTAPPLIEDOP_FIELD_NUMBER: _ClassVar[int]
    ageOfLastAppliedOp: int
    timeStampsOfLastAppliedOp: int
    def __init__(self, ageOfLastAppliedOp: _Optional[int] = ..., timeStampsOfLastAppliedOp: _Optional[int] = ...) -> None: ...

class ReplicationLoadSource(_message.Message):
    __slots__ = ("peerID", "ageOfLastShippedOp", "sizeOfLogQueue", "timeStampOfLastShippedOp", "replicationLag")
    PEERID_FIELD_NUMBER: _ClassVar[int]
    AGEOFLASTSHIPPEDOP_FIELD_NUMBER: _ClassVar[int]
    SIZEOFLOGQUEUE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMPOFLASTSHIPPEDOP_FIELD_NUMBER: _ClassVar[int]
    REPLICATIONLAG_FIELD_NUMBER: _ClassVar[int]
    peerID: str
    ageOfLastShippedOp: int
    sizeOfLogQueue: int
    timeStampOfLastShippedOp: int
    replicationLag: int
    def __init__(self, peerID: _Optional[str] = ..., ageOfLastShippedOp: _Optional[int] = ..., sizeOfLogQueue: _Optional[int] = ..., timeStampOfLastShippedOp: _Optional[int] = ..., replicationLag: _Optional[int] = ...) -> None: ...

class ServerLoad(_message.Message):
    __slots__ = ("number_of_requests", "total_number_of_requests", "used_heap_MB", "max_heap_MB", "region_loads", "coprocessors", "report_start_time", "report_end_time", "info_server_port", "replLoadSource", "replLoadSink", "userLoads")
    NUMBER_OF_REQUESTS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_NUMBER_OF_REQUESTS_FIELD_NUMBER: _ClassVar[int]
    USED_HEAP_MB_FIELD_NUMBER: _ClassVar[int]
    MAX_HEAP_MB_FIELD_NUMBER: _ClassVar[int]
    REGION_LOADS_FIELD_NUMBER: _ClassVar[int]
    COPROCESSORS_FIELD_NUMBER: _ClassVar[int]
    REPORT_START_TIME_FIELD_NUMBER: _ClassVar[int]
    REPORT_END_TIME_FIELD_NUMBER: _ClassVar[int]
    INFO_SERVER_PORT_FIELD_NUMBER: _ClassVar[int]
    REPLLOADSOURCE_FIELD_NUMBER: _ClassVar[int]
    REPLLOADSINK_FIELD_NUMBER: _ClassVar[int]
    USERLOADS_FIELD_NUMBER: _ClassVar[int]
    number_of_requests: int
    total_number_of_requests: int
    used_heap_MB: int
    max_heap_MB: int
    region_loads: _containers.RepeatedCompositeFieldContainer[RegionLoad]
    coprocessors: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.Coprocessor]
    report_start_time: int
    report_end_time: int
    info_server_port: int
    replLoadSource: _containers.RepeatedCompositeFieldContainer[ReplicationLoadSource]
    replLoadSink: ReplicationLoadSink
    userLoads: _containers.RepeatedCompositeFieldContainer[UserLoad]
    def __init__(self, number_of_requests: _Optional[int] = ..., total_number_of_requests: _Optional[int] = ..., used_heap_MB: _Optional[int] = ..., max_heap_MB: _Optional[int] = ..., region_loads: _Optional[_Iterable[_Union[RegionLoad, _Mapping]]] = ..., coprocessors: _Optional[_Iterable[_Union[_HBase_pb2.Coprocessor, _Mapping]]] = ..., report_start_time: _Optional[int] = ..., report_end_time: _Optional[int] = ..., info_server_port: _Optional[int] = ..., replLoadSource: _Optional[_Iterable[_Union[ReplicationLoadSource, _Mapping]]] = ..., replLoadSink: _Optional[_Union[ReplicationLoadSink, _Mapping]] = ..., userLoads: _Optional[_Iterable[_Union[UserLoad, _Mapping]]] = ...) -> None: ...

class LiveServerInfo(_message.Message):
    __slots__ = ("server", "server_load")
    SERVER_FIELD_NUMBER: _ClassVar[int]
    SERVER_LOAD_FIELD_NUMBER: _ClassVar[int]
    server: _HBase_pb2.ServerName
    server_load: ServerLoad
    def __init__(self, server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., server_load: _Optional[_Union[ServerLoad, _Mapping]] = ...) -> None: ...

class ClusterStatus(_message.Message):
    __slots__ = ("hbase_version", "live_servers", "dead_servers", "regions_in_transition", "cluster_id", "master_coprocessors", "master", "backup_masters", "balancer_on")
    HBASE_VERSION_FIELD_NUMBER: _ClassVar[int]
    LIVE_SERVERS_FIELD_NUMBER: _ClassVar[int]
    DEAD_SERVERS_FIELD_NUMBER: _ClassVar[int]
    REGIONS_IN_TRANSITION_FIELD_NUMBER: _ClassVar[int]
    CLUSTER_ID_FIELD_NUMBER: _ClassVar[int]
    MASTER_COPROCESSORS_FIELD_NUMBER: _ClassVar[int]
    MASTER_FIELD_NUMBER: _ClassVar[int]
    BACKUP_MASTERS_FIELD_NUMBER: _ClassVar[int]
    BALANCER_ON_FIELD_NUMBER: _ClassVar[int]
    hbase_version: _FS_pb2.HBaseVersionFileContent
    live_servers: _containers.RepeatedCompositeFieldContainer[LiveServerInfo]
    dead_servers: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.ServerName]
    regions_in_transition: _containers.RepeatedCompositeFieldContainer[RegionInTransition]
    cluster_id: _ClusterId_pb2.ClusterId
    master_coprocessors: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.Coprocessor]
    master: _HBase_pb2.ServerName
    backup_masters: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.ServerName]
    balancer_on: bool
    def __init__(self, hbase_version: _Optional[_Union[_FS_pb2.HBaseVersionFileContent, _Mapping]] = ..., live_servers: _Optional[_Iterable[_Union[LiveServerInfo, _Mapping]]] = ..., dead_servers: _Optional[_Iterable[_Union[_HBase_pb2.ServerName, _Mapping]]] = ..., regions_in_transition: _Optional[_Iterable[_Union[RegionInTransition, _Mapping]]] = ..., cluster_id: _Optional[_Union[_ClusterId_pb2.ClusterId, _Mapping]] = ..., master_coprocessors: _Optional[_Iterable[_Union[_HBase_pb2.Coprocessor, _Mapping]]] = ..., master: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., backup_masters: _Optional[_Iterable[_Union[_HBase_pb2.ServerName, _Mapping]]] = ..., balancer_on: bool = ...) -> None: ...
