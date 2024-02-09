import HBase_pb2 as _HBase_pb2
import WAL_pb2 as _WAL_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetRegionInfoRequest(_message.Message):
    __slots__ = ("region", "compaction_state")
    REGION_FIELD_NUMBER: _ClassVar[int]
    COMPACTION_STATE_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    compaction_state: bool
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., compaction_state: bool = ...) -> None: ...

class GetRegionInfoResponse(_message.Message):
    __slots__ = ("region_info", "compaction_state")
    class CompactionState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        NONE: _ClassVar[GetRegionInfoResponse.CompactionState]
        MINOR: _ClassVar[GetRegionInfoResponse.CompactionState]
        MAJOR: _ClassVar[GetRegionInfoResponse.CompactionState]
        MAJOR_AND_MINOR: _ClassVar[GetRegionInfoResponse.CompactionState]
    NONE: GetRegionInfoResponse.CompactionState
    MINOR: GetRegionInfoResponse.CompactionState
    MAJOR: GetRegionInfoResponse.CompactionState
    MAJOR_AND_MINOR: GetRegionInfoResponse.CompactionState
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    COMPACTION_STATE_FIELD_NUMBER: _ClassVar[int]
    region_info: _HBase_pb2.RegionInfo
    compaction_state: GetRegionInfoResponse.CompactionState
    def __init__(self, region_info: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ..., compaction_state: _Optional[_Union[GetRegionInfoResponse.CompactionState, str]] = ...) -> None: ...

class GetStoreFileRequest(_message.Message):
    __slots__ = ("region", "family")
    REGION_FIELD_NUMBER: _ClassVar[int]
    FAMILY_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    family: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., family: _Optional[_Iterable[bytes]] = ...) -> None: ...

class GetStoreFileResponse(_message.Message):
    __slots__ = ("store_file",)
    STORE_FILE_FIELD_NUMBER: _ClassVar[int]
    store_file: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, store_file: _Optional[_Iterable[str]] = ...) -> None: ...

class GetOnlineRegionRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetOnlineRegionResponse(_message.Message):
    __slots__ = ("region_info",)
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    region_info: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    def __init__(self, region_info: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ...) -> None: ...

class OpenRegionRequest(_message.Message):
    __slots__ = ("open_info", "serverStartCode", "master_system_time")
    class RegionOpenInfo(_message.Message):
        __slots__ = ("region", "version_of_offline_node", "favored_nodes")
        REGION_FIELD_NUMBER: _ClassVar[int]
        VERSION_OF_OFFLINE_NODE_FIELD_NUMBER: _ClassVar[int]
        FAVORED_NODES_FIELD_NUMBER: _ClassVar[int]
        region: _HBase_pb2.RegionInfo
        version_of_offline_node: int
        favored_nodes: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.ServerName]
        def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ..., version_of_offline_node: _Optional[int] = ..., favored_nodes: _Optional[_Iterable[_Union[_HBase_pb2.ServerName, _Mapping]]] = ...) -> None: ...
    OPEN_INFO_FIELD_NUMBER: _ClassVar[int]
    SERVERSTARTCODE_FIELD_NUMBER: _ClassVar[int]
    MASTER_SYSTEM_TIME_FIELD_NUMBER: _ClassVar[int]
    open_info: _containers.RepeatedCompositeFieldContainer[OpenRegionRequest.RegionOpenInfo]
    serverStartCode: int
    master_system_time: int
    def __init__(self, open_info: _Optional[_Iterable[_Union[OpenRegionRequest.RegionOpenInfo, _Mapping]]] = ..., serverStartCode: _Optional[int] = ..., master_system_time: _Optional[int] = ...) -> None: ...

class OpenRegionResponse(_message.Message):
    __slots__ = ("opening_state",)
    class RegionOpeningState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        OPENED: _ClassVar[OpenRegionResponse.RegionOpeningState]
        ALREADY_OPENED: _ClassVar[OpenRegionResponse.RegionOpeningState]
        FAILED_OPENING: _ClassVar[OpenRegionResponse.RegionOpeningState]
    OPENED: OpenRegionResponse.RegionOpeningState
    ALREADY_OPENED: OpenRegionResponse.RegionOpeningState
    FAILED_OPENING: OpenRegionResponse.RegionOpeningState
    OPENING_STATE_FIELD_NUMBER: _ClassVar[int]
    opening_state: _containers.RepeatedScalarFieldContainer[OpenRegionResponse.RegionOpeningState]
    def __init__(self, opening_state: _Optional[_Iterable[_Union[OpenRegionResponse.RegionOpeningState, str]]] = ...) -> None: ...

class WarmupRegionRequest(_message.Message):
    __slots__ = ("regionInfo",)
    REGIONINFO_FIELD_NUMBER: _ClassVar[int]
    regionInfo: _HBase_pb2.RegionInfo
    def __init__(self, regionInfo: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ...) -> None: ...

class WarmupRegionResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CloseRegionRequest(_message.Message):
    __slots__ = ("region", "version_of_closing_node", "transition_in_ZK", "destination_server", "serverStartCode")
    REGION_FIELD_NUMBER: _ClassVar[int]
    VERSION_OF_CLOSING_NODE_FIELD_NUMBER: _ClassVar[int]
    TRANSITION_IN_ZK_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_SERVER_FIELD_NUMBER: _ClassVar[int]
    SERVERSTARTCODE_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    version_of_closing_node: int
    transition_in_ZK: bool
    destination_server: _HBase_pb2.ServerName
    serverStartCode: int
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., version_of_closing_node: _Optional[int] = ..., transition_in_ZK: bool = ..., destination_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., serverStartCode: _Optional[int] = ...) -> None: ...

class CloseRegionResponse(_message.Message):
    __slots__ = ("closed",)
    CLOSED_FIELD_NUMBER: _ClassVar[int]
    closed: bool
    def __init__(self, closed: bool = ...) -> None: ...

class FlushRegionRequest(_message.Message):
    __slots__ = ("region", "if_older_than_ts", "write_flush_wal_marker", "family")
    REGION_FIELD_NUMBER: _ClassVar[int]
    IF_OLDER_THAN_TS_FIELD_NUMBER: _ClassVar[int]
    WRITE_FLUSH_WAL_MARKER_FIELD_NUMBER: _ClassVar[int]
    FAMILY_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    if_older_than_ts: int
    write_flush_wal_marker: bool
    family: bytes
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., if_older_than_ts: _Optional[int] = ..., write_flush_wal_marker: bool = ..., family: _Optional[bytes] = ...) -> None: ...

class FlushRegionResponse(_message.Message):
    __slots__ = ("last_flush_time", "flushed", "wrote_flush_wal_marker")
    LAST_FLUSH_TIME_FIELD_NUMBER: _ClassVar[int]
    FLUSHED_FIELD_NUMBER: _ClassVar[int]
    WROTE_FLUSH_WAL_MARKER_FIELD_NUMBER: _ClassVar[int]
    last_flush_time: int
    flushed: bool
    wrote_flush_wal_marker: bool
    def __init__(self, last_flush_time: _Optional[int] = ..., flushed: bool = ..., wrote_flush_wal_marker: bool = ...) -> None: ...

class SplitRegionRequest(_message.Message):
    __slots__ = ("region", "split_point")
    REGION_FIELD_NUMBER: _ClassVar[int]
    SPLIT_POINT_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    split_point: bytes
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., split_point: _Optional[bytes] = ...) -> None: ...

class SplitRegionResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CompactRegionRequest(_message.Message):
    __slots__ = ("region", "major", "family")
    REGION_FIELD_NUMBER: _ClassVar[int]
    MAJOR_FIELD_NUMBER: _ClassVar[int]
    FAMILY_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    major: bool
    family: bytes
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., major: bool = ..., family: _Optional[bytes] = ...) -> None: ...

class CompactRegionResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class UpdateFavoredNodesRequest(_message.Message):
    __slots__ = ("update_info",)
    class RegionUpdateInfo(_message.Message):
        __slots__ = ("region", "favored_nodes")
        REGION_FIELD_NUMBER: _ClassVar[int]
        FAVORED_NODES_FIELD_NUMBER: _ClassVar[int]
        region: _HBase_pb2.RegionInfo
        favored_nodes: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.ServerName]
        def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ..., favored_nodes: _Optional[_Iterable[_Union[_HBase_pb2.ServerName, _Mapping]]] = ...) -> None: ...
    UPDATE_INFO_FIELD_NUMBER: _ClassVar[int]
    update_info: _containers.RepeatedCompositeFieldContainer[UpdateFavoredNodesRequest.RegionUpdateInfo]
    def __init__(self, update_info: _Optional[_Iterable[_Union[UpdateFavoredNodesRequest.RegionUpdateInfo, _Mapping]]] = ...) -> None: ...

class UpdateFavoredNodesResponse(_message.Message):
    __slots__ = ("response",)
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    response: int
    def __init__(self, response: _Optional[int] = ...) -> None: ...

class MergeRegionsRequest(_message.Message):
    __slots__ = ("region_a", "region_b", "forcible", "master_system_time")
    REGION_A_FIELD_NUMBER: _ClassVar[int]
    REGION_B_FIELD_NUMBER: _ClassVar[int]
    FORCIBLE_FIELD_NUMBER: _ClassVar[int]
    MASTER_SYSTEM_TIME_FIELD_NUMBER: _ClassVar[int]
    region_a: _HBase_pb2.RegionSpecifier
    region_b: _HBase_pb2.RegionSpecifier
    forcible: bool
    master_system_time: int
    def __init__(self, region_a: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., region_b: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., forcible: bool = ..., master_system_time: _Optional[int] = ...) -> None: ...

class MergeRegionsResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class WALEntry(_message.Message):
    __slots__ = ("key", "key_value_bytes", "associated_cell_count")
    KEY_FIELD_NUMBER: _ClassVar[int]
    KEY_VALUE_BYTES_FIELD_NUMBER: _ClassVar[int]
    ASSOCIATED_CELL_COUNT_FIELD_NUMBER: _ClassVar[int]
    key: _WAL_pb2.WALKey
    key_value_bytes: _containers.RepeatedScalarFieldContainer[bytes]
    associated_cell_count: int
    def __init__(self, key: _Optional[_Union[_WAL_pb2.WALKey, _Mapping]] = ..., key_value_bytes: _Optional[_Iterable[bytes]] = ..., associated_cell_count: _Optional[int] = ...) -> None: ...

class ReplicateWALEntryRequest(_message.Message):
    __slots__ = ("entry", "replicationClusterId", "sourceBaseNamespaceDirPath", "sourceHFileArchiveDirPath")
    ENTRY_FIELD_NUMBER: _ClassVar[int]
    REPLICATIONCLUSTERID_FIELD_NUMBER: _ClassVar[int]
    SOURCEBASENAMESPACEDIRPATH_FIELD_NUMBER: _ClassVar[int]
    SOURCEHFILEARCHIVEDIRPATH_FIELD_NUMBER: _ClassVar[int]
    entry: _containers.RepeatedCompositeFieldContainer[WALEntry]
    replicationClusterId: str
    sourceBaseNamespaceDirPath: str
    sourceHFileArchiveDirPath: str
    def __init__(self, entry: _Optional[_Iterable[_Union[WALEntry, _Mapping]]] = ..., replicationClusterId: _Optional[str] = ..., sourceBaseNamespaceDirPath: _Optional[str] = ..., sourceHFileArchiveDirPath: _Optional[str] = ...) -> None: ...

class ReplicateWALEntryResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RollWALWriterRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RollWALWriterResponse(_message.Message):
    __slots__ = ("region_to_flush",)
    REGION_TO_FLUSH_FIELD_NUMBER: _ClassVar[int]
    region_to_flush: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, region_to_flush: _Optional[_Iterable[bytes]] = ...) -> None: ...

class StopServerRequest(_message.Message):
    __slots__ = ("reason",)
    REASON_FIELD_NUMBER: _ClassVar[int]
    reason: str
    def __init__(self, reason: _Optional[str] = ...) -> None: ...

class StopServerResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetServerInfoRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ServerInfo(_message.Message):
    __slots__ = ("server_name", "webui_port")
    SERVER_NAME_FIELD_NUMBER: _ClassVar[int]
    WEBUI_PORT_FIELD_NUMBER: _ClassVar[int]
    server_name: _HBase_pb2.ServerName
    webui_port: int
    def __init__(self, server_name: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., webui_port: _Optional[int] = ...) -> None: ...

class GetServerInfoResponse(_message.Message):
    __slots__ = ("server_info",)
    SERVER_INFO_FIELD_NUMBER: _ClassVar[int]
    server_info: ServerInfo
    def __init__(self, server_info: _Optional[_Union[ServerInfo, _Mapping]] = ...) -> None: ...

class UpdateConfigurationRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class UpdateConfigurationResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
