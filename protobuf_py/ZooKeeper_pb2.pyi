import Table_pb2 as _Table_pb2
import HBase_pb2 as _HBase_pb2
import ClusterStatus_pb2 as _ClusterStatus_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MetaRegionServer(_message.Message):
    __slots__ = ("server", "rpc_version", "state")
    SERVER_FIELD_NUMBER: _ClassVar[int]
    RPC_VERSION_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    server: _HBase_pb2.ServerName
    rpc_version: int
    state: _ClusterStatus_pb2.RegionState.State
    def __init__(self, server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., rpc_version: _Optional[int] = ..., state: _Optional[_Union[_ClusterStatus_pb2.RegionState.State, str]] = ...) -> None: ...

class Master(_message.Message):
    __slots__ = ("master", "rpc_version", "info_port")
    MASTER_FIELD_NUMBER: _ClassVar[int]
    RPC_VERSION_FIELD_NUMBER: _ClassVar[int]
    INFO_PORT_FIELD_NUMBER: _ClassVar[int]
    master: _HBase_pb2.ServerName
    rpc_version: int
    info_port: int
    def __init__(self, master: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., rpc_version: _Optional[int] = ..., info_port: _Optional[int] = ...) -> None: ...

class ClusterUp(_message.Message):
    __slots__ = ("start_date",)
    START_DATE_FIELD_NUMBER: _ClassVar[int]
    start_date: str
    def __init__(self, start_date: _Optional[str] = ...) -> None: ...

class SplitLogTask(_message.Message):
    __slots__ = ("state", "server_name")
    class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        UNASSIGNED: _ClassVar[SplitLogTask.State]
        OWNED: _ClassVar[SplitLogTask.State]
        RESIGNED: _ClassVar[SplitLogTask.State]
        DONE: _ClassVar[SplitLogTask.State]
        ERR: _ClassVar[SplitLogTask.State]
    UNASSIGNED: SplitLogTask.State
    OWNED: SplitLogTask.State
    RESIGNED: SplitLogTask.State
    DONE: SplitLogTask.State
    ERR: SplitLogTask.State
    STATE_FIELD_NUMBER: _ClassVar[int]
    SERVER_NAME_FIELD_NUMBER: _ClassVar[int]
    state: SplitLogTask.State
    server_name: _HBase_pb2.ServerName
    def __init__(self, state: _Optional[_Union[SplitLogTask.State, str]] = ..., server_name: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ...) -> None: ...

class DeprecatedTableState(_message.Message):
    __slots__ = ("state",)
    class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        ENABLED: _ClassVar[DeprecatedTableState.State]
        DISABLED: _ClassVar[DeprecatedTableState.State]
        DISABLING: _ClassVar[DeprecatedTableState.State]
        ENABLING: _ClassVar[DeprecatedTableState.State]
    ENABLED: DeprecatedTableState.State
    DISABLED: DeprecatedTableState.State
    DISABLING: DeprecatedTableState.State
    ENABLING: DeprecatedTableState.State
    STATE_FIELD_NUMBER: _ClassVar[int]
    state: DeprecatedTableState.State
    def __init__(self, state: _Optional[_Union[DeprecatedTableState.State, str]] = ...) -> None: ...

class TableCF(_message.Message):
    __slots__ = ("table_name", "families")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    FAMILIES_FIELD_NUMBER: _ClassVar[int]
    table_name: _Table_pb2.TableName
    families: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, table_name: _Optional[_Union[_Table_pb2.TableName, _Mapping]] = ..., families: _Optional[_Iterable[bytes]] = ...) -> None: ...

class ReplicationPeer(_message.Message):
    __slots__ = ("clusterkey", "replicationEndpointImpl", "data", "configuration", "table_cfs", "namespaces", "bandwidth")
    CLUSTERKEY_FIELD_NUMBER: _ClassVar[int]
    REPLICATIONENDPOINTIMPL_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    CONFIGURATION_FIELD_NUMBER: _ClassVar[int]
    TABLE_CFS_FIELD_NUMBER: _ClassVar[int]
    NAMESPACES_FIELD_NUMBER: _ClassVar[int]
    BANDWIDTH_FIELD_NUMBER: _ClassVar[int]
    clusterkey: str
    replicationEndpointImpl: str
    data: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.BytesBytesPair]
    configuration: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.NameStringPair]
    table_cfs: _containers.RepeatedCompositeFieldContainer[TableCF]
    namespaces: _containers.RepeatedScalarFieldContainer[bytes]
    bandwidth: int
    def __init__(self, clusterkey: _Optional[str] = ..., replicationEndpointImpl: _Optional[str] = ..., data: _Optional[_Iterable[_Union[_HBase_pb2.BytesBytesPair, _Mapping]]] = ..., configuration: _Optional[_Iterable[_Union[_HBase_pb2.NameStringPair, _Mapping]]] = ..., table_cfs: _Optional[_Iterable[_Union[TableCF, _Mapping]]] = ..., namespaces: _Optional[_Iterable[bytes]] = ..., bandwidth: _Optional[int] = ...) -> None: ...

class ReplicationState(_message.Message):
    __slots__ = ("state",)
    class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        ENABLED: _ClassVar[ReplicationState.State]
        DISABLED: _ClassVar[ReplicationState.State]
    ENABLED: ReplicationState.State
    DISABLED: ReplicationState.State
    STATE_FIELD_NUMBER: _ClassVar[int]
    state: ReplicationState.State
    def __init__(self, state: _Optional[_Union[ReplicationState.State, str]] = ...) -> None: ...

class ReplicationHLogPosition(_message.Message):
    __slots__ = ("position",)
    POSITION_FIELD_NUMBER: _ClassVar[int]
    position: int
    def __init__(self, position: _Optional[int] = ...) -> None: ...

class SwitchState(_message.Message):
    __slots__ = ("enabled",)
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    enabled: bool
    def __init__(self, enabled: bool = ...) -> None: ...
