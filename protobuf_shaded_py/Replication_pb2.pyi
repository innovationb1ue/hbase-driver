import HBase_pb2 as _HBase_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TableCF(_message.Message):
    __slots__ = ("table_name", "families")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    FAMILIES_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    families: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., families: _Optional[_Iterable[bytes]] = ...) -> None: ...

class ReplicationPeer(_message.Message):
    __slots__ = ("clusterkey", "replicationEndpointImpl", "data", "configuration", "table_cfs", "namespaces", "bandwidth", "replicate_all", "exclude_table_cfs", "exclude_namespaces", "serial")
    CLUSTERKEY_FIELD_NUMBER: _ClassVar[int]
    REPLICATIONENDPOINTIMPL_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    CONFIGURATION_FIELD_NUMBER: _ClassVar[int]
    TABLE_CFS_FIELD_NUMBER: _ClassVar[int]
    NAMESPACES_FIELD_NUMBER: _ClassVar[int]
    BANDWIDTH_FIELD_NUMBER: _ClassVar[int]
    REPLICATE_ALL_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_TABLE_CFS_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_NAMESPACES_FIELD_NUMBER: _ClassVar[int]
    SERIAL_FIELD_NUMBER: _ClassVar[int]
    clusterkey: str
    replicationEndpointImpl: str
    data: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.BytesBytesPair]
    configuration: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.NameStringPair]
    table_cfs: _containers.RepeatedCompositeFieldContainer[TableCF]
    namespaces: _containers.RepeatedScalarFieldContainer[bytes]
    bandwidth: int
    replicate_all: bool
    exclude_table_cfs: _containers.RepeatedCompositeFieldContainer[TableCF]
    exclude_namespaces: _containers.RepeatedScalarFieldContainer[bytes]
    serial: bool
    def __init__(self, clusterkey: _Optional[str] = ..., replicationEndpointImpl: _Optional[str] = ..., data: _Optional[_Iterable[_Union[_HBase_pb2.BytesBytesPair, _Mapping]]] = ..., configuration: _Optional[_Iterable[_Union[_HBase_pb2.NameStringPair, _Mapping]]] = ..., table_cfs: _Optional[_Iterable[_Union[TableCF, _Mapping]]] = ..., namespaces: _Optional[_Iterable[bytes]] = ..., bandwidth: _Optional[int] = ..., replicate_all: bool = ..., exclude_table_cfs: _Optional[_Iterable[_Union[TableCF, _Mapping]]] = ..., exclude_namespaces: _Optional[_Iterable[bytes]] = ..., serial: bool = ...) -> None: ...

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

class ReplicationPeerDescription(_message.Message):
    __slots__ = ("id", "state", "config")
    ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    id: str
    state: ReplicationState
    config: ReplicationPeer
    def __init__(self, id: _Optional[str] = ..., state: _Optional[_Union[ReplicationState, _Mapping]] = ..., config: _Optional[_Union[ReplicationPeer, _Mapping]] = ...) -> None: ...

class ReplicationHLogPosition(_message.Message):
    __slots__ = ("position",)
    POSITION_FIELD_NUMBER: _ClassVar[int]
    position: int
    def __init__(self, position: _Optional[int] = ...) -> None: ...

class AddReplicationPeerRequest(_message.Message):
    __slots__ = ("peer_id", "peer_config", "peer_state")
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    PEER_CONFIG_FIELD_NUMBER: _ClassVar[int]
    PEER_STATE_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    peer_config: ReplicationPeer
    peer_state: ReplicationState
    def __init__(self, peer_id: _Optional[str] = ..., peer_config: _Optional[_Union[ReplicationPeer, _Mapping]] = ..., peer_state: _Optional[_Union[ReplicationState, _Mapping]] = ...) -> None: ...

class AddReplicationPeerResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class RemoveReplicationPeerRequest(_message.Message):
    __slots__ = ("peer_id",)
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    def __init__(self, peer_id: _Optional[str] = ...) -> None: ...

class RemoveReplicationPeerResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class EnableReplicationPeerRequest(_message.Message):
    __slots__ = ("peer_id",)
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    def __init__(self, peer_id: _Optional[str] = ...) -> None: ...

class EnableReplicationPeerResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class DisableReplicationPeerRequest(_message.Message):
    __slots__ = ("peer_id",)
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    def __init__(self, peer_id: _Optional[str] = ...) -> None: ...

class DisableReplicationPeerResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class GetReplicationPeerConfigRequest(_message.Message):
    __slots__ = ("peer_id",)
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    def __init__(self, peer_id: _Optional[str] = ...) -> None: ...

class GetReplicationPeerConfigResponse(_message.Message):
    __slots__ = ("peer_id", "peer_config")
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    PEER_CONFIG_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    peer_config: ReplicationPeer
    def __init__(self, peer_id: _Optional[str] = ..., peer_config: _Optional[_Union[ReplicationPeer, _Mapping]] = ...) -> None: ...

class UpdateReplicationPeerConfigRequest(_message.Message):
    __slots__ = ("peer_id", "peer_config")
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    PEER_CONFIG_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    peer_config: ReplicationPeer
    def __init__(self, peer_id: _Optional[str] = ..., peer_config: _Optional[_Union[ReplicationPeer, _Mapping]] = ...) -> None: ...

class UpdateReplicationPeerConfigResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class ListReplicationPeersRequest(_message.Message):
    __slots__ = ("regex",)
    REGEX_FIELD_NUMBER: _ClassVar[int]
    regex: str
    def __init__(self, regex: _Optional[str] = ...) -> None: ...

class ListReplicationPeersResponse(_message.Message):
    __slots__ = ("peer_desc",)
    PEER_DESC_FIELD_NUMBER: _ClassVar[int]
    peer_desc: _containers.RepeatedCompositeFieldContainer[ReplicationPeerDescription]
    def __init__(self, peer_desc: _Optional[_Iterable[_Union[ReplicationPeerDescription, _Mapping]]] = ...) -> None: ...
