import HBase_pb2 as _HBase_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetClusterIdRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetClusterIdResponse(_message.Message):
    __slots__ = ("cluster_id",)
    CLUSTER_ID_FIELD_NUMBER: _ClassVar[int]
    cluster_id: str
    def __init__(self, cluster_id: _Optional[str] = ...) -> None: ...

class GetActiveMasterRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetActiveMasterResponse(_message.Message):
    __slots__ = ("server_name",)
    SERVER_NAME_FIELD_NUMBER: _ClassVar[int]
    server_name: _HBase_pb2.ServerName
    def __init__(self, server_name: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ...) -> None: ...

class GetMastersRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetMastersResponseEntry(_message.Message):
    __slots__ = ("server_name", "is_active")
    SERVER_NAME_FIELD_NUMBER: _ClassVar[int]
    IS_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    server_name: _HBase_pb2.ServerName
    is_active: bool
    def __init__(self, server_name: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., is_active: bool = ...) -> None: ...

class GetMastersResponse(_message.Message):
    __slots__ = ("master_servers",)
    MASTER_SERVERS_FIELD_NUMBER: _ClassVar[int]
    master_servers: _containers.RepeatedCompositeFieldContainer[GetMastersResponseEntry]
    def __init__(self, master_servers: _Optional[_Iterable[_Union[GetMastersResponseEntry, _Mapping]]] = ...) -> None: ...

class GetMetaRegionLocationsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetMetaRegionLocationsResponse(_message.Message):
    __slots__ = ("meta_locations",)
    META_LOCATIONS_FIELD_NUMBER: _ClassVar[int]
    meta_locations: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionLocation]
    def __init__(self, meta_locations: _Optional[_Iterable[_Union[_HBase_pb2.RegionLocation, _Mapping]]] = ...) -> None: ...

class GetBootstrapNodesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetBootstrapNodesResponse(_message.Message):
    __slots__ = ("server_name",)
    SERVER_NAME_FIELD_NUMBER: _ClassVar[int]
    server_name: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.ServerName]
    def __init__(self, server_name: _Optional[_Iterable[_Union[_HBase_pb2.ServerName, _Mapping]]] = ...) -> None: ...
