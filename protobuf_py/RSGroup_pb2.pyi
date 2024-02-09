import Table_pb2 as _Table_pb2
import HBase_pb2 as _HBase_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RSGroupInfo(_message.Message):
    __slots__ = ("name", "servers", "tables", "configuration")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SERVERS_FIELD_NUMBER: _ClassVar[int]
    TABLES_FIELD_NUMBER: _ClassVar[int]
    CONFIGURATION_FIELD_NUMBER: _ClassVar[int]
    name: str
    servers: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.ServerName]
    tables: _containers.RepeatedCompositeFieldContainer[_Table_pb2.TableName]
    configuration: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.NameStringPair]
    def __init__(self, name: _Optional[str] = ..., servers: _Optional[_Iterable[_Union[_HBase_pb2.ServerName, _Mapping]]] = ..., tables: _Optional[_Iterable[_Union[_Table_pb2.TableName, _Mapping]]] = ..., configuration: _Optional[_Iterable[_Union[_HBase_pb2.NameStringPair, _Mapping]]] = ...) -> None: ...
