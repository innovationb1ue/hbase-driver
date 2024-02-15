import HBase_pb2 as _HBase_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ScanMetrics(_message.Message):
    __slots__ = ("metrics",)
    METRICS_FIELD_NUMBER: _ClassVar[int]
    metrics: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.NameInt64Pair]
    def __init__(self, metrics: _Optional[_Iterable[_Union[_HBase_pb2.NameInt64Pair, _Mapping]]] = ...) -> None: ...

class TableSnapshotRegionSplit(_message.Message):
    __slots__ = ("locations", "table", "region")
    LOCATIONS_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    locations: _containers.RepeatedScalarFieldContainer[str]
    table: _HBase_pb2.TableSchema
    region: _HBase_pb2.RegionInfo
    def __init__(self, locations: _Optional[_Iterable[str]] = ..., table: _Optional[_Union[_HBase_pb2.TableSchema, _Mapping]] = ..., region: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ...) -> None: ...
