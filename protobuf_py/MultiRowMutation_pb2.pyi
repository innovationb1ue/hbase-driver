import Client_pb2 as _Client_pb2
import HBase_pb2 as _HBase_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MultiRowMutationProcessorRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class MultiRowMutationProcessorResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class MutateRowsRequest(_message.Message):
    __slots__ = ("mutation_request", "nonce_group", "nonce", "region", "condition")
    MUTATION_REQUEST_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    mutation_request: _containers.RepeatedCompositeFieldContainer[_Client_pb2.MutationProto]
    nonce_group: int
    nonce: int
    region: _HBase_pb2.RegionSpecifier
    condition: _containers.RepeatedCompositeFieldContainer[_Client_pb2.Condition]
    def __init__(self, mutation_request: _Optional[_Iterable[_Union[_Client_pb2.MutationProto, _Mapping]]] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ..., region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., condition: _Optional[_Iterable[_Union[_Client_pb2.Condition, _Mapping]]] = ...) -> None: ...

class MutateRowsResponse(_message.Message):
    __slots__ = ("processed",)
    PROCESSED_FIELD_NUMBER: _ClassVar[int]
    processed: bool
    def __init__(self, processed: bool = ...) -> None: ...
