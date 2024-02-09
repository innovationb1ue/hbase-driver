from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class RPCTInfo(_message.Message):
    __slots__ = ("trace_id", "parent_id")
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    trace_id: int
    parent_id: int
    def __init__(self, trace_id: _Optional[int] = ..., parent_id: _Optional[int] = ...) -> None: ...
