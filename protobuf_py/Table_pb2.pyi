from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class TableName(_message.Message):
    __slots__ = ("namespace", "qualifier")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    QUALIFIER_FIELD_NUMBER: _ClassVar[int]
    namespace: bytes
    qualifier: bytes
    def __init__(self, namespace: _Optional[bytes] = ..., qualifier: _Optional[bytes] = ...) -> None: ...
