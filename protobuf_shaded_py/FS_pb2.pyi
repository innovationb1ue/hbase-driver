from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class HBaseVersionFileContent(_message.Message):
    __slots__ = ("version",)
    VERSION_FIELD_NUMBER: _ClassVar[int]
    version: str
    def __init__(self, version: _Optional[str] = ...) -> None: ...

class Reference(_message.Message):
    __slots__ = ("splitkey", "range")
    class Range(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        TOP: _ClassVar[Reference.Range]
        BOTTOM: _ClassVar[Reference.Range]
    TOP: Reference.Range
    BOTTOM: Reference.Range
    SPLITKEY_FIELD_NUMBER: _ClassVar[int]
    RANGE_FIELD_NUMBER: _ClassVar[int]
    splitkey: bytes
    range: Reference.Range
    def __init__(self, splitkey: _Optional[bytes] = ..., range: _Optional[_Union[Reference.Range, str]] = ...) -> None: ...
