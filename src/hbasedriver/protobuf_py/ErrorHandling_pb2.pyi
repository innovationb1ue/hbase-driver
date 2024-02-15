from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class StackTraceElementMessage(_message.Message):
    __slots__ = ("declaring_class", "method_name", "file_name", "line_number")
    DECLARING_CLASS_FIELD_NUMBER: _ClassVar[int]
    METHOD_NAME_FIELD_NUMBER: _ClassVar[int]
    FILE_NAME_FIELD_NUMBER: _ClassVar[int]
    LINE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    declaring_class: str
    method_name: str
    file_name: str
    line_number: int
    def __init__(self, declaring_class: _Optional[str] = ..., method_name: _Optional[str] = ..., file_name: _Optional[str] = ..., line_number: _Optional[int] = ...) -> None: ...

class GenericExceptionMessage(_message.Message):
    __slots__ = ("class_name", "message", "error_info", "trace")
    CLASS_NAME_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_INFO_FIELD_NUMBER: _ClassVar[int]
    TRACE_FIELD_NUMBER: _ClassVar[int]
    class_name: str
    message: str
    error_info: bytes
    trace: _containers.RepeatedCompositeFieldContainer[StackTraceElementMessage]
    def __init__(self, class_name: _Optional[str] = ..., message: _Optional[str] = ..., error_info: _Optional[bytes] = ..., trace: _Optional[_Iterable[_Union[StackTraceElementMessage, _Mapping]]] = ...) -> None: ...

class ForeignExceptionMessage(_message.Message):
    __slots__ = ("source", "generic_exception")
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    GENERIC_EXCEPTION_FIELD_NUMBER: _ClassVar[int]
    source: str
    generic_exception: GenericExceptionMessage
    def __init__(self, source: _Optional[str] = ..., generic_exception: _Optional[_Union[GenericExceptionMessage, _Mapping]] = ...) -> None: ...
