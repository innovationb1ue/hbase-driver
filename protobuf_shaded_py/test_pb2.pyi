from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class EmptyRequestProto(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class EmptyResponseProto(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class EchoRequestProto(_message.Message):
    __slots__ = ("message",)
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    message: str
    def __init__(self, message: _Optional[str] = ...) -> None: ...

class EchoResponseProto(_message.Message):
    __slots__ = ("message",)
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    message: str
    def __init__(self, message: _Optional[str] = ...) -> None: ...

class PauseRequestProto(_message.Message):
    __slots__ = ("ms",)
    MS_FIELD_NUMBER: _ClassVar[int]
    ms: int
    def __init__(self, ms: _Optional[int] = ...) -> None: ...

class AddrResponseProto(_message.Message):
    __slots__ = ("addr",)
    ADDR_FIELD_NUMBER: _ClassVar[int]
    addr: str
    def __init__(self, addr: _Optional[str] = ...) -> None: ...
