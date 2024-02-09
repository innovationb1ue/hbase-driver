from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class PingRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PingResponse(_message.Message):
    __slots__ = ("pong",)
    PONG_FIELD_NUMBER: _ClassVar[int]
    pong: str
    def __init__(self, pong: _Optional[str] = ...) -> None: ...

class CountRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CountResponse(_message.Message):
    __slots__ = ("count",)
    COUNT_FIELD_NUMBER: _ClassVar[int]
    count: int
    def __init__(self, count: _Optional[int] = ...) -> None: ...

class IncrementCountRequest(_message.Message):
    __slots__ = ("diff",)
    DIFF_FIELD_NUMBER: _ClassVar[int]
    diff: int
    def __init__(self, diff: _Optional[int] = ...) -> None: ...

class IncrementCountResponse(_message.Message):
    __slots__ = ("count",)
    COUNT_FIELD_NUMBER: _ClassVar[int]
    count: int
    def __init__(self, count: _Optional[int] = ...) -> None: ...

class HelloRequest(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class HelloResponse(_message.Message):
    __slots__ = ("response",)
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    response: str
    def __init__(self, response: _Optional[str] = ...) -> None: ...

class NoopRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class NoopResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
