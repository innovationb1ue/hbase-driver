import Client_pb2 as _Client_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class VisibilityLabelsRequest(_message.Message):
    __slots__ = ("visLabel",)
    VISLABEL_FIELD_NUMBER: _ClassVar[int]
    visLabel: _containers.RepeatedCompositeFieldContainer[VisibilityLabel]
    def __init__(self, visLabel: _Optional[_Iterable[_Union[VisibilityLabel, _Mapping]]] = ...) -> None: ...

class VisibilityLabel(_message.Message):
    __slots__ = ("label", "ordinal")
    LABEL_FIELD_NUMBER: _ClassVar[int]
    ORDINAL_FIELD_NUMBER: _ClassVar[int]
    label: bytes
    ordinal: int
    def __init__(self, label: _Optional[bytes] = ..., ordinal: _Optional[int] = ...) -> None: ...

class VisibilityLabelsResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: _containers.RepeatedCompositeFieldContainer[_Client_pb2.RegionActionResult]
    def __init__(self, result: _Optional[_Iterable[_Union[_Client_pb2.RegionActionResult, _Mapping]]] = ...) -> None: ...

class SetAuthsRequest(_message.Message):
    __slots__ = ("user", "auth")
    USER_FIELD_NUMBER: _ClassVar[int]
    AUTH_FIELD_NUMBER: _ClassVar[int]
    user: bytes
    auth: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, user: _Optional[bytes] = ..., auth: _Optional[_Iterable[bytes]] = ...) -> None: ...

class UserAuthorizations(_message.Message):
    __slots__ = ("user", "auth")
    USER_FIELD_NUMBER: _ClassVar[int]
    AUTH_FIELD_NUMBER: _ClassVar[int]
    user: bytes
    auth: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, user: _Optional[bytes] = ..., auth: _Optional[_Iterable[int]] = ...) -> None: ...

class MultiUserAuthorizations(_message.Message):
    __slots__ = ("userAuths",)
    USERAUTHS_FIELD_NUMBER: _ClassVar[int]
    userAuths: _containers.RepeatedCompositeFieldContainer[UserAuthorizations]
    def __init__(self, userAuths: _Optional[_Iterable[_Union[UserAuthorizations, _Mapping]]] = ...) -> None: ...

class GetAuthsRequest(_message.Message):
    __slots__ = ("user",)
    USER_FIELD_NUMBER: _ClassVar[int]
    user: bytes
    def __init__(self, user: _Optional[bytes] = ...) -> None: ...

class GetAuthsResponse(_message.Message):
    __slots__ = ("user", "auth")
    USER_FIELD_NUMBER: _ClassVar[int]
    AUTH_FIELD_NUMBER: _ClassVar[int]
    user: bytes
    auth: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, user: _Optional[bytes] = ..., auth: _Optional[_Iterable[bytes]] = ...) -> None: ...

class ListLabelsRequest(_message.Message):
    __slots__ = ("regex",)
    REGEX_FIELD_NUMBER: _ClassVar[int]
    regex: str
    def __init__(self, regex: _Optional[str] = ...) -> None: ...

class ListLabelsResponse(_message.Message):
    __slots__ = ("label",)
    LABEL_FIELD_NUMBER: _ClassVar[int]
    label: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, label: _Optional[_Iterable[bytes]] = ...) -> None: ...
