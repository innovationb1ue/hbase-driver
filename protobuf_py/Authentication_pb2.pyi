from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AuthenticationKey(_message.Message):
    __slots__ = ("id", "expiration_date", "key")
    ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRATION_DATE_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    id: int
    expiration_date: int
    key: bytes
    def __init__(self, id: _Optional[int] = ..., expiration_date: _Optional[int] = ..., key: _Optional[bytes] = ...) -> None: ...

class TokenIdentifier(_message.Message):
    __slots__ = ("kind", "username", "key_id", "issue_date", "expiration_date", "sequence_number")
    class Kind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        HBASE_AUTH_TOKEN: _ClassVar[TokenIdentifier.Kind]
    HBASE_AUTH_TOKEN: TokenIdentifier.Kind
    KIND_FIELD_NUMBER: _ClassVar[int]
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    ISSUE_DATE_FIELD_NUMBER: _ClassVar[int]
    EXPIRATION_DATE_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    kind: TokenIdentifier.Kind
    username: bytes
    key_id: int
    issue_date: int
    expiration_date: int
    sequence_number: int
    def __init__(self, kind: _Optional[_Union[TokenIdentifier.Kind, str]] = ..., username: _Optional[bytes] = ..., key_id: _Optional[int] = ..., issue_date: _Optional[int] = ..., expiration_date: _Optional[int] = ..., sequence_number: _Optional[int] = ...) -> None: ...

class Token(_message.Message):
    __slots__ = ("identifier", "password", "service")
    IDENTIFIER_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    identifier: bytes
    password: bytes
    service: bytes
    def __init__(self, identifier: _Optional[bytes] = ..., password: _Optional[bytes] = ..., service: _Optional[bytes] = ...) -> None: ...

class GetAuthenticationTokenRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetAuthenticationTokenResponse(_message.Message):
    __slots__ = ("token",)
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    token: Token
    def __init__(self, token: _Optional[_Union[Token, _Mapping]] = ...) -> None: ...

class WhoAmIRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class WhoAmIResponse(_message.Message):
    __slots__ = ("username", "auth_method")
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    AUTH_METHOD_FIELD_NUMBER: _ClassVar[int]
    username: str
    auth_method: str
    def __init__(self, username: _Optional[str] = ..., auth_method: _Optional[str] = ...) -> None: ...
