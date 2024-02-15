from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class WrappedKey(_message.Message):
    __slots__ = ("algorithm", "length", "data", "iv", "hash", "hash_algorithm")
    ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    IV_FIELD_NUMBER: _ClassVar[int]
    HASH_FIELD_NUMBER: _ClassVar[int]
    HASH_ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    algorithm: str
    length: int
    data: bytes
    iv: bytes
    hash: bytes
    hash_algorithm: str
    def __init__(self, algorithm: _Optional[str] = ..., length: _Optional[int] = ..., data: _Optional[bytes] = ..., iv: _Optional[bytes] = ..., hash: _Optional[bytes] = ..., hash_algorithm: _Optional[str] = ...) -> None: ...
