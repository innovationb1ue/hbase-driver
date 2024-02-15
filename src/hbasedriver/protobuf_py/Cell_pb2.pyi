from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CellType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MINIMUM: _ClassVar[CellType]
    PUT: _ClassVar[CellType]
    DELETE: _ClassVar[CellType]
    DELETE_FAMILY_VERSION: _ClassVar[CellType]
    DELETE_COLUMN: _ClassVar[CellType]
    DELETE_FAMILY: _ClassVar[CellType]
    MAXIMUM: _ClassVar[CellType]
MINIMUM: CellType
PUT: CellType
DELETE: CellType
DELETE_FAMILY_VERSION: CellType
DELETE_COLUMN: CellType
DELETE_FAMILY: CellType
MAXIMUM: CellType

class Cell(_message.Message):
    __slots__ = ("row", "family", "qualifier", "timestamp", "cell_type", "value", "tags")
    ROW_FIELD_NUMBER: _ClassVar[int]
    FAMILY_FIELD_NUMBER: _ClassVar[int]
    QUALIFIER_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    CELL_TYPE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    row: bytes
    family: bytes
    qualifier: bytes
    timestamp: int
    cell_type: CellType
    value: bytes
    tags: bytes
    def __init__(self, row: _Optional[bytes] = ..., family: _Optional[bytes] = ..., qualifier: _Optional[bytes] = ..., timestamp: _Optional[int] = ..., cell_type: _Optional[_Union[CellType, str]] = ..., value: _Optional[bytes] = ..., tags: _Optional[bytes] = ...) -> None: ...

class KeyValue(_message.Message):
    __slots__ = ("row", "family", "qualifier", "timestamp", "key_type", "value", "tags")
    ROW_FIELD_NUMBER: _ClassVar[int]
    FAMILY_FIELD_NUMBER: _ClassVar[int]
    QUALIFIER_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    KEY_TYPE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    row: bytes
    family: bytes
    qualifier: bytes
    timestamp: int
    key_type: CellType
    value: bytes
    tags: bytes
    def __init__(self, row: _Optional[bytes] = ..., family: _Optional[bytes] = ..., qualifier: _Optional[bytes] = ..., timestamp: _Optional[int] = ..., key_type: _Optional[_Union[CellType, str]] = ..., value: _Optional[bytes] = ..., tags: _Optional[bytes] = ...) -> None: ...
