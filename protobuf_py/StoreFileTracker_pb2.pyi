from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class StoreFileEntry(_message.Message):
    __slots__ = ("name", "size")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    name: str
    size: int
    def __init__(self, name: _Optional[str] = ..., size: _Optional[int] = ...) -> None: ...

class StoreFileList(_message.Message):
    __slots__ = ("timestamp", "store_file")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    STORE_FILE_FIELD_NUMBER: _ClassVar[int]
    timestamp: int
    store_file: _containers.RepeatedCompositeFieldContainer[StoreFileEntry]
    def __init__(self, timestamp: _Optional[int] = ..., store_file: _Optional[_Iterable[_Union[StoreFileEntry, _Mapping]]] = ...) -> None: ...
