from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class TestTableDDLStateData(_message.Message):
    __slots__ = ("table_name",)
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    def __init__(self, table_name: _Optional[str] = ...) -> None: ...
