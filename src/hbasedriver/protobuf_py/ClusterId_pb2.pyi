from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class ClusterId(_message.Message):
    __slots__ = ("cluster_id",)
    CLUSTER_ID_FIELD_NUMBER: _ClassVar[int]
    cluster_id: str
    def __init__(self, cluster_id: _Optional[str] = ...) -> None: ...
