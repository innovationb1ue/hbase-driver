from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class SnapshotCleanupState(_message.Message):
    __slots__ = ("snapshot_cleanup_enabled",)
    SNAPSHOT_CLEANUP_ENABLED_FIELD_NUMBER: _ClassVar[int]
    snapshot_cleanup_enabled: bool
    def __init__(self, snapshot_cleanup_enabled: bool = ...) -> None: ...
