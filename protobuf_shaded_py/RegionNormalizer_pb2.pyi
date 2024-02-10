from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class RegionNormalizerState(_message.Message):
    __slots__ = ("normalizer_on",)
    NORMALIZER_ON_FIELD_NUMBER: _ClassVar[int]
    normalizer_on: bool
    def __init__(self, normalizer_on: bool = ...) -> None: ...
