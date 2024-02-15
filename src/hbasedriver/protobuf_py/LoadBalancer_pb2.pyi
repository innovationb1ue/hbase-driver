from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class LoadBalancerState(_message.Message):
    __slots__ = ("balancer_on",)
    BALANCER_ON_FIELD_NUMBER: _ClassVar[int]
    balancer_on: bool
    def __init__(self, balancer_on: bool = ...) -> None: ...
