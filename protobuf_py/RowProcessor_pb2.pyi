from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class ProcessRequest(_message.Message):
    __slots__ = ("row_processor_class_name", "row_processor_initializer_message_name", "row_processor_initializer_message", "nonce_group", "nonce")
    ROW_PROCESSOR_CLASS_NAME_FIELD_NUMBER: _ClassVar[int]
    ROW_PROCESSOR_INITIALIZER_MESSAGE_NAME_FIELD_NUMBER: _ClassVar[int]
    ROW_PROCESSOR_INITIALIZER_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    row_processor_class_name: str
    row_processor_initializer_message_name: str
    row_processor_initializer_message: bytes
    nonce_group: int
    nonce: int
    def __init__(self, row_processor_class_name: _Optional[str] = ..., row_processor_initializer_message_name: _Optional[str] = ..., row_processor_initializer_message: _Optional[bytes] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class ProcessResponse(_message.Message):
    __slots__ = ("row_processor_result",)
    ROW_PROCESSOR_RESULT_FIELD_NUMBER: _ClassVar[int]
    row_processor_result: bytes
    def __init__(self, row_processor_result: _Optional[bytes] = ...) -> None: ...
