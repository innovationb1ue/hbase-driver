from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SlowLogPayload(_message.Message):
    __slots__ = ("start_time", "processing_time", "queue_time", "response_size", "client_address", "server_class", "method_name", "call_details", "param", "user_name", "region_name", "multi_gets", "multi_mutations", "multi_service_calls", "type")
    class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SLOW_LOG: _ClassVar[SlowLogPayload.Type]
        LARGE_LOG: _ClassVar[SlowLogPayload.Type]
        ALL: _ClassVar[SlowLogPayload.Type]
    SLOW_LOG: SlowLogPayload.Type
    LARGE_LOG: SlowLogPayload.Type
    ALL: SlowLogPayload.Type
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    PROCESSING_TIME_FIELD_NUMBER: _ClassVar[int]
    QUEUE_TIME_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_SIZE_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    SERVER_CLASS_FIELD_NUMBER: _ClassVar[int]
    METHOD_NAME_FIELD_NUMBER: _ClassVar[int]
    CALL_DETAILS_FIELD_NUMBER: _ClassVar[int]
    PARAM_FIELD_NUMBER: _ClassVar[int]
    USER_NAME_FIELD_NUMBER: _ClassVar[int]
    REGION_NAME_FIELD_NUMBER: _ClassVar[int]
    MULTI_GETS_FIELD_NUMBER: _ClassVar[int]
    MULTI_MUTATIONS_FIELD_NUMBER: _ClassVar[int]
    MULTI_SERVICE_CALLS_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    start_time: int
    processing_time: int
    queue_time: int
    response_size: int
    client_address: str
    server_class: str
    method_name: str
    call_details: str
    param: str
    user_name: str
    region_name: str
    multi_gets: int
    multi_mutations: int
    multi_service_calls: int
    type: SlowLogPayload.Type
    def __init__(self, start_time: _Optional[int] = ..., processing_time: _Optional[int] = ..., queue_time: _Optional[int] = ..., response_size: _Optional[int] = ..., client_address: _Optional[str] = ..., server_class: _Optional[str] = ..., method_name: _Optional[str] = ..., call_details: _Optional[str] = ..., param: _Optional[str] = ..., user_name: _Optional[str] = ..., region_name: _Optional[str] = ..., multi_gets: _Optional[int] = ..., multi_mutations: _Optional[int] = ..., multi_service_calls: _Optional[int] = ..., type: _Optional[_Union[SlowLogPayload.Type, str]] = ...) -> None: ...
