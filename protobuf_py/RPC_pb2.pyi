import Tracing_pb2 as _Tracing_pb2
import HBase_pb2 as _HBase_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class UserInformation(_message.Message):
    __slots__ = ("effective_user", "real_user")
    EFFECTIVE_USER_FIELD_NUMBER: _ClassVar[int]
    REAL_USER_FIELD_NUMBER: _ClassVar[int]
    effective_user: str
    real_user: str
    def __init__(self, effective_user: _Optional[str] = ..., real_user: _Optional[str] = ...) -> None: ...

class ConnectionHeader(_message.Message):
    __slots__ = ("user_info", "service_name", "cell_block_codec_class", "cell_block_compressor_class", "version_info")
    USER_INFO_FIELD_NUMBER: _ClassVar[int]
    SERVICE_NAME_FIELD_NUMBER: _ClassVar[int]
    CELL_BLOCK_CODEC_CLASS_FIELD_NUMBER: _ClassVar[int]
    CELL_BLOCK_COMPRESSOR_CLASS_FIELD_NUMBER: _ClassVar[int]
    VERSION_INFO_FIELD_NUMBER: _ClassVar[int]
    user_info: UserInformation
    service_name: str
    cell_block_codec_class: str
    cell_block_compressor_class: str
    version_info: _HBase_pb2.VersionInfo
    def __init__(self, user_info: _Optional[_Union[UserInformation, _Mapping]] = ..., service_name: _Optional[str] = ..., cell_block_codec_class: _Optional[str] = ..., cell_block_compressor_class: _Optional[str] = ..., version_info: _Optional[_Union[_HBase_pb2.VersionInfo, _Mapping]] = ...) -> None: ...

class CellBlockMeta(_message.Message):
    __slots__ = ("length",)
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    length: int
    def __init__(self, length: _Optional[int] = ...) -> None: ...

class ExceptionResponse(_message.Message):
    __slots__ = ("exception_class_name", "stack_trace", "hostname", "port", "do_not_retry")
    EXCEPTION_CLASS_NAME_FIELD_NUMBER: _ClassVar[int]
    STACK_TRACE_FIELD_NUMBER: _ClassVar[int]
    HOSTNAME_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    DO_NOT_RETRY_FIELD_NUMBER: _ClassVar[int]
    exception_class_name: str
    stack_trace: str
    hostname: str
    port: int
    do_not_retry: bool
    def __init__(self, exception_class_name: _Optional[str] = ..., stack_trace: _Optional[str] = ..., hostname: _Optional[str] = ..., port: _Optional[int] = ..., do_not_retry: bool = ...) -> None: ...

class RequestHeader(_message.Message):
    __slots__ = ("call_id", "trace_info", "method_name", "request_param", "cell_block_meta", "priority", "timeout")
    CALL_ID_FIELD_NUMBER: _ClassVar[int]
    TRACE_INFO_FIELD_NUMBER: _ClassVar[int]
    METHOD_NAME_FIELD_NUMBER: _ClassVar[int]
    REQUEST_PARAM_FIELD_NUMBER: _ClassVar[int]
    CELL_BLOCK_META_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    call_id: int
    trace_info: _Tracing_pb2.RPCTInfo
    method_name: str
    request_param: bool
    cell_block_meta: CellBlockMeta
    priority: int
    timeout: int
    def __init__(self, call_id: _Optional[int] = ..., trace_info: _Optional[_Union[_Tracing_pb2.RPCTInfo, _Mapping]] = ..., method_name: _Optional[str] = ..., request_param: bool = ..., cell_block_meta: _Optional[_Union[CellBlockMeta, _Mapping]] = ..., priority: _Optional[int] = ..., timeout: _Optional[int] = ...) -> None: ...

class ResponseHeader(_message.Message):
    __slots__ = ("call_id", "exception", "cell_block_meta")
    CALL_ID_FIELD_NUMBER: _ClassVar[int]
    EXCEPTION_FIELD_NUMBER: _ClassVar[int]
    CELL_BLOCK_META_FIELD_NUMBER: _ClassVar[int]
    call_id: int
    exception: ExceptionResponse
    cell_block_meta: CellBlockMeta
    def __init__(self, call_id: _Optional[int] = ..., exception: _Optional[_Union[ExceptionResponse, _Mapping]] = ..., cell_block_meta: _Optional[_Union[CellBlockMeta, _Mapping]] = ...) -> None: ...
