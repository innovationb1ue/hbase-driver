import HBase_pb2 as _HBase_pb2
import ClusterStatus_pb2 as _ClusterStatus_pb2
import ErrorHandling_pb2 as _ErrorHandling_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RegionServerStartupRequest(_message.Message):
    __slots__ = ("port", "server_start_code", "server_current_time", "use_this_hostname_instead")
    PORT_FIELD_NUMBER: _ClassVar[int]
    SERVER_START_CODE_FIELD_NUMBER: _ClassVar[int]
    SERVER_CURRENT_TIME_FIELD_NUMBER: _ClassVar[int]
    USE_THIS_HOSTNAME_INSTEAD_FIELD_NUMBER: _ClassVar[int]
    port: int
    server_start_code: int
    server_current_time: int
    use_this_hostname_instead: str
    def __init__(self, port: _Optional[int] = ..., server_start_code: _Optional[int] = ..., server_current_time: _Optional[int] = ..., use_this_hostname_instead: _Optional[str] = ...) -> None: ...

class RegionServerStartupResponse(_message.Message):
    __slots__ = ("map_entries",)
    MAP_ENTRIES_FIELD_NUMBER: _ClassVar[int]
    map_entries: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.NameStringPair]
    def __init__(self, map_entries: _Optional[_Iterable[_Union[_HBase_pb2.NameStringPair, _Mapping]]] = ...) -> None: ...

class RegionServerReportRequest(_message.Message):
    __slots__ = ("server", "load")
    SERVER_FIELD_NUMBER: _ClassVar[int]
    LOAD_FIELD_NUMBER: _ClassVar[int]
    server: _HBase_pb2.ServerName
    load: _ClusterStatus_pb2.ServerLoad
    def __init__(self, server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., load: _Optional[_Union[_ClusterStatus_pb2.ServerLoad, _Mapping]] = ...) -> None: ...

class RegionServerReportResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ReportRSFatalErrorRequest(_message.Message):
    __slots__ = ("server", "error_message")
    SERVER_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    server: _HBase_pb2.ServerName
    error_message: str
    def __init__(self, server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., error_message: _Optional[str] = ...) -> None: ...

class ReportRSFatalErrorResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetLastFlushedSequenceIdRequest(_message.Message):
    __slots__ = ("region_name",)
    REGION_NAME_FIELD_NUMBER: _ClassVar[int]
    region_name: bytes
    def __init__(self, region_name: _Optional[bytes] = ...) -> None: ...

class GetLastFlushedSequenceIdResponse(_message.Message):
    __slots__ = ("last_flushed_sequence_id", "store_last_flushed_sequence_id")
    LAST_FLUSHED_SEQUENCE_ID_FIELD_NUMBER: _ClassVar[int]
    STORE_LAST_FLUSHED_SEQUENCE_ID_FIELD_NUMBER: _ClassVar[int]
    last_flushed_sequence_id: int
    store_last_flushed_sequence_id: _containers.RepeatedCompositeFieldContainer[_ClusterStatus_pb2.StoreSequenceId]
    def __init__(self, last_flushed_sequence_id: _Optional[int] = ..., store_last_flushed_sequence_id: _Optional[_Iterable[_Union[_ClusterStatus_pb2.StoreSequenceId, _Mapping]]] = ...) -> None: ...

class RegionStateTransition(_message.Message):
    __slots__ = ("transition_code", "region_info", "open_seq_num", "proc_id")
    class TransitionCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        OPENED: _ClassVar[RegionStateTransition.TransitionCode]
        FAILED_OPEN: _ClassVar[RegionStateTransition.TransitionCode]
        CLOSED: _ClassVar[RegionStateTransition.TransitionCode]
        READY_TO_SPLIT: _ClassVar[RegionStateTransition.TransitionCode]
        READY_TO_MERGE: _ClassVar[RegionStateTransition.TransitionCode]
        SPLIT: _ClassVar[RegionStateTransition.TransitionCode]
        MERGED: _ClassVar[RegionStateTransition.TransitionCode]
        SPLIT_REVERTED: _ClassVar[RegionStateTransition.TransitionCode]
        MERGE_REVERTED: _ClassVar[RegionStateTransition.TransitionCode]
    OPENED: RegionStateTransition.TransitionCode
    FAILED_OPEN: RegionStateTransition.TransitionCode
    CLOSED: RegionStateTransition.TransitionCode
    READY_TO_SPLIT: RegionStateTransition.TransitionCode
    READY_TO_MERGE: RegionStateTransition.TransitionCode
    SPLIT: RegionStateTransition.TransitionCode
    MERGED: RegionStateTransition.TransitionCode
    SPLIT_REVERTED: RegionStateTransition.TransitionCode
    MERGE_REVERTED: RegionStateTransition.TransitionCode
    TRANSITION_CODE_FIELD_NUMBER: _ClassVar[int]
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    OPEN_SEQ_NUM_FIELD_NUMBER: _ClassVar[int]
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    transition_code: RegionStateTransition.TransitionCode
    region_info: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    open_seq_num: int
    proc_id: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, transition_code: _Optional[_Union[RegionStateTransition.TransitionCode, str]] = ..., region_info: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ..., open_seq_num: _Optional[int] = ..., proc_id: _Optional[_Iterable[int]] = ...) -> None: ...

class ReportRegionStateTransitionRequest(_message.Message):
    __slots__ = ("server", "transition")
    SERVER_FIELD_NUMBER: _ClassVar[int]
    TRANSITION_FIELD_NUMBER: _ClassVar[int]
    server: _HBase_pb2.ServerName
    transition: _containers.RepeatedCompositeFieldContainer[RegionStateTransition]
    def __init__(self, server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., transition: _Optional[_Iterable[_Union[RegionStateTransition, _Mapping]]] = ...) -> None: ...

class ReportRegionStateTransitionResponse(_message.Message):
    __slots__ = ("error_message",)
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    error_message: str
    def __init__(self, error_message: _Optional[str] = ...) -> None: ...

class RegionSpaceUse(_message.Message):
    __slots__ = ("region_info", "region_size")
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    REGION_SIZE_FIELD_NUMBER: _ClassVar[int]
    region_info: _HBase_pb2.RegionInfo
    region_size: int
    def __init__(self, region_info: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ..., region_size: _Optional[int] = ...) -> None: ...

class RegionSpaceUseReportRequest(_message.Message):
    __slots__ = ("space_use",)
    SPACE_USE_FIELD_NUMBER: _ClassVar[int]
    space_use: _containers.RepeatedCompositeFieldContainer[RegionSpaceUse]
    def __init__(self, space_use: _Optional[_Iterable[_Union[RegionSpaceUse, _Mapping]]] = ...) -> None: ...

class RegionSpaceUseReportResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RemoteProcedureResult(_message.Message):
    __slots__ = ("proc_id", "status", "error")
    class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SUCCESS: _ClassVar[RemoteProcedureResult.Status]
        ERROR: _ClassVar[RemoteProcedureResult.Status]
    SUCCESS: RemoteProcedureResult.Status
    ERROR: RemoteProcedureResult.Status
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    status: RemoteProcedureResult.Status
    error: _ErrorHandling_pb2.ForeignExceptionMessage
    def __init__(self, proc_id: _Optional[int] = ..., status: _Optional[_Union[RemoteProcedureResult.Status, str]] = ..., error: _Optional[_Union[_ErrorHandling_pb2.ForeignExceptionMessage, _Mapping]] = ...) -> None: ...

class ReportProcedureDoneRequest(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: _containers.RepeatedCompositeFieldContainer[RemoteProcedureResult]
    def __init__(self, result: _Optional[_Iterable[_Union[RemoteProcedureResult, _Mapping]]] = ...) -> None: ...

class ReportProcedureDoneResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class FileArchiveNotificationRequest(_message.Message):
    __slots__ = ("archived_files",)
    class FileWithSize(_message.Message):
        __slots__ = ("table_name", "name", "size")
        TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
        NAME_FIELD_NUMBER: _ClassVar[int]
        SIZE_FIELD_NUMBER: _ClassVar[int]
        table_name: _HBase_pb2.TableName
        name: str
        size: int
        def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., name: _Optional[str] = ..., size: _Optional[int] = ...) -> None: ...
    ARCHIVED_FILES_FIELD_NUMBER: _ClassVar[int]
    archived_files: _containers.RepeatedCompositeFieldContainer[FileArchiveNotificationRequest.FileWithSize]
    def __init__(self, archived_files: _Optional[_Iterable[_Union[FileArchiveNotificationRequest.FileWithSize, _Mapping]]] = ...) -> None: ...

class FileArchiveNotificationResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
