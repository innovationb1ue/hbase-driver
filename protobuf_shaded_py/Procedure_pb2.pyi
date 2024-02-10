from google.protobuf import any_pb2 as _any_pb2
import ErrorHandling_pb2 as _ErrorHandling_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ProcedureState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INITIALIZING: _ClassVar[ProcedureState]
    RUNNABLE: _ClassVar[ProcedureState]
    WAITING: _ClassVar[ProcedureState]
    WAITING_TIMEOUT: _ClassVar[ProcedureState]
    ROLLEDBACK: _ClassVar[ProcedureState]
    SUCCESS: _ClassVar[ProcedureState]
    FAILED: _ClassVar[ProcedureState]
INITIALIZING: ProcedureState
RUNNABLE: ProcedureState
WAITING: ProcedureState
WAITING_TIMEOUT: ProcedureState
ROLLEDBACK: ProcedureState
SUCCESS: ProcedureState
FAILED: ProcedureState

class Procedure(_message.Message):
    __slots__ = ("class_name", "parent_id", "proc_id", "submitted_time", "owner", "state", "stack_id", "last_update", "timeout", "exception", "result", "state_data", "state_message", "nonce_group", "nonce", "locked", "bypass", "executed")
    CLASS_NAME_FIELD_NUMBER: _ClassVar[int]
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    SUBMITTED_TIME_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    STACK_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_UPDATE_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    EXCEPTION_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    STATE_DATA_FIELD_NUMBER: _ClassVar[int]
    STATE_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    LOCKED_FIELD_NUMBER: _ClassVar[int]
    BYPASS_FIELD_NUMBER: _ClassVar[int]
    EXECUTED_FIELD_NUMBER: _ClassVar[int]
    class_name: str
    parent_id: int
    proc_id: int
    submitted_time: int
    owner: str
    state: ProcedureState
    stack_id: _containers.RepeatedScalarFieldContainer[int]
    last_update: int
    timeout: int
    exception: _ErrorHandling_pb2.ForeignExceptionMessage
    result: bytes
    state_data: bytes
    state_message: _containers.RepeatedCompositeFieldContainer[_any_pb2.Any]
    nonce_group: int
    nonce: int
    locked: bool
    bypass: bool
    executed: bool
    def __init__(self, class_name: _Optional[str] = ..., parent_id: _Optional[int] = ..., proc_id: _Optional[int] = ..., submitted_time: _Optional[int] = ..., owner: _Optional[str] = ..., state: _Optional[_Union[ProcedureState, str]] = ..., stack_id: _Optional[_Iterable[int]] = ..., last_update: _Optional[int] = ..., timeout: _Optional[int] = ..., exception: _Optional[_Union[_ErrorHandling_pb2.ForeignExceptionMessage, _Mapping]] = ..., result: _Optional[bytes] = ..., state_data: _Optional[bytes] = ..., state_message: _Optional[_Iterable[_Union[_any_pb2.Any, _Mapping]]] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ..., locked: bool = ..., bypass: bool = ..., executed: bool = ...) -> None: ...

class SequentialProcedureData(_message.Message):
    __slots__ = ("executed",)
    EXECUTED_FIELD_NUMBER: _ClassVar[int]
    executed: bool
    def __init__(self, executed: bool = ...) -> None: ...

class StateMachineProcedureData(_message.Message):
    __slots__ = ("state",)
    STATE_FIELD_NUMBER: _ClassVar[int]
    state: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, state: _Optional[_Iterable[int]] = ...) -> None: ...

class ProcedureWALHeader(_message.Message):
    __slots__ = ("version", "type", "log_id", "min_proc_id")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    LOG_ID_FIELD_NUMBER: _ClassVar[int]
    MIN_PROC_ID_FIELD_NUMBER: _ClassVar[int]
    version: int
    type: int
    log_id: int
    min_proc_id: int
    def __init__(self, version: _Optional[int] = ..., type: _Optional[int] = ..., log_id: _Optional[int] = ..., min_proc_id: _Optional[int] = ...) -> None: ...

class ProcedureWALTrailer(_message.Message):
    __slots__ = ("version", "tracker_pos")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    TRACKER_POS_FIELD_NUMBER: _ClassVar[int]
    version: int
    tracker_pos: int
    def __init__(self, version: _Optional[int] = ..., tracker_pos: _Optional[int] = ...) -> None: ...

class ProcedureStoreTracker(_message.Message):
    __slots__ = ("node",)
    class TrackerNode(_message.Message):
        __slots__ = ("start_id", "updated", "deleted")
        START_ID_FIELD_NUMBER: _ClassVar[int]
        UPDATED_FIELD_NUMBER: _ClassVar[int]
        DELETED_FIELD_NUMBER: _ClassVar[int]
        start_id: int
        updated: _containers.RepeatedScalarFieldContainer[int]
        deleted: _containers.RepeatedScalarFieldContainer[int]
        def __init__(self, start_id: _Optional[int] = ..., updated: _Optional[_Iterable[int]] = ..., deleted: _Optional[_Iterable[int]] = ...) -> None: ...
    NODE_FIELD_NUMBER: _ClassVar[int]
    node: _containers.RepeatedCompositeFieldContainer[ProcedureStoreTracker.TrackerNode]
    def __init__(self, node: _Optional[_Iterable[_Union[ProcedureStoreTracker.TrackerNode, _Mapping]]] = ...) -> None: ...

class ProcedureWALEntry(_message.Message):
    __slots__ = ("type", "procedure", "proc_id", "child_id")
    class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        PROCEDURE_WAL_EOF: _ClassVar[ProcedureWALEntry.Type]
        PROCEDURE_WAL_INIT: _ClassVar[ProcedureWALEntry.Type]
        PROCEDURE_WAL_INSERT: _ClassVar[ProcedureWALEntry.Type]
        PROCEDURE_WAL_UPDATE: _ClassVar[ProcedureWALEntry.Type]
        PROCEDURE_WAL_DELETE: _ClassVar[ProcedureWALEntry.Type]
        PROCEDURE_WAL_COMPACT: _ClassVar[ProcedureWALEntry.Type]
    PROCEDURE_WAL_EOF: ProcedureWALEntry.Type
    PROCEDURE_WAL_INIT: ProcedureWALEntry.Type
    PROCEDURE_WAL_INSERT: ProcedureWALEntry.Type
    PROCEDURE_WAL_UPDATE: ProcedureWALEntry.Type
    PROCEDURE_WAL_DELETE: ProcedureWALEntry.Type
    PROCEDURE_WAL_COMPACT: ProcedureWALEntry.Type
    TYPE_FIELD_NUMBER: _ClassVar[int]
    PROCEDURE_FIELD_NUMBER: _ClassVar[int]
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    CHILD_ID_FIELD_NUMBER: _ClassVar[int]
    type: ProcedureWALEntry.Type
    procedure: _containers.RepeatedCompositeFieldContainer[Procedure]
    proc_id: int
    child_id: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, type: _Optional[_Union[ProcedureWALEntry.Type, str]] = ..., procedure: _Optional[_Iterable[_Union[Procedure, _Mapping]]] = ..., proc_id: _Optional[int] = ..., child_id: _Optional[_Iterable[int]] = ...) -> None: ...
