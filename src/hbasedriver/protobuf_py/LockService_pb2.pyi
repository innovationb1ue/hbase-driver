import HBase_pb2 as _HBase_pb2
import Procedure_pb2 as _Procedure_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LockType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EXCLUSIVE: _ClassVar[LockType]
    SHARED: _ClassVar[LockType]

class LockedResourceType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SERVER: _ClassVar[LockedResourceType]
    NAMESPACE: _ClassVar[LockedResourceType]
    TABLE: _ClassVar[LockedResourceType]
    REGION: _ClassVar[LockedResourceType]
    PEER: _ClassVar[LockedResourceType]
EXCLUSIVE: LockType
SHARED: LockType
SERVER: LockedResourceType
NAMESPACE: LockedResourceType
TABLE: LockedResourceType
REGION: LockedResourceType
PEER: LockedResourceType

class LockRequest(_message.Message):
    __slots__ = ("lock_type", "namespace", "table_name", "region_info", "description", "nonce_group", "nonce")
    LOCK_TYPE_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    lock_type: LockType
    namespace: str
    table_name: _HBase_pb2.TableName
    region_info: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    description: str
    nonce_group: int
    nonce: int
    def __init__(self, lock_type: _Optional[_Union[LockType, str]] = ..., namespace: _Optional[str] = ..., table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., region_info: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ..., description: _Optional[str] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class LockResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class LockHeartbeatRequest(_message.Message):
    __slots__ = ("proc_id", "keep_alive")
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    KEEP_ALIVE_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    keep_alive: bool
    def __init__(self, proc_id: _Optional[int] = ..., keep_alive: bool = ...) -> None: ...

class LockHeartbeatResponse(_message.Message):
    __slots__ = ("lock_status", "timeout_ms")
    class LockStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        UNLOCKED: _ClassVar[LockHeartbeatResponse.LockStatus]
        LOCKED: _ClassVar[LockHeartbeatResponse.LockStatus]
    UNLOCKED: LockHeartbeatResponse.LockStatus
    LOCKED: LockHeartbeatResponse.LockStatus
    LOCK_STATUS_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    lock_status: LockHeartbeatResponse.LockStatus
    timeout_ms: int
    def __init__(self, lock_status: _Optional[_Union[LockHeartbeatResponse.LockStatus, str]] = ..., timeout_ms: _Optional[int] = ...) -> None: ...

class LockProcedureData(_message.Message):
    __slots__ = ("lock_type", "namespace", "table_name", "region_info", "description", "is_master_lock")
    LOCK_TYPE_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    IS_MASTER_LOCK_FIELD_NUMBER: _ClassVar[int]
    lock_type: LockType
    namespace: str
    table_name: _HBase_pb2.TableName
    region_info: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    description: str
    is_master_lock: bool
    def __init__(self, lock_type: _Optional[_Union[LockType, str]] = ..., namespace: _Optional[str] = ..., table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., region_info: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ..., description: _Optional[str] = ..., is_master_lock: bool = ...) -> None: ...

class LockedResource(_message.Message):
    __slots__ = ("resource_type", "resource_name", "lock_type", "exclusive_lock_owner_procedure", "shared_lock_count", "waitingProcedures")
    RESOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    LOCK_TYPE_FIELD_NUMBER: _ClassVar[int]
    EXCLUSIVE_LOCK_OWNER_PROCEDURE_FIELD_NUMBER: _ClassVar[int]
    SHARED_LOCK_COUNT_FIELD_NUMBER: _ClassVar[int]
    WAITINGPROCEDURES_FIELD_NUMBER: _ClassVar[int]
    resource_type: LockedResourceType
    resource_name: str
    lock_type: LockType
    exclusive_lock_owner_procedure: _Procedure_pb2.Procedure
    shared_lock_count: int
    waitingProcedures: _containers.RepeatedCompositeFieldContainer[_Procedure_pb2.Procedure]
    def __init__(self, resource_type: _Optional[_Union[LockedResourceType, str]] = ..., resource_name: _Optional[str] = ..., lock_type: _Optional[_Union[LockType, str]] = ..., exclusive_lock_owner_procedure: _Optional[_Union[_Procedure_pb2.Procedure, _Mapping]] = ..., shared_lock_count: _Optional[int] = ..., waitingProcedures: _Optional[_Iterable[_Union[_Procedure_pb2.Procedure, _Mapping]]] = ...) -> None: ...
