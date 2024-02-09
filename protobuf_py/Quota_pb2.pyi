import HBase_pb2 as _HBase_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class QuotaScope(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CLUSTER: _ClassVar[QuotaScope]
    MACHINE: _ClassVar[QuotaScope]

class ThrottleType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REQUEST_NUMBER: _ClassVar[ThrottleType]
    REQUEST_SIZE: _ClassVar[ThrottleType]
    WRITE_NUMBER: _ClassVar[ThrottleType]
    WRITE_SIZE: _ClassVar[ThrottleType]
    READ_NUMBER: _ClassVar[ThrottleType]
    READ_SIZE: _ClassVar[ThrottleType]

class QuotaType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    THROTTLE: _ClassVar[QuotaType]
    SPACE: _ClassVar[QuotaType]

class SpaceViolationPolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DISABLE: _ClassVar[SpaceViolationPolicy]
    NO_WRITES_COMPACTIONS: _ClassVar[SpaceViolationPolicy]
    NO_WRITES: _ClassVar[SpaceViolationPolicy]
    NO_INSERTS: _ClassVar[SpaceViolationPolicy]
CLUSTER: QuotaScope
MACHINE: QuotaScope
REQUEST_NUMBER: ThrottleType
REQUEST_SIZE: ThrottleType
WRITE_NUMBER: ThrottleType
WRITE_SIZE: ThrottleType
READ_NUMBER: ThrottleType
READ_SIZE: ThrottleType
THROTTLE: QuotaType
SPACE: QuotaType
DISABLE: SpaceViolationPolicy
NO_WRITES_COMPACTIONS: SpaceViolationPolicy
NO_WRITES: SpaceViolationPolicy
NO_INSERTS: SpaceViolationPolicy

class TimedQuota(_message.Message):
    __slots__ = ("time_unit", "soft_limit", "share", "scope")
    TIME_UNIT_FIELD_NUMBER: _ClassVar[int]
    SOFT_LIMIT_FIELD_NUMBER: _ClassVar[int]
    SHARE_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    time_unit: _HBase_pb2.TimeUnit
    soft_limit: int
    share: float
    scope: QuotaScope
    def __init__(self, time_unit: _Optional[_Union[_HBase_pb2.TimeUnit, str]] = ..., soft_limit: _Optional[int] = ..., share: _Optional[float] = ..., scope: _Optional[_Union[QuotaScope, str]] = ...) -> None: ...

class Throttle(_message.Message):
    __slots__ = ("req_num", "req_size", "write_num", "write_size", "read_num", "read_size")
    REQ_NUM_FIELD_NUMBER: _ClassVar[int]
    REQ_SIZE_FIELD_NUMBER: _ClassVar[int]
    WRITE_NUM_FIELD_NUMBER: _ClassVar[int]
    WRITE_SIZE_FIELD_NUMBER: _ClassVar[int]
    READ_NUM_FIELD_NUMBER: _ClassVar[int]
    READ_SIZE_FIELD_NUMBER: _ClassVar[int]
    req_num: TimedQuota
    req_size: TimedQuota
    write_num: TimedQuota
    write_size: TimedQuota
    read_num: TimedQuota
    read_size: TimedQuota
    def __init__(self, req_num: _Optional[_Union[TimedQuota, _Mapping]] = ..., req_size: _Optional[_Union[TimedQuota, _Mapping]] = ..., write_num: _Optional[_Union[TimedQuota, _Mapping]] = ..., write_size: _Optional[_Union[TimedQuota, _Mapping]] = ..., read_num: _Optional[_Union[TimedQuota, _Mapping]] = ..., read_size: _Optional[_Union[TimedQuota, _Mapping]] = ...) -> None: ...

class ThrottleRequest(_message.Message):
    __slots__ = ("type", "timed_quota")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TIMED_QUOTA_FIELD_NUMBER: _ClassVar[int]
    type: ThrottleType
    timed_quota: TimedQuota
    def __init__(self, type: _Optional[_Union[ThrottleType, str]] = ..., timed_quota: _Optional[_Union[TimedQuota, _Mapping]] = ...) -> None: ...

class Quotas(_message.Message):
    __slots__ = ("bypass_globals", "throttle", "space")
    BYPASS_GLOBALS_FIELD_NUMBER: _ClassVar[int]
    THROTTLE_FIELD_NUMBER: _ClassVar[int]
    SPACE_FIELD_NUMBER: _ClassVar[int]
    bypass_globals: bool
    throttle: Throttle
    space: SpaceQuota
    def __init__(self, bypass_globals: bool = ..., throttle: _Optional[_Union[Throttle, _Mapping]] = ..., space: _Optional[_Union[SpaceQuota, _Mapping]] = ...) -> None: ...

class QuotaUsage(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SpaceQuota(_message.Message):
    __slots__ = ("soft_limit", "violation_policy", "remove")
    SOFT_LIMIT_FIELD_NUMBER: _ClassVar[int]
    VIOLATION_POLICY_FIELD_NUMBER: _ClassVar[int]
    REMOVE_FIELD_NUMBER: _ClassVar[int]
    soft_limit: int
    violation_policy: SpaceViolationPolicy
    remove: bool
    def __init__(self, soft_limit: _Optional[int] = ..., violation_policy: _Optional[_Union[SpaceViolationPolicy, str]] = ..., remove: bool = ...) -> None: ...

class SpaceLimitRequest(_message.Message):
    __slots__ = ("quota",)
    QUOTA_FIELD_NUMBER: _ClassVar[int]
    quota: SpaceQuota
    def __init__(self, quota: _Optional[_Union[SpaceQuota, _Mapping]] = ...) -> None: ...

class SpaceQuotaStatus(_message.Message):
    __slots__ = ("violation_policy", "in_violation")
    VIOLATION_POLICY_FIELD_NUMBER: _ClassVar[int]
    IN_VIOLATION_FIELD_NUMBER: _ClassVar[int]
    violation_policy: SpaceViolationPolicy
    in_violation: bool
    def __init__(self, violation_policy: _Optional[_Union[SpaceViolationPolicy, str]] = ..., in_violation: bool = ...) -> None: ...

class SpaceQuotaSnapshot(_message.Message):
    __slots__ = ("quota_status", "quota_usage", "quota_limit")
    QUOTA_STATUS_FIELD_NUMBER: _ClassVar[int]
    QUOTA_USAGE_FIELD_NUMBER: _ClassVar[int]
    QUOTA_LIMIT_FIELD_NUMBER: _ClassVar[int]
    quota_status: SpaceQuotaStatus
    quota_usage: int
    quota_limit: int
    def __init__(self, quota_status: _Optional[_Union[SpaceQuotaStatus, _Mapping]] = ..., quota_usage: _Optional[int] = ..., quota_limit: _Optional[int] = ...) -> None: ...
