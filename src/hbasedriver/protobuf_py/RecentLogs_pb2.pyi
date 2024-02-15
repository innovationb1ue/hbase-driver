from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class BalancerDecision(_message.Message):
    __slots__ = ("initial_function_costs", "final_function_costs", "init_total_cost", "computed_total_cost", "computed_steps", "region_plans")
    INITIAL_FUNCTION_COSTS_FIELD_NUMBER: _ClassVar[int]
    FINAL_FUNCTION_COSTS_FIELD_NUMBER: _ClassVar[int]
    INIT_TOTAL_COST_FIELD_NUMBER: _ClassVar[int]
    COMPUTED_TOTAL_COST_FIELD_NUMBER: _ClassVar[int]
    COMPUTED_STEPS_FIELD_NUMBER: _ClassVar[int]
    REGION_PLANS_FIELD_NUMBER: _ClassVar[int]
    initial_function_costs: str
    final_function_costs: str
    init_total_cost: float
    computed_total_cost: float
    computed_steps: int
    region_plans: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, initial_function_costs: _Optional[str] = ..., final_function_costs: _Optional[str] = ..., init_total_cost: _Optional[float] = ..., computed_total_cost: _Optional[float] = ..., computed_steps: _Optional[int] = ..., region_plans: _Optional[_Iterable[str]] = ...) -> None: ...

class BalancerRejection(_message.Message):
    __slots__ = ("reason", "cost_func_info")
    REASON_FIELD_NUMBER: _ClassVar[int]
    COST_FUNC_INFO_FIELD_NUMBER: _ClassVar[int]
    reason: str
    cost_func_info: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, reason: _Optional[str] = ..., cost_func_info: _Optional[_Iterable[str]] = ...) -> None: ...
