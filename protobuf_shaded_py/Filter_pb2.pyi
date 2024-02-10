import HBase_pb2 as _HBase_pb2
import Comparator_pb2 as _Comparator_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Filter(_message.Message):
    __slots__ = ("name", "serialized_filter")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SERIALIZED_FILTER_FIELD_NUMBER: _ClassVar[int]
    name: str
    serialized_filter: bytes
    def __init__(self, name: _Optional[str] = ..., serialized_filter: _Optional[bytes] = ...) -> None: ...

class ColumnCountGetFilter(_message.Message):
    __slots__ = ("limit",)
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    limit: int
    def __init__(self, limit: _Optional[int] = ...) -> None: ...

class ColumnPaginationFilter(_message.Message):
    __slots__ = ("limit", "offset", "column_offset")
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    COLUMN_OFFSET_FIELD_NUMBER: _ClassVar[int]
    limit: int
    offset: int
    column_offset: bytes
    def __init__(self, limit: _Optional[int] = ..., offset: _Optional[int] = ..., column_offset: _Optional[bytes] = ...) -> None: ...

class ColumnPrefixFilter(_message.Message):
    __slots__ = ("prefix",)
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    prefix: bytes
    def __init__(self, prefix: _Optional[bytes] = ...) -> None: ...

class ColumnRangeFilter(_message.Message):
    __slots__ = ("min_column", "min_column_inclusive", "max_column", "max_column_inclusive")
    MIN_COLUMN_FIELD_NUMBER: _ClassVar[int]
    MIN_COLUMN_INCLUSIVE_FIELD_NUMBER: _ClassVar[int]
    MAX_COLUMN_FIELD_NUMBER: _ClassVar[int]
    MAX_COLUMN_INCLUSIVE_FIELD_NUMBER: _ClassVar[int]
    min_column: bytes
    min_column_inclusive: bool
    max_column: bytes
    max_column_inclusive: bool
    def __init__(self, min_column: _Optional[bytes] = ..., min_column_inclusive: bool = ..., max_column: _Optional[bytes] = ..., max_column_inclusive: bool = ...) -> None: ...

class CompareFilter(_message.Message):
    __slots__ = ("compare_op", "comparator")
    COMPARE_OP_FIELD_NUMBER: _ClassVar[int]
    COMPARATOR_FIELD_NUMBER: _ClassVar[int]
    compare_op: _HBase_pb2.CompareType
    comparator: _Comparator_pb2.Comparator
    def __init__(self, compare_op: _Optional[_Union[_HBase_pb2.CompareType, str]] = ..., comparator: _Optional[_Union[_Comparator_pb2.Comparator, _Mapping]] = ...) -> None: ...

class DependentColumnFilter(_message.Message):
    __slots__ = ("compare_filter", "column_family", "column_qualifier", "drop_dependent_column")
    COMPARE_FILTER_FIELD_NUMBER: _ClassVar[int]
    COLUMN_FAMILY_FIELD_NUMBER: _ClassVar[int]
    COLUMN_QUALIFIER_FIELD_NUMBER: _ClassVar[int]
    DROP_DEPENDENT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    compare_filter: CompareFilter
    column_family: bytes
    column_qualifier: bytes
    drop_dependent_column: bool
    def __init__(self, compare_filter: _Optional[_Union[CompareFilter, _Mapping]] = ..., column_family: _Optional[bytes] = ..., column_qualifier: _Optional[bytes] = ..., drop_dependent_column: bool = ...) -> None: ...

class FamilyFilter(_message.Message):
    __slots__ = ("compare_filter",)
    COMPARE_FILTER_FIELD_NUMBER: _ClassVar[int]
    compare_filter: CompareFilter
    def __init__(self, compare_filter: _Optional[_Union[CompareFilter, _Mapping]] = ...) -> None: ...

class FilterList(_message.Message):
    __slots__ = ("operator", "filters")
    class Operator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        MUST_PASS_ALL: _ClassVar[FilterList.Operator]
        MUST_PASS_ONE: _ClassVar[FilterList.Operator]
    MUST_PASS_ALL: FilterList.Operator
    MUST_PASS_ONE: FilterList.Operator
    OPERATOR_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    operator: FilterList.Operator
    filters: _containers.RepeatedCompositeFieldContainer[Filter]
    def __init__(self, operator: _Optional[_Union[FilterList.Operator, str]] = ..., filters: _Optional[_Iterable[_Union[Filter, _Mapping]]] = ...) -> None: ...

class FilterWrapper(_message.Message):
    __slots__ = ("filter",)
    FILTER_FIELD_NUMBER: _ClassVar[int]
    filter: Filter
    def __init__(self, filter: _Optional[_Union[Filter, _Mapping]] = ...) -> None: ...

class FirstKeyOnlyFilter(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class FirstKeyValueMatchingQualifiersFilter(_message.Message):
    __slots__ = ("qualifiers",)
    QUALIFIERS_FIELD_NUMBER: _ClassVar[int]
    qualifiers: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, qualifiers: _Optional[_Iterable[bytes]] = ...) -> None: ...

class FuzzyRowFilter(_message.Message):
    __slots__ = ("fuzzy_keys_data", "is_mask_v2")
    FUZZY_KEYS_DATA_FIELD_NUMBER: _ClassVar[int]
    IS_MASK_V2_FIELD_NUMBER: _ClassVar[int]
    fuzzy_keys_data: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.BytesBytesPair]
    is_mask_v2: bool
    def __init__(self, fuzzy_keys_data: _Optional[_Iterable[_Union[_HBase_pb2.BytesBytesPair, _Mapping]]] = ..., is_mask_v2: bool = ...) -> None: ...

class InclusiveStopFilter(_message.Message):
    __slots__ = ("stop_row_key",)
    STOP_ROW_KEY_FIELD_NUMBER: _ClassVar[int]
    stop_row_key: bytes
    def __init__(self, stop_row_key: _Optional[bytes] = ...) -> None: ...

class KeyOnlyFilter(_message.Message):
    __slots__ = ("len_as_val",)
    LEN_AS_VAL_FIELD_NUMBER: _ClassVar[int]
    len_as_val: bool
    def __init__(self, len_as_val: bool = ...) -> None: ...

class MultipleColumnPrefixFilter(_message.Message):
    __slots__ = ("sorted_prefixes",)
    SORTED_PREFIXES_FIELD_NUMBER: _ClassVar[int]
    sorted_prefixes: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, sorted_prefixes: _Optional[_Iterable[bytes]] = ...) -> None: ...

class PageFilter(_message.Message):
    __slots__ = ("page_size",)
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    page_size: int
    def __init__(self, page_size: _Optional[int] = ...) -> None: ...

class PrefixFilter(_message.Message):
    __slots__ = ("prefix",)
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    prefix: bytes
    def __init__(self, prefix: _Optional[bytes] = ...) -> None: ...

class QualifierFilter(_message.Message):
    __slots__ = ("compare_filter",)
    COMPARE_FILTER_FIELD_NUMBER: _ClassVar[int]
    compare_filter: CompareFilter
    def __init__(self, compare_filter: _Optional[_Union[CompareFilter, _Mapping]] = ...) -> None: ...

class RandomRowFilter(_message.Message):
    __slots__ = ("chance",)
    CHANCE_FIELD_NUMBER: _ClassVar[int]
    chance: float
    def __init__(self, chance: _Optional[float] = ...) -> None: ...

class RowFilter(_message.Message):
    __slots__ = ("compare_filter",)
    COMPARE_FILTER_FIELD_NUMBER: _ClassVar[int]
    compare_filter: CompareFilter
    def __init__(self, compare_filter: _Optional[_Union[CompareFilter, _Mapping]] = ...) -> None: ...

class SingleColumnValueExcludeFilter(_message.Message):
    __slots__ = ("single_column_value_filter",)
    SINGLE_COLUMN_VALUE_FILTER_FIELD_NUMBER: _ClassVar[int]
    single_column_value_filter: SingleColumnValueFilter
    def __init__(self, single_column_value_filter: _Optional[_Union[SingleColumnValueFilter, _Mapping]] = ...) -> None: ...

class SingleColumnValueFilter(_message.Message):
    __slots__ = ("column_family", "column_qualifier", "compare_op", "comparator", "filter_if_missing", "latest_version_only")
    COLUMN_FAMILY_FIELD_NUMBER: _ClassVar[int]
    COLUMN_QUALIFIER_FIELD_NUMBER: _ClassVar[int]
    COMPARE_OP_FIELD_NUMBER: _ClassVar[int]
    COMPARATOR_FIELD_NUMBER: _ClassVar[int]
    FILTER_IF_MISSING_FIELD_NUMBER: _ClassVar[int]
    LATEST_VERSION_ONLY_FIELD_NUMBER: _ClassVar[int]
    column_family: bytes
    column_qualifier: bytes
    compare_op: _HBase_pb2.CompareType
    comparator: _Comparator_pb2.Comparator
    filter_if_missing: bool
    latest_version_only: bool
    def __init__(self, column_family: _Optional[bytes] = ..., column_qualifier: _Optional[bytes] = ..., compare_op: _Optional[_Union[_HBase_pb2.CompareType, str]] = ..., comparator: _Optional[_Union[_Comparator_pb2.Comparator, _Mapping]] = ..., filter_if_missing: bool = ..., latest_version_only: bool = ...) -> None: ...

class SkipFilter(_message.Message):
    __slots__ = ("filter",)
    FILTER_FIELD_NUMBER: _ClassVar[int]
    filter: Filter
    def __init__(self, filter: _Optional[_Union[Filter, _Mapping]] = ...) -> None: ...

class TimestampsFilter(_message.Message):
    __slots__ = ("timestamps", "can_hint")
    TIMESTAMPS_FIELD_NUMBER: _ClassVar[int]
    CAN_HINT_FIELD_NUMBER: _ClassVar[int]
    timestamps: _containers.RepeatedScalarFieldContainer[int]
    can_hint: bool
    def __init__(self, timestamps: _Optional[_Iterable[int]] = ..., can_hint: bool = ...) -> None: ...

class ValueFilter(_message.Message):
    __slots__ = ("compare_filter",)
    COMPARE_FILTER_FIELD_NUMBER: _ClassVar[int]
    compare_filter: CompareFilter
    def __init__(self, compare_filter: _Optional[_Union[CompareFilter, _Mapping]] = ...) -> None: ...

class WhileMatchFilter(_message.Message):
    __slots__ = ("filter",)
    FILTER_FIELD_NUMBER: _ClassVar[int]
    filter: Filter
    def __init__(self, filter: _Optional[_Union[Filter, _Mapping]] = ...) -> None: ...

class FilterAllFilter(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RowRange(_message.Message):
    __slots__ = ("start_row", "start_row_inclusive", "stop_row", "stop_row_inclusive")
    START_ROW_FIELD_NUMBER: _ClassVar[int]
    START_ROW_INCLUSIVE_FIELD_NUMBER: _ClassVar[int]
    STOP_ROW_FIELD_NUMBER: _ClassVar[int]
    STOP_ROW_INCLUSIVE_FIELD_NUMBER: _ClassVar[int]
    start_row: bytes
    start_row_inclusive: bool
    stop_row: bytes
    stop_row_inclusive: bool
    def __init__(self, start_row: _Optional[bytes] = ..., start_row_inclusive: bool = ..., stop_row: _Optional[bytes] = ..., stop_row_inclusive: bool = ...) -> None: ...

class MultiRowRangeFilter(_message.Message):
    __slots__ = ("row_range_list",)
    ROW_RANGE_LIST_FIELD_NUMBER: _ClassVar[int]
    row_range_list: _containers.RepeatedCompositeFieldContainer[RowRange]
    def __init__(self, row_range_list: _Optional[_Iterable[_Union[RowRange, _Mapping]]] = ...) -> None: ...

class ColumnValueFilter(_message.Message):
    __slots__ = ("family", "qualifier", "compare_op", "comparator")
    FAMILY_FIELD_NUMBER: _ClassVar[int]
    QUALIFIER_FIELD_NUMBER: _ClassVar[int]
    COMPARE_OP_FIELD_NUMBER: _ClassVar[int]
    COMPARATOR_FIELD_NUMBER: _ClassVar[int]
    family: bytes
    qualifier: bytes
    compare_op: _HBase_pb2.CompareType
    comparator: _Comparator_pb2.Comparator
    def __init__(self, family: _Optional[bytes] = ..., qualifier: _Optional[bytes] = ..., compare_op: _Optional[_Union[_HBase_pb2.CompareType, str]] = ..., comparator: _Optional[_Union[_Comparator_pb2.Comparator, _Mapping]] = ...) -> None: ...
