import HBase_pb2 as _HBase_pb2
import Client_pb2 as _Client_pb2
import ClusterStatus_pb2 as _ClusterStatus_pb2
import ErrorHandling_pb2 as _ErrorHandling_pb2
import LockService_pb2 as _LockService_pb2
import Procedure_pb2 as _Procedure_pb2
import Quota_pb2 as _Quota_pb2
import Replication_pb2 as _Replication_pb2
import Snapshot_pb2 as _Snapshot_pb2
import AccessControl_pb2 as _AccessControl_pb2
import RecentLogs_pb2 as _RecentLogs_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MasterSwitchType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SPLIT: _ClassVar[MasterSwitchType]
    MERGE: _ClassVar[MasterSwitchType]
SPLIT: MasterSwitchType
MERGE: MasterSwitchType

class AddColumnRequest(_message.Message):
    __slots__ = ("table_name", "column_families", "nonce_group", "nonce")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLUMN_FAMILIES_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    column_families: _HBase_pb2.ColumnFamilySchema
    nonce_group: int
    nonce: int
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., column_families: _Optional[_Union[_HBase_pb2.ColumnFamilySchema, _Mapping]] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class AddColumnResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class DeleteColumnRequest(_message.Message):
    __slots__ = ("table_name", "column_name", "nonce_group", "nonce")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLUMN_NAME_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    column_name: bytes
    nonce_group: int
    nonce: int
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., column_name: _Optional[bytes] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class DeleteColumnResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class ModifyColumnRequest(_message.Message):
    __slots__ = ("table_name", "column_families", "nonce_group", "nonce")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLUMN_FAMILIES_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    column_families: _HBase_pb2.ColumnFamilySchema
    nonce_group: int
    nonce: int
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., column_families: _Optional[_Union[_HBase_pb2.ColumnFamilySchema, _Mapping]] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class ModifyColumnResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class MoveRegionRequest(_message.Message):
    __slots__ = ("region", "dest_server_name")
    REGION_FIELD_NUMBER: _ClassVar[int]
    DEST_SERVER_NAME_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    dest_server_name: _HBase_pb2.ServerName
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., dest_server_name: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ...) -> None: ...

class MoveRegionResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class MergeTableRegionsRequest(_message.Message):
    __slots__ = ("region", "forcible", "nonce_group", "nonce")
    REGION_FIELD_NUMBER: _ClassVar[int]
    FORCIBLE_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    region: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionSpecifier]
    forcible: bool
    nonce_group: int
    nonce: int
    def __init__(self, region: _Optional[_Iterable[_Union[_HBase_pb2.RegionSpecifier, _Mapping]]] = ..., forcible: bool = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class MergeTableRegionsResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class AssignRegionRequest(_message.Message):
    __slots__ = ("region", "override")
    REGION_FIELD_NUMBER: _ClassVar[int]
    OVERRIDE_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    override: bool
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., override: bool = ...) -> None: ...

class AssignRegionResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class UnassignRegionRequest(_message.Message):
    __slots__ = ("region", "force")
    REGION_FIELD_NUMBER: _ClassVar[int]
    FORCE_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    force: bool
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., force: bool = ...) -> None: ...

class UnassignRegionResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class OfflineRegionRequest(_message.Message):
    __slots__ = ("region",)
    REGION_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ...) -> None: ...

class OfflineRegionResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SplitTableRegionRequest(_message.Message):
    __slots__ = ("region_info", "split_row", "nonce_group", "nonce")
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    SPLIT_ROW_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    region_info: _HBase_pb2.RegionInfo
    split_row: bytes
    nonce_group: int
    nonce: int
    def __init__(self, region_info: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ..., split_row: _Optional[bytes] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class SplitTableRegionResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class CreateTableRequest(_message.Message):
    __slots__ = ("table_schema", "split_keys", "nonce_group", "nonce")
    TABLE_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    SPLIT_KEYS_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    table_schema: _HBase_pb2.TableSchema
    split_keys: _containers.RepeatedScalarFieldContainer[bytes]
    nonce_group: int
    nonce: int
    def __init__(self, table_schema: _Optional[_Union[_HBase_pb2.TableSchema, _Mapping]] = ..., split_keys: _Optional[_Iterable[bytes]] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class CreateTableResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class DeleteTableRequest(_message.Message):
    __slots__ = ("table_name", "nonce_group", "nonce")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    nonce_group: int
    nonce: int
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class DeleteTableResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class TruncateTableRequest(_message.Message):
    __slots__ = ("tableName", "preserveSplits", "nonce_group", "nonce")
    TABLENAME_FIELD_NUMBER: _ClassVar[int]
    PRESERVESPLITS_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    tableName: _HBase_pb2.TableName
    preserveSplits: bool
    nonce_group: int
    nonce: int
    def __init__(self, tableName: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., preserveSplits: bool = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class TruncateTableResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class EnableTableRequest(_message.Message):
    __slots__ = ("table_name", "nonce_group", "nonce")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    nonce_group: int
    nonce: int
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class EnableTableResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class DisableTableRequest(_message.Message):
    __slots__ = ("table_name", "nonce_group", "nonce")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    nonce_group: int
    nonce: int
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class DisableTableResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class ModifyTableRequest(_message.Message):
    __slots__ = ("table_name", "table_schema", "nonce_group", "nonce", "reopen_regions")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    REOPEN_REGIONS_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    table_schema: _HBase_pb2.TableSchema
    nonce_group: int
    nonce: int
    reopen_regions: bool
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., table_schema: _Optional[_Union[_HBase_pb2.TableSchema, _Mapping]] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ..., reopen_regions: bool = ...) -> None: ...

class ModifyTableResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class CreateNamespaceRequest(_message.Message):
    __slots__ = ("namespaceDescriptor", "nonce_group", "nonce")
    NAMESPACEDESCRIPTOR_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    namespaceDescriptor: _HBase_pb2.NamespaceDescriptor
    nonce_group: int
    nonce: int
    def __init__(self, namespaceDescriptor: _Optional[_Union[_HBase_pb2.NamespaceDescriptor, _Mapping]] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class CreateNamespaceResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class DeleteNamespaceRequest(_message.Message):
    __slots__ = ("namespaceName", "nonce_group", "nonce")
    NAMESPACENAME_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    namespaceName: str
    nonce_group: int
    nonce: int
    def __init__(self, namespaceName: _Optional[str] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class DeleteNamespaceResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class ModifyNamespaceRequest(_message.Message):
    __slots__ = ("namespaceDescriptor", "nonce_group", "nonce")
    NAMESPACEDESCRIPTOR_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    namespaceDescriptor: _HBase_pb2.NamespaceDescriptor
    nonce_group: int
    nonce: int
    def __init__(self, namespaceDescriptor: _Optional[_Union[_HBase_pb2.NamespaceDescriptor, _Mapping]] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class ModifyNamespaceResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class GetNamespaceDescriptorRequest(_message.Message):
    __slots__ = ("namespaceName",)
    NAMESPACENAME_FIELD_NUMBER: _ClassVar[int]
    namespaceName: str
    def __init__(self, namespaceName: _Optional[str] = ...) -> None: ...

class GetNamespaceDescriptorResponse(_message.Message):
    __slots__ = ("namespaceDescriptor",)
    NAMESPACEDESCRIPTOR_FIELD_NUMBER: _ClassVar[int]
    namespaceDescriptor: _HBase_pb2.NamespaceDescriptor
    def __init__(self, namespaceDescriptor: _Optional[_Union[_HBase_pb2.NamespaceDescriptor, _Mapping]] = ...) -> None: ...

class ListNamespacesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListNamespacesResponse(_message.Message):
    __slots__ = ("namespaceName",)
    NAMESPACENAME_FIELD_NUMBER: _ClassVar[int]
    namespaceName: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, namespaceName: _Optional[_Iterable[str]] = ...) -> None: ...

class ListNamespaceDescriptorsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListNamespaceDescriptorsResponse(_message.Message):
    __slots__ = ("namespaceDescriptor",)
    NAMESPACEDESCRIPTOR_FIELD_NUMBER: _ClassVar[int]
    namespaceDescriptor: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.NamespaceDescriptor]
    def __init__(self, namespaceDescriptor: _Optional[_Iterable[_Union[_HBase_pb2.NamespaceDescriptor, _Mapping]]] = ...) -> None: ...

class ListTableDescriptorsByNamespaceRequest(_message.Message):
    __slots__ = ("namespaceName",)
    NAMESPACENAME_FIELD_NUMBER: _ClassVar[int]
    namespaceName: str
    def __init__(self, namespaceName: _Optional[str] = ...) -> None: ...

class ListTableDescriptorsByNamespaceResponse(_message.Message):
    __slots__ = ("tableSchema",)
    TABLESCHEMA_FIELD_NUMBER: _ClassVar[int]
    tableSchema: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.TableSchema]
    def __init__(self, tableSchema: _Optional[_Iterable[_Union[_HBase_pb2.TableSchema, _Mapping]]] = ...) -> None: ...

class ListTableNamesByNamespaceRequest(_message.Message):
    __slots__ = ("namespaceName",)
    NAMESPACENAME_FIELD_NUMBER: _ClassVar[int]
    namespaceName: str
    def __init__(self, namespaceName: _Optional[str] = ...) -> None: ...

class ListTableNamesByNamespaceResponse(_message.Message):
    __slots__ = ("tableName",)
    TABLENAME_FIELD_NUMBER: _ClassVar[int]
    tableName: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.TableName]
    def __init__(self, tableName: _Optional[_Iterable[_Union[_HBase_pb2.TableName, _Mapping]]] = ...) -> None: ...

class ShutdownRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ShutdownResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class StopMasterRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class StopMasterResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class IsInMaintenanceModeRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class IsInMaintenanceModeResponse(_message.Message):
    __slots__ = ("inMaintenanceMode",)
    INMAINTENANCEMODE_FIELD_NUMBER: _ClassVar[int]
    inMaintenanceMode: bool
    def __init__(self, inMaintenanceMode: bool = ...) -> None: ...

class BalanceRequest(_message.Message):
    __slots__ = ("ignore_rit", "dry_run")
    IGNORE_RIT_FIELD_NUMBER: _ClassVar[int]
    DRY_RUN_FIELD_NUMBER: _ClassVar[int]
    ignore_rit: bool
    dry_run: bool
    def __init__(self, ignore_rit: bool = ..., dry_run: bool = ...) -> None: ...

class BalanceResponse(_message.Message):
    __slots__ = ("balancer_ran", "moves_calculated", "moves_executed")
    BALANCER_RAN_FIELD_NUMBER: _ClassVar[int]
    MOVES_CALCULATED_FIELD_NUMBER: _ClassVar[int]
    MOVES_EXECUTED_FIELD_NUMBER: _ClassVar[int]
    balancer_ran: bool
    moves_calculated: int
    moves_executed: int
    def __init__(self, balancer_ran: bool = ..., moves_calculated: _Optional[int] = ..., moves_executed: _Optional[int] = ...) -> None: ...

class SetBalancerRunningRequest(_message.Message):
    __slots__ = ("on", "synchronous")
    ON_FIELD_NUMBER: _ClassVar[int]
    SYNCHRONOUS_FIELD_NUMBER: _ClassVar[int]
    on: bool
    synchronous: bool
    def __init__(self, on: bool = ..., synchronous: bool = ...) -> None: ...

class SetBalancerRunningResponse(_message.Message):
    __slots__ = ("prev_balance_value",)
    PREV_BALANCE_VALUE_FIELD_NUMBER: _ClassVar[int]
    prev_balance_value: bool
    def __init__(self, prev_balance_value: bool = ...) -> None: ...

class IsBalancerEnabledRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class IsBalancerEnabledResponse(_message.Message):
    __slots__ = ("enabled",)
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    enabled: bool
    def __init__(self, enabled: bool = ...) -> None: ...

class SetSnapshotCleanupRequest(_message.Message):
    __slots__ = ("enabled", "synchronous")
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    SYNCHRONOUS_FIELD_NUMBER: _ClassVar[int]
    enabled: bool
    synchronous: bool
    def __init__(self, enabled: bool = ..., synchronous: bool = ...) -> None: ...

class SetSnapshotCleanupResponse(_message.Message):
    __slots__ = ("prev_snapshot_cleanup",)
    PREV_SNAPSHOT_CLEANUP_FIELD_NUMBER: _ClassVar[int]
    prev_snapshot_cleanup: bool
    def __init__(self, prev_snapshot_cleanup: bool = ...) -> None: ...

class IsSnapshotCleanupEnabledRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class IsSnapshotCleanupEnabledResponse(_message.Message):
    __slots__ = ("enabled",)
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    enabled: bool
    def __init__(self, enabled: bool = ...) -> None: ...

class SetSplitOrMergeEnabledRequest(_message.Message):
    __slots__ = ("enabled", "synchronous", "switch_types")
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    SYNCHRONOUS_FIELD_NUMBER: _ClassVar[int]
    SWITCH_TYPES_FIELD_NUMBER: _ClassVar[int]
    enabled: bool
    synchronous: bool
    switch_types: _containers.RepeatedScalarFieldContainer[MasterSwitchType]
    def __init__(self, enabled: bool = ..., synchronous: bool = ..., switch_types: _Optional[_Iterable[_Union[MasterSwitchType, str]]] = ...) -> None: ...

class SetSplitOrMergeEnabledResponse(_message.Message):
    __slots__ = ("prev_value",)
    PREV_VALUE_FIELD_NUMBER: _ClassVar[int]
    prev_value: _containers.RepeatedScalarFieldContainer[bool]
    def __init__(self, prev_value: _Optional[_Iterable[bool]] = ...) -> None: ...

class IsSplitOrMergeEnabledRequest(_message.Message):
    __slots__ = ("switch_type",)
    SWITCH_TYPE_FIELD_NUMBER: _ClassVar[int]
    switch_type: MasterSwitchType
    def __init__(self, switch_type: _Optional[_Union[MasterSwitchType, str]] = ...) -> None: ...

class IsSplitOrMergeEnabledResponse(_message.Message):
    __slots__ = ("enabled",)
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    enabled: bool
    def __init__(self, enabled: bool = ...) -> None: ...

class NormalizeRequest(_message.Message):
    __slots__ = ("table_names", "regex", "namespace")
    TABLE_NAMES_FIELD_NUMBER: _ClassVar[int]
    REGEX_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    table_names: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.TableName]
    regex: str
    namespace: str
    def __init__(self, table_names: _Optional[_Iterable[_Union[_HBase_pb2.TableName, _Mapping]]] = ..., regex: _Optional[str] = ..., namespace: _Optional[str] = ...) -> None: ...

class NormalizeResponse(_message.Message):
    __slots__ = ("normalizer_ran",)
    NORMALIZER_RAN_FIELD_NUMBER: _ClassVar[int]
    normalizer_ran: bool
    def __init__(self, normalizer_ran: bool = ...) -> None: ...

class SetNormalizerRunningRequest(_message.Message):
    __slots__ = ("on",)
    ON_FIELD_NUMBER: _ClassVar[int]
    on: bool
    def __init__(self, on: bool = ...) -> None: ...

class SetNormalizerRunningResponse(_message.Message):
    __slots__ = ("prev_normalizer_value",)
    PREV_NORMALIZER_VALUE_FIELD_NUMBER: _ClassVar[int]
    prev_normalizer_value: bool
    def __init__(self, prev_normalizer_value: bool = ...) -> None: ...

class IsNormalizerEnabledRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class IsNormalizerEnabledResponse(_message.Message):
    __slots__ = ("enabled",)
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    enabled: bool
    def __init__(self, enabled: bool = ...) -> None: ...

class RunHbckChoreRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RunHbckChoreResponse(_message.Message):
    __slots__ = ("ran",)
    RAN_FIELD_NUMBER: _ClassVar[int]
    ran: bool
    def __init__(self, ran: bool = ...) -> None: ...

class RunCatalogScanRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RunCatalogScanResponse(_message.Message):
    __slots__ = ("scan_result",)
    SCAN_RESULT_FIELD_NUMBER: _ClassVar[int]
    scan_result: int
    def __init__(self, scan_result: _Optional[int] = ...) -> None: ...

class EnableCatalogJanitorRequest(_message.Message):
    __slots__ = ("enable",)
    ENABLE_FIELD_NUMBER: _ClassVar[int]
    enable: bool
    def __init__(self, enable: bool = ...) -> None: ...

class EnableCatalogJanitorResponse(_message.Message):
    __slots__ = ("prev_value",)
    PREV_VALUE_FIELD_NUMBER: _ClassVar[int]
    prev_value: bool
    def __init__(self, prev_value: bool = ...) -> None: ...

class IsCatalogJanitorEnabledRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class IsCatalogJanitorEnabledResponse(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bool
    def __init__(self, value: bool = ...) -> None: ...

class RunCleanerChoreRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RunCleanerChoreResponse(_message.Message):
    __slots__ = ("cleaner_chore_ran",)
    CLEANER_CHORE_RAN_FIELD_NUMBER: _ClassVar[int]
    cleaner_chore_ran: bool
    def __init__(self, cleaner_chore_ran: bool = ...) -> None: ...

class SetCleanerChoreRunningRequest(_message.Message):
    __slots__ = ("on",)
    ON_FIELD_NUMBER: _ClassVar[int]
    on: bool
    def __init__(self, on: bool = ...) -> None: ...

class SetCleanerChoreRunningResponse(_message.Message):
    __slots__ = ("prev_value",)
    PREV_VALUE_FIELD_NUMBER: _ClassVar[int]
    prev_value: bool
    def __init__(self, prev_value: bool = ...) -> None: ...

class IsCleanerChoreEnabledRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class IsCleanerChoreEnabledResponse(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bool
    def __init__(self, value: bool = ...) -> None: ...

class SnapshotRequest(_message.Message):
    __slots__ = ("snapshot",)
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    snapshot: _Snapshot_pb2.SnapshotDescription
    def __init__(self, snapshot: _Optional[_Union[_Snapshot_pb2.SnapshotDescription, _Mapping]] = ...) -> None: ...

class SnapshotResponse(_message.Message):
    __slots__ = ("expected_timeout",)
    EXPECTED_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    expected_timeout: int
    def __init__(self, expected_timeout: _Optional[int] = ...) -> None: ...

class GetCompletedSnapshotsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetCompletedSnapshotsResponse(_message.Message):
    __slots__ = ("snapshots",)
    SNAPSHOTS_FIELD_NUMBER: _ClassVar[int]
    snapshots: _containers.RepeatedCompositeFieldContainer[_Snapshot_pb2.SnapshotDescription]
    def __init__(self, snapshots: _Optional[_Iterable[_Union[_Snapshot_pb2.SnapshotDescription, _Mapping]]] = ...) -> None: ...

class DeleteSnapshotRequest(_message.Message):
    __slots__ = ("snapshot",)
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    snapshot: _Snapshot_pb2.SnapshotDescription
    def __init__(self, snapshot: _Optional[_Union[_Snapshot_pb2.SnapshotDescription, _Mapping]] = ...) -> None: ...

class DeleteSnapshotResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RestoreSnapshotRequest(_message.Message):
    __slots__ = ("snapshot", "nonce_group", "nonce", "restoreACL", "customSFT")
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    RESTOREACL_FIELD_NUMBER: _ClassVar[int]
    CUSTOMSFT_FIELD_NUMBER: _ClassVar[int]
    snapshot: _Snapshot_pb2.SnapshotDescription
    nonce_group: int
    nonce: int
    restoreACL: bool
    customSFT: str
    def __init__(self, snapshot: _Optional[_Union[_Snapshot_pb2.SnapshotDescription, _Mapping]] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ..., restoreACL: bool = ..., customSFT: _Optional[str] = ...) -> None: ...

class RestoreSnapshotResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class IsSnapshotDoneRequest(_message.Message):
    __slots__ = ("snapshot",)
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    snapshot: _Snapshot_pb2.SnapshotDescription
    def __init__(self, snapshot: _Optional[_Union[_Snapshot_pb2.SnapshotDescription, _Mapping]] = ...) -> None: ...

class IsSnapshotDoneResponse(_message.Message):
    __slots__ = ("done", "snapshot")
    DONE_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    done: bool
    snapshot: _Snapshot_pb2.SnapshotDescription
    def __init__(self, done: bool = ..., snapshot: _Optional[_Union[_Snapshot_pb2.SnapshotDescription, _Mapping]] = ...) -> None: ...

class IsRestoreSnapshotDoneRequest(_message.Message):
    __slots__ = ("snapshot",)
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    snapshot: _Snapshot_pb2.SnapshotDescription
    def __init__(self, snapshot: _Optional[_Union[_Snapshot_pb2.SnapshotDescription, _Mapping]] = ...) -> None: ...

class IsRestoreSnapshotDoneResponse(_message.Message):
    __slots__ = ("done",)
    DONE_FIELD_NUMBER: _ClassVar[int]
    done: bool
    def __init__(self, done: bool = ...) -> None: ...

class GetSchemaAlterStatusRequest(_message.Message):
    __slots__ = ("table_name",)
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ...) -> None: ...

class GetSchemaAlterStatusResponse(_message.Message):
    __slots__ = ("yet_to_update_regions", "total_regions")
    YET_TO_UPDATE_REGIONS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_REGIONS_FIELD_NUMBER: _ClassVar[int]
    yet_to_update_regions: int
    total_regions: int
    def __init__(self, yet_to_update_regions: _Optional[int] = ..., total_regions: _Optional[int] = ...) -> None: ...

class GetTableDescriptorsRequest(_message.Message):
    __slots__ = ("table_names", "regex", "include_sys_tables", "namespace")
    TABLE_NAMES_FIELD_NUMBER: _ClassVar[int]
    REGEX_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_SYS_TABLES_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    table_names: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.TableName]
    regex: str
    include_sys_tables: bool
    namespace: str
    def __init__(self, table_names: _Optional[_Iterable[_Union[_HBase_pb2.TableName, _Mapping]]] = ..., regex: _Optional[str] = ..., include_sys_tables: bool = ..., namespace: _Optional[str] = ...) -> None: ...

class GetTableDescriptorsResponse(_message.Message):
    __slots__ = ("table_schema",)
    TABLE_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    table_schema: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.TableSchema]
    def __init__(self, table_schema: _Optional[_Iterable[_Union[_HBase_pb2.TableSchema, _Mapping]]] = ...) -> None: ...

class GetTableNamesRequest(_message.Message):
    __slots__ = ("regex", "include_sys_tables", "namespace")
    REGEX_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_SYS_TABLES_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    regex: str
    include_sys_tables: bool
    namespace: str
    def __init__(self, regex: _Optional[str] = ..., include_sys_tables: bool = ..., namespace: _Optional[str] = ...) -> None: ...

class GetTableNamesResponse(_message.Message):
    __slots__ = ("table_names",)
    TABLE_NAMES_FIELD_NUMBER: _ClassVar[int]
    table_names: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.TableName]
    def __init__(self, table_names: _Optional[_Iterable[_Union[_HBase_pb2.TableName, _Mapping]]] = ...) -> None: ...

class GetTableStateRequest(_message.Message):
    __slots__ = ("table_name",)
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ...) -> None: ...

class GetTableStateResponse(_message.Message):
    __slots__ = ("table_state",)
    TABLE_STATE_FIELD_NUMBER: _ClassVar[int]
    table_state: _HBase_pb2.TableState
    def __init__(self, table_state: _Optional[_Union[_HBase_pb2.TableState, _Mapping]] = ...) -> None: ...

class GetClusterStatusRequest(_message.Message):
    __slots__ = ("options",)
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    options: _containers.RepeatedScalarFieldContainer[_ClusterStatus_pb2.Option]
    def __init__(self, options: _Optional[_Iterable[_Union[_ClusterStatus_pb2.Option, str]]] = ...) -> None: ...

class GetClusterStatusResponse(_message.Message):
    __slots__ = ("cluster_status",)
    CLUSTER_STATUS_FIELD_NUMBER: _ClassVar[int]
    cluster_status: _ClusterStatus_pb2.ClusterStatus
    def __init__(self, cluster_status: _Optional[_Union[_ClusterStatus_pb2.ClusterStatus, _Mapping]] = ...) -> None: ...

class IsMasterRunningRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class IsMasterRunningResponse(_message.Message):
    __slots__ = ("is_master_running",)
    IS_MASTER_RUNNING_FIELD_NUMBER: _ClassVar[int]
    is_master_running: bool
    def __init__(self, is_master_running: bool = ...) -> None: ...

class ExecProcedureRequest(_message.Message):
    __slots__ = ("procedure",)
    PROCEDURE_FIELD_NUMBER: _ClassVar[int]
    procedure: _HBase_pb2.ProcedureDescription
    def __init__(self, procedure: _Optional[_Union[_HBase_pb2.ProcedureDescription, _Mapping]] = ...) -> None: ...

class ExecProcedureResponse(_message.Message):
    __slots__ = ("expected_timeout", "return_data")
    EXPECTED_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    RETURN_DATA_FIELD_NUMBER: _ClassVar[int]
    expected_timeout: int
    return_data: bytes
    def __init__(self, expected_timeout: _Optional[int] = ..., return_data: _Optional[bytes] = ...) -> None: ...

class IsProcedureDoneRequest(_message.Message):
    __slots__ = ("procedure",)
    PROCEDURE_FIELD_NUMBER: _ClassVar[int]
    procedure: _HBase_pb2.ProcedureDescription
    def __init__(self, procedure: _Optional[_Union[_HBase_pb2.ProcedureDescription, _Mapping]] = ...) -> None: ...

class IsProcedureDoneResponse(_message.Message):
    __slots__ = ("done", "snapshot")
    DONE_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    done: bool
    snapshot: _HBase_pb2.ProcedureDescription
    def __init__(self, done: bool = ..., snapshot: _Optional[_Union[_HBase_pb2.ProcedureDescription, _Mapping]] = ...) -> None: ...

class GetProcedureResultRequest(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class GetProcedureResultResponse(_message.Message):
    __slots__ = ("state", "submitted_time", "last_update", "result", "exception")
    class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        NOT_FOUND: _ClassVar[GetProcedureResultResponse.State]
        RUNNING: _ClassVar[GetProcedureResultResponse.State]
        FINISHED: _ClassVar[GetProcedureResultResponse.State]
    NOT_FOUND: GetProcedureResultResponse.State
    RUNNING: GetProcedureResultResponse.State
    FINISHED: GetProcedureResultResponse.State
    STATE_FIELD_NUMBER: _ClassVar[int]
    SUBMITTED_TIME_FIELD_NUMBER: _ClassVar[int]
    LAST_UPDATE_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    EXCEPTION_FIELD_NUMBER: _ClassVar[int]
    state: GetProcedureResultResponse.State
    submitted_time: int
    last_update: int
    result: bytes
    exception: _ErrorHandling_pb2.ForeignExceptionMessage
    def __init__(self, state: _Optional[_Union[GetProcedureResultResponse.State, str]] = ..., submitted_time: _Optional[int] = ..., last_update: _Optional[int] = ..., result: _Optional[bytes] = ..., exception: _Optional[_Union[_ErrorHandling_pb2.ForeignExceptionMessage, _Mapping]] = ...) -> None: ...

class AbortProcedureRequest(_message.Message):
    __slots__ = ("proc_id", "mayInterruptIfRunning")
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    MAYINTERRUPTIFRUNNING_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    mayInterruptIfRunning: bool
    def __init__(self, proc_id: _Optional[int] = ..., mayInterruptIfRunning: bool = ...) -> None: ...

class AbortProcedureResponse(_message.Message):
    __slots__ = ("is_procedure_aborted",)
    IS_PROCEDURE_ABORTED_FIELD_NUMBER: _ClassVar[int]
    is_procedure_aborted: bool
    def __init__(self, is_procedure_aborted: bool = ...) -> None: ...

class GetProceduresRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetProceduresResponse(_message.Message):
    __slots__ = ("procedure",)
    PROCEDURE_FIELD_NUMBER: _ClassVar[int]
    procedure: _containers.RepeatedCompositeFieldContainer[_Procedure_pb2.Procedure]
    def __init__(self, procedure: _Optional[_Iterable[_Union[_Procedure_pb2.Procedure, _Mapping]]] = ...) -> None: ...

class GetLocksRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetLocksResponse(_message.Message):
    __slots__ = ("lock",)
    LOCK_FIELD_NUMBER: _ClassVar[int]
    lock: _containers.RepeatedCompositeFieldContainer[_LockService_pb2.LockedResource]
    def __init__(self, lock: _Optional[_Iterable[_Union[_LockService_pb2.LockedResource, _Mapping]]] = ...) -> None: ...

class SetQuotaRequest(_message.Message):
    __slots__ = ("user_name", "user_group", "namespace", "table_name", "remove_all", "bypass_globals", "throttle", "space_limit", "region_server")
    USER_NAME_FIELD_NUMBER: _ClassVar[int]
    USER_GROUP_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    REMOVE_ALL_FIELD_NUMBER: _ClassVar[int]
    BYPASS_GLOBALS_FIELD_NUMBER: _ClassVar[int]
    THROTTLE_FIELD_NUMBER: _ClassVar[int]
    SPACE_LIMIT_FIELD_NUMBER: _ClassVar[int]
    REGION_SERVER_FIELD_NUMBER: _ClassVar[int]
    user_name: str
    user_group: str
    namespace: str
    table_name: _HBase_pb2.TableName
    remove_all: bool
    bypass_globals: bool
    throttle: _Quota_pb2.ThrottleRequest
    space_limit: _Quota_pb2.SpaceLimitRequest
    region_server: str
    def __init__(self, user_name: _Optional[str] = ..., user_group: _Optional[str] = ..., namespace: _Optional[str] = ..., table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., remove_all: bool = ..., bypass_globals: bool = ..., throttle: _Optional[_Union[_Quota_pb2.ThrottleRequest, _Mapping]] = ..., space_limit: _Optional[_Union[_Quota_pb2.SpaceLimitRequest, _Mapping]] = ..., region_server: _Optional[str] = ...) -> None: ...

class SetQuotaResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class MajorCompactionTimestampRequest(_message.Message):
    __slots__ = ("table_name",)
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ...) -> None: ...

class MajorCompactionTimestampForRegionRequest(_message.Message):
    __slots__ = ("region",)
    REGION_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionSpecifier
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ...) -> None: ...

class MajorCompactionTimestampResponse(_message.Message):
    __slots__ = ("compaction_timestamp",)
    COMPACTION_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    compaction_timestamp: int
    def __init__(self, compaction_timestamp: _Optional[int] = ...) -> None: ...

class SecurityCapabilitiesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SecurityCapabilitiesResponse(_message.Message):
    __slots__ = ("capabilities",)
    class Capability(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SIMPLE_AUTHENTICATION: _ClassVar[SecurityCapabilitiesResponse.Capability]
        SECURE_AUTHENTICATION: _ClassVar[SecurityCapabilitiesResponse.Capability]
        AUTHORIZATION: _ClassVar[SecurityCapabilitiesResponse.Capability]
        CELL_AUTHORIZATION: _ClassVar[SecurityCapabilitiesResponse.Capability]
        CELL_VISIBILITY: _ClassVar[SecurityCapabilitiesResponse.Capability]
    SIMPLE_AUTHENTICATION: SecurityCapabilitiesResponse.Capability
    SECURE_AUTHENTICATION: SecurityCapabilitiesResponse.Capability
    AUTHORIZATION: SecurityCapabilitiesResponse.Capability
    CELL_AUTHORIZATION: SecurityCapabilitiesResponse.Capability
    CELL_VISIBILITY: SecurityCapabilitiesResponse.Capability
    CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    capabilities: _containers.RepeatedScalarFieldContainer[SecurityCapabilitiesResponse.Capability]
    def __init__(self, capabilities: _Optional[_Iterable[_Union[SecurityCapabilitiesResponse.Capability, str]]] = ...) -> None: ...

class ListDecommissionedRegionServersRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListDecommissionedRegionServersResponse(_message.Message):
    __slots__ = ("server_name",)
    SERVER_NAME_FIELD_NUMBER: _ClassVar[int]
    server_name: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.ServerName]
    def __init__(self, server_name: _Optional[_Iterable[_Union[_HBase_pb2.ServerName, _Mapping]]] = ...) -> None: ...

class DecommissionRegionServersRequest(_message.Message):
    __slots__ = ("server_name", "offload")
    SERVER_NAME_FIELD_NUMBER: _ClassVar[int]
    OFFLOAD_FIELD_NUMBER: _ClassVar[int]
    server_name: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.ServerName]
    offload: bool
    def __init__(self, server_name: _Optional[_Iterable[_Union[_HBase_pb2.ServerName, _Mapping]]] = ..., offload: bool = ...) -> None: ...

class DecommissionRegionServersResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RecommissionRegionServerRequest(_message.Message):
    __slots__ = ("server_name", "region")
    SERVER_NAME_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    server_name: _HBase_pb2.ServerName
    region: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionSpecifier]
    def __init__(self, server_name: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., region: _Optional[_Iterable[_Union[_HBase_pb2.RegionSpecifier, _Mapping]]] = ...) -> None: ...

class RecommissionRegionServerResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ClearDeadServersRequest(_message.Message):
    __slots__ = ("server_name",)
    SERVER_NAME_FIELD_NUMBER: _ClassVar[int]
    server_name: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.ServerName]
    def __init__(self, server_name: _Optional[_Iterable[_Union[_HBase_pb2.ServerName, _Mapping]]] = ...) -> None: ...

class ClearDeadServersResponse(_message.Message):
    __slots__ = ("server_name",)
    SERVER_NAME_FIELD_NUMBER: _ClassVar[int]
    server_name: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.ServerName]
    def __init__(self, server_name: _Optional[_Iterable[_Union[_HBase_pb2.ServerName, _Mapping]]] = ...) -> None: ...

class SwitchRpcThrottleRequest(_message.Message):
    __slots__ = ("rpc_throttle_enabled",)
    RPC_THROTTLE_ENABLED_FIELD_NUMBER: _ClassVar[int]
    rpc_throttle_enabled: bool
    def __init__(self, rpc_throttle_enabled: bool = ...) -> None: ...

class SwitchRpcThrottleResponse(_message.Message):
    __slots__ = ("previous_rpc_throttle_enabled",)
    PREVIOUS_RPC_THROTTLE_ENABLED_FIELD_NUMBER: _ClassVar[int]
    previous_rpc_throttle_enabled: bool
    def __init__(self, previous_rpc_throttle_enabled: bool = ...) -> None: ...

class IsRpcThrottleEnabledRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class IsRpcThrottleEnabledResponse(_message.Message):
    __slots__ = ("rpc_throttle_enabled",)
    RPC_THROTTLE_ENABLED_FIELD_NUMBER: _ClassVar[int]
    rpc_throttle_enabled: bool
    def __init__(self, rpc_throttle_enabled: bool = ...) -> None: ...

class SwitchExceedThrottleQuotaRequest(_message.Message):
    __slots__ = ("exceed_throttle_quota_enabled",)
    EXCEED_THROTTLE_QUOTA_ENABLED_FIELD_NUMBER: _ClassVar[int]
    exceed_throttle_quota_enabled: bool
    def __init__(self, exceed_throttle_quota_enabled: bool = ...) -> None: ...

class SwitchExceedThrottleQuotaResponse(_message.Message):
    __slots__ = ("previous_exceed_throttle_quota_enabled",)
    PREVIOUS_EXCEED_THROTTLE_QUOTA_ENABLED_FIELD_NUMBER: _ClassVar[int]
    previous_exceed_throttle_quota_enabled: bool
    def __init__(self, previous_exceed_throttle_quota_enabled: bool = ...) -> None: ...

class BalancerDecisionsRequest(_message.Message):
    __slots__ = ("limit",)
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    limit: int
    def __init__(self, limit: _Optional[int] = ...) -> None: ...

class BalancerRejectionsRequest(_message.Message):
    __slots__ = ("limit",)
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    limit: int
    def __init__(self, limit: _Optional[int] = ...) -> None: ...

class BalancerDecisionsResponse(_message.Message):
    __slots__ = ("balancer_decision",)
    BALANCER_DECISION_FIELD_NUMBER: _ClassVar[int]
    balancer_decision: _containers.RepeatedCompositeFieldContainer[_RecentLogs_pb2.BalancerDecision]
    def __init__(self, balancer_decision: _Optional[_Iterable[_Union[_RecentLogs_pb2.BalancerDecision, _Mapping]]] = ...) -> None: ...

class BalancerRejectionsResponse(_message.Message):
    __slots__ = ("balancer_rejection",)
    BALANCER_REJECTION_FIELD_NUMBER: _ClassVar[int]
    balancer_rejection: _containers.RepeatedCompositeFieldContainer[_RecentLogs_pb2.BalancerRejection]
    def __init__(self, balancer_rejection: _Optional[_Iterable[_Union[_RecentLogs_pb2.BalancerRejection, _Mapping]]] = ...) -> None: ...

class ModifyTableStoreFileTrackerRequest(_message.Message):
    __slots__ = ("table_Name", "dst_sft", "nonce_group", "nonce")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    DST_SFT_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    table_Name: _HBase_pb2.TableName
    dst_sft: str
    nonce_group: int
    nonce: int
    def __init__(self, table_Name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., dst_sft: _Optional[str] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class ModifyTableStoreFileTrackerResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class ModifyColumnStoreFileTrackerRequest(_message.Message):
    __slots__ = ("table_Name", "family", "dst_sft", "nonce_group", "nonce")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    FAMILY_FIELD_NUMBER: _ClassVar[int]
    DST_SFT_FIELD_NUMBER: _ClassVar[int]
    NONCE_GROUP_FIELD_NUMBER: _ClassVar[int]
    NONCE_FIELD_NUMBER: _ClassVar[int]
    table_Name: _HBase_pb2.TableName
    family: bytes
    dst_sft: str
    nonce_group: int
    nonce: int
    def __init__(self, table_Name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., family: _Optional[bytes] = ..., dst_sft: _Optional[str] = ..., nonce_group: _Optional[int] = ..., nonce: _Optional[int] = ...) -> None: ...

class ModifyColumnStoreFileTrackerResponse(_message.Message):
    __slots__ = ("proc_id",)
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    proc_id: int
    def __init__(self, proc_id: _Optional[int] = ...) -> None: ...

class FlushMasterStoreRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class FlushMasterStoreResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SetTableStateInMetaRequest(_message.Message):
    __slots__ = ("table_name", "table_state")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_STATE_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    table_state: _HBase_pb2.TableState
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., table_state: _Optional[_Union[_HBase_pb2.TableState, _Mapping]] = ...) -> None: ...

class RegionSpecifierAndState(_message.Message):
    __slots__ = ("region_specifier", "state")
    REGION_SPECIFIER_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    region_specifier: _HBase_pb2.RegionSpecifier
    state: _ClusterStatus_pb2.RegionState.State
    def __init__(self, region_specifier: _Optional[_Union[_HBase_pb2.RegionSpecifier, _Mapping]] = ..., state: _Optional[_Union[_ClusterStatus_pb2.RegionState.State, str]] = ...) -> None: ...

class SetRegionStateInMetaRequest(_message.Message):
    __slots__ = ("states",)
    STATES_FIELD_NUMBER: _ClassVar[int]
    states: _containers.RepeatedCompositeFieldContainer[RegionSpecifierAndState]
    def __init__(self, states: _Optional[_Iterable[_Union[RegionSpecifierAndState, _Mapping]]] = ...) -> None: ...

class SetRegionStateInMetaResponse(_message.Message):
    __slots__ = ("states",)
    STATES_FIELD_NUMBER: _ClassVar[int]
    states: _containers.RepeatedCompositeFieldContainer[RegionSpecifierAndState]
    def __init__(self, states: _Optional[_Iterable[_Union[RegionSpecifierAndState, _Mapping]]] = ...) -> None: ...

class AssignsRequest(_message.Message):
    __slots__ = ("region", "override")
    REGION_FIELD_NUMBER: _ClassVar[int]
    OVERRIDE_FIELD_NUMBER: _ClassVar[int]
    region: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionSpecifier]
    override: bool
    def __init__(self, region: _Optional[_Iterable[_Union[_HBase_pb2.RegionSpecifier, _Mapping]]] = ..., override: bool = ...) -> None: ...

class AssignsResponse(_message.Message):
    __slots__ = ("pid",)
    PID_FIELD_NUMBER: _ClassVar[int]
    pid: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, pid: _Optional[_Iterable[int]] = ...) -> None: ...

class UnassignsRequest(_message.Message):
    __slots__ = ("region", "override")
    REGION_FIELD_NUMBER: _ClassVar[int]
    OVERRIDE_FIELD_NUMBER: _ClassVar[int]
    region: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionSpecifier]
    override: bool
    def __init__(self, region: _Optional[_Iterable[_Union[_HBase_pb2.RegionSpecifier, _Mapping]]] = ..., override: bool = ...) -> None: ...

class UnassignsResponse(_message.Message):
    __slots__ = ("pid",)
    PID_FIELD_NUMBER: _ClassVar[int]
    pid: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, pid: _Optional[_Iterable[int]] = ...) -> None: ...

class BypassProcedureRequest(_message.Message):
    __slots__ = ("proc_id", "waitTime", "override", "recursive")
    PROC_ID_FIELD_NUMBER: _ClassVar[int]
    WAITTIME_FIELD_NUMBER: _ClassVar[int]
    OVERRIDE_FIELD_NUMBER: _ClassVar[int]
    RECURSIVE_FIELD_NUMBER: _ClassVar[int]
    proc_id: _containers.RepeatedScalarFieldContainer[int]
    waitTime: int
    override: bool
    recursive: bool
    def __init__(self, proc_id: _Optional[_Iterable[int]] = ..., waitTime: _Optional[int] = ..., override: bool = ..., recursive: bool = ...) -> None: ...

class BypassProcedureResponse(_message.Message):
    __slots__ = ("bypassed",)
    BYPASSED_FIELD_NUMBER: _ClassVar[int]
    bypassed: _containers.RepeatedScalarFieldContainer[bool]
    def __init__(self, bypassed: _Optional[_Iterable[bool]] = ...) -> None: ...

class ScheduleServerCrashProcedureRequest(_message.Message):
    __slots__ = ("serverName",)
    SERVERNAME_FIELD_NUMBER: _ClassVar[int]
    serverName: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.ServerName]
    def __init__(self, serverName: _Optional[_Iterable[_Union[_HBase_pb2.ServerName, _Mapping]]] = ...) -> None: ...

class ScheduleServerCrashProcedureResponse(_message.Message):
    __slots__ = ("pid",)
    PID_FIELD_NUMBER: _ClassVar[int]
    pid: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, pid: _Optional[_Iterable[int]] = ...) -> None: ...

class ScheduleSCPsForUnknownServersRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ScheduleSCPsForUnknownServersResponse(_message.Message):
    __slots__ = ("pid",)
    PID_FIELD_NUMBER: _ClassVar[int]
    pid: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, pid: _Optional[_Iterable[int]] = ...) -> None: ...

class FixMetaRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class FixMetaResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
