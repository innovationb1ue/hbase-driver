import HBase_pb2 as _HBase_pb2
import RPC_pb2 as _RPC_pb2
import Snapshot_pb2 as _Snapshot_pb2
import Replication_pb2 as _Replication_pb2
import RegionServerStatus_pb2 as _RegionServerStatus_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateTableState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CREATE_TABLE_PRE_OPERATION: _ClassVar[CreateTableState]
    CREATE_TABLE_WRITE_FS_LAYOUT: _ClassVar[CreateTableState]
    CREATE_TABLE_ADD_TO_META: _ClassVar[CreateTableState]
    CREATE_TABLE_ASSIGN_REGIONS: _ClassVar[CreateTableState]
    CREATE_TABLE_UPDATE_DESC_CACHE: _ClassVar[CreateTableState]
    CREATE_TABLE_POST_OPERATION: _ClassVar[CreateTableState]

class ModifyTableState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MODIFY_TABLE_PREPARE: _ClassVar[ModifyTableState]
    MODIFY_TABLE_PRE_OPERATION: _ClassVar[ModifyTableState]
    MODIFY_TABLE_UPDATE_TABLE_DESCRIPTOR: _ClassVar[ModifyTableState]
    MODIFY_TABLE_REMOVE_REPLICA_COLUMN: _ClassVar[ModifyTableState]
    MODIFY_TABLE_DELETE_FS_LAYOUT: _ClassVar[ModifyTableState]
    MODIFY_TABLE_POST_OPERATION: _ClassVar[ModifyTableState]
    MODIFY_TABLE_REOPEN_ALL_REGIONS: _ClassVar[ModifyTableState]
    MODIFY_TABLE_CLOSE_EXCESS_REPLICAS: _ClassVar[ModifyTableState]
    MODIFY_TABLE_ASSIGN_NEW_REPLICAS: _ClassVar[ModifyTableState]

class TruncateTableState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TRUNCATE_TABLE_PRE_OPERATION: _ClassVar[TruncateTableState]
    TRUNCATE_TABLE_REMOVE_FROM_META: _ClassVar[TruncateTableState]
    TRUNCATE_TABLE_CLEAR_FS_LAYOUT: _ClassVar[TruncateTableState]
    TRUNCATE_TABLE_CREATE_FS_LAYOUT: _ClassVar[TruncateTableState]
    TRUNCATE_TABLE_ADD_TO_META: _ClassVar[TruncateTableState]
    TRUNCATE_TABLE_ASSIGN_REGIONS: _ClassVar[TruncateTableState]
    TRUNCATE_TABLE_POST_OPERATION: _ClassVar[TruncateTableState]

class DeleteTableState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DELETE_TABLE_PRE_OPERATION: _ClassVar[DeleteTableState]
    DELETE_TABLE_REMOVE_FROM_META: _ClassVar[DeleteTableState]
    DELETE_TABLE_CLEAR_FS_LAYOUT: _ClassVar[DeleteTableState]
    DELETE_TABLE_UPDATE_DESC_CACHE: _ClassVar[DeleteTableState]
    DELETE_TABLE_UNASSIGN_REGIONS: _ClassVar[DeleteTableState]
    DELETE_TABLE_POST_OPERATION: _ClassVar[DeleteTableState]

class CreateNamespaceState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CREATE_NAMESPACE_PREPARE: _ClassVar[CreateNamespaceState]
    CREATE_NAMESPACE_CREATE_DIRECTORY: _ClassVar[CreateNamespaceState]
    CREATE_NAMESPACE_INSERT_INTO_NS_TABLE: _ClassVar[CreateNamespaceState]
    CREATE_NAMESPACE_UPDATE_ZK: _ClassVar[CreateNamespaceState]
    CREATE_NAMESPACE_SET_NAMESPACE_QUOTA: _ClassVar[CreateNamespaceState]

class ModifyNamespaceState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MODIFY_NAMESPACE_PREPARE: _ClassVar[ModifyNamespaceState]
    MODIFY_NAMESPACE_UPDATE_NS_TABLE: _ClassVar[ModifyNamespaceState]
    MODIFY_NAMESPACE_UPDATE_ZK: _ClassVar[ModifyNamespaceState]

class DeleteNamespaceState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DELETE_NAMESPACE_PREPARE: _ClassVar[DeleteNamespaceState]
    DELETE_NAMESPACE_DELETE_FROM_NS_TABLE: _ClassVar[DeleteNamespaceState]
    DELETE_NAMESPACE_REMOVE_FROM_ZK: _ClassVar[DeleteNamespaceState]
    DELETE_NAMESPACE_DELETE_DIRECTORIES: _ClassVar[DeleteNamespaceState]
    DELETE_NAMESPACE_REMOVE_NAMESPACE_QUOTA: _ClassVar[DeleteNamespaceState]

class EnableTableState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ENABLE_TABLE_PREPARE: _ClassVar[EnableTableState]
    ENABLE_TABLE_PRE_OPERATION: _ClassVar[EnableTableState]
    ENABLE_TABLE_SET_ENABLING_TABLE_STATE: _ClassVar[EnableTableState]
    ENABLE_TABLE_MARK_REGIONS_ONLINE: _ClassVar[EnableTableState]
    ENABLE_TABLE_SET_ENABLED_TABLE_STATE: _ClassVar[EnableTableState]
    ENABLE_TABLE_POST_OPERATION: _ClassVar[EnableTableState]

class DisableTableState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DISABLE_TABLE_PREPARE: _ClassVar[DisableTableState]
    DISABLE_TABLE_PRE_OPERATION: _ClassVar[DisableTableState]
    DISABLE_TABLE_SET_DISABLING_TABLE_STATE: _ClassVar[DisableTableState]
    DISABLE_TABLE_MARK_REGIONS_OFFLINE: _ClassVar[DisableTableState]
    DISABLE_TABLE_SET_DISABLED_TABLE_STATE: _ClassVar[DisableTableState]
    DISABLE_TABLE_POST_OPERATION: _ClassVar[DisableTableState]
    DISABLE_TABLE_ADD_REPLICATION_BARRIER: _ClassVar[DisableTableState]

class CloneSnapshotState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CLONE_SNAPSHOT_PRE_OPERATION: _ClassVar[CloneSnapshotState]
    CLONE_SNAPSHOT_WRITE_FS_LAYOUT: _ClassVar[CloneSnapshotState]
    CLONE_SNAPSHOT_ADD_TO_META: _ClassVar[CloneSnapshotState]
    CLONE_SNAPSHOT_ASSIGN_REGIONS: _ClassVar[CloneSnapshotState]
    CLONE_SNAPSHOT_UPDATE_DESC_CACHE: _ClassVar[CloneSnapshotState]
    CLONE_SNAPSHOT_POST_OPERATION: _ClassVar[CloneSnapshotState]
    CLONE_SNAPHOST_RESTORE_ACL: _ClassVar[CloneSnapshotState]

class RestoreSnapshotState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RESTORE_SNAPSHOT_PRE_OPERATION: _ClassVar[RestoreSnapshotState]
    RESTORE_SNAPSHOT_UPDATE_TABLE_DESCRIPTOR: _ClassVar[RestoreSnapshotState]
    RESTORE_SNAPSHOT_WRITE_FS_LAYOUT: _ClassVar[RestoreSnapshotState]
    RESTORE_SNAPSHOT_UPDATE_META: _ClassVar[RestoreSnapshotState]
    RESTORE_SNAPSHOT_RESTORE_ACL: _ClassVar[RestoreSnapshotState]

class DispatchMergingRegionsState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DISPATCH_MERGING_REGIONS_PREPARE: _ClassVar[DispatchMergingRegionsState]
    DISPATCH_MERGING_REGIONS_PRE_OPERATION: _ClassVar[DispatchMergingRegionsState]
    DISPATCH_MERGING_REGIONS_MOVE_REGION_TO_SAME_RS: _ClassVar[DispatchMergingRegionsState]
    DISPATCH_MERGING_REGIONS_DO_MERGE_IN_RS: _ClassVar[DispatchMergingRegionsState]
    DISPATCH_MERGING_REGIONS_POST_OPERATION: _ClassVar[DispatchMergingRegionsState]

class SplitTableRegionState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SPLIT_TABLE_REGION_PREPARE: _ClassVar[SplitTableRegionState]
    SPLIT_TABLE_REGION_PRE_OPERATION: _ClassVar[SplitTableRegionState]
    SPLIT_TABLE_REGION_CLOSE_PARENT_REGION: _ClassVar[SplitTableRegionState]
    SPLIT_TABLE_REGION_CREATE_DAUGHTER_REGIONS: _ClassVar[SplitTableRegionState]
    SPLIT_TABLE_REGION_WRITE_MAX_SEQUENCE_ID_FILE: _ClassVar[SplitTableRegionState]
    SPLIT_TABLE_REGION_PRE_OPERATION_BEFORE_META: _ClassVar[SplitTableRegionState]
    SPLIT_TABLE_REGION_UPDATE_META: _ClassVar[SplitTableRegionState]
    SPLIT_TABLE_REGION_PRE_OPERATION_AFTER_META: _ClassVar[SplitTableRegionState]
    SPLIT_TABLE_REGION_OPEN_CHILD_REGIONS: _ClassVar[SplitTableRegionState]
    SPLIT_TABLE_REGION_POST_OPERATION: _ClassVar[SplitTableRegionState]
    SPLIT_TABLE_REGIONS_CHECK_CLOSED_REGIONS: _ClassVar[SplitTableRegionState]

class MergeTableRegionsState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MERGE_TABLE_REGIONS_PREPARE: _ClassVar[MergeTableRegionsState]
    MERGE_TABLE_REGIONS_PRE_OPERATION: _ClassVar[MergeTableRegionsState]
    MERGE_TABLE_REGIONS_PRE_MERGE_OPERATION: _ClassVar[MergeTableRegionsState]
    MERGE_TABLE_REGIONS_CLOSE_REGIONS: _ClassVar[MergeTableRegionsState]
    MERGE_TABLE_REGIONS_CREATE_MERGED_REGION: _ClassVar[MergeTableRegionsState]
    MERGE_TABLE_REGIONS_WRITE_MAX_SEQUENCE_ID_FILE: _ClassVar[MergeTableRegionsState]
    MERGE_TABLE_REGIONS_PRE_MERGE_COMMIT_OPERATION: _ClassVar[MergeTableRegionsState]
    MERGE_TABLE_REGIONS_UPDATE_META: _ClassVar[MergeTableRegionsState]
    MERGE_TABLE_REGIONS_POST_MERGE_COMMIT_OPERATION: _ClassVar[MergeTableRegionsState]
    MERGE_TABLE_REGIONS_OPEN_MERGED_REGION: _ClassVar[MergeTableRegionsState]
    MERGE_TABLE_REGIONS_POST_OPERATION: _ClassVar[MergeTableRegionsState]
    MERGE_TABLE_REGIONS_CHECK_CLOSED_REGIONS: _ClassVar[MergeTableRegionsState]

class ServerCrashState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SERVER_CRASH_START: _ClassVar[ServerCrashState]
    SERVER_CRASH_PROCESS_META: _ClassVar[ServerCrashState]
    SERVER_CRASH_GET_REGIONS: _ClassVar[ServerCrashState]
    SERVER_CRASH_NO_SPLIT_LOGS: _ClassVar[ServerCrashState]
    SERVER_CRASH_SPLIT_LOGS: _ClassVar[ServerCrashState]
    SERVER_CRASH_ASSIGN: _ClassVar[ServerCrashState]
    SERVER_CRASH_WAIT_ON_ASSIGN: _ClassVar[ServerCrashState]
    SERVER_CRASH_SPLIT_META_LOGS: _ClassVar[ServerCrashState]
    SERVER_CRASH_ASSIGN_META: _ClassVar[ServerCrashState]
    SERVER_CRASH_DELETE_SPLIT_META_WALS_DIR: _ClassVar[ServerCrashState]
    SERVER_CRASH_DELETE_SPLIT_WALS_DIR: _ClassVar[ServerCrashState]
    SERVER_CRASH_CLAIM_REPLICATION_QUEUES: _ClassVar[ServerCrashState]
    SERVER_CRASH_HANDLE_RIT2: _ClassVar[ServerCrashState]
    SERVER_CRASH_FINISH: _ClassVar[ServerCrashState]

class RecoverMetaState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RECOVER_META_PREPARE: _ClassVar[RecoverMetaState]
    RECOVER_META_SPLIT_LOGS: _ClassVar[RecoverMetaState]
    RECOVER_META_ASSIGN_REGIONS: _ClassVar[RecoverMetaState]

class RegionTransitionState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REGION_TRANSITION_QUEUE: _ClassVar[RegionTransitionState]
    REGION_TRANSITION_DISPATCH: _ClassVar[RegionTransitionState]
    REGION_TRANSITION_FINISH: _ClassVar[RegionTransitionState]

class MoveRegionState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MOVE_REGION_PREPARE: _ClassVar[MoveRegionState]
    MOVE_REGION_UNASSIGN: _ClassVar[MoveRegionState]
    MOVE_REGION_ASSIGN: _ClassVar[MoveRegionState]

class GCRegionState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GC_REGION_PREPARE: _ClassVar[GCRegionState]
    GC_REGION_ARCHIVE: _ClassVar[GCRegionState]
    GC_REGION_PURGE_METADATA: _ClassVar[GCRegionState]

class GCMergedRegionsState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GC_MERGED_REGIONS_PREPARE: _ClassVar[GCMergedRegionsState]
    GC_MERGED_REGIONS_PURGE: _ClassVar[GCMergedRegionsState]
    GC_REGION_EDIT_METADATA: _ClassVar[GCMergedRegionsState]

class PeerModificationState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PRE_PEER_MODIFICATION: _ClassVar[PeerModificationState]
    UPDATE_PEER_STORAGE: _ClassVar[PeerModificationState]
    REFRESH_PEER_ON_RS: _ClassVar[PeerModificationState]
    SERIAL_PEER_REOPEN_REGIONS: _ClassVar[PeerModificationState]
    SERIAL_PEER_UPDATE_LAST_PUSHED_SEQ_ID: _ClassVar[PeerModificationState]
    SERIAL_PEER_SET_PEER_ENABLED: _ClassVar[PeerModificationState]
    SERIAL_PEER_ENABLE_PEER_REFRESH_PEER_ON_RS: _ClassVar[PeerModificationState]
    POST_PEER_MODIFICATION: _ClassVar[PeerModificationState]

class PeerModificationType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ADD_PEER: _ClassVar[PeerModificationType]
    REMOVE_PEER: _ClassVar[PeerModificationType]
    ENABLE_PEER: _ClassVar[PeerModificationType]
    DISABLE_PEER: _ClassVar[PeerModificationType]
    UPDATE_PEER_CONFIG: _ClassVar[PeerModificationType]

class ReopenTableRegionsState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REOPEN_TABLE_REGIONS_GET_REGIONS: _ClassVar[ReopenTableRegionsState]
    REOPEN_TABLE_REGIONS_REOPEN_REGIONS: _ClassVar[ReopenTableRegionsState]
    REOPEN_TABLE_REGIONS_CONFIRM_REOPENED: _ClassVar[ReopenTableRegionsState]

class InitMetaState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INIT_META_WRITE_FS_LAYOUT: _ClassVar[InitMetaState]
    INIT_META_ASSIGN_META: _ClassVar[InitMetaState]

class RegionStateTransitionState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE: _ClassVar[RegionStateTransitionState]
    REGION_STATE_TRANSITION_OPEN: _ClassVar[RegionStateTransitionState]
    REGION_STATE_TRANSITION_CONFIRM_OPENED: _ClassVar[RegionStateTransitionState]
    REGION_STATE_TRANSITION_CLOSE: _ClassVar[RegionStateTransitionState]
    REGION_STATE_TRANSITION_CONFIRM_CLOSED: _ClassVar[RegionStateTransitionState]

class RegionTransitionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ASSIGN: _ClassVar[RegionTransitionType]
    UNASSIGN: _ClassVar[RegionTransitionType]
    MOVE: _ClassVar[RegionTransitionType]
    REOPEN: _ClassVar[RegionTransitionType]

class RegionRemoteProcedureBaseState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REGION_REMOTE_PROCEDURE_DISPATCH: _ClassVar[RegionRemoteProcedureBaseState]
    REGION_REMOTE_PROCEDURE_REPORT_SUCCEED: _ClassVar[RegionRemoteProcedureBaseState]
    REGION_REMOTE_PROCEDURE_DISPATCH_FAIL: _ClassVar[RegionRemoteProcedureBaseState]
    REGION_REMOTE_PROCEDURE_SERVER_CRASH: _ClassVar[RegionRemoteProcedureBaseState]

class SwitchRpcThrottleState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    UPDATE_SWITCH_RPC_THROTTLE_STORAGE: _ClassVar[SwitchRpcThrottleState]
    SWITCH_RPC_THROTTLE_ON_RS: _ClassVar[SwitchRpcThrottleState]
    POST_SWITCH_RPC_THROTTLE: _ClassVar[SwitchRpcThrottleState]

class SplitWALState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACQUIRE_SPLIT_WAL_WORKER: _ClassVar[SplitWALState]
    DISPATCH_WAL_TO_WORKER: _ClassVar[SplitWALState]
    RELEASE_SPLIT_WORKER: _ClassVar[SplitWALState]

class ClaimReplicationQueuesState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CLAIM_REPLICATION_QUEUES_DISPATCH: _ClassVar[ClaimReplicationQueuesState]
    CLAIM_REPLICATION_QUEUES_FINISH: _ClassVar[ClaimReplicationQueuesState]

class ModifyTableDescriptorState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MODIFY_TABLE_DESCRIPTOR_PREPARE: _ClassVar[ModifyTableDescriptorState]
    MODIFY_TABLE_DESCRIPTOR_UPDATE: _ClassVar[ModifyTableDescriptorState]

class ModifyStoreFileTrackerState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MODIFY_STORE_FILE_TRACKER_FINISH_PREVIOUS_MIGRATION: _ClassVar[ModifyStoreFileTrackerState]
    MODIFY_STORE_FILE_TRACKER_START_MIGRATION: _ClassVar[ModifyStoreFileTrackerState]
    MODIFY_STORE_FILE_TRACKER_FINISH_MIGRATION: _ClassVar[ModifyStoreFileTrackerState]
CREATE_TABLE_PRE_OPERATION: CreateTableState
CREATE_TABLE_WRITE_FS_LAYOUT: CreateTableState
CREATE_TABLE_ADD_TO_META: CreateTableState
CREATE_TABLE_ASSIGN_REGIONS: CreateTableState
CREATE_TABLE_UPDATE_DESC_CACHE: CreateTableState
CREATE_TABLE_POST_OPERATION: CreateTableState
MODIFY_TABLE_PREPARE: ModifyTableState
MODIFY_TABLE_PRE_OPERATION: ModifyTableState
MODIFY_TABLE_UPDATE_TABLE_DESCRIPTOR: ModifyTableState
MODIFY_TABLE_REMOVE_REPLICA_COLUMN: ModifyTableState
MODIFY_TABLE_DELETE_FS_LAYOUT: ModifyTableState
MODIFY_TABLE_POST_OPERATION: ModifyTableState
MODIFY_TABLE_REOPEN_ALL_REGIONS: ModifyTableState
MODIFY_TABLE_CLOSE_EXCESS_REPLICAS: ModifyTableState
MODIFY_TABLE_ASSIGN_NEW_REPLICAS: ModifyTableState
TRUNCATE_TABLE_PRE_OPERATION: TruncateTableState
TRUNCATE_TABLE_REMOVE_FROM_META: TruncateTableState
TRUNCATE_TABLE_CLEAR_FS_LAYOUT: TruncateTableState
TRUNCATE_TABLE_CREATE_FS_LAYOUT: TruncateTableState
TRUNCATE_TABLE_ADD_TO_META: TruncateTableState
TRUNCATE_TABLE_ASSIGN_REGIONS: TruncateTableState
TRUNCATE_TABLE_POST_OPERATION: TruncateTableState
DELETE_TABLE_PRE_OPERATION: DeleteTableState
DELETE_TABLE_REMOVE_FROM_META: DeleteTableState
DELETE_TABLE_CLEAR_FS_LAYOUT: DeleteTableState
DELETE_TABLE_UPDATE_DESC_CACHE: DeleteTableState
DELETE_TABLE_UNASSIGN_REGIONS: DeleteTableState
DELETE_TABLE_POST_OPERATION: DeleteTableState
CREATE_NAMESPACE_PREPARE: CreateNamespaceState
CREATE_NAMESPACE_CREATE_DIRECTORY: CreateNamespaceState
CREATE_NAMESPACE_INSERT_INTO_NS_TABLE: CreateNamespaceState
CREATE_NAMESPACE_UPDATE_ZK: CreateNamespaceState
CREATE_NAMESPACE_SET_NAMESPACE_QUOTA: CreateNamespaceState
MODIFY_NAMESPACE_PREPARE: ModifyNamespaceState
MODIFY_NAMESPACE_UPDATE_NS_TABLE: ModifyNamespaceState
MODIFY_NAMESPACE_UPDATE_ZK: ModifyNamespaceState
DELETE_NAMESPACE_PREPARE: DeleteNamespaceState
DELETE_NAMESPACE_DELETE_FROM_NS_TABLE: DeleteNamespaceState
DELETE_NAMESPACE_REMOVE_FROM_ZK: DeleteNamespaceState
DELETE_NAMESPACE_DELETE_DIRECTORIES: DeleteNamespaceState
DELETE_NAMESPACE_REMOVE_NAMESPACE_QUOTA: DeleteNamespaceState
ENABLE_TABLE_PREPARE: EnableTableState
ENABLE_TABLE_PRE_OPERATION: EnableTableState
ENABLE_TABLE_SET_ENABLING_TABLE_STATE: EnableTableState
ENABLE_TABLE_MARK_REGIONS_ONLINE: EnableTableState
ENABLE_TABLE_SET_ENABLED_TABLE_STATE: EnableTableState
ENABLE_TABLE_POST_OPERATION: EnableTableState
DISABLE_TABLE_PREPARE: DisableTableState
DISABLE_TABLE_PRE_OPERATION: DisableTableState
DISABLE_TABLE_SET_DISABLING_TABLE_STATE: DisableTableState
DISABLE_TABLE_MARK_REGIONS_OFFLINE: DisableTableState
DISABLE_TABLE_SET_DISABLED_TABLE_STATE: DisableTableState
DISABLE_TABLE_POST_OPERATION: DisableTableState
DISABLE_TABLE_ADD_REPLICATION_BARRIER: DisableTableState
CLONE_SNAPSHOT_PRE_OPERATION: CloneSnapshotState
CLONE_SNAPSHOT_WRITE_FS_LAYOUT: CloneSnapshotState
CLONE_SNAPSHOT_ADD_TO_META: CloneSnapshotState
CLONE_SNAPSHOT_ASSIGN_REGIONS: CloneSnapshotState
CLONE_SNAPSHOT_UPDATE_DESC_CACHE: CloneSnapshotState
CLONE_SNAPSHOT_POST_OPERATION: CloneSnapshotState
CLONE_SNAPHOST_RESTORE_ACL: CloneSnapshotState
RESTORE_SNAPSHOT_PRE_OPERATION: RestoreSnapshotState
RESTORE_SNAPSHOT_UPDATE_TABLE_DESCRIPTOR: RestoreSnapshotState
RESTORE_SNAPSHOT_WRITE_FS_LAYOUT: RestoreSnapshotState
RESTORE_SNAPSHOT_UPDATE_META: RestoreSnapshotState
RESTORE_SNAPSHOT_RESTORE_ACL: RestoreSnapshotState
DISPATCH_MERGING_REGIONS_PREPARE: DispatchMergingRegionsState
DISPATCH_MERGING_REGIONS_PRE_OPERATION: DispatchMergingRegionsState
DISPATCH_MERGING_REGIONS_MOVE_REGION_TO_SAME_RS: DispatchMergingRegionsState
DISPATCH_MERGING_REGIONS_DO_MERGE_IN_RS: DispatchMergingRegionsState
DISPATCH_MERGING_REGIONS_POST_OPERATION: DispatchMergingRegionsState
SPLIT_TABLE_REGION_PREPARE: SplitTableRegionState
SPLIT_TABLE_REGION_PRE_OPERATION: SplitTableRegionState
SPLIT_TABLE_REGION_CLOSE_PARENT_REGION: SplitTableRegionState
SPLIT_TABLE_REGION_CREATE_DAUGHTER_REGIONS: SplitTableRegionState
SPLIT_TABLE_REGION_WRITE_MAX_SEQUENCE_ID_FILE: SplitTableRegionState
SPLIT_TABLE_REGION_PRE_OPERATION_BEFORE_META: SplitTableRegionState
SPLIT_TABLE_REGION_UPDATE_META: SplitTableRegionState
SPLIT_TABLE_REGION_PRE_OPERATION_AFTER_META: SplitTableRegionState
SPLIT_TABLE_REGION_OPEN_CHILD_REGIONS: SplitTableRegionState
SPLIT_TABLE_REGION_POST_OPERATION: SplitTableRegionState
SPLIT_TABLE_REGIONS_CHECK_CLOSED_REGIONS: SplitTableRegionState
MERGE_TABLE_REGIONS_PREPARE: MergeTableRegionsState
MERGE_TABLE_REGIONS_PRE_OPERATION: MergeTableRegionsState
MERGE_TABLE_REGIONS_PRE_MERGE_OPERATION: MergeTableRegionsState
MERGE_TABLE_REGIONS_CLOSE_REGIONS: MergeTableRegionsState
MERGE_TABLE_REGIONS_CREATE_MERGED_REGION: MergeTableRegionsState
MERGE_TABLE_REGIONS_WRITE_MAX_SEQUENCE_ID_FILE: MergeTableRegionsState
MERGE_TABLE_REGIONS_PRE_MERGE_COMMIT_OPERATION: MergeTableRegionsState
MERGE_TABLE_REGIONS_UPDATE_META: MergeTableRegionsState
MERGE_TABLE_REGIONS_POST_MERGE_COMMIT_OPERATION: MergeTableRegionsState
MERGE_TABLE_REGIONS_OPEN_MERGED_REGION: MergeTableRegionsState
MERGE_TABLE_REGIONS_POST_OPERATION: MergeTableRegionsState
MERGE_TABLE_REGIONS_CHECK_CLOSED_REGIONS: MergeTableRegionsState
SERVER_CRASH_START: ServerCrashState
SERVER_CRASH_PROCESS_META: ServerCrashState
SERVER_CRASH_GET_REGIONS: ServerCrashState
SERVER_CRASH_NO_SPLIT_LOGS: ServerCrashState
SERVER_CRASH_SPLIT_LOGS: ServerCrashState
SERVER_CRASH_ASSIGN: ServerCrashState
SERVER_CRASH_WAIT_ON_ASSIGN: ServerCrashState
SERVER_CRASH_SPLIT_META_LOGS: ServerCrashState
SERVER_CRASH_ASSIGN_META: ServerCrashState
SERVER_CRASH_DELETE_SPLIT_META_WALS_DIR: ServerCrashState
SERVER_CRASH_DELETE_SPLIT_WALS_DIR: ServerCrashState
SERVER_CRASH_CLAIM_REPLICATION_QUEUES: ServerCrashState
SERVER_CRASH_HANDLE_RIT2: ServerCrashState
SERVER_CRASH_FINISH: ServerCrashState
RECOVER_META_PREPARE: RecoverMetaState
RECOVER_META_SPLIT_LOGS: RecoverMetaState
RECOVER_META_ASSIGN_REGIONS: RecoverMetaState
REGION_TRANSITION_QUEUE: RegionTransitionState
REGION_TRANSITION_DISPATCH: RegionTransitionState
REGION_TRANSITION_FINISH: RegionTransitionState
MOVE_REGION_PREPARE: MoveRegionState
MOVE_REGION_UNASSIGN: MoveRegionState
MOVE_REGION_ASSIGN: MoveRegionState
GC_REGION_PREPARE: GCRegionState
GC_REGION_ARCHIVE: GCRegionState
GC_REGION_PURGE_METADATA: GCRegionState
GC_MERGED_REGIONS_PREPARE: GCMergedRegionsState
GC_MERGED_REGIONS_PURGE: GCMergedRegionsState
GC_REGION_EDIT_METADATA: GCMergedRegionsState
PRE_PEER_MODIFICATION: PeerModificationState
UPDATE_PEER_STORAGE: PeerModificationState
REFRESH_PEER_ON_RS: PeerModificationState
SERIAL_PEER_REOPEN_REGIONS: PeerModificationState
SERIAL_PEER_UPDATE_LAST_PUSHED_SEQ_ID: PeerModificationState
SERIAL_PEER_SET_PEER_ENABLED: PeerModificationState
SERIAL_PEER_ENABLE_PEER_REFRESH_PEER_ON_RS: PeerModificationState
POST_PEER_MODIFICATION: PeerModificationState
ADD_PEER: PeerModificationType
REMOVE_PEER: PeerModificationType
ENABLE_PEER: PeerModificationType
DISABLE_PEER: PeerModificationType
UPDATE_PEER_CONFIG: PeerModificationType
REOPEN_TABLE_REGIONS_GET_REGIONS: ReopenTableRegionsState
REOPEN_TABLE_REGIONS_REOPEN_REGIONS: ReopenTableRegionsState
REOPEN_TABLE_REGIONS_CONFIRM_REOPENED: ReopenTableRegionsState
INIT_META_WRITE_FS_LAYOUT: InitMetaState
INIT_META_ASSIGN_META: InitMetaState
REGION_STATE_TRANSITION_GET_ASSIGN_CANDIDATE: RegionStateTransitionState
REGION_STATE_TRANSITION_OPEN: RegionStateTransitionState
REGION_STATE_TRANSITION_CONFIRM_OPENED: RegionStateTransitionState
REGION_STATE_TRANSITION_CLOSE: RegionStateTransitionState
REGION_STATE_TRANSITION_CONFIRM_CLOSED: RegionStateTransitionState
ASSIGN: RegionTransitionType
UNASSIGN: RegionTransitionType
MOVE: RegionTransitionType
REOPEN: RegionTransitionType
REGION_REMOTE_PROCEDURE_DISPATCH: RegionRemoteProcedureBaseState
REGION_REMOTE_PROCEDURE_REPORT_SUCCEED: RegionRemoteProcedureBaseState
REGION_REMOTE_PROCEDURE_DISPATCH_FAIL: RegionRemoteProcedureBaseState
REGION_REMOTE_PROCEDURE_SERVER_CRASH: RegionRemoteProcedureBaseState
UPDATE_SWITCH_RPC_THROTTLE_STORAGE: SwitchRpcThrottleState
SWITCH_RPC_THROTTLE_ON_RS: SwitchRpcThrottleState
POST_SWITCH_RPC_THROTTLE: SwitchRpcThrottleState
ACQUIRE_SPLIT_WAL_WORKER: SplitWALState
DISPATCH_WAL_TO_WORKER: SplitWALState
RELEASE_SPLIT_WORKER: SplitWALState
CLAIM_REPLICATION_QUEUES_DISPATCH: ClaimReplicationQueuesState
CLAIM_REPLICATION_QUEUES_FINISH: ClaimReplicationQueuesState
MODIFY_TABLE_DESCRIPTOR_PREPARE: ModifyTableDescriptorState
MODIFY_TABLE_DESCRIPTOR_UPDATE: ModifyTableDescriptorState
MODIFY_STORE_FILE_TRACKER_FINISH_PREVIOUS_MIGRATION: ModifyStoreFileTrackerState
MODIFY_STORE_FILE_TRACKER_START_MIGRATION: ModifyStoreFileTrackerState
MODIFY_STORE_FILE_TRACKER_FINISH_MIGRATION: ModifyStoreFileTrackerState

class CreateTableStateData(_message.Message):
    __slots__ = ("user_info", "table_schema", "region_info")
    USER_INFO_FIELD_NUMBER: _ClassVar[int]
    TABLE_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    user_info: _RPC_pb2.UserInformation
    table_schema: _HBase_pb2.TableSchema
    region_info: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    def __init__(self, user_info: _Optional[_Union[_RPC_pb2.UserInformation, _Mapping]] = ..., table_schema: _Optional[_Union[_HBase_pb2.TableSchema, _Mapping]] = ..., region_info: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ...) -> None: ...

class ModifyTableStateData(_message.Message):
    __slots__ = ("user_info", "unmodified_table_schema", "modified_table_schema", "delete_column_family_in_modify", "should_check_descriptor", "reopen_regions")
    USER_INFO_FIELD_NUMBER: _ClassVar[int]
    UNMODIFIED_TABLE_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    MODIFIED_TABLE_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    DELETE_COLUMN_FAMILY_IN_MODIFY_FIELD_NUMBER: _ClassVar[int]
    SHOULD_CHECK_DESCRIPTOR_FIELD_NUMBER: _ClassVar[int]
    REOPEN_REGIONS_FIELD_NUMBER: _ClassVar[int]
    user_info: _RPC_pb2.UserInformation
    unmodified_table_schema: _HBase_pb2.TableSchema
    modified_table_schema: _HBase_pb2.TableSchema
    delete_column_family_in_modify: bool
    should_check_descriptor: bool
    reopen_regions: bool
    def __init__(self, user_info: _Optional[_Union[_RPC_pb2.UserInformation, _Mapping]] = ..., unmodified_table_schema: _Optional[_Union[_HBase_pb2.TableSchema, _Mapping]] = ..., modified_table_schema: _Optional[_Union[_HBase_pb2.TableSchema, _Mapping]] = ..., delete_column_family_in_modify: bool = ..., should_check_descriptor: bool = ..., reopen_regions: bool = ...) -> None: ...

class TruncateTableStateData(_message.Message):
    __slots__ = ("user_info", "preserve_splits", "table_name", "table_schema", "region_info")
    USER_INFO_FIELD_NUMBER: _ClassVar[int]
    PRESERVE_SPLITS_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    user_info: _RPC_pb2.UserInformation
    preserve_splits: bool
    table_name: _HBase_pb2.TableName
    table_schema: _HBase_pb2.TableSchema
    region_info: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    def __init__(self, user_info: _Optional[_Union[_RPC_pb2.UserInformation, _Mapping]] = ..., preserve_splits: bool = ..., table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., table_schema: _Optional[_Union[_HBase_pb2.TableSchema, _Mapping]] = ..., region_info: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ...) -> None: ...

class DeleteTableStateData(_message.Message):
    __slots__ = ("user_info", "table_name", "region_info")
    USER_INFO_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    user_info: _RPC_pb2.UserInformation
    table_name: _HBase_pb2.TableName
    region_info: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    def __init__(self, user_info: _Optional[_Union[_RPC_pb2.UserInformation, _Mapping]] = ..., table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., region_info: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ...) -> None: ...

class CreateNamespaceStateData(_message.Message):
    __slots__ = ("namespace_descriptor",)
    NAMESPACE_DESCRIPTOR_FIELD_NUMBER: _ClassVar[int]
    namespace_descriptor: _HBase_pb2.NamespaceDescriptor
    def __init__(self, namespace_descriptor: _Optional[_Union[_HBase_pb2.NamespaceDescriptor, _Mapping]] = ...) -> None: ...

class ModifyNamespaceStateData(_message.Message):
    __slots__ = ("namespace_descriptor", "unmodified_namespace_descriptor")
    NAMESPACE_DESCRIPTOR_FIELD_NUMBER: _ClassVar[int]
    UNMODIFIED_NAMESPACE_DESCRIPTOR_FIELD_NUMBER: _ClassVar[int]
    namespace_descriptor: _HBase_pb2.NamespaceDescriptor
    unmodified_namespace_descriptor: _HBase_pb2.NamespaceDescriptor
    def __init__(self, namespace_descriptor: _Optional[_Union[_HBase_pb2.NamespaceDescriptor, _Mapping]] = ..., unmodified_namespace_descriptor: _Optional[_Union[_HBase_pb2.NamespaceDescriptor, _Mapping]] = ...) -> None: ...

class DeleteNamespaceStateData(_message.Message):
    __slots__ = ("namespace_name", "namespace_descriptor")
    NAMESPACE_NAME_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_DESCRIPTOR_FIELD_NUMBER: _ClassVar[int]
    namespace_name: str
    namespace_descriptor: _HBase_pb2.NamespaceDescriptor
    def __init__(self, namespace_name: _Optional[str] = ..., namespace_descriptor: _Optional[_Union[_HBase_pb2.NamespaceDescriptor, _Mapping]] = ...) -> None: ...

class EnableTableStateData(_message.Message):
    __slots__ = ("user_info", "table_name", "skip_table_state_check")
    USER_INFO_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    SKIP_TABLE_STATE_CHECK_FIELD_NUMBER: _ClassVar[int]
    user_info: _RPC_pb2.UserInformation
    table_name: _HBase_pb2.TableName
    skip_table_state_check: bool
    def __init__(self, user_info: _Optional[_Union[_RPC_pb2.UserInformation, _Mapping]] = ..., table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., skip_table_state_check: bool = ...) -> None: ...

class DisableTableStateData(_message.Message):
    __slots__ = ("user_info", "table_name", "skip_table_state_check")
    USER_INFO_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    SKIP_TABLE_STATE_CHECK_FIELD_NUMBER: _ClassVar[int]
    user_info: _RPC_pb2.UserInformation
    table_name: _HBase_pb2.TableName
    skip_table_state_check: bool
    def __init__(self, user_info: _Optional[_Union[_RPC_pb2.UserInformation, _Mapping]] = ..., table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., skip_table_state_check: bool = ...) -> None: ...

class RestoreParentToChildRegionsPair(_message.Message):
    __slots__ = ("parent_region_name", "child1_region_name", "child2_region_name")
    PARENT_REGION_NAME_FIELD_NUMBER: _ClassVar[int]
    CHILD1_REGION_NAME_FIELD_NUMBER: _ClassVar[int]
    CHILD2_REGION_NAME_FIELD_NUMBER: _ClassVar[int]
    parent_region_name: str
    child1_region_name: str
    child2_region_name: str
    def __init__(self, parent_region_name: _Optional[str] = ..., child1_region_name: _Optional[str] = ..., child2_region_name: _Optional[str] = ...) -> None: ...

class CloneSnapshotStateData(_message.Message):
    __slots__ = ("user_info", "snapshot", "table_schema", "region_info", "parent_to_child_regions_pair_list", "restore_acl", "customSFT")
    USER_INFO_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    TABLE_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    PARENT_TO_CHILD_REGIONS_PAIR_LIST_FIELD_NUMBER: _ClassVar[int]
    RESTORE_ACL_FIELD_NUMBER: _ClassVar[int]
    CUSTOMSFT_FIELD_NUMBER: _ClassVar[int]
    user_info: _RPC_pb2.UserInformation
    snapshot: _Snapshot_pb2.SnapshotDescription
    table_schema: _HBase_pb2.TableSchema
    region_info: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    parent_to_child_regions_pair_list: _containers.RepeatedCompositeFieldContainer[RestoreParentToChildRegionsPair]
    restore_acl: bool
    customSFT: str
    def __init__(self, user_info: _Optional[_Union[_RPC_pb2.UserInformation, _Mapping]] = ..., snapshot: _Optional[_Union[_Snapshot_pb2.SnapshotDescription, _Mapping]] = ..., table_schema: _Optional[_Union[_HBase_pb2.TableSchema, _Mapping]] = ..., region_info: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ..., parent_to_child_regions_pair_list: _Optional[_Iterable[_Union[RestoreParentToChildRegionsPair, _Mapping]]] = ..., restore_acl: bool = ..., customSFT: _Optional[str] = ...) -> None: ...

class RestoreSnapshotStateData(_message.Message):
    __slots__ = ("user_info", "snapshot", "modified_table_schema", "region_info_for_restore", "region_info_for_remove", "region_info_for_add", "parent_to_child_regions_pair_list", "restore_acl")
    USER_INFO_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    MODIFIED_TABLE_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    REGION_INFO_FOR_RESTORE_FIELD_NUMBER: _ClassVar[int]
    REGION_INFO_FOR_REMOVE_FIELD_NUMBER: _ClassVar[int]
    REGION_INFO_FOR_ADD_FIELD_NUMBER: _ClassVar[int]
    PARENT_TO_CHILD_REGIONS_PAIR_LIST_FIELD_NUMBER: _ClassVar[int]
    RESTORE_ACL_FIELD_NUMBER: _ClassVar[int]
    user_info: _RPC_pb2.UserInformation
    snapshot: _Snapshot_pb2.SnapshotDescription
    modified_table_schema: _HBase_pb2.TableSchema
    region_info_for_restore: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    region_info_for_remove: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    region_info_for_add: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    parent_to_child_regions_pair_list: _containers.RepeatedCompositeFieldContainer[RestoreParentToChildRegionsPair]
    restore_acl: bool
    def __init__(self, user_info: _Optional[_Union[_RPC_pb2.UserInformation, _Mapping]] = ..., snapshot: _Optional[_Union[_Snapshot_pb2.SnapshotDescription, _Mapping]] = ..., modified_table_schema: _Optional[_Union[_HBase_pb2.TableSchema, _Mapping]] = ..., region_info_for_restore: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ..., region_info_for_remove: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ..., region_info_for_add: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ..., parent_to_child_regions_pair_list: _Optional[_Iterable[_Union[RestoreParentToChildRegionsPair, _Mapping]]] = ..., restore_acl: bool = ...) -> None: ...

class DispatchMergingRegionsStateData(_message.Message):
    __slots__ = ("user_info", "table_name", "region_info", "forcible")
    USER_INFO_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    FORCIBLE_FIELD_NUMBER: _ClassVar[int]
    user_info: _RPC_pb2.UserInformation
    table_name: _HBase_pb2.TableName
    region_info: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    forcible: bool
    def __init__(self, user_info: _Optional[_Union[_RPC_pb2.UserInformation, _Mapping]] = ..., table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., region_info: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ..., forcible: bool = ...) -> None: ...

class SplitTableRegionStateData(_message.Message):
    __slots__ = ("user_info", "parent_region_info", "child_region_info")
    USER_INFO_FIELD_NUMBER: _ClassVar[int]
    PARENT_REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    CHILD_REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    user_info: _RPC_pb2.UserInformation
    parent_region_info: _HBase_pb2.RegionInfo
    child_region_info: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    def __init__(self, user_info: _Optional[_Union[_RPC_pb2.UserInformation, _Mapping]] = ..., parent_region_info: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ..., child_region_info: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ...) -> None: ...

class MergeTableRegionsStateData(_message.Message):
    __slots__ = ("user_info", "region_info", "merged_region_info", "forcible")
    USER_INFO_FIELD_NUMBER: _ClassVar[int]
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    MERGED_REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    FORCIBLE_FIELD_NUMBER: _ClassVar[int]
    user_info: _RPC_pb2.UserInformation
    region_info: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    merged_region_info: _HBase_pb2.RegionInfo
    forcible: bool
    def __init__(self, user_info: _Optional[_Union[_RPC_pb2.UserInformation, _Mapping]] = ..., region_info: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ..., merged_region_info: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ..., forcible: bool = ...) -> None: ...

class ServerCrashStateData(_message.Message):
    __slots__ = ("server_name", "regions_on_crashed_server", "regions_assigned", "carrying_meta", "should_split_wal")
    SERVER_NAME_FIELD_NUMBER: _ClassVar[int]
    REGIONS_ON_CRASHED_SERVER_FIELD_NUMBER: _ClassVar[int]
    REGIONS_ASSIGNED_FIELD_NUMBER: _ClassVar[int]
    CARRYING_META_FIELD_NUMBER: _ClassVar[int]
    SHOULD_SPLIT_WAL_FIELD_NUMBER: _ClassVar[int]
    server_name: _HBase_pb2.ServerName
    regions_on_crashed_server: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    regions_assigned: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    carrying_meta: bool
    should_split_wal: bool
    def __init__(self, server_name: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., regions_on_crashed_server: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ..., regions_assigned: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ..., carrying_meta: bool = ..., should_split_wal: bool = ...) -> None: ...

class RecoverMetaStateData(_message.Message):
    __slots__ = ("failed_meta_server", "should_split_wal", "replica_id")
    FAILED_META_SERVER_FIELD_NUMBER: _ClassVar[int]
    SHOULD_SPLIT_WAL_FIELD_NUMBER: _ClassVar[int]
    REPLICA_ID_FIELD_NUMBER: _ClassVar[int]
    failed_meta_server: _HBase_pb2.ServerName
    should_split_wal: bool
    replica_id: int
    def __init__(self, failed_meta_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., should_split_wal: bool = ..., replica_id: _Optional[int] = ...) -> None: ...

class AssignRegionStateData(_message.Message):
    __slots__ = ("transition_state", "region_info", "force_new_plan", "target_server", "attempt")
    TRANSITION_STATE_FIELD_NUMBER: _ClassVar[int]
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    FORCE_NEW_PLAN_FIELD_NUMBER: _ClassVar[int]
    TARGET_SERVER_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    transition_state: RegionTransitionState
    region_info: _HBase_pb2.RegionInfo
    force_new_plan: bool
    target_server: _HBase_pb2.ServerName
    attempt: int
    def __init__(self, transition_state: _Optional[_Union[RegionTransitionState, str]] = ..., region_info: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ..., force_new_plan: bool = ..., target_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., attempt: _Optional[int] = ...) -> None: ...

class UnassignRegionStateData(_message.Message):
    __slots__ = ("transition_state", "region_info", "destination_server", "hosting_server", "force", "remove_after_unassigning", "attempt")
    TRANSITION_STATE_FIELD_NUMBER: _ClassVar[int]
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_SERVER_FIELD_NUMBER: _ClassVar[int]
    HOSTING_SERVER_FIELD_NUMBER: _ClassVar[int]
    FORCE_FIELD_NUMBER: _ClassVar[int]
    REMOVE_AFTER_UNASSIGNING_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    transition_state: RegionTransitionState
    region_info: _HBase_pb2.RegionInfo
    destination_server: _HBase_pb2.ServerName
    hosting_server: _HBase_pb2.ServerName
    force: bool
    remove_after_unassigning: bool
    attempt: int
    def __init__(self, transition_state: _Optional[_Union[RegionTransitionState, str]] = ..., region_info: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ..., destination_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., hosting_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., force: bool = ..., remove_after_unassigning: bool = ..., attempt: _Optional[int] = ...) -> None: ...

class MoveRegionStateData(_message.Message):
    __slots__ = ("region_info", "source_server", "destination_server")
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    SOURCE_SERVER_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_SERVER_FIELD_NUMBER: _ClassVar[int]
    region_info: _HBase_pb2.RegionInfo
    source_server: _HBase_pb2.ServerName
    destination_server: _HBase_pb2.ServerName
    def __init__(self, region_info: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ..., source_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., destination_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ...) -> None: ...

class GCRegionStateData(_message.Message):
    __slots__ = ("region_info",)
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    region_info: _HBase_pb2.RegionInfo
    def __init__(self, region_info: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ...) -> None: ...

class GCMergedRegionsStateData(_message.Message):
    __slots__ = ("parent_a", "parent_b", "merged_child")
    PARENT_A_FIELD_NUMBER: _ClassVar[int]
    PARENT_B_FIELD_NUMBER: _ClassVar[int]
    MERGED_CHILD_FIELD_NUMBER: _ClassVar[int]
    parent_a: _HBase_pb2.RegionInfo
    parent_b: _HBase_pb2.RegionInfo
    merged_child: _HBase_pb2.RegionInfo
    def __init__(self, parent_a: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ..., parent_b: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ..., merged_child: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ...) -> None: ...

class GCMultipleMergedRegionsStateData(_message.Message):
    __slots__ = ("parents", "merged_child")
    PARENTS_FIELD_NUMBER: _ClassVar[int]
    MERGED_CHILD_FIELD_NUMBER: _ClassVar[int]
    parents: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionInfo]
    merged_child: _HBase_pb2.RegionInfo
    def __init__(self, parents: _Optional[_Iterable[_Union[_HBase_pb2.RegionInfo, _Mapping]]] = ..., merged_child: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ...) -> None: ...

class PeerModificationStateData(_message.Message):
    __slots__ = ("peer_id",)
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    def __init__(self, peer_id: _Optional[str] = ...) -> None: ...

class RefreshPeerStateData(_message.Message):
    __slots__ = ("peer_id", "type", "target_server")
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TARGET_SERVER_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    type: PeerModificationType
    target_server: _HBase_pb2.ServerName
    def __init__(self, peer_id: _Optional[str] = ..., type: _Optional[_Union[PeerModificationType, str]] = ..., target_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ...) -> None: ...

class RefreshPeerParameter(_message.Message):
    __slots__ = ("peer_id", "type", "target_server")
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TARGET_SERVER_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    type: PeerModificationType
    target_server: _HBase_pb2.ServerName
    def __init__(self, peer_id: _Optional[str] = ..., type: _Optional[_Union[PeerModificationType, str]] = ..., target_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ...) -> None: ...

class PeerProcedureStateData(_message.Message):
    __slots__ = ("peer_id",)
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    def __init__(self, peer_id: _Optional[str] = ...) -> None: ...

class AddPeerStateData(_message.Message):
    __slots__ = ("peer_config", "enabled")
    PEER_CONFIG_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    peer_config: _Replication_pb2.ReplicationPeer
    enabled: bool
    def __init__(self, peer_config: _Optional[_Union[_Replication_pb2.ReplicationPeer, _Mapping]] = ..., enabled: bool = ...) -> None: ...

class UpdatePeerConfigStateData(_message.Message):
    __slots__ = ("peer_config", "old_peer_config", "enabled")
    PEER_CONFIG_FIELD_NUMBER: _ClassVar[int]
    OLD_PEER_CONFIG_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    peer_config: _Replication_pb2.ReplicationPeer
    old_peer_config: _Replication_pb2.ReplicationPeer
    enabled: bool
    def __init__(self, peer_config: _Optional[_Union[_Replication_pb2.ReplicationPeer, _Mapping]] = ..., old_peer_config: _Optional[_Union[_Replication_pb2.ReplicationPeer, _Mapping]] = ..., enabled: bool = ...) -> None: ...

class RemovePeerStateData(_message.Message):
    __slots__ = ("peer_config",)
    PEER_CONFIG_FIELD_NUMBER: _ClassVar[int]
    peer_config: _Replication_pb2.ReplicationPeer
    def __init__(self, peer_config: _Optional[_Union[_Replication_pb2.ReplicationPeer, _Mapping]] = ...) -> None: ...

class EnablePeerStateData(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DisablePeerStateData(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ReopenTableRegionsStateData(_message.Message):
    __slots__ = ("table_name", "region", "region_names")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    REGION_NAMES_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    region: _containers.RepeatedCompositeFieldContainer[_HBase_pb2.RegionLocation]
    region_names: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., region: _Optional[_Iterable[_Union[_HBase_pb2.RegionLocation, _Mapping]]] = ..., region_names: _Optional[_Iterable[bytes]] = ...) -> None: ...

class InitMetaStateData(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RegionStateTransitionStateData(_message.Message):
    __slots__ = ("type", "assign_candidate", "force_new_plan")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    ASSIGN_CANDIDATE_FIELD_NUMBER: _ClassVar[int]
    FORCE_NEW_PLAN_FIELD_NUMBER: _ClassVar[int]
    type: RegionTransitionType
    assign_candidate: _HBase_pb2.ServerName
    force_new_plan: bool
    def __init__(self, type: _Optional[_Union[RegionTransitionType, str]] = ..., assign_candidate: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., force_new_plan: bool = ...) -> None: ...

class RegionRemoteProcedureBaseStateData(_message.Message):
    __slots__ = ("region", "target_server", "state", "transition_code", "seq_id")
    REGION_FIELD_NUMBER: _ClassVar[int]
    TARGET_SERVER_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    TRANSITION_CODE_FIELD_NUMBER: _ClassVar[int]
    SEQ_ID_FIELD_NUMBER: _ClassVar[int]
    region: _HBase_pb2.RegionInfo
    target_server: _HBase_pb2.ServerName
    state: RegionRemoteProcedureBaseState
    transition_code: _RegionServerStatus_pb2.RegionStateTransition.TransitionCode
    seq_id: int
    def __init__(self, region: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ..., target_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., state: _Optional[_Union[RegionRemoteProcedureBaseState, str]] = ..., transition_code: _Optional[_Union[_RegionServerStatus_pb2.RegionStateTransition.TransitionCode, str]] = ..., seq_id: _Optional[int] = ...) -> None: ...

class OpenRegionProcedureStateData(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CloseRegionProcedureStateData(_message.Message):
    __slots__ = ("assign_candidate",)
    ASSIGN_CANDIDATE_FIELD_NUMBER: _ClassVar[int]
    assign_candidate: _HBase_pb2.ServerName
    def __init__(self, assign_candidate: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ...) -> None: ...

class SwitchRpcThrottleStateData(_message.Message):
    __slots__ = ("rpc_throttle_enabled",)
    RPC_THROTTLE_ENABLED_FIELD_NUMBER: _ClassVar[int]
    rpc_throttle_enabled: bool
    def __init__(self, rpc_throttle_enabled: bool = ...) -> None: ...

class SwitchRpcThrottleRemoteStateData(_message.Message):
    __slots__ = ("target_server", "rpc_throttle_enabled")
    TARGET_SERVER_FIELD_NUMBER: _ClassVar[int]
    RPC_THROTTLE_ENABLED_FIELD_NUMBER: _ClassVar[int]
    target_server: _HBase_pb2.ServerName
    rpc_throttle_enabled: bool
    def __init__(self, target_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., rpc_throttle_enabled: bool = ...) -> None: ...

class SplitWALParameter(_message.Message):
    __slots__ = ("wal_path",)
    WAL_PATH_FIELD_NUMBER: _ClassVar[int]
    wal_path: str
    def __init__(self, wal_path: _Optional[str] = ...) -> None: ...

class SplitWALData(_message.Message):
    __slots__ = ("wal_path", "crashed_server", "worker")
    WAL_PATH_FIELD_NUMBER: _ClassVar[int]
    CRASHED_SERVER_FIELD_NUMBER: _ClassVar[int]
    WORKER_FIELD_NUMBER: _ClassVar[int]
    wal_path: str
    crashed_server: _HBase_pb2.ServerName
    worker: _HBase_pb2.ServerName
    def __init__(self, wal_path: _Optional[str] = ..., crashed_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., worker: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ...) -> None: ...

class SplitWALRemoteData(_message.Message):
    __slots__ = ("wal_path", "crashed_server", "worker")
    WAL_PATH_FIELD_NUMBER: _ClassVar[int]
    CRASHED_SERVER_FIELD_NUMBER: _ClassVar[int]
    WORKER_FIELD_NUMBER: _ClassVar[int]
    wal_path: str
    crashed_server: _HBase_pb2.ServerName
    worker: _HBase_pb2.ServerName
    def __init__(self, wal_path: _Optional[str] = ..., crashed_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., worker: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ...) -> None: ...

class ClaimReplicationQueuesStateData(_message.Message):
    __slots__ = ("crashed_server",)
    CRASHED_SERVER_FIELD_NUMBER: _ClassVar[int]
    crashed_server: _HBase_pb2.ServerName
    def __init__(self, crashed_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ...) -> None: ...

class ClaimReplicationQueueRemoteStateData(_message.Message):
    __slots__ = ("crashed_server", "queue", "target_server")
    CRASHED_SERVER_FIELD_NUMBER: _ClassVar[int]
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    TARGET_SERVER_FIELD_NUMBER: _ClassVar[int]
    crashed_server: _HBase_pb2.ServerName
    queue: str
    target_server: _HBase_pb2.ServerName
    def __init__(self, crashed_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., queue: _Optional[str] = ..., target_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ...) -> None: ...

class ClaimReplicationQueueRemoteParameter(_message.Message):
    __slots__ = ("crashed_server", "queue")
    CRASHED_SERVER_FIELD_NUMBER: _ClassVar[int]
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    crashed_server: _HBase_pb2.ServerName
    queue: str
    def __init__(self, crashed_server: _Optional[_Union[_HBase_pb2.ServerName, _Mapping]] = ..., queue: _Optional[str] = ...) -> None: ...

class ModifyTableDescriptorStateData(_message.Message):
    __slots__ = ("table_name", "modified_table_schema")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    MODIFIED_TABLE_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    modified_table_schema: _HBase_pb2.TableSchema
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., modified_table_schema: _Optional[_Union[_HBase_pb2.TableSchema, _Mapping]] = ...) -> None: ...

class ModifyStoreFileTrackerStateData(_message.Message):
    __slots__ = ("table_name", "dst_sft")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    DST_SFT_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    dst_sft: str
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., dst_sft: _Optional[str] = ...) -> None: ...

class ModifyColumnFamilyStoreFileTrackerStateData(_message.Message):
    __slots__ = ("family",)
    FAMILY_FIELD_NUMBER: _ClassVar[int]
    family: bytes
    def __init__(self, family: _Optional[bytes] = ...) -> None: ...
