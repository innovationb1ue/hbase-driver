from __future__ import annotations

import time
from typing import Optional

from hbasedriver.protobuf_py.HBase_pb2 import (
    ColumnFamilySchema,
    TableName as PBTableName,
    RegionInfo,
    RegionSpecifier,
)
from hbasedriver.protobuf_py.Master_pb2 import (
    CreateTableRequest,
    DeleteTableRequest,
    DisableTableRequest,
    EnableTableRequest,
    GetTableDescriptorsRequest,
    GetTableDescriptorsResponse,
    TruncateTableRequest,
    CreateNamespaceRequest,
    DeleteNamespaceRequest,
    ListNamespacesRequest,
    ListNamespacesResponse,
    # Column family operations
    AddColumnRequest,
    DeleteColumnRequest,
    ModifyColumnRequest,
    # Region operations
    SplitTableRegionRequest,
    MergeTableRegionsRequest,
    AssignRegionRequest,
    UnassignRegionRequest,
    # Cluster operations
    GetClusterStatusRequest,
    GetClusterStatusResponse,
    BalanceRequest,
    BalanceResponse,
    SetBalancerRunningRequest,
    SetBalancerRunningResponse,
    IsBalancerEnabledRequest,
    IsBalancerEnabledResponse,
    # Snapshot operations
    SnapshotRequest,
    SnapshotResponse,
    GetCompletedSnapshotsRequest,
    GetCompletedSnapshotsResponse,
    DeleteSnapshotRequest,
    RestoreSnapshotRequest,
    RestoreSnapshotResponse,
    IsSnapshotDoneRequest,
)
from hbasedriver.protobuf_py.Snapshot_pb2 import SnapshotDescription
from hbasedriver.Connection import Connection


class MasterConnection(Connection):
    def __init__(self):
        super().__init__("MasterService")

    def clone(self):
        return MasterConnection().connect(self.host, self.port, self.timeout)

    def create_table(self, namespace: bytes, table: bytes, columns: list[ColumnFamilySchema], split_keys=None):
        rq = CreateTableRequest()
        rq.split_keys.extend(split_keys or [])
        rq.table_schema.table_name.namespace = namespace or b'default'
        rq.table_schema.table_name.qualifier = table
        for c in columns:
            rq.table_schema.column_families.append(c)
        self.send_request(rq, "CreateTable")

    def enable_table(self, namespace: bytes, table: bytes):
        rq = EnableTableRequest()
        rq.table_name.namespace = namespace or b"default"
        rq.table_name.qualifier = table
        self.send_request(rq, "EnableTable")
        time.sleep(1)

    def disable_table(self, namespace: bytes, table: bytes):
        rq = DisableTableRequest()
        rq.table_name.namespace = namespace or b'default'
        rq.table_name.qualifier = table
        self.send_request(rq, "DisableTable")

    def delete_table(self, namespace: bytes, table: bytes):
        rq = DeleteTableRequest()
        rq.table_name.namespace = namespace or b'default'
        rq.table_name.qualifier = table
        self.send_request(rq, "DeleteTable")

    def describe_table(self, namespace: bytes, table: bytes) -> GetTableDescriptorsResponse:
        rq = GetTableDescriptorsRequest()
        pb_name = PBTableName(namespace=namespace or b'default', qualifier=table)
        rq.table_names.append(pb_name)
        return self.send_request(rq, "GetTableDescriptors")

    def list_table_descriptors(self, pattern: str = ".*",
                               include_sys_table: bool = False) -> GetTableDescriptorsResponse:
        rq = GetTableDescriptorsRequest()
        rq.regex = pattern
        rq.include_sys_tables = include_sys_table
        return self.send_request(rq, "GetTableDescriptors")

    def create_namespace(self, namespace):
        """Create a namespace. Accepts bytes or str."""
        if isinstance(namespace, bytes):
            ns_bytes = namespace
        else:
            ns_bytes = str(namespace).encode('utf-8')
        rq = CreateNamespaceRequest()
        # NamespaceDescriptor.name is a bytes field in HBase proto
        rq.namespaceDescriptor.name = ns_bytes
        self.send_request(rq, "CreateNamespace")

    def delete_namespace(self, namespace):
        """Delete a namespace. Accepts bytes or str."""
        if isinstance(namespace, bytes):
            ns_str = namespace.decode('utf-8')
        else:
            ns_str = str(namespace)
        rq = DeleteNamespaceRequest()
        rq.namespaceName = ns_str
        self.send_request(rq, "DeleteNamespace")

    def list_namespaces(self) -> list:
        """Return a list of namespace names (strings)."""
        rq = ListNamespacesRequest()
        resp = self.send_request(rq, "ListNamespaces")
        if resp is None:
            return []
        return list(resp.namespaceName)

    def truncate_table(self, namespace: bytes, table: bytes, preserve_splits: bool = False):
        """Truncate a table. preserve_splits decides whether to keep split points."""
        rq = TruncateTableRequest()
        rq.tableName.namespace = namespace or b'default'
        rq.tableName.qualifier = table
        rq.preserveSplits = bool(preserve_splits)
        # The master will schedule a procedure; no detailed response parsing here.
        self.send_request(rq, "truncateTable")

    # ==================== Column Family Operations ====================

    def add_column(self, namespace: bytes, table: bytes, column_family: ColumnFamilySchema):
        """Add a column family to an existing table.

        Args:
            namespace: Namespace name (bytes)
            table: Table name (bytes)
            column_family: ColumnFamilySchema with name and attributes
        """
        rq = AddColumnRequest()
        rq.table_name.namespace = namespace or b'default'
        rq.table_name.qualifier = table
        rq.column_families.CopyFrom(column_family)
        self.send_request(rq, "AddColumn")

    def delete_column(self, namespace: bytes, table: bytes, column_name: bytes):
        """Delete a column family from a table.

        Args:
            namespace: Namespace name (bytes)
            table: Table name (bytes)
            column_name: Column family name to delete (bytes)
        """
        rq = DeleteColumnRequest()
        rq.table_name.namespace = namespace or b'default'
        rq.table_name.qualifier = table
        rq.column_name = column_name
        self.send_request(rq, "DeleteColumn")

    def modify_column(self, namespace: bytes, table: bytes, column_family: ColumnFamilySchema):
        """Modify an existing column family.

        Args:
            namespace: Namespace name (bytes)
            table: Table name (bytes)
            column_family: ColumnFamilySchema with updated attributes
        """
        rq = ModifyColumnRequest()
        rq.table_name.namespace = namespace or b'default'
        rq.table_name.qualifier = table
        rq.column_families.CopyFrom(column_family)
        self.send_request(rq, "ModifyColumn")

    # ==================== Snapshot Operations ====================

    def snapshot(self, namespace: bytes, table: bytes, snapshot_name: str,
                 snapshot_type: int = 1) -> SnapshotResponse:
        """Create a snapshot of a table.

        Args:
            namespace: Namespace name (bytes)
            table: Table name (bytes)
            snapshot_name: Name for the snapshot
            snapshot_type: Snapshot type (0=DISABLED, 1=FLUSH, 2=SKIPFLUSH)

        Returns:
            SnapshotResponse with expected_timeout
        """
        rq = SnapshotRequest()
        rq.snapshot.name = snapshot_name
        rq.snapshot.table = (namespace or b'default').decode('utf-8') + ':' + table.decode('utf-8')
        rq.snapshot.type = snapshot_type
        return self.send_request(rq, "Snapshot")

    def list_snapshots(self) -> GetCompletedSnapshotsResponse:
        """List all completed snapshots.

        Returns:
            GetCompletedSnapshotsResponse with list of SnapshotDescription
        """
        rq = GetCompletedSnapshotsRequest()
        return self.send_request(rq, "GetCompletedSnapshots")

    def delete_snapshot(self, snapshot_name: str):
        """Delete a snapshot.

        Args:
            snapshot_name: Name of the snapshot to delete
        """
        rq = DeleteSnapshotRequest()
        rq.snapshot.name = snapshot_name
        self.send_request(rq, "DeleteSnapshot")

    def restore_snapshot(self, snapshot_name: str, table_name: str = None,
                         restore_acl: bool = False) -> RestoreSnapshotResponse:
        """Restore a table from a snapshot.

        Args:
            snapshot_name: Name of the snapshot to restore
            table_name: Optional target table name (if different from original)
            restore_acl: Whether to restore ACL permissions

        Returns:
            RestoreSnapshotResponse with proc_id
        """
        rq = RestoreSnapshotRequest()
        rq.snapshot.name = snapshot_name
        if table_name:
            rq.snapshot.table = table_name
        rq.restoreACL = restore_acl
        return self.send_request(rq, "RestoreSnapshot")

    def is_snapshot_done(self, snapshot_name: str) -> bool:
        """Check if a snapshot operation is complete.

        Args:
            snapshot_name: Name of the snapshot to check

        Returns:
            True if snapshot is done, False otherwise
        """
        rq = IsSnapshotDoneRequest()
        rq.snapshot.name = snapshot_name
        resp = self.send_request(rq, "IsSnapshotDone")
        return resp.done if resp else False

    # ==================== Region Operations ====================

    def split_region(self, region_info: RegionInfo, split_key: Optional[bytes] = None):
        """Split a region using RegionInfo.

        Args:
            region_info: RegionInfo of the region to split
            split_key: Optional key to split at (if None, auto-determined)
        """
        rq = SplitTableRegionRequest()
        rq.region_info.CopyFrom(region_info)
        if split_key:
            rq.split_row = split_key
        self.send_request(rq, "SplitRegion")

    def merge_regions(self, region1: RegionSpecifier, region2: RegionSpecifier,
                      forcible: bool = False):
        """Merge two adjacent regions.

        Args:
            region1: RegionSpecifier for first region
            region2: RegionSpecifier for second region
            forcible: Force merge even if not adjacent
        """
        rq = MergeTableRegionsRequest()
        rq.region.append(region1)
        rq.region.append(region2)
        rq.forcible = forcible
        self.send_request(rq, "MergeTableRegions")

    def assign_region(self, region: RegionSpecifier, override: bool = False):
        """Assign a region to a server.

        Args:
            region: RegionSpecifier for the region to assign
            override: Override current assignment
        """
        rq = AssignRegionRequest()
        rq.region.CopyFrom(region)
        rq.override = override
        self.send_request(rq, "AssignRegion")

    def unassign_region(self, region: RegionSpecifier, force: bool = False):
        """Unassign a region from its current server.

        Args:
            region: RegionSpecifier for the region to unassign
            force: Force unassignment
        """
        rq = UnassignRegionRequest()
        rq.region.CopyFrom(region)
        rq.force = force
        self.send_request(rq, "UnassignRegion")

    # ==================== Cluster Operations ====================

    def get_cluster_status(self) -> GetClusterStatusResponse:
        """Get cluster status and metrics.

        Returns:
            GetClusterStatusResponse with cluster information
        """
        rq = GetClusterStatusRequest()
        return self.send_request(rq, "GetClusterStatus")

    def balance(self, ignore_rit: bool = False, dry_run: bool = False) -> BalanceResponse:
        """Trigger cluster load balancing.

        Args:
            ignore_rit: Ignore regions in transition
            dry_run: Calculate but don't execute moves

        Returns:
            BalanceResponse with balancer_ran status
        """
        rq = BalanceRequest()
        rq.ignore_rit = ignore_rit
        rq.dry_run = dry_run
        return self.send_request(rq, "Balance")

    def set_balancer_running(self, on: bool, synchronous: bool = False) -> SetBalancerRunningResponse:
        """Enable or disable the load balancer.

        Args:
            on: True to enable, False to disable
            synchronous: Wait for current balance to complete

        Returns:
            SetBalancerRunningResponse with previous balance value
        """
        rq = SetBalancerRunningRequest()
        rq.on = on
        rq.synchronous = synchronous
        return self.send_request(rq, "SetBalancerRunning")

    def is_balancer_enabled(self) -> IsBalancerEnabledResponse:
        """Check if the load balancer is enabled.

        Returns:
            IsBalancerEnabledResponse with enabled status
        """
        rq = IsBalancerEnabledRequest()
        return self.send_request(rq, "IsBalancerEnabled")
