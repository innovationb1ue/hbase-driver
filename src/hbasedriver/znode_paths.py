class ZNodePaths:
    ZNODE_PATH_SEPARATOR = '/'
    META_ZNODE_PREFIX_CONF_KEY = "zookeeper.znode.metaserver"
    META_ZNODE_PREFIX = "meta-region-server"
    DEFAULT_SNAPSHOT_CLEANUP_ZNODE = "snapshot-cleanup"

    def __init__(self, conf: dict):
        self.baseZNode = conf.get("zookeeper.znode.parent", "/hbase")
        self.metaZNodePrefix = conf.get(self.META_ZNODE_PREFIX_CONF_KEY, self.META_ZNODE_PREFIX)
        self.rsZNode = self.join_znode(self.baseZNode, conf.get("zookeeper.znode.rs", "rs"))
        self.drainingZNode = self.join_znode(self.baseZNode, conf.get("zookeeper.znode.draining.rs", "draining"))
        self.masterAddressZNode = self.join_znode(self.baseZNode, conf.get("zookeeper.znode.master", "master"))
        self.backupMasterAddressesZNode = self.join_znode(self.baseZNode,
                                                          conf.get("zookeeper.znode.backup.masters", "backup-masters"))
        self.clusterStateZNode = self.join_znode(self.baseZNode, conf.get("zookeeper.znode.state", "running"))
        self.tableZNode = self.join_znode(self.baseZNode, conf.get("zookeeper.znode.tableEnableDisable", "table"))
        self.clusterIdZNode = self.join_znode(self.baseZNode, conf.get("zookeeper.znode.clusterId", "hbaseid"))
        self.splitLogZNode = self.join_znode(self.baseZNode, conf.get("zookeeper.znode.splitlog", "SPLIT_LOGDIR_NAME"))
        self.balancerZNode = self.join_znode(self.baseZNode, conf.get("zookeeper.znode.balancer", "balancer"))
        self.regionNormalizerZNode = self.join_znode(self.baseZNode,
                                                     conf.get("zookeeper.znode.regionNormalizer", "normalizer"))
        self.switchZNode = self.join_znode(self.baseZNode, conf.get("zookeeper.znode.switch", "switch"))
        self.namespaceZNode = self.join_znode(self.baseZNode, conf.get("zookeeper.znode.namespace", "namespace"))
        self.masterMaintZNode = self.join_znode(self.baseZNode,
                                                conf.get("zookeeper.znode.masterMaintenance", "master-maintenance"))
        self.replicationZNode = self.join_znode(self.baseZNode, conf.get("zookeeper.znode.replication", "replication"))
        self.peersZNode = self.join_znode(self.replicationZNode, conf.get("zookeeper.znode.replication.peers", "peers"))
        self.queuesZNode = self.join_znode(self.replicationZNode, conf.get("zookeeper.znode.replication.rs", "rs"))
        self.hfileRefsZNode = self.join_znode(self.replicationZNode,
                                              conf.get("zookeeper.znode.replication.hfile.refs", "hfile-refs"))
        self.snapshotCleanupZNode = self.join_znode(self.baseZNode, conf.get("zookeeper.znode.snapshot.cleanup",
                                                                             self.DEFAULT_SNAPSHOT_CLEANUP_ZNODE))

    def __str__(self):
        return (
            f"ZNodePaths [baseZNode={self.baseZNode}, rsZNode={self.rsZNode}, drainingZNode={self.drainingZNode}, "
            f"masterAddressZNode={self.masterAddressZNode}, backupMasterAddressesZNode={self.backupMasterAddressesZNode}, "
            f"clusterStateZNode={self.clusterStateZNode}, tableZNode={self.tableZNode}, clusterIdZNode={self.clusterIdZNode}, "
            f"splitLogZNode={self.splitLogZNode}, balancerZNode={self.balancerZNode}, regionNormalizerZNode={self.regionNormalizerZNode}, "
            f"switchZNode={self.switchZNode}, namespaceZNode={self.namespaceZNode}, masterMaintZNode={self.masterMaintZNode}, "
            f"replicationZNode={self.replicationZNode}, peersZNode={self.peersZNode}, queuesZNode={self.queuesZNode}, "
            f"hfileRefsZNode={self.hfileRefsZNode}, snapshotCleanupZNode={self.snapshotCleanupZNode}]"
        )

    def get_znode_for_replica(self, replica_id: int) -> str:
        if self.is_default_replica(replica_id):
            return self.join_znode(self.baseZNode, self.metaZNodePrefix)
        else:
            return self.join_znode(self.baseZNode, f"{self.metaZNodePrefix}-{replica_id}")

    def get_meta_replica_id_from_path(self, path: str) -> int:
        prefix_len = len(self.baseZNode) + 1
        return self.get_meta_replica_id_from_znode(path[prefix_len:])

    def get_meta_replica_id_from_znode(self, znode: str) -> int:
        return 0 if znode == self.metaZNodePrefix else int(znode[len(self.metaZNodePrefix) + 1:])

    def is_meta_znode_prefix(self, znode: str) -> bool:
        return znode is not None and znode.startswith(self.metaZNodePrefix)

    def is_meta_znode_path(self, path: str) -> bool:
        prefix_len = len(self.baseZNode) + 1
        return len(path) > prefix_len and self.is_meta_znode_prefix(path[prefix_len:])

    def is_client_readable(self, path: str) -> bool:
        return (
                path == self.baseZNode or
                self.is_meta_znode_path(path) or
                path == self.masterAddressZNode or
                path == self.clusterIdZNode or
                path == self.rsZNode or
                path == self.tableZNode or
                path.startswith(f"{self.tableZNode}/")
        )

    def get_rs_path(self, sn: str) -> str:
        return self.join_znode(self.rsZNode, str(sn))

    @staticmethod
    def join_znode(prefix: str, suffix: str) -> str:
        return f"{prefix}{ZNodePaths.ZNODE_PATH_SEPARATOR}{suffix}"

    @staticmethod
    def is_default_replica(replica_id: int) -> bool:
        return replica_id == 0
