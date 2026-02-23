"""HBase configuration constants.

Compatible with HBase Java driver HConstants for easy migration.
"""

class HConstants:
    """HBase configuration constants.

    This class provides named constants for HBase configuration options,
    compatible with the Java HBase driver's HConstants class.
    Using these constants instead of string literals improves code readability
    and reduces typos in configuration keys.

    Example:
        >>> config = {
        ...     HConstants.ZOOKEEPER_QUORUM: "localhost:2181",
        ...     HConstants.CONNECTION_POOL_SIZE: 20
        ... }
        >>> client = Client(config)
    """

    # ZooKeeper settings
    ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum"
    ZOOKEEPER_ZNODE_PARENT = "zookeeper.znode.parent"
    ZOOKEEPER_SESSION_TIMEOUT = "zookeeper.session.timeout"
    ZOOKEEPER_CLIENT_PORT = "hbase.zookeeper.property.clientPort"

    # Connection pool settings
    CONNECTION_POOL_SIZE = "hbase.connection.pool.size"
    CONNECTION_IDLE_TIMEOUT = "hbase.connection.idle.timeout"

    # Default values
    DEFAULT_CONNECTION_POOL_SIZE = 10
    DEFAULT_CONNECTION_IDLE_TIMEOUT = 300
    DEFAULT_ZOOKEEPER_CLIENT_PORT = 2181
    DEFAULT_ZOOKEEPER_ZNODE_PARENT = "/hbase"
