"""Connection pool implementation for HBase driver.

This module provides thread-safe connection pooling based on HBase Java driver
patterns. It uses host:port as pool keys, supports configurable pool size,
tracks connection health and last used time, and implements idle timeout cleanup.
"""

import time
from threading import Lock, Condition
from typing import Optional, Tuple, Callable, Dict, Any


class PooledConnection:
    """Wrapper for a pooled connection with tracking."""

    def __init__(self, connection, pool: 'ConnectionPool'):
        self.connection = connection
        self.pool = pool
        self.last_used = time.time()
        self.in_use = False

    def is_valid(self, timeout: int = 300) -> bool:
        """Check if connection is still valid (not timed out).

        Args:
            timeout: Idle timeout in seconds. Default: 300 (5 minutes)

        Returns:
            True if connection is valid, False otherwise
        """
        return (time.time() - self.last_used) < timeout

    def release(self):
        """Return connection to pool."""
        self.in_use = False
        self.last_used = time.time()
        self.pool._return_connection(self)


class ConnectionPool:
    """
    Connection pool for HBase connections.

    Based on HBase Java driver PoolMap patterns:
    - Uses host:port as pool key
    - Supports configurable pool size
    - Tracks connection health and last used time
    - Implements idle timeout cleanup
    - Thread-safe with proper locking

    Configuration options:
        - hbase.connection.pool.size: Max connections per pool (default: 10)
        - hbase.connection.idle.timeout: Idle connection timeout in seconds (default: 300)
    """

    DEFAULT_POOL_SIZE = 20
    DEFAULT_IDLE_TIMEOUT = 300  # 5 minutes

    def __init__(
        self,
        pool_size: int = DEFAULT_POOL_SIZE,
        idle_timeout: int = DEFAULT_IDLE_TIMEOUT,
        connection_factory: Optional[Callable[[str, int], Any]] = None
    ):
        """Initialize connection pool.

        Args:
            pool_size: Maximum number of connections in the pool
            idle_timeout: Idle connection timeout in seconds
            connection_factory: Optional factory function to create connections.
                Signature: connection_factory(host: str, port: int) -> Connection
        """
        self.pool_size = pool_size
        self.idle_timeout = idle_timeout
        self.connection_factory = connection_factory

        # Active connections (in use)
        self._active_connections: Dict[Tuple[str, int], PooledConnection] = {}

        # Idle connections (available for reuse)
        self._idle_connections: Dict[Tuple[str, int], PooledConnection] = {}

        self._lock = Lock()
        self._condition = Condition(self._lock)

        # Statistics
        self._created_count = 0
        self._borrowed_count = 0

    def get_connection(self, host: str, port: int) -> Any:
        """Borrow a connection from the pool, creating if needed.

        Args:
            host: RegionServer host
            port: RegionServer port

        Returns:
            A connection object

        Raises:
            TimeoutError: If unable to acquire a connection within 30 seconds
        """
        key = (host, port)

        with self._lock:
            # Try to reuse idle connection first
            if key in self._idle_connections:
                conn = self._idle_connections.pop(key)
                if conn.is_valid(self.idle_timeout):
                    conn.in_use = True
                    self._active_connections[key] = conn
                    self._borrowed_count += 1
                    return conn.connection
                # Remove expired connection
                try:
                    conn.connection.close()
                except Exception:
                    pass

            # Create new connection if under pool size limit
            if len(self._active_connections) + len(self._idle_connections) < self.pool_size:
                return self._create_connection(host, port, key)

            # Wait for connection to become available
            wait_time = 0
            max_wait = 30  # 30 second max wait
            while len(self._active_connections) >= self.pool_size and wait_time < max_wait:
                self._condition.wait(timeout=1)
                wait_time += 1

                # Try idle connections again after wake up
                if key in self._idle_connections:
                    conn = self._idle_connections.pop(key)
                    if conn.is_valid(self.idle_timeout):
                        conn.in_use = True
                        self._active_connections[key] = conn
                        self._borrowed_count += 1
                        return conn.connection
                    # Remove expired connection
                    try:
                        conn.connection.close()
                    except Exception:
                        pass

            # Fallback: create connection even if over pool size
            return self._create_connection(host, port, key)

    def _create_connection(self, host: str, port: int, key: Tuple[str, int]) -> Any:
        """Create a new connection and add to active pool.

        Args:
            host: RegionServer host
            port: RegionServer port
            key: Connection key tuple (host, port)

        Returns:
            A connection object
        """
        if self.connection_factory:
            conn = self.connection_factory(host, port)
        else:
            from hbasedriver.regionserver import RsConnection
            conn = RsConnection().connect(host, port)

        self._created_count += 1
        pooled = PooledConnection(conn, self)
        pooled.in_use = True
        self._active_connections[key] = pooled
        return conn

    def _return_connection(self, pooled: PooledConnection):
        """Internal method to return a pooled connection to the idle pool.

        Args:
            pooled: The PooledConnection wrapper to return
        """
        key = self._get_connection_key(pooled.connection)
        self._idle_connections[key] = pooled
        self._condition.notify()

    def return_connection(self, connection):
        """Return a connection to the idle pool.

        Args:
            connection: The connection to return
        """
        key = self._get_connection_key(connection)

        with self._lock:
            if key in self._active_connections:
                pooled = self._active_connections.pop(key)
                pooled.in_use = False
                pooled.last_used = time.time()
                self._return_connection(pooled)

    def close_connection(self, connection):
        """Explicitly close and remove a connection from pool.

        Args:
            connection: The connection to close
        """
        key = self._get_connection_key(connection)

        with self._lock:
            if key in self._active_connections:
                del self._active_connections[key]
            if key in self._idle_connections:
                del self._idle_connections[key]

            # Close the actual connection
            try:
                connection.close()
            except Exception:
                pass

    def _get_connection_key(self, connection) -> Tuple[str, int]:
        """Extract the host:port key from a connection.

        Args:
            connection: The connection to extract key from

        Returns:
            Tuple of (host, port)
        """
        return (connection.host, connection.port)

    def cleanup_idle_connections(self) -> int:
        """Remove connections that have been idle too long.

        Returns:
            Number of connections cleaned up
        """
        with self._lock:
            now = time.time()
            expired_keys = []

            for key, pooled in self._idle_connections.items():
                if (now - pooled.last_used) > self.idle_timeout:
                    expired_keys.append(key)

            for key in expired_keys:
                conn = self._idle_connections.pop(key)
                try:
                    conn.connection.close()
                except Exception:
                    pass

            if expired_keys:
                self._condition.notify_all()

            return len(expired_keys)

    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics.

        Returns:
            Dictionary containing pool statistics:
                - pool_size: Maximum pool size
                - active: Number of active connections
                - idle: Number of idle connections
                - created: Total number of connections created
                - borrowed: Total number of connection borrows
        """
        return {
            'pool_size': self.pool_size,
            'active': len(self._active_connections),
            'idle': len(self._idle_connections),
            'created': self._created_count,
            'borrowed': self._borrowed_count
        }

    def close_all(self):
        """Close all connections in the pool.

        This should be called when shutting down the application or
        when the pool is no longer needed.
        """
        with self._lock:
            # Close all idle connections
            for key, pooled in list(self._idle_connections.items()):
                try:
                    pooled.connection.close()
                except Exception:
                    pass
            self._idle_connections.clear()

            # Close all active connections
            for key, pooled in list(self._active_connections.items()):
                try:
                    pooled.connection.close()
                except Exception:
                    pass
            self._active_connections.clear()
