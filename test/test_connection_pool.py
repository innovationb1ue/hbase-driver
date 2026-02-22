"""Unit tests for connection pool functionality."""

import time
import pytest
from unittest.mock import Mock, MagicMock
from hbasedriver.connection.connection_pool import ConnectionPool, PooledConnection


class MockConnection:
    """Mock connection for testing."""

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.closed = False

    def close(self):
        self.closed = True


def mock_connection_factory():
    """Create a mock connection factory for testing."""
    def factory(host, port):
        return MockConnection(host, port)
    return factory


class MockConnection:
    """Mock connection for testing."""

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.closed = False

    def close(self):
        self.closed = True


class TestPooledConnection:
    """Tests for PooledConnection wrapper class."""

    def test_initialization(self):
        """Test PooledConnection initialization."""
        mock_pool = Mock()
        mock_conn = MockConnection("localhost", 16000)
        pooled = PooledConnection(mock_conn, mock_pool)

        assert pooled.connection == mock_conn
        assert pooled.pool == mock_pool
        assert not pooled.in_use
        assert isinstance(pooled.last_used, float)

    def test_is_valid_within_timeout(self):
        """Test is_valid returns True for connections within timeout."""
        mock_pool = Mock()
        mock_conn = MockConnection("localhost", 16000)
        pooled = PooledConnection(mock_conn, mock_pool)

        assert pooled.is_valid(timeout=300) is True

    def test_is_valid_expired(self):
        """Test is_valid returns False for expired connections."""
        mock_pool = Mock()
        mock_conn = MockConnection("localhost", 16000)
        pooled = PooledConnection(mock_conn, mock_pool)
        # Set last_used to past
        pooled.last_used = time.time() - 400

        assert pooled.is_valid(timeout=300) is False

    def test_release(self):
        """Test release marks connection as not in use and updates timestamp."""
        mock_pool = Mock()
        mock_conn = MockConnection("localhost", 16000)
        pooled = PooledConnection(mock_conn, mock_pool)
        pooled.in_use = True
        old_last_used = pooled.last_used

        time.sleep(0.01)  # Small delay to ensure timestamp changes
        pooled.release()

        assert not pooled.in_use
        assert pooled.last_used > old_last_used


class TestConnectionPool:
    """Tests for ConnectionPool class."""

    def test_initialization(self):
        """Test ConnectionPool initialization with defaults."""
        pool = ConnectionPool()

        assert pool.pool_size == ConnectionPool.DEFAULT_POOL_SIZE
        assert pool.idle_timeout == ConnectionPool.DEFAULT_IDLE_TIMEOUT
        assert pool.connection_factory is None
        assert len(pool._active_connections) == 0
        assert len(pool._idle_connections) == 0

    def test_initialization_with_custom_params(self):
        """Test ConnectionPool initialization with custom parameters."""
        pool = ConnectionPool(pool_size=20, idle_timeout=600)

        assert pool.pool_size == 20
        assert pool.idle_timeout == 600

    def test_initialization_with_factory(self):
        """Test ConnectionPool initialization with custom factory."""
        def factory(host, port):
            return MockConnection(host, port)

        pool = ConnectionPool(connection_factory=factory)

        assert pool.connection_factory == factory

    def test_get_connection_creates_new(self):
        """Test get_connection creates new connection when pool is empty."""
        pool = ConnectionPool(connection_factory=mock_connection_factory())

        conn = pool.get_connection("localhost", 16000)

        assert conn is not None
        assert conn.host == "localhost"
        assert conn.port == 16000
        assert len(pool._active_connections) == 1
        assert len(pool._idle_connections) == 0
        assert pool._created_count == 1

    def test_get_connection_reuses_idle(self):
        """Test get_connection reuses idle connection."""
        pool = ConnectionPool(connection_factory=mock_connection_factory())
        host, port = "localhost", 16000

        # Borrow and return connection
        conn1 = pool.get_connection(host, port)
        pool.return_connection(conn1)

        assert len(pool._idle_connections) == 1
        assert len(pool._active_connections) == 0

        # Borrow again - should reuse
        conn2 = pool.get_connection(host, port)

        assert len(pool._idle_connections) == 0
        assert len(pool._active_connections) == 1
        assert conn1 == conn2  # Same connection
        assert pool._borrowed_count == 1

    def test_get_connection_multiple_keys(self):
        """Test get_connection handles multiple host:port keys."""
        pool = ConnectionPool(connection_factory=mock_connection_factory())

        conn1 = pool.get_connection("localhost", 16000)
        conn2 = pool.get_connection("localhost", 16001)
        conn3 = pool.get_connection("otherhost", 16000)

        assert len(pool._active_connections) == 3
        assert pool._created_count == 3

    def test_return_connection(self):
        """Test return_connection moves connection to idle pool."""
        pool = ConnectionPool(connection_factory=mock_connection_factory())
        host, port = "localhost", 16000

        conn = pool.get_connection(host, port)
        assert len(pool._active_connections) == 1

        pool.return_connection(conn)
        assert len(pool._active_connections) == 0
        assert len(pool._idle_connections) == 1

    def test_close_connection(self):
        """Test close_connection removes and closes connection."""
        pool = ConnectionPool(connection_factory=mock_connection_factory())
        host, port = "localhost", 16000

        conn = pool.get_connection(host, port)
        pool.return_connection(conn)
        assert conn.closed is False

        pool.close_connection(conn)
        assert len(pool._idle_connections) == 0
        assert conn.closed is True

    def test_close_connection_active(self):
        """Test close_connection works on active connections."""
        pool = ConnectionPool(connection_factory=mock_connection_factory())
        host, port = "localhost", 16000

        conn = pool.get_connection(host, port)
        assert len(pool._active_connections) == 1

        pool.close_connection(conn)
        assert len(pool._active_connections) == 0
        assert conn.closed is True

    def test_cleanup_idle_connections(self):
        """Test cleanup_idle_connections removes expired connections."""
        pool = ConnectionPool(idle_timeout=1, connection_factory=mock_connection_factory())  # 1 second timeout
        host, port = "localhost", 16000

        # Create and return a connection
        conn = pool.get_connection(host, port)
        pool.return_connection(conn)

        # Manually set last_used to past
        for key, pooled in pool._idle_connections.items():
            pooled.last_used = time.time() - 2

        # Run cleanup
        cleaned = pool.cleanup_idle_connections()

        assert cleaned == 1
        assert len(pool._idle_connections) == 0
        assert conn.closed is True

    def test_cleanup_idle_connections_not_expired(self):
        """Test cleanup_idle_connections keeps valid connections."""
        pool = ConnectionPool(idle_timeout=300, connection_factory=mock_connection_factory())  # 5 minute timeout
        host, port = "localhost", 16000

        # Create and return a connection
        conn = pool.get_connection(host, port)
        pool.return_connection(conn)

        # Run cleanup immediately
        cleaned = pool.cleanup_idle_connections()

        assert cleaned == 0
        assert len(pool._idle_connections) == 1
        assert conn.closed is False

    def test_get_stats(self):
        """Test get_stats returns correct statistics."""
        pool = ConnectionPool(connection_factory=mock_connection_factory())

        stats = pool.get_stats()

        assert stats['pool_size'] == pool.pool_size
        assert stats['active'] == 0
        assert stats['idle'] == 0
        assert stats['created'] == 0
        assert stats['borrowed'] == 0

        # Create some connections
        conn1 = pool.get_connection("localhost", 16000)
        conn2 = pool.get_connection("localhost", 16001)

        stats = pool.get_stats()
        assert stats['active'] == 2
        assert stats['created'] == 2

        # Return one connection
        pool.return_connection(conn1)

        stats = pool.get_stats()
        assert stats['active'] == 1
        assert stats['idle'] == 1

    def test_close_all(self):
        """Test close_all closes all connections."""
        pool = ConnectionPool(connection_factory=mock_connection_factory())

        # Create and return multiple connections
        conn1 = pool.get_connection("localhost", 16000)
        conn2 = pool.get_connection("localhost", 16001)
        pool.return_connection(conn1)

        pool.close_all()

        assert len(pool._active_connections) == 0
        assert len(pool._idle_connections) == 0
        assert conn1.closed is True
        assert conn2.closed is True

    def test_custom_factory(self):
        """Test connection pool with custom factory function."""
        def custom_factory(host, port):
            return MockConnection(f"custom-{host}", port)

        pool = ConnectionPool(connection_factory=custom_factory)
        conn = pool.get_connection("localhost", 16000)

        assert conn.host == "custom-localhost"

    def test_pool_size_limit(self):
        """Test pool respects size limit when creating new connections."""
        pool = ConnectionPool(pool_size=2, connection_factory=mock_connection_factory())

        conn1 = pool.get_connection("host1", 16000)
        conn2 = pool.get_connection("host2", 16001)

        # At limit now, should create anyway (fallback)
        conn3 = pool.get_connection("host3", 16002)

        assert pool._created_count == 3

    def test_expired_connection_removal_on_borrow(self):
        """Test that expired idle connections are removed when borrowing."""
        pool = ConnectionPool(idle_timeout=1, connection_factory=mock_connection_factory())
        host, port = "localhost", 16000

        # Create and return a connection
        conn1 = pool.get_connection(host, port)
        pool.return_connection(conn1)

        # Manually set last_used to past to simulate expiration
        for key, pooled in pool._idle_connections.items():
            pooled.last_used = time.time() - 2

        # Borrow again - should create new connection due to expiration
        conn2 = pool.get_connection(host, port)

        assert conn1.closed is True  # Expired connection was closed
        assert len(pool._idle_connections) == 0
        assert len(pool._active_connections) == 1
        assert pool._created_count == 2
