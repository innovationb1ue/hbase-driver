"""Connection pooling module for HBase driver.

This module provides connection pooling functionality to reduce connection overhead
and improve resource management when connecting to HBase RegionServers.
"""

from hbasedriver.connection.connection_pool import ConnectionPool, PooledConnection

__all__ = ['ConnectionPool', 'PooledConnection']
