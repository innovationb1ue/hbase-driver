"""
Pytest configuration and fixtures.
Handles HBase connection retries and graceful test skipping.
"""
import os
import uuid
import pytest
from hbasedriver.client.client import Client
from hbasedriver.common.table_name import TableName
from hbasedriver.operations.column_family_builder import ColumnFamilyDescriptorBuilder

conf = {"hbase.zookeeper.quorum": os.getenv("HBASE_ZK", "127.0.0.1")}


@pytest.fixture(scope="session")
def hbase_available():
    """Check if HBase is available and ready."""
    max_retries = 30
    for attempt in range(max_retries):
        try:
            client = Client(conf)
            # If we can create a client, HBase is ready
            return True
        except Exception as e:
            if attempt == max_retries - 1:
                pytest.skip(f"HBase not available after {max_retries} retries: {e}")
            import time
            time.sleep(1)
    return False


@pytest.fixture(scope="module")
def admin(hbase_available):
    """Get HBase admin connection with retries.

    Module-scoped for better isolation between test modules.
    """
    max_retries = 30
    last_error = None
    client = None

    for attempt in range(max_retries):
        try:
            client = Client(conf)
            admin = client.get_admin()
            break
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                import time
                time.sleep(1)
    else:
        pytest.fail(f"Could not connect to HBase admin after {max_retries} retries: {last_error}")

    yield admin

    # Cleanup: close client connection
    if client is not None:
        try:
            client.close()
        except Exception:
            pass


@pytest.fixture(scope="module")
def client(hbase_available):
    """Get HBase client connection with retries.

    Module-scoped for better isolation between test modules.
    """
    max_retries = 30
    last_error = None
    c = None

    for attempt in range(max_retries):
        try:
            c = Client(conf)
            break
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                import time
                time.sleep(1)
    else:
        pytest.fail(f"Could not connect to HBase after {max_retries} retries: {last_error}")

    yield c

    # Cleanup: close client connection
    if c is not None:
        try:
            c.close()
        except Exception:
            pass


@pytest.fixture
def unique_table_name():
    """Generate unique table name for each test to avoid collisions."""
    return f"test_{uuid.uuid4().hex[:8]}"
