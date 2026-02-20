"""Runnable example helpers for hbase-driver.

These helpers accept a `client` object (either the real hbasedriver.client.Client configured
for a cluster, or a lightweight fake client used in unit tests). They purposely avoid
importing generated protobuf types so they remain lightweight and safe to import in CI.
"""

from typing import Any


def create_table_example(client: Any, ns: bytes = b"default", tb: bytes = b"example_table",
                         column_families: list = None, split_keys: list[bytes] = None) -> None:
    """Create a table using the public Client.create_table API.

    Note: For real usage pass ColumnFamilySchema proto objects for `column_families`.
    This helper uses a simple placeholder list so it can be exercised in unit tests.
    """
    if column_families is None:
        # placeholder value accepted by the fake client used in tests
        column_families = [b"cf"]
    client.create_table(ns, tb, column_families, split_keys)


def data_ops_example(client: Any, ns: bytes = b"default", tb: bytes = b"mytable"):
    """Perform basic data operations against a table.

    The real driver expects Put/Get/Scan/Delete objects; this lightweight helper
    uses simple payloads so tests can run without a live HBase instance.
    Returns: (get_result, scanned_rows)
    """
    tbl = client.get_table(ns, tb)

    # Put: real code uses hbasedriver.operations.Put(...)
    tbl.put(b"put:row1:cf:q:val")

    # Get: real code uses hbasedriver.operations.Get(...)
    result = tbl.get(b"get:row1:cf:q")

    # Scan: real code uses Scan objects; here we request a scan iterator
    rows = list(tbl.scan(b"scan:row1"))

    # Delete whole row
    tbl.delete(b"delete:row1")

    return result, rows


def admin_namespace_example(client: Any, namespace_name: bytes = b"ns1") -> list:
    """Create, list, and delete a namespace using the Admin API (client.get_admin()).

    Returns the list of namespaces returned by list_namespaces().
    """
    admin = client.get_admin()
    admin.create_namespace(namespace_name)
    namespaces = admin.list_namespaces()
    admin.delete_namespace(namespace_name)
    return namespaces


def truncate_table_example(client: Any, ns: bytes = b"default", tb: bytes = b"mytable") -> bool:
    """Truncate a table using the Admin API.

    The Admin.truncate_table implementation in the real driver accepts a TableName or
    similar object; this helper uses a simple tuple for tests.
    """
    admin = client.get_admin()
    admin.truncate_table((ns, tb))
    return True


def describe_table_example(client: Any, ns: bytes = b"default", tb: bytes = b"mytable") -> Any:
    """Return the table description from the master via Client.describe_table.
    """
    return client.describe_table(ns, tb)
