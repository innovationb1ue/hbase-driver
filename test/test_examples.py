import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import examples


class FakeTable:
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def put(self, op):
        self.client.calls.append(("put", self.name, op))

    def get(self, op):
        self.client.calls.append(("get", self.name, op))

        class Res:
            def get(self, family, qualifier):
                return b"val"

        return Res()

    def scan(self, op):
        self.client.calls.append(("scan", self.name, op))
        # yield a single row-like object
        yield {b"row1": {b"cf": b"v"}}

    def delete(self, op):
        self.client.calls.append(("delete", self.name, op))


class FakeAdmin:
    def __init__(self, client):
        self.client = client

    def create_namespace(self, ns):
        self.client.calls.append(("create_namespace", ns))

    def list_namespaces(self):
        self.client.calls.append(("list_namespaces",))
        return [b"ns1", b"ns2"]

    def delete_namespace(self, ns):
        self.client.calls.append(("delete_namespace", ns))

    def truncate_table(self, table_name):
        self.client.calls.append(("truncate_table", table_name))


class FakeClient:
    def __init__(self):
        self.calls = []

    def create_table(self, ns, tb, column_families, split_keys=None):
        self.calls.append(("create_table", ns, tb, column_families, split_keys))

    def get_table(self, ns, tb):
        self.calls.append(("get_table", ns, tb))
        return FakeTable(self, tb)

    def get_admin(self):
        self.calls.append(("get_admin",))
        return FakeAdmin(self)

    def describe_table(self, ns, tb):
        self.calls.append(("describe_table", ns, tb))
        return {"table_schema": [{"column_families": []}]}


def test_create_table_example():
    fake = FakeClient()
    examples.create_table_example(fake, b"ns", b"t1")
    assert ("create_table", b"ns", b"t1", [b"cf"], None) in fake.calls


def test_data_ops_example():
    fake = FakeClient()
    res, rows = examples.data_ops_example(fake, b"ns", b"t1")
    assert ("get_table", b"ns", b"t1") in fake.calls
    assert ("put", b"t1", b"put:row1:cf:q:val") in fake.calls
    assert ("get", b"t1", b"get:row1:cf:q") in fake.calls
    assert ("scan", b"t1", b"scan:row1") in fake.calls
    assert ("delete", b"t1", b"delete:row1") in fake.calls
    assert hasattr(res, "get")


def test_admin_namespace_example():
    fake = FakeClient()
    ns = examples.admin_namespace_example(fake, b"ns1")
    assert ("get_admin",) in fake.calls
    assert ("create_namespace", b"ns1") in fake.calls
    assert ("list_namespaces",) in fake.calls
    assert ("delete_namespace", b"ns1") in fake.calls


def test_truncate_table_example():
    fake = FakeClient()
    examples.truncate_table_example(fake, b"ns", b"t1")
    assert ("get_admin",) in fake.calls
    assert ("truncate_table", (b"ns", b"t1")) in fake.calls


def test_describe_table_example():
    fake = FakeClient()
    desc = examples.describe_table_example(fake, b"ns", b"t1")
    assert ("describe_table", b"ns", b"t1") in fake.calls
    assert isinstance(desc, dict)
