import random

from hbasedriver.client.client import Client
from hbasedriver.operations.delete import Delete
from hbasedriver.operations.get import Get
from hbasedriver.operations.put import Put
from hbasedriver.operations.scan import Scan


def test_scan():
    client = Client(["127.0.0.1"])
    table = client.get_table("", "test_table")
    resp = table.scan(Scan(b"row1").add_column(b'cf1', b'qf1'))

    print(resp)
    res = next(resp)
    print(res)
