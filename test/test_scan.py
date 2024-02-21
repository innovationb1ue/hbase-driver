import random

from hbasedriver.client.client import Client
from hbasedriver.operations import Put
from hbasedriver.operations.scan import Scan


def test_scan():
    client = Client(["127.0.0.1"])
    table = client.get_table("", "test_table")
    table.put(Put(b'scan_row_key1').add_column(b'cf1', b'qf1', b'666').add_column(b'cf2', b'qf16', b'6666').add_column(
        b'cf2', b'qf17777', b'666'))
    table.put(Put(b'scan_row_key2').add_column(b'cf1', b'qf1', b'666').add_column(b'cf2', b'qf16', b'6666').add_column(
        b'cf2', b'qf17777', b'666'))
    table.put(Put(b'scan_row_key3').add_column(b'cf1', b'qf1', b'666').add_column(b'cf2', b'qf16', b'6666').add_column(
        b'cf2', b'qf17777', b'666'))
    resp = table.scan(Scan(b"scan_row_key1").add_family(b'cf1').add_family(b'cf2'))

    print(resp)
    for row in resp:
        print(row)
