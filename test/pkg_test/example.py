from hbasedriver.client import Client
from hbasedriver.exceptions import TableExistsException


def test_example():
    # lets say your hbase instance runs on 127.0.0.1
    client = Client(["127.0.0.1"])
    try:
        client.create_table("", "mytable", ['cf1', 'cf2'])
    except TableExistsException:
        pass
    table = client.get_table("", "mytable")
    table.put("row1", {'cf1': {'qf': '666'}})
    result = table.get("row1", {'cf1': ['qf']})
    print(result)
