from hbasedriver.client import Client
from hbasedriver.exceptions.RemoteException import TableExistsException


def test_example():
    # lets say your hbase instance runs on 127.0.0.1
    client = Client(["127.0.0.1"])
    try:
        client.create_table("", "mytable", ['cf1', 'cf2'])
    except TableExistsException:
        pass
    table = client.get_table("", "mytable")
    table.put(b"row1", {'cf1': {'qf': '666'}})
    result = table.get(b"row1", {'cf1': ['qf']})
    print(result)


if __name__ == '__main__':
    test_example()
