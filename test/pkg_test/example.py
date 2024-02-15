from hbasedriver.client import Client
from hbasedriver.exceptions.RemoteException import TableExistsException
from hbasedriver.operations.get import Get
from hbasedriver.operations.put import Put


def test_example():
    # lets say your hbase instance runs on 127.0.0.1
    client = Client(["127.0.0.1"])
    try:
        client.create_table("", "mytable", ['cf1', 'cf2'])
    except TableExistsException:
        pass
    table = client.get_table("", "mytable")
    put = Put(b"row1").add_column(b'cf1', b'qf', b'666')
    table.put(put)
    get = Get(b"row1").add_column(b'cf1', b'qf')
    result = table.get(get)
    print(result)


if __name__ == '__main__':
    test_example()
