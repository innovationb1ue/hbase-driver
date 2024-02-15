def test_example():
    from hbasedriver.client import Client
    from hbasedriver.operations import Put, Get
    from hbasedriver.exceptions.RemoteException import TableExistsException

    # lets say your hbase instance runs on 127.0.0.1 (zk quorum address)
    client = Client(["127.0.0.1"])
    try:
        client.create_table("", "mytable", ['cf1', 'cf2'])
    except TableExistsException:
        pass
    table = client.get_table("", "mytable")
    table.put(Put(b'row1').add_column(b'cf1', b'qf', b'666'))
    result = table.get(Get(b"row1").add_column(b'cf1', b'qf'))
    print(result)


if __name__ == '__main__':
    test_example()
