def test_example():
    import time
    from hbasedriver.client import Client
    from hbasedriver.operations import Put, Get, Scan, Delete
    from hbasedriver.exceptions.RemoteException import TableExistsException
    from hbasedriver.operations.column_family_builder import ColumnFamilyDescriptorBuilder

    conf = dict()
    conf[HConst]

    # lets say your hbase instance runs on 127.0.0.1 (zk quorum address)
    client = Client(["127.0.0.1"])

    # Define column families
    cf1_builder = ColumnFamilyDescriptorBuilder(b"cf1")
    cf1_builder.set_compression_type(b"SNAPPY")
    cf1_builder.set_max_versions(5)
    cf1_builder.set_time_to_live(86400)
    cf1_builder.set_block_size(65536)
    cf1_descriptor = cf1_builder.build()

    cf2_builder = ColumnFamilyDescriptorBuilder(b"cf2")
    cf2_builder.set_compression_type(b"LZO")
    cf2_builder.set_max_versions(3)
    cf2_builder.set_time_to_live(3600)
    cf2_builder.set_block_size(32768)
    cf2_descriptor = cf2_builder.build()

    column_families = [cf1_descriptor, cf2_descriptor]

    try:
        client.create_table(b"", b"test_table_master", column_families, split_keys=[b"111111", b"222222", b"333333"])
    except TableExistsException:
        pass

    table = client.get_table("", "mytable")
    # put
    table.put(Put(b'row1').add_column(b'cf1', b'qf', b'666'))
    table.put(Put(b'row1').add_column(b'cf1', b'qf2', b'999'))
    table.put(Put(b'row1').add_column(b'cf2', b'qf', b'777'))
    table.put(Put(b'row2').add_column(b'cf1', b'qf123', b'777'))
    # get
    result = table.get(Get(b"row1").add_column(b'cf1', b'qf'))
    print("get result =", result)
    assert b'666' == result.get(b'cf1', b'qf')

    # scan
    scan_result = table.scan(Scan(b"row1").add_family(b'cf1'))
    # retrieve all results from the iterator.
    scan_result = list(scan_result)
    print("scan result below:")
    for row in scan_result:
        print(row)

    # delete
    table.delete(Delete(b"row1"))  # this will delete the whole row

    # check cf deleted
    result = table.get(Get(b"row1").add_column(b'cf1', b'qf'))
    assert result is None

    # disable table
    client.master_conn.disable_table(None, b"mytable")
    time.sleep(1)
    client.master_conn.delete_table(None, b"mytable")


if __name__ == '__main__':
    test_example()
