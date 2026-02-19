import os, time, traceback
from hbasedriver.client.client import Client
from hbasedriver.operations.put import Put
from hbasedriver.operations.get import Get
from hbasedriver.operations.column_family_builder import ColumnFamilyDescriptorBuilder
from hbasedriver.table_name import TableName

conf = {"hbase.zookeeper.quorum": os.getenv("HBASE_ZK", "hbase:2181")}
print('conf', conf)
client = Client(conf)
admin = client.get_admin()

tn = TableName.value_of(b"", b"truncate_debug")
if admin.table_exists(tn):
    try:
        admin.disable_table(tn)
    except Exception:
        pass
    admin.delete_table(tn)

cf = ColumnFamilyDescriptorBuilder(b"cf").build()
admin.create_table(tn, [cf])
print('created table')
table = client.get_table(tn.ns, tn.tb)

# Put a row
from hbasedriver.operations.put import Put
from hbasedriver.operations.get import Get

table.put(Put(b'row1').add_column(b'cf', b'q', b'v1'))
print('inserted row1')

try:
    val = table.get(Get(b'row1'))
    print('before truncate get:', val)
except Exception as e:
    print('before truncate get exception:', e)

print('calling truncate')
admin.truncate_table(tn, preserve_splits=False)
print('truncate returned')

for i in range(60):
    try:
        val = table.get(Get(b'row1'))
        print('attempt', i, 'get:', val)
        if val is None:
            print('truncated ok')
            break
    except Exception as e:
        print('attempt', i, 'exception:', type(e).__name__, e)
    time.sleep(1)

print('describe', admin.describe_table(tn))
print('Done')
