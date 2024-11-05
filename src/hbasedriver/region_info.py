# /**
#  * Information about a region. A region is a range of keys in the whole keyspace of a table, an
#  * identifier (a timestamp) for differentiating between subset ranges (after region split) and a
#  * replicaId for differentiating the instance for the same range and some status information about
#  * the region. The region has a unique name which consists of the following fields:
#  * <ul>
#  * <li>tableName : The name of the table</li>
#  * <li>startKey : The startKey for the region.</li>
#  * <li>regionId : A timestamp when the region is created.</li>
#  * <li>replicaId : An id starting from 0 to differentiate replicas of the same region range but
#  * hosted in separated servers. The same region range can be hosted in multiple locations.</li>
#  * <li>encodedName : An MD5 encoded string for the region name.</li>
#  * </ul>
#  * <br>
#  * Other than the fields in the region name, region info contains:
#  * <ul>
#  * <li>endKey : the endKey for the region (exclusive)</li>
#  * <li>split : Whether the region is split</li>
#  * <li>offline : Whether the region is offline</li>
#  * </ul>
#  */
import hashlib

from hbasedriver.common.table_name import TableName
import struct

# Constants
HConstants_DELIMITER = b','
REPLICA_ID_DELIMITER = b'_'
ENC_SEPARATOR = ord('.')
MD5_HEX_LENGTH = 32
REPLICA_ID_FORMAT = "%04X"
DEFAULT_REPLICA_ID = 0


class RegionInfo:
    def __init__(self, table_name: 'TableName', start_key: bytes, end_key: bytes, split: bool, region_id: int,
                 replica_id: int,
                 offline: bool):
        self.table_name = table_name
        self.start_key = start_key
        self.end_key = end_key
        self.split = split
        self.region_id = region_id
        self.replica_id = replica_id
        self.offline = offline

    @staticmethod
    def create_region_name(table_name: bytes, start_key, id, replica_id, new_format):
        # Calculate the length of the resulting byte array
        len_table_name = len(table_name)
        len_start_key = len(start_key) if start_key else 0
        len_id = len(id)
        len_replica_id = len(struct.pack(">I", replica_id)) if replica_id > 0 else 0
        len_total = len_table_name + 2 + len_id + len_start_key
        if new_format:
            len_total += MD5_HEX_LENGTH + 2
        if replica_id > 0:
            len_total += 1 + len_replica_id

        # Initialize the byte array
        b = bytearray(len_total)

        # Copy table name
        offset = len_table_name
        b[:offset] = table_name
        b[offset] = HConstants_DELIMITER
        offset += 1

        # Copy start key if it exists
        if start_key:
            b[offset:offset + len_start_key] = start_key
            offset += len_start_key
        b[offset] = HConstants_DELIMITER
        offset += 1

        # Copy id
        b[offset:offset + len_id] = id
        offset += len_id

        # Append replica id if it's greater than 0
        if replica_id > 0:
            b[offset] = REPLICA_ID_DELIMITER
            offset += 1
            replica_id_bytes = struct.pack(">I", replica_id)
            b[offset:offset + len_replica_id] = replica_id_bytes
            offset += len_replica_id

        # Append MD5 hash if new_format is True
        if new_format:
            md5_hash = hashlib.md5(b[:offset]).hexdigest().encode('utf-8')
            if len(md5_hash) != MD5_HEX_LENGTH:
                print(f"MD5-hash length mismatch: Expected={MD5_HEX_LENGTH}; Got={len(md5_hash)}")

            b[offset] = ENC_SEPARATOR
            offset += 1
            b[offset:offset + MD5_HEX_LENGTH] = md5_hash
            offset += MD5_HEX_LENGTH
            b[offset] = ENC_SEPARATOR

        return bytes(b)


FIRST_META_REGIONINFO = RegionInfo(TableName.META_TABLE_NAME, b'', b'', False, 1, 0, False)
