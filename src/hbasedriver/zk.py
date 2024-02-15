import logging
from struct import unpack

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.handlers.threading import KazooTimeoutError

from hbasedriver.protobuf_py import ZooKeeper_pb2
from hbasedriver.protobuf_py.ZooKeeper_pb2 import Master

logger = logging.getLogger('pybase.' + __name__)
logger.setLevel(logging.DEBUG)

znode = "/hbase"


# locate_master takes a string representing the location of the ZooKeeper
# quorum. It then asks ZK for the location of the MetaRegionServer.
# i.e. this gets the region server that holds the hbase:meta table.
def locate_meta_region(zkquorum: list, establish_connection_timeout=5, missing_znode_retries=5) -> (bytes, bytes):
    if type(zkquorum) != list:
        raise ValueError("must provide a list for zookeeper quorum.")
    try:
        for host in zkquorum:
            zk = KazooClient(hosts=host, timeout=3)
            zk.start(timeout=establish_connection_timeout)
            break
        else:
            raise Exception("can not connect to zk via any contact point {}".format(zkquorum))
    except KazooTimeoutError:
        raise Exception("Cannot connect to ZooKeeper at {}".format(zkquorum[0]))

    # locate meta region
    try:
        rsp, znodestat = zk.get(znode + "/meta-region-server")
    except NoNodeError:
        logger.error("cant locate meta-region-server, zk has no such node. ")
        raise Exception("zk locate meta failed")

    zk.stop()
    if len(rsp) == 0:
        # Empty response is bad.
        raise Exception("ZooKeeper returned an empty response")
    # The first byte must be \xff.
    # 4 byte: length of id
    first_byte, id_length = unpack(">cI", rsp[:5])
    if first_byte != b'\xff':
        # Malformed response
        raise Exception(
            "ZooKeeper returned an invalid response")
    # skip bytes already read , id and an 8-byte long type salt.
    rsp = rsp[5 + id_length:]
    # data is prepended with PBMagic
    assert rsp[:4] == b'PBUF'
    rsp = rsp[4:]

    meta = ZooKeeper_pb2.MetaRegionServer()
    meta.ParseFromString(rsp)
    # here we got the master host and port.
    hostname = meta.server.host_name
    port = meta.server.port

    logger.info('Discovered meta region at %s:%s', hostname, port)
    return hostname, port


def locate_master(zkquorum: list, establish_connection_timeout=5, missing_znode_retries=5):
    if type(zkquorum) != list:
        raise ValueError("must provide a list for zookeeper quorum.")
    zk = None
    try:
        for host in zkquorum:
            zk = KazooClient(hosts=host, timeout=3)
            zk.start(timeout=establish_connection_timeout)
            break
    except KazooTimeoutError:
        raise Exception("Cannot connect to ZooKeeper at {}".format(zkquorum[0]))

    if not zk:
        raise Exception("can not connect to zk via any contact point {}".format(zkquorum))

    # locate master
    try:
        rsp, stat = zk.get("/hbase/master")
        first_byte, id_length = unpack(">cI", rsp[:5])
        if first_byte != b'\xff':
            # Malformed response
            raise Exception(
                "ZooKeeper returned an invalid response")
        # skip bytes already read , id and an 8-byte long type salt.
        rsp = rsp[5 + id_length:]
        # skip PBUF
        rsp = rsp[4:]
        master = Master.FromString(rsp)
        master_host = master.master.host_name
        master_port = master.master.port
        return master_host, master_port
    except Exception:
        raise
