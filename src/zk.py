from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.exceptions import NoNodeError

import protobuf_py.Admin_pb2
from protobuf_py import ZooKeeper_pb2
import protobuf_py.ZooKeeper_pb2
from struct import unpack
from time import sleep
import logging

logger = logging.getLogger('pybase.' + __name__)
logger.setLevel(logging.DEBUG)

znode = "/hbase"


# LocateMeta takes a string representing the location of the ZooKeeper
# quorum. It then asks ZK for the location of the MetaRegionServer,
# returning a tuple containing (host_name, port).
# i.e. this gets the master server.
def locate_meta(zkquorum, establish_connection_timeout=5, missing_znode_retries=5, zk=None):
    if zk is None:
        # Using Kazoo for interfacing with ZK
        zk = KazooClient(hosts=zkquorum, timeout=3)
    try:
        zk.start(timeout=establish_connection_timeout)
    except KazooTimeoutError:
        raise Exception("Cannot connect to ZooKeeper at {}".format(zkquorum))
    # MetaRegionServer information is located at /hbase/meta-region-server
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

    logger.info('Discovered Master at %s:%s', hostname, port)
    return hostname, port
