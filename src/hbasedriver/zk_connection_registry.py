from struct import unpack

from hbasedriver.master1.region_state import RegionState
from hbasedriver.protobuf_py import ZooKeeper_pb2
from kazoo.client import KazooClient

from hbasedriver import hconstants
from hbasedriver.hregion_location import HRegionLocation
from hbasedriver.protobuf_py.ClusterStatus_pb2 import RegionState as RegionStateProto
from hbasedriver.znode_paths import ZNodePaths

import logging

logger = logging.getLogger('pybase.' + __name__)
logger.setLevel(logging.DEBUG)


class ZKConnectionRegistry:
    def __init__(self, conf: dict):
        self.conf = conf
        self.znode_paths = ZNodePaths(conf)
        self.zk = KazooClient(hosts=self.__build_zk_quorum_server_string(), timeout=3)
        self.zk.start(timeout=10)

    def get_meta_region_locations(self):

        locations: dict[int, HRegionLocation] = {}

        paths: list[str] = self.zk.get_children(self.znode_paths.baseZNode)
        for path in paths:
            if not self.znode_paths.is_meta_znode_prefix(path):
                continue
            # todo: adapt for replicated hbase cluster
            replica_id_str = path[len(self.znode_paths.metaZNodePrefix):]
            if not replica_id_str:
                replica_id = 0
            else:
                replica_id = int(replica_id_str)
            meta_znode_path = self.znode_paths.baseZNode + '/' + path
            rsp, znodestat = self.zk.get(meta_znode_path)

            meta_proto = self.__get_meta_proto(rsp)
            state = RegionState.convert(meta_proto.state)
            sn_proto = meta_proto.server
            from hbasedriver.server_name import ServerName
            server_name = ServerName(sn_proto.host_name, sn_proto.port, sn_proto.start_code)
            if state != RegionStateProto.OPEN:
                logger.warning("Meta region is in state %s", state)
            from hbasedriver.region_info import FIRST_META_REGIONINFO
            locations[replica_id] = HRegionLocation(FIRST_META_REGIONINFO, server_name, hconstants.NO_SEQNUM)
            break
        from hbasedriver.region_locations import RegionLocations
        return RegionLocations(list(locations.values()))

    def __build_zk_quorum_server_string(self):
        hosts: list[str] = self.conf.get(hconstants.ZOOKEEPER_QUORUM, ["localhost"])
        port: int = int(self.conf.get(hconstants.ZOOKEEPER_CLIENT_PORT, 2181))
        res = ""
        first = True
        for host in hosts:
            host_and_port = ""
            if ':' in host:
                host_and_port += host
            else:
                host_and_port += host + ":" + str(port)
            if not first:
                res += ","
            res += host_and_port
            first = False
        logger.debug("build zk server string = %s", res)
        return res

    # decode encrypted data from zk vnode.
    def __get_meta_proto(self, rsp: bytes):
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
        return meta
