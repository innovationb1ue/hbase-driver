from Client_pb2 import GetRequest, Column, ScanRequest, ScanResponse, MutateRequest, MutationProto, MutateResponse
from HBase_pb2 import RegionLocation, RegionInfo

from src.hbasedriver.Connection import Connection
from src.hbasedriver.model.row import Row
from src.hbasedriver.region import Region
from src.hbasedriver.util import to_bytes


class RsConnection(Connection):
    def __init__(self):
        super().__init__("ClientService")

    def put(self, region_name_encoded, rowkey: bytes, cf_to_qf_vals: dict):
        """
        Perform a PUT request on this regionserver.
        :param region_name_encoded: encoded region name get by scanning meta.
        :param rowkey: row key in bytes.
        :param cf_to_qf_vals: in the format of dict{"cf": {"qf1": "val1", "qf2": "val2"}, ...}
        :return: is the request get processed? (return by server. )
        """
        # send put request to the target region and receive response(processed?)
        rq = MutateRequest()
        # set target region
        rq.region.type = 1
        rq.region.value = region_name_encoded
        # set kv pairs
        rq.mutation.mutate_type = MutationProto.MutationType.PUT
        rq.mutation.row = rowkey
        for cf, qf_value_pairs in cf_to_qf_vals.items():
            col = MutationProto.ColumnValue(family=bytes(cf, 'utf-8'))
            for qf, val in qf_value_pairs.items():
                col.qualifier_value.append(
                    MutationProto.ColumnValue.QualifierValue(qualifier=bytes(qf, "utf-8"), value=bytes(val, 'utf-8')))
            rq.mutation.column_value.append(col)
        resp: MutateResponse = self.send_request(rq, "Mutate")
        return resp.processed

    def get(self, region: Region, rowkey: bytes, cf_to_qfs: dict):
        # send GET request to that region and receive response
        rq = GetRequest()
        # set target region
        rq.region.type = 1
        rq.region.value = region.region_encoded
        # rowkey
        rq.get.row = rowkey
        # cfs
        for cf, qfs in cf_to_qfs.items():
            # get all qualifiers
            if len(qfs) == 0:
                rq.get.column.append(Column(family=bytes(cf, 'utf-8')))
                continue
            # get target qualifiers only
            if type(qfs[0]) != bytes:
                qfs = to_bytes(qfs)
            col = Column(family=bytes(cf, 'utf-8'), qualifier=qfs)
            rq.get.column.append(col)

        resp = self.send_request(rq, "Get")
        result = Row.from_result(resp.result)
        return result

    def delete(self, region: Region, rowkey: bytes, cf_to_qfs: dict):
        """
        If provided with cf to no qualifiers, we delete the whole cf.
        If provided with cf to some qualifiers, we delete those qualifiers only.
        :param region:
        :param rowkey:
        :param cf_to_qfs:
        :return:
        """
        rq = MutateRequest()
        rq.mutation.mutate_type = MutationProto.MutationType.DELETE
        rq.region.type = 1
        rq.region.value = region.region_encoded

        rq.mutation.row = rowkey

        # cfs
        for cf, qfs in cf_to_qfs.items():
            col = MutationProto.ColumnValue(family=bytes(cf, 'utf-8'))
            # deleting the whole family by adding a pseudo QualifierValue with no qualifier specified.
            if len(qfs) == 0:
                col.qualifier_value.append(
                    MutationProto.ColumnValue.QualifierValue(delete_type=MutationProto.DELETE_MULTIPLE_VERSIONS))
            else:
                # add any qualifier if provided.
                for qf in qfs:
                    # todo: support more delete types.
                    # currently we delete all columns smaller than the provided timestamp.
                    col.qualifier_value.append(
                        MutationProto.ColumnValue.QualifierValue(qualifier=bytes(qf, "utf-8"),
                                                                 delete_type=MutationProto.DELETE_MULTIPLE_VERSIONS))
            rq.mutation.column_value.append(col)

        resp: MutateResponse = self.send_request(rq, "Mutate")
        return resp.processed
