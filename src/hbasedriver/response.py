from hbasedriver.protobuf_py.Client_pb2 import GetResponse, MutateResponse, ScanResponse
from hbasedriver.protobuf_py.Master_pb2 import GetTableDescriptorsResponse

response_types = {
    # master responses
    "GetTableDescriptors": GetTableDescriptorsResponse,
    # client respnses
    "Get": GetResponse,
    "Mutate": MutateResponse,
    "Scan": ScanResponse,

}
