from hbasedriver.protobuf_py.Client_pb2 import GetResponse, MutateResponse, ScanResponse
from hbasedriver.protobuf_py.Master_pb2 import GetTableDescriptorsResponse, ListNamespacesResponse

response_types = {
    # master responses
    "GetTableDescriptors": GetTableDescriptorsResponse,
    "ListNamespaces": ListNamespacesResponse,
    # client respnses
    "Get": GetResponse,
    "Mutate": MutateResponse,
    "Scan": ScanResponse,

}
