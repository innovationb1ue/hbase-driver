from Client_pb2 import GetResponse, MutateResponse, ScanResponse

response_types = {
    "Get": GetResponse,
    "Mutate": MutateResponse,
    "Scan": ScanResponse
}
