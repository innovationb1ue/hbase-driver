import socket
from struct import pack, unpack

from google.protobuf import message

from hbasedriver.protobuf_py.RPC_pb2 import ConnectionHeader, RequestHeader, ResponseHeader
from hbasedriver.exceptions.RemoteException import RemoteException
from hbasedriver.exceptions.RemoteException import TableExistsException
from hbasedriver.response import response_types
from hbasedriver.util import to_varint, decoder


class Connection:
    def __init__(self, service_name):
        self.conn: socket.socket | None = None
        assert service_name in ["ClientService", "MasterService"]
        self.service_name = service_name
        self.meta_region = None

    def connect(self, host, port=16000, timeout=60, user="pythonHbaseDriver"):
        if type(host) != str:
            host = host.decode("utf-8")
        if type(port) != int:
            port = int(port)
        self.conn = socket.create_connection((host, port), timeout=timeout)
        ch = ConnectionHeader()
        ch.user_info.effective_user = user
        ch.service_name = self.service_name
        serialized = ch.SerializeToString()
        # 6 bytes : 'HBas' + RPC_VERSION(0) + AUTH_CODE(80) +
        msg = b"HBas\x00\x50" + pack(">I", len(serialized)) + serialized
        self.conn.send(msg)
        return self

    def send_request(self, req: message.Message, method_name: str, need_response=True):
        rpc_serialized = req.SerializeToString()
        # todo: save id and check result later
        # call_id = random.randint(1, 999)
        call_id = 66
        serialized_header = self._get_call_header_bytes(method_name, call_id)
        rpc_length_bytes = to_varint(len(rpc_serialized)).encode('utf-8')
        total_size = 4 + 1 + len(serialized_header) + len(rpc_length_bytes) + len(rpc_serialized)

        # Total length doesn't include the initial 4 bytes (for the total_length uint32)
        # size(4bytes) + header size(1byte)
        to_send = pack(">IB", total_size - 4, len(serialized_header))
        to_send += serialized_header + rpc_length_bytes + rpc_serialized

        self.conn.send(to_send)
        if need_response:
            return self.receive_rpc(call_id, req, method_name)
        else:
            return

    def _get_call_header_bytes(self, method_name, call_id: int):
        header = RequestHeader()
        header.call_id = call_id
        header.method_name = method_name
        header.request_param = True
        serialized_header: bytes = header.SerializeToString()
        return serialized_header

    # Receives exactly n bytes from the socket. Will block until n bytes are
    # received. If a socket is closed (RegionServer died) then raise an
    # exception that goes all the way back to the main client
    def _recv_n(self, sock: socket.socket, n):
        res = b''
        partial_len = 0
        while partial_len < n:
            packet = sock.recv(n - partial_len)
            if not packet:
                raise socket.error()
            partial_len += len(packet)
            res += packet
        return res

    def receive_rpc(self, call_id, rq: message.Message, rq_type: str, data=None):
        # Total message length is going to be the first four bytes
        # (little-endian uint32)
        try:
            msg_length = self._recv_n(self.conn, 4)
            if msg_length is None:
                raise
            msg_length = unpack(">I", msg_length)[0]
            # The message is then going to be however many bytes the first four
            # bytes specified. We don't want to overread or underread as that'll
            # cause havoc.
            full_data = self._recv_n(self.conn, msg_length)
        except socket.error:
            raise Exception("cant read enough data when receiving response. ")
        # Pass in the full data as well as your current position to the
        # decoder. It'll then return two variables:
        #       - next_pos: The number of bytes of data specified by the varint
        #       - pos: The starting location of the data to read.

        header_size, pos = decoder(full_data, 0)

        header = ResponseHeader()
        header.ParseFromString(full_data[pos: pos + header_size])

        if header.exception.exception_class_name != '':
            if header.exception.exception_class_name == "org.apache.hadoop.hbase.TableExistsException":
                raise TableExistsException(header.exception.exception_class_name, header.exception.stack_trace)

            raise RemoteException(header.exception.exception_class_name, header.exception.stack_trace)

        pos += header_size
        if header.call_id != call_id:
            # this should not happen, currently we don't provide concurrent access throw a single instance.
            raise Exception("call id is wrong. ")

        rpc_size, pos = decoder(full_data, pos)
        # if we didn't put the related response type, it means that we do not need a response.
        if response_types.get(rq_type) is None:
            return

        rpc: message.Message = response_types[rq_type]()
        rpc.ParseFromString(full_data[pos: pos + rpc_size])
        # The rpc is fully built!
        return rpc
