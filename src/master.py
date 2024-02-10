import socket
from io import StringIO
from struct import pack, unpack

from Client_pb2 import GetRequest
from RPC_pb2 import ConnectionHeader


class MasterConnection:
    def __init__(self):
        self.conn: socket.socket = None

    def connect(self, host, port, timeout=3, user="pythonHbaseDriver"):
        self.conn = socket.create_connection((host, port), timeout=timeout)
        ch = ConnectionHeader()
        ch.user_info.effective_user = user
        ch.service_name = "ClientService"
        serialized = ch.SerializeToString()
        # 6 bytes : 'HBas' + RPC_VERSION(0) + AUTH_CODE(80) +
        msg = b"HBas\x00\x50" + pack(">I", len(serialized)) + serialized
        self.conn.send(msg)

    def get(self):
        rq = GetRequest()
        rq.get.row = "hbase:meta,,"

    def create_table(self):
        rq = CreateTableRequest

    def receive_rpc(self, call_id, rq, data=None):
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
            pass
        # Pass in the full data as well as your current position to the
        # decoder. It'll then return two variables:
        #       - next_pos: The number of bytes of data specified by the varint
        #       - pos: The starting location of the data to read.
        next_pos, pos = decoder(full_data, 0)
        header = ResponseHeader()
        header.ParseFromString(full_data[pos: pos + next_pos])
        pos += next_pos
        if header.call_id != call_id:
            # call_ids don't match? Looks like a different thread nabbed our
            # response.
            return self._bad_call_id(call_id, rq, header.call_id, full_data)

    # Receives exactly n bytes from the socket. Will block until n bytes are
    # received. If a socket is closed (RegionServer died) then raise an
    # exception that goes all the way back to the main client
    def _recv_n(self, sock: socket.socket, n):
        partial_str = StringIO()
        partial_len = 0
        while partial_len < n:
            packet = sock.recv(n - partial_len)
            if not packet:
                raise socket.error()
            partial_len += len(packet)
            partial_str.write(packet)
        return partial_str.getvalue()
