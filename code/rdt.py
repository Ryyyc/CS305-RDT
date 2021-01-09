from USocket import UnreliableSocket
import struct, queue, threading, time, random, logging

LOG_FORMAT = "[%(sock_type)s %(levelname)s] %(filename)s[func: %(filename)s  line:%(lineno)d] %(asctime)s: %(message)s"
DATE_FORMAT = "%Y/%m/%d %H:%M:%S,uuu"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
rdt_logger = logging.getLogger("RDTSOCK")
RDT_SOCK_LOG_INFO = [{'sock_type': 'RDTSOCK'}, {'sock_type': 'CLIENT'},
                     {'sock_type': 'SERVER'}, {'sock_type': 'SERVER'}]

lock = threading.Lock()

RDT_SOCK_CLIENT = 1
RDT_SOCK_MASTER_SERVER = 2
RDT_SOCK_SERVER = 3

DEFAULT_BUFF_SIZE = 2048
DEFAULT_WIN_SIZE = 65535
CONSTANT2P32 = 4294967296  # 2^32

alpha = 0.125
beta = 0.25


class RDTSocket(UnreliableSocket):
    """
    Ref: https://docs.python.org/3/library/socket.html
    """

    def __init__(self, rate=None, debug=True):
        super().__init__(rate=rate)
        self._rate = rate
        self.debug = debug
        self._type = 0

        # master server
        self._allow_accept = False
        self._data_center = None
        self._super_recvfrom = super().recvfrom  # Provided to data_center for use
        self._conn_cnt = -1
        self._backlog = 10

        # server
        self._segment_buff = queue.Queue()

        # Retransmission mechanism
        self._recv_win_size = DEFAULT_WIN_SIZE  # recv window size
        self._local_seq_num = 0  # the position of next byte send in send window
        self._local_ack_num = 0  # the position of the next byte of the expected ack in recv window
        self._seq_base = 0
        self._ack_base = 0
        self._mss = 1460
        self._rtt_orign = 0.005

        # socket
        self._send_to = None
        self._recv_from = None
        self._receiving = False
        self._sending = False
        self._timeout = None
        self._local_addr = ('0.0.0.0', 0)
        self._peer_addr = ('255.255.255.255', 0)
        self._ack_num_option_buff = queue.Queue()
        self._seq_num_payload_buff = queue.Queue()
        self._data_buff_recv = bytearray()
        self._data_buff_send = bytearray()
        self._recv_thread = None
        self._send_thread = None
        self._local_closing = False
        self._local_closed = False
        self._peer_closing = False
        self._peer_closed = False

        # sack variable
        self._main_pq = queue.PriorityQueue()
        self._sub_pq = queue.PriorityQueue()
        self._ack_set = set()
        self._sub_ack_set = set()

        rdt_logger.setLevel(logging.INFO if debug else logging.WARNING)

    def bind(self, address: (str, int)) -> None:
        """
        Bind the socket to address. The socket must not already be bound.

        :param address: (ip, port), ip type is string, port type is integer(0~65535)
        """
        assert not self._local_closed, "Socket already closed, could not use again."
        super().bind(address)
        self._local_addr = super().getsockname()

    def getsockname(self) -> (str, int):
        """
        Return the socket’s own address.

        :return: (ip, port)
        """
        return self._local_addr

    def gettimeout(self):
        """
        Return the timeout in seconds (float) associated with socket operations,
        or None if no timeout is set.
        """
        return self._timeout

    def settimeout(self, timeout_sec: float):
        """
        Set a timeout on blocking socket operations.

        :param timeout_sec: nonnegative floating point number expressing seconds,
         or None, None express infinity
        """
        self._timeout = timeout_sec

    def listen(self, backlog: int = 0):
        """
        Enable a server to accept connections. If backlog is specified,
        it must be at least 0 (if it is lower, it is set to 0).

        :param backlog: the number of unaccepted connections
         that the system will allow before refusing new connections
        """
        assert not self._local_closed, "Socket already closed, could not use again."

        if backlog < 0:
            backlog = 0
        self._backlog = backlog
        if self._conn_cnt == -1:
            self._conn_cnt = 0
            self._type = RDT_SOCK_MASTER_SERVER
            self._recv_from = self._server_recvfrom
            self._send_to = self._server_sendto
            if self._local_addr == ('0.0.0.0', 0):
                self.bind(('127.0.0.1', 0))
                rdt_logger.info(f"Unbinding address, randomly bind one, at {self._local_addr}",
                                extra=RDT_SOCK_LOG_INFO[self._type])
            self._data_center = DataCenter(self)
            self._data_center.start()

    def accept(self) -> ('RDTSocket', (str, int)):
        """
        Accept a connection. The socket must be bound to an address and listening for
        connections.

        This function is blocked.

        :return: (conn, address) conn is a new socket object usable to send and
         receive data on the connection, and address is the address bound to
         the socket on the other end of the connection
        """
        assert not self._local_closed, "Socket already closed, could not use again."

        conn, addr = None, None
        if self._conn_cnt == -1:
            self.listen(0)

        self._allow_accept = True
        peer_info = {}

        rdt_logger.info(f"Server [{self._local_addr}] is waiting for the client to connect",
                        extra=RDT_SOCK_LOG_INFO[self._type])
        while True:
            rdt_seg, addr = self._recv_from()
            if rdt_seg is None:
                continue

            if rdt_seg.syn:
                if addr in peer_info:
                    seq_num = peer_info[addr]
                else:
                    if len(peer_info) > self._backlog:
                        continue
                    seq_num = random.randint(0, RDTSegment.SEQ_NUM_BOUND - 1)

                self._ack_base = rdt_seg.seq_num
                self._seq_base = seq_num
                ack_num = rdt_seg.seq_num + 1
                syn_ack_seg = RDTSegment(rdt_seg.dest_port, rdt_seg.src_port, seq_num, ack_num,
                                         ack=True, syn=True)
                self._send_to(syn_ack_seg, addr)
                peer_info[addr] = seq_num
                rdt_logger.info(f"Connection establishment request received from client [{addr}]",
                                extra=RDT_SOCK_LOG_INFO[self._type])
                rdt_logger.info(f"Client request connection pool: {peer_info}", extra=RDT_SOCK_LOG_INFO[self._type])
            elif addr in peer_info and rdt_seg.ack_num - 1 == peer_info[addr]:
                conn = RDTSocket(self._rate, self.debug)
                # master server
                conn._data_center = self._data_center
                # server
                conn._type = RDT_SOCK_SERVER
                # Retransmission mechanism
                conn._recv_win_size = DEFAULT_WIN_SIZE
                conn._local_seq_num = rdt_seg.ack_num
                conn._local_ack_num = rdt_seg.seq_num
                conn._seq_base = self._seq_base
                conn._ack_base = self._ack_base
                # socket
                conn._local_addr = self._local_addr
                conn._peer_addr = addr
                conn._recv_from = conn._server_recvfrom
                conn._send_to = conn._server_sendto

                if len(rdt_seg.payload) != 0:
                    conn._segment_buff.put((rdt_seg.encode(), addr), block=False)
                rdt_logger.info(f"Server [{self._local_addr}] accepts the connection from client [{addr}], "
                                f"start processing segment", extra=RDT_SOCK_LOG_INFO[self._type])
                self._data_center.add_sock(addr, conn)
                conn._recv_thread = ProcessingSegment(conn)
                conn._recv_thread.start()
                break

        self._allow_accept = False
        return conn, addr

    def connect(self, address: (str, int)):
        """
        Connect to a remote socket at address.
        Corresponds to the process of establishing a connection on the client side.

        :param address: (ip, port)
        """
        assert not self._local_closed, "Socket already closed, could not use again."

        # If there is no binding address, randomly bind one
        if self._local_addr == ('0.0.0.0', 0):
            self.bind(("127.0.0.1", 0))
            rdt_logger.info(f"Unbinding address, randomly bind one, at {self._local_addr}",
                            extra=RDT_SOCK_LOG_INFO[self._type])

        self._type = RDT_SOCK_CLIENT
        self._send_to = self._client_sendto
        self._recv_from = self._client_recvfrom
        self._recv_win_size = DEFAULT_WIN_SIZE
        self._peer_addr = address

        rdt_logger.info(f"client [{self._local_addr}] initiate connection request to server [{address}].",
                        extra=RDT_SOCK_LOG_INFO[self._type])

        # send syn pkt
        self._local_seq_num = random.randint(0, RDTSegment.SEQ_NUM_BOUND - 1)
        self._local_ack_num = 0
        self._seq_base = self._local_seq_num
        syn_seg = RDTSegment(self._local_addr[1], address[1], self._local_seq_num, self._local_ack_num, syn=True)
        self._send_to(syn_seg, address)

        start_time = time.time()
        try_conn_cnt = 1
        while True:
            rdt_seg, addr = self._recv_from(timeout_sec=0.4)
            if rdt_seg:
                try_conn_cnt = 1
            else:
                if try_conn_cnt > 15:
                    rdt_logger.warning("Request no response, exit connection request.",
                                       extra=RDT_SOCK_LOG_INFO[self._type])
                    self._peer_addr = ('255.255.255.255', 0)
                    return False
                self._send_to(syn_seg, address)
                start_time = time.time()
                try_conn_cnt += 1
                rdt_logger.info("request timeout, try again.", extra=RDT_SOCK_LOG_INFO[self._type])
                time.sleep(try_conn_cnt * 0.01)
                continue

            if address == addr and rdt_seg.syn and rdt_seg.ack and self._local_seq_num + 1 == rdt_seg.ack_num:
                self._rtt_orign = time.time() - start_time
                self._local_ack_num = rdt_seg.seq_num + 1
                self._ack_base = rdt_seg.seq_num
                self._local_seq_num = rdt_seg.ack_num
                ack_seg = RDTSegment(self._local_addr[1], address[1], self._local_seq_num, self._local_ack_num,
                                     ack=True)
                self._send_to(ack_seg, address)
                break

        rdt_logger.info(f"Client [{self._local_addr}] is connected to server [{address}], start processing segment.",
                        extra=RDT_SOCK_LOG_INFO[self._type])
        self._recv_thread = ProcessingSegment(self)
        self._recv_thread.start()
        return True

    def recv(self, buff_size: int) -> bytes:
        """
        Receive data from the socket.
        The return value is a bytes object representing the data received.
        The maximum amount of data to be received at once is specified by buff size.

        :param buff_size: Maximum number of bytes taken out at one time
        """
        assert self._peer_addr != ('255.255.255.255', 0), "Connection not established."
        assert not self._local_closed, "Socket already closed, could not use again."

        if self._timeout is None:
            while len(self._data_buff_recv) == 0:
                if self._local_closing or self._peer_closed:
                    break
        else:
            tout = time.time() + self._timeout
            while len(self._data_buff_recv) == 0 and time.time() < tout:
                pass
            if time.time() > tout:
                # raise
                return bytes()
        data = bytes(self._data_buff_recv[:buff_size])
        self._data_buff_recv = self._data_buff_recv[buff_size:]

        return data

    def send(self, data: bytes):
        """
        Send data to the socket.
        The socket must be connected to a remote socket.

        :param data: data to be sent, bytes object
        """
        assert self._peer_addr != ('255.255.255.255', 0), "Connection not established."
        assert not self._local_closed, "Socket already closed, could not use again."

        self._data_buff_send.extend(data)
        if not self._sending:
            self._send_thread = ProcessingSegment(self, is_recv=False)
            self._send_thread.start()

    def close(self):
        """
        Finish the connection and release resources.
        """
        assert self._peer_addr != ('255.255.255.255', 0), "Connection not established."

        if self._local_closed:
            return
        self._local_closing = True
        while self._receiving or self._sending:
            rdt_logger.info(f'Local socket [{self._local_addr}] closing, wait sub recv and send thread stop. sending='
                            f'{self._sending}  receiving={self._receiving}', extra=RDT_SOCK_LOG_INFO[self._type])
            time.sleep(2)

        self._close_resrc()
        super().close()

    def _close_peer(self):
        """
        Ack peer socket's fin segment
        """
        ack_seg = RDTSegment(self._local_addr[1], self._peer_addr[1], self._local_seq_num,
                             self._local_ack_num, ack=True, fin=True)
        self._peer_closing = True
        tout = time.time() + 3
        while True:
            rdt_seg, addr = self._recv_from(0.1)
            # peer close
            if rdt_seg and rdt_seg.fin and not rdt_seg.ack:
                ack_seg.ack_num = rdt_seg.seq_num + 1
                self._send_to(ack_seg, self._peer_addr)
                tout = time.time() + 3
                rdt_logger.info(f'Receive fin seg, wait two max segment lifetime.', extra=RDT_SOCK_LOG_INFO[self._type])

            if time.time() > tout:
                self._peer_closed = True
                break
        rdt_logger.info(f'Peer socket {self._local_addr} closed', extra=RDT_SOCK_LOG_INFO[self._type])

    def _close_local(self):
        """
        Send fin segment to peer socket, and wait ack segment
        """
        seq_num = random.randint(0, RDTSegment.SEQ_NUM_BOUND - 1)
        fin_seg = RDTSegment(self._local_addr[1], self._peer_addr[1], seq_num, self._local_ack_num,
                             fin=True)
        tout = time.time()
        while True:
            # local close
            if self._local_closing and time.time() > tout:
                self._send_to(fin_seg, self._peer_addr)
                tout = time.time() + 0.8

            rdt_seg, addr = self._recv_from(1)

            if rdt_seg and rdt_seg.ack and rdt_seg.fin and rdt_seg.ack_num == seq_num + 1:
                self._local_closing = False
                self._local_closed = True
                break
        rdt_logger.info(f'Local socket {self._local_addr} closed', extra=RDT_SOCK_LOG_INFO[self._type])

    def _close_resrc(self):
        """
        Release resources
        """
        if self._type == RDT_SOCK_SERVER:
            self._data_center.rvm_sock(self._peer_addr)
            master_sock = self._data_center.data_entrance
            if master_sock._local_closed and master_sock._conn_cnt == 0:
                self._data_center.stop()
        elif self._type == RDT_SOCK_MASTER_SERVER:
            pass
        elif self._type == RDT_SOCK_CLIENT:
            rdt_logger.info(f"client [{self._local_addr}] successfully closed the connection "
                            f"with server [{self._peer_addr}]", extra=RDT_SOCK_LOG_INFO[self._type])

    def _server_recvfrom(self, timeout_sec: float = None) -> ('RDTSegment', tuple):
        """
        Get RDTSegment from segment buffer, Provided to the server or master server for use

        :param timeout_sec: unit seconds
        :return: None if timeout, else not corrupt RDTSegment
        """
        if timeout_sec is None:
            timeout_sec = 1000
        tout = time.time() + timeout_sec
        while time.time() < tout:
            if self._segment_buff.qsize():
                seg, addr = self._segment_buff.get(block=False)
                rdt_logger.info(f'Take out segment from  ({self._local_addr[0]}, {self._local_addr[1]}, '
                                f'{addr[0]}, {addr[1]})', extra=RDT_SOCK_LOG_INFO[self._type])
                rdt_seg = RDTSegment.parse(seg)
                if rdt_seg:
                    rdt_logger.info(f"Received: {addr[0]} -> {self._local_addr[0]}  "
                                    f"{rdt_seg.log_info(self._ack_base, self._seq_base)}",
                                    extra=RDT_SOCK_LOG_INFO[self._type])
                    return rdt_seg, addr
                rdt_logger.info(f"Server [{self._local_addr}] Received corrupted segment",
                                extra=RDT_SOCK_LOG_INFO[self._type])
        return None, None

    def _client_recvfrom(self, timeout_sec: float = None) -> ('RDTSegment', tuple):
        """
        Get RDTSegment from segment buffer, Provided to the client for use

        :param timeout_sec: unit seconds
        :return: None if timeout, else not corrupt RDTSegment
        """
        org_timeout_sec = super().gettimeout()
        super().settimeout(timeout_sec)
        try:
            while True:
                data, frm = super().recvfrom(DEFAULT_BUFF_SIZE)
                rdt_seg = RDTSegment.parse(data)
                if rdt_seg:
                    rdt_logger.info(f"Received: {frm[0]} -> {self._local_addr[0]}  "
                                    f"{rdt_seg.log_info(self._ack_base, self._seq_base)}",
                                    extra=RDT_SOCK_LOG_INFO[self._type])
                    super().settimeout(org_timeout_sec)
                    return rdt_seg, frm
                rdt_logger.info(f"Client [{self._local_addr}] Received corrupted segment",
                                extra=RDT_SOCK_LOG_INFO[self._type])
        except OSError:
            return None, None

    def _server_sendto(self, rdt_seg: 'RDTSegment', addr):
        """
        Send RDTSegment, Provided to the server or master server for use

        :param rdt_seg: RDTSegment needed to send
        :param addr: Destination address
        """
        rdt_logger.info(f"Sent    : {self._local_addr[0]} -> {addr[0]} "
                        f"{rdt_seg.log_info(self._seq_base, self._ack_base)}",
                        extra=RDT_SOCK_LOG_INFO[self._type])
        self._data_center.data_entrance.sendto(rdt_seg.encode(), addr)

    def _client_sendto(self, rdt_seg: 'RDTSegment', addr):
        """
        Send RDTSegment, Provided to the client for use

        :param rdt_seg: RDTSegment needed to send
        :param addr: Destination address
        """
        rdt_logger.info(
            f"Sent    : {self._local_addr[0]} -> {addr[0]}  "
            f"{rdt_seg.log_info(self._seq_base, self._ack_base)}", extra=RDT_SOCK_LOG_INFO[self._type])
        self.sendto(rdt_seg.encode(), addr)

    def _send_task(self):
        # 0--slow start;
        # 1--congestion avoidance
        # 2--fast recovery

        while len(self._data_buff_send) != 0:
            time.sleep(0.1)
            need_send = True
            congestionCtrl_state = 0
            threash = CONSTANT2P32
            with lock:
                data = bytes(self._data_buff_send)
                self._data_buff_send.clear()
            data_len = len(data)

            # first sequence num
            seq_base = self._local_seq_num
            # location at data
            send_base = 0
            # location at data
            next_seq = 0
            send_window_len = self._mss * 12

            # parameter for timeout_judge
            time_out = self._rtt_orign
            sample_rtt = 0
            estimate_rtt = self._rtt_orign
            dev_rtt = 0
            #
            fr_cnt = 0
            seq_size = RDTSegment.SEQ_NUM_BOUND

            def congestion_control_newAck(ack_size):
                nonlocal congestionCtrl_state, send_window_len, threash
                if congestionCtrl_state == 0:
                    send_window_len += ack_size
                    if send_window_len >= threash:
                        congestionCtrl_state = 1
                        send_window_len = threash
                if congestionCtrl_state == 1:
                    send_window_len += int((ack_size / send_window_len) * self._mss)
                if congestionCtrl_state == 2:
                    threash = send_window_len / 2
                    send_window_len = threash + 3 * self._mss
                    congestionCtrl_state = 1

            def timeout_rst(sample_new):
                nonlocal sample_rtt, estimate_rtt, dev_rtt, time_out
                sample_rtt = sample_new
                estimate_rtt = (1 - alpha) * estimate_rtt + alpha * sample_rtt
                dev_rtt = (1 - beta) * dev_rtt + beta * abs(estimate_rtt - sample_rtt)
                time_out = estimate_rtt + 4 * dev_rtt

            while need_send:
                # divide segments in send_window and send them
                timer_start = time.time()
                while True:
                    if send_base + send_window_len - next_seq >= self._mss and next_seq + self._mss <= data_len:
                        self._local_seq_num = (seq_base + next_seq) % seq_size
                        self._seq_num_payload_buff.put(
                            (self._local_seq_num, data[next_seq: next_seq + self._mss]), block=False)
                        rdt_logger.info(f'SEND MODEL: Please send segment, seq_num={seq_base + next_seq}',
                                        extra=RDT_SOCK_LOG_INFO[self._type])
                        next_seq += self._mss
                    elif send_base + send_window_len > data_len > next_seq and next_seq + self._mss > data_len:
                        self._local_seq_num = (seq_base + next_seq) % seq_size
                        self._seq_num_payload_buff.put((self._local_seq_num, data[next_seq:]), block=False)
                        rdt_logger.info(f'SEND MODEL: Please send segment, seq_num={seq_base + next_seq}, it is the '
                                        f'last one, len={data_len - next_seq}', extra=RDT_SOCK_LOG_INFO[self._type])
                        next_seq = data_len
                    else:
                        break
                # receive ack
                while True:
                    send_base_seq = (seq_base + send_base) % seq_size
                    if not self._ack_num_option_buff.empty():
                        ack_tuple = self._ack_num_option_buff.get(block=False)
                        ack_num = ack_tuple[0]
                        options = ack_tuple[1]

                        # move window
                        if ack_num > send_base_seq:
                            rdt_logger.info(f'move window!, send_base_from={send_base} send_base_to='
                                            f'{send_base + (ack_num - send_base_seq)} ack_num={ack_num} send_base_seq='
                                            f'{send_base_seq}', extra=RDT_SOCK_LOG_INFO[self._type])
                            send_base += (ack_num - send_base_seq)
                            timeout_rst(time.time() - timer_start)
                            congestion_control_newAck(ack_num - send_base_seq)
                            fr_cnt = 0
                            break
                        elif ack_num < send_base_seq and send_base_seq + send_window_len >= seq_size + ack_num:
                            rdt_logger.info(f'move window rollback!, send_base_from={send_base} send_base_to='
                                            f'{send_base + (seq_size + ack_num - send_base_seq)} ack_num={ack_num} '
                                            f'send_base_seq={send_base_seq}', extra=RDT_SOCK_LOG_INFO[self._type])
                            send_base += (seq_size + ack_num - send_base_seq)
                            timeout_rst(time.time() - timer_start)
                            congestion_control_newAck(seq_size + ack_num - send_base_seq)
                            fr_cnt = 0
                            break
                        # acculate for fast retransmission
                        elif 5 in options:
                            timeout_rst(time.time() - timer_start)
                            if fr_cnt < 3:
                                fr_cnt += 1
                            else:
                                for unACK_range in options[5]:
                                    fr_base = send_base + unACK_range[0] - send_base_seq
                                    fr_len = unACK_range[1] - unACK_range[0]
                                    if fr_len < 0:
                                        fr_len += RDTSegment.SEQ_NUM_BOUND
                                    fr_next = fr_base
                                    while True:
                                        rdt_logger.info(
                                            f'retransimission, seq_num={seq_base + fr_next} fr_base={fr_base}'
                                            f' fr_len={fr_len} fr_next={fr_next} unack={unACK_range}',
                                            extra=RDT_SOCK_LOG_INFO[self._type])
                                        if fr_base + fr_len - fr_next >= self._mss:
                                            self._seq_num_payload_buff.put(
                                                ((seq_base + fr_next) % seq_size, data[fr_next: fr_next + self._mss]),
                                                block=False)
                                            fr_next += self._mss
                                        elif fr_base + fr_len - data_len == 0:
                                            self._seq_num_payload_buff.put(
                                                ((seq_base + fr_next) % seq_size, data[fr_next:]),
                                                block=False)
                                            fr_next = data_len
                                        else:
                                            break
                                fr_cnt = 0
                                congestionCtrl_state = 2
                                congestion_control_newAck(self._mss)
                                timer_start = time.time()

                        # without operation
                        else:
                            timeout_rst(time.time() - timer_start)
                            continue
                    if send_base == data_len:
                        self._local_seq_num = (seq_base + next_seq) % RDTSegment.SEQ_NUM_BOUND
                        rdt_logger.info(f'All data in this task sent. data_len={data_len}',
                                        extra=RDT_SOCK_LOG_INFO[self._type])
                        need_send = False
                        break

                    # judge time out
                    if time.time() - timer_start > time_out:
                        buff = (
                            (seq_base + send_base) % seq_size, data[send_base: min(send_base + self._mss, data_len)])
                        if buff not in self._seq_num_payload_buff.queue:
                            rdt_logger.info(
                                f'timeout, time_from_start:{time.time() - timer_start}, time_out:{time_out}',
                                extra=RDT_SOCK_LOG_INFO[self._type])
                            self._seq_num_payload_buff.put(buff, block=False)
                            timer_start = time.time()
                            time_out *= 1.5
                            congestion_control_newAck(self._mss)

    def _recv_task(self):
        """
        Receive and process the segment and add the processed data to the recv buff
        """

        need_option = False
        ack_seg = RDTSegment(self._local_addr[1], self._peer_addr[1], 0, self._local_ack_num)
        while True:
            rdt_seg, addr = self._recv_from(0.1)
            if rdt_seg is None:
                seg_cnt = self._seq_num_payload_buff.qsize()
                if seg_cnt != 0:
                    # need send data which come from func send()
                    for i in range(seg_cnt):
                        seq_payload = self._seq_num_payload_buff.get(block=False)
                        seq_seg = RDTSegment(self._local_addr[1], self._peer_addr[1], seq_payload[0],
                                             self._local_ack_num, payload=seq_payload[1])
                        self._send_to(seq_seg, self._peer_addr)
                elif self._local_closing and (not self._sending):
                    self._close_local()

                if self._local_closed and self._peer_closed:
                    rdt_logger.info(f'Local socket [{self._local_addr}] and Peer socket [{self._peer_addr}] closed.',
                                    extra=RDT_SOCK_LOG_INFO[self._type])
                    return
                continue

            if rdt_seg.ack:
                self._ack_num_option_buff.put((rdt_seg.ack_num, rdt_seg.options), block=False)

            data_len = len(rdt_seg.payload)
            if data_len != 0:
                # seq num is valid
                ack_seg.ack = True

                ack_range_tuple = (rdt_seg.seq_num, (rdt_seg.seq_num + data_len) % rdt_seg.SEQ_NUM_BOUND)
                if self._local_ack_num < rdt_seg.seq_num < self._local_ack_num + self._recv_win_size:
                    # seg_num inside recv window
                    rdt_logger.info(
                        f'Recv seg inside recv-win, peer_port={rdt_seg.src_port}, local_port={rdt_seg.dest_port} '
                        f'win_base={self._local_ack_num}, seq_num='
                        f'{rdt_seg.seq_num}, payload len={len(rdt_seg.payload)}',
                        extra=RDT_SOCK_LOG_INFO[self._type])
                    if ack_range_tuple not in self._ack_set:
                        self._main_pq.put(AckRange(ack_range_tuple, rdt_seg.payload))
                        self._ack_set.add(ack_range_tuple)

                    need_option = True
                elif self._local_ack_num == rdt_seg.seq_num:
                    # seq_num equal recv window base
                    rdt_logger.info(f'Recv seg, is the expected seq_num, seq_num={rdt_seg.seq_num}, payload len'
                                    f'={len(rdt_seg.payload)}, next expected={rdt_seg.seq_num + len(rdt_seg.payload)}'
                                    , extra=RDT_SOCK_LOG_INFO[self._type])

                    self._local_ack_num = (self._local_ack_num + data_len) % RDTSegment.SEQ_NUM_BOUND
                    self._data_buff_recv.extend(rdt_seg.payload)
                    ack_seg.ack_num = self._local_ack_num
                    while True:
                        # Cumulative confirmation
                        if self._main_pq.empty():
                            if self._sub_pq.empty():
                                break
                            rdt_logger.info('window rollback successfully.', extra=RDT_SOCK_LOG_INFO[self._type])
                            self._main_pq.queue = self._sub_pq.queue
                            self._sub_pq.queue = []
                            self._ack_set = self._sub_ack_set
                            self._sub_ack_set = set()
                            continue
                        min_ack_range = self._main_pq.queue[0]
                        if self._local_ack_num == min_ack_range.value[0]:
                            rdt_logger.info(f'Cumulative confirmation, local_ack_num={self._local_ack_num}',
                                            extra=RDT_SOCK_LOG_INFO[self._type])
                            self._main_pq.get()
                            self._ack_set.remove(min_ack_range.value)
                            self._local_ack_num = min_ack_range.value[1]
                            self._data_buff_recv.extend(min_ack_range.data)
                            continue
                        else:
                            # construct an option sack
                            need_option = True
                            break
                elif rdt_seg.seq_num < self._local_ack_num + self._recv_win_size - RDTSegment.SEQ_NUM_BOUND:
                    # window rollback
                    rdt_logger.info('recv seg, window rollback', extra=RDT_SOCK_LOG_INFO[self._type])
                    if ack_range_tuple not in self._sub_ack_set:
                        self._sub_pq.put(AckRange(ack_range_tuple, rdt_seg.payload))
                        self._sub_ack_set.add(ack_range_tuple)
                elif self._local_ack_num - self._recv_win_size < rdt_seg.seq_num < self._local_ack_num:
                    # seq_num in left of window within one win_size
                    rdt_logger.info('recv seg, seq_num in left of window within one win_size',
                                    extra=RDT_SOCK_LOG_INFO[self._type])
                    ack_seg.ack_num = self._local_ack_num
                elif RDTSegment.SEQ_NUM_BOUND + self._local_ack_num - self._recv_win_size < rdt_seg.seq_num:
                    # seq_num in left of window within one win_size, window rollback case
                    rdt_logger.info('recv seg, seq_num in left of window within one win_size, window rollback case',
                                    extra=RDT_SOCK_LOG_INFO[self._type])
                    ack_seg.ack_num = self._local_ack_num
                else:
                    rdt_logger.info('recv seg, Alien segment', extra=RDT_SOCK_LOG_INFO[self._type])
                    ack_seg.ack = False
                    ack_seg.options = {}

                if need_option:
                    range_back_list = []
                    cnt = 1
                    new_op_5 = [(self._local_ack_num, self._main_pq.queue[0].value[0])]

                    while cnt < 5 and self._main_pq.qsize() > 1:
                        tmp = self._main_pq.get()
                        tmp_range = tmp.value
                        new_range = list(tmp_range)
                        new_data = bytearray(tmp.data)

                        while self._main_pq.qsize() > 0:
                            top_range = self._main_pq.queue[0].value
                            if top_range[0] == tmp_range[1]:
                                new_range[1] = top_range[1]
                                tmp = self._main_pq.get()
                                new_data.extend(tmp.data)
                            else:
                                break
                        range_back_list.append(AckRange(tuple(new_range), bytes(new_data)))

                        if self._main_pq.qsize():
                            new_op_5.append((new_range[1], self._main_pq.queue[0].value[0]))
                            cnt += 1

                    for i in range_back_list:
                        self._main_pq.put(i)
                        self._ack_set.add(i.value)

                    ack_seg.ack_num = self._local_ack_num
                    ack_seg.options[5] = new_op_5
                    need_option = False
                else:
                    ack_seg.options = {}
            else:
                ack_seg.ack = False
                ack_seg.options = {}

            if rdt_seg.fin and not rdt_seg.ack:
                self._close_peer()

            if self._seq_num_payload_buff.qsize():
                # need send data which come from func send()
                seq_payload = self._seq_num_payload_buff.get(block=False)
                ack_seg.seq_num = seq_payload[0]
                ack_seg.payload = seq_payload[1]
            else:
                ack_seg.seq_num = self._local_seq_num
                ack_seg.payload = b''

            if ack_seg.ack or len(ack_seg.payload) != 0:
                self._send_to(ack_seg, addr)


class ProcessingSegment(threading.Thread):
    def __init__(self, rdt_socket: RDTSocket, is_recv: bool = True):
        threading.Thread.__init__(self)
        self.rdt_socket = rdt_socket
        self.is_recv = is_recv

    def run(self):
        if self.is_recv:
            self.rdt_socket._receiving = True
            self.rdt_socket._recv_task()
            self.rdt_socket._receiving = False
        else:
            self.rdt_socket._sending = True
            self.rdt_socket._send_task()
            self.rdt_socket._sending = False


class AckRange:
    def __init__(self, value: tuple, data: bytes):
        self.value = value
        self.data = data

    def __lt__(self, other: 'AckRange'):
        return self.value[0] < other.value[0]


class DataCenter(threading.Thread):
    def __init__(self, data_entrance: 'RDTSocket'):
        threading.Thread.__init__(self)
        self.__flag = threading.Event()  # The identity used to pause the thread
        self.__flag.set()  # Initialization does not block threads
        self.__running = threading.Event()  # The identity used to stop the thread
        self.__running.set()  # Initialization thread running
        self.data_entrance = data_entrance
        self.socket_table = {}

    def set_data_entrance(self, data_entrance: 'RDTSocket'):
        self.data_entrance = data_entrance

    def start(self) -> None:
        super().start()

    def getName(self) -> str:
        return super().getName()

    def setName(self, name) -> None:
        super().setName(name)

    def run(self):
        rdt_logger.info("DataCenter start work...", extra=RDT_SOCK_LOG_INFO[self.data_entrance._type])
        while self.__running.isSet():
            self.__flag.wait()  # 为True时立即返回, 为False时阻塞直到self.__flag为True后返回
            data, addr = self.data_entrance._super_recvfrom(DEFAULT_BUFF_SIZE)
            if data:
                if addr in self.socket_table:
                    sock = self.socket_table.get(addr)
                    sock._segment_buff.put((data, addr), block=False)
                    rdt_logger.info(f'Data distribution to ({sock._local_addr[0]}, {sock._local_addr[1]}, '
                                    f'{addr[0]}, {addr[1]})', extra=RDT_SOCK_LOG_INFO[self.data_entrance._type])
                elif self.data_entrance._allow_accept:
                    self.data_entrance._segment_buff.put((data, addr), block=False)

    def pause(self) -> None:
        """
        Block thread, pause receiving segment
        """
        self.__flag.clear()

    def resume(self) -> None:
        """
        Stop blocking thread, continue to receive segment
        """
        self.__flag.set()

    def stop(self) -> None:
        """
        Stop thread
        """
        self.__flag.set()
        self.__running.clear()

    def add_sock(self, key: tuple, value: 'RDTSocket') -> bool:
        """
        Add socket to buff table

        :param key: address, (ip,port), type tuple (str, int)
        :param value: socket
        :return: True if not exist this socket and it added to socket table, else False
        """
        if key not in self.socket_table:
            self.socket_table[key] = value
            self.data_entrance._conn_cnt += 1
            rdt_logger.info(f"new socket add to data center, accept data from {value._peer_addr}",
                            extra=RDT_SOCK_LOG_INFO[self.data_entrance._type])
            return True
        return False

    def rvm_sock(self, key: tuple) -> bool:
        """
        Remove socket from buff table

        :param key: address, (ip,port), type tuple (str, int)
        :return: True if exist this socket and it deleted from socket table, else False
        """
        if key in self.socket_table:
            del self.socket_table[key]
            self.data_entrance._conn_cnt -= 1
            return True
        return False


class RDTSegment:
    """
    Reliable Data Transfer Segment Class
    """

    SEQ_NUM_BOUND = 4294967296  # 2 ** (4 * 8)
    DEFAULT_WIN_SIZE = 65535

    def __init__(self, src_port: int, dest_port: int, seq_num: int, ack_num: int, recv_win: int = DEFAULT_WIN_SIZE,
                 payload: bytes = b'', options: dict = None,
                 ack: bool = False, rst: bool = False, syn: bool = False, fin: bool = False):
        if options is None:
            options = {}
        self.src_port = src_port
        self.dest_port = dest_port
        self.seq_num = seq_num % self.SEQ_NUM_BOUND
        self.ack_num = ack_num % self.SEQ_NUM_BOUND
        self.options = options
        self.header_len = 0
        self.ack = ack
        self.rst = rst
        self.syn = syn
        self.fin = fin
        self.recv_win = recv_win
        self.payload = payload

    def __str__(self) -> str:
        return f'RDTSegment[' \
               f'src_port: {self.src_port}  dest_port: {self.dest_port}  seq_num: {self.seq_num}  ' \
               f'ack_num: {self.ack_num}  recv_win: {self.recv_win}  ack: {self.ack}  ' \
               f'rst: {self.rst}  syn: {self.syn}  fin: {self.fin} payload len = {len(self.payload)}]'

    def _decode_flags(self, flags: int) -> None:
        """
          [header length]        unit           size
            in segment           Word   (flags & 0xFF) >> 4
         in RDTSegment class     Byte   (flags & 0xFF) >> 2
        """
        self.ack = (flags & 0x08) != 0
        self.rst = (flags & 0x04) != 0
        self.syn = (flags & 0x02) != 0
        self.fin = (flags & 0x01) != 0
        self.header_len = (flags & 0xF0) >> 2

    def _encode_flags(self) -> int:
        flags = 0
        if self.ack:
            flags |= 0x08
        if self.rst:
            flags |= 0x04
        if self.syn:
            flags |= 0x02
        if self.fin:
            flags |= 0x01
        flags |= ((self.header_len & 0x3c) << 2)
        return flags & 0xFF

    def _decode_options(self, options: bytes) -> None:
        i = 0
        op_len = len(options)
        while i < op_len:
            kind = int(options[i])
            if kind == 5:
                length = int(options[i + 1])
                edges_cnt = (length - 2) // 4
                # edges: (leftEdge1, rightEdge1, leftEdge2, rightEdge2 ...)
                edges = struct.unpack('!' + 'L' * edges_cnt, options[i + 2:i + length])
                # sack_edge: ((leftEdge1, rightEdge1), (leftEdge2, rightEdge2) ...)
                it = iter(edges)
                sack_edge = tuple(b for b in zip(it, it))
                self.options[kind] = sack_edge
                i += length
            elif kind == 0:
                break
            elif kind == 1:
                i += 1
            else:
                break

    def _encode_options(self) -> bytes:
        options_byte = bytearray()
        if 5 in self.options:
            sack_edge = self.options.get(5)
            sack_cnt = len(sack_edge)
            options_byte.extend(struct.pack('!BBBB', 1, 1, 5, ((sack_cnt << 3) + 2)))
            for i, j in sack_edge:
                options_byte.extend(struct.pack('!LL', i, j))
        return bytes(options_byte)

    def encode(self) -> bytes:
        """
        Encode RDTSegment object to segment (bytes)

        python struct format string: https://docs.python.org/zh-cn/3.10/library/struct.html?highlight=struct#struct-format-strings
        """
        options = self._encode_options()
        self.header_len = (len(options) + 16)
        flags = self._encode_flags()
        # B 1 H 2 L 4
        # src_port   dest_port  seq_num  ack_num  flags  unused   checksum
        head = struct.pack('!HHLLBBH', self.src_port, self.dest_port, self.seq_num, self.ack_num, flags, 0, 0)
        segment = bytearray(head)

        if self.options:
            segment.extend(options)

        segment.extend(self.payload)
        checksum = RDTSegment.calc_checksum(segment)
        segment[14] = checksum >> 8
        segment[15] = checksum & 0xFF
        return bytes(segment)

    @staticmethod
    def parse(segment: bytes) -> 'RDTSegment':
        """
        Parse raw segment into an RDTSegment object

        :return : RDTSegment object, if segment is corrupted, return None
        """
        if RDTSegment.calc_checksum(segment) != 0:
            return None
        head = segment[0:16]

        src_port, dest_port, seq_num, ack_num = struct.unpack('!HHLL', head[0:12])
        flags, unused, checksum = struct.unpack('!BBH', head[12:16])
        head_length = (flags & 0xF0) >> 2
        payload = segment[head_length:]
        rdt_seg = RDTSegment(src_port, dest_port, seq_num, ack_num, DEFAULT_WIN_SIZE, payload)
        rdt_seg._decode_flags(flags)
        if rdt_seg.header_len > 16:
            rdt_seg._decode_options(segment[16:head_length])
        return rdt_seg

    @staticmethod
    def calc_checksum(segment: bytes) -> int:
        """
        16 bit one's complement of the one's complement sum

        :param segment: raw bytes of a segment, with its checksum set to 0
        :return: 16-bit unsigned checksum
        """
        it = iter(segment)
        bytes_sum = sum(((a << 8) + b for a, b in zip(it, it)))  # (seg[0], seg[1]) (seg[2], seg[3]) ...
        # pad the data with zero to a multiple of length 16
        if len(segment) % 2 == 1:
            bytes_sum += segment[-1] << 8  # (seg[-1], 0)
        # wraparound
        while bytes_sum > 65535:
            bytes_sum = (bytes_sum & 0xFFFF) + (bytes_sum >> 16)
        return ~bytes_sum & 0xFFFF

    def flag_str(self) -> str:
        if self.ack:
            if self.syn:
                return '[SYN, ACK] '
            elif self.fin:
                return '[FIN, ACK] '
            else:
                return '[ACK] '
        if self.syn:
            return '[SYN] '
        if self.fin:
            return '[FIN] '
        return ''

    def log_info(self, seq_offset: int, ack_offset: int) -> str:
        op = []
        bound = RDTSegment.SEQ_NUM_BOUND
        if 5 in self.options:
            opts = self.options[5]
            for i in range(len(opts)):
                op.append(((opts[i][0] + bound - ack_offset) % bound, (opts[i][1] + bound - ack_offset) % bound))
        return f'{self.src_port} -> {self.dest_port} {self.flag_str()}Seq={(self.seq_num + bound - seq_offset) % bound} Ack={(self.ack_num + bound - ack_offset) % bound} ' \
               f'Win={self.recv_win} Len={len(self.payload)} Op={op}'

    def log_raw_info(self) -> str:
        op = None
        if 5 in self.options:
            op = self.options[5]
        return f'{self.src_port} -> {self.dest_port} {self.flag_str()}Seq={self.seq_num} Ack={self.ack_num} ' \
               f'Win={self.recv_win} Len={len(self.payload)} Op={op}'
