#!/usr/bin/env python3
import argparse
import random
import time
import threading
from rdt import RDTSegment
from socketserver import ThreadingUDPServer
from utils import bytes_to_addr, addr_to_bytes

lock = threading.Lock()

# network addr and port in network.py and USocket.py should be same
NETWORK_ADDR = '127.0.0.1'
NETWORK_PORT = 11223
RATE = None  # None means no rate limit, unit Byte/s
BUF_SIZE = 50000
LOSS_RATE = 0.1
CORRUPT_RATE = 0.00001  # corrupt per bit
DELAY = 0  # Random delay DELAY seconds


class Server(ThreadingUDPServer):
    def __init__(self, addr, rate, buf_size, loss_rate, delay, corrupt_rate):
        super().__init__(addr, None)
        self.buffer = 0

        self.rate = rate
        self.buf_size = buf_size
        self.loss_rate = loss_rate
        self.corrupt_rate_byte = (1 - corrupt_rate) ** 8
        self.delay = delay

    def verify_request(self, request, client_address):
        """
        request is a tuple (data, socket)
        data is the received bytes object
        socket is new socket created automatically to handle the request

        if this function returns False， the request will not be processed, i.e. is discarded.
        details: https://docs.python.org/3/library/socketserver.html
        """
        data, socket = request
        data_len = len(data)
        if self.buffer + data_len < self.buf_size:  # some finite buffer size (in bytes)
            self.buffer += data_len
            return True
        else:
            return False

    def finish_request(self, request, client_address):
        """
        Imitate router and link behavior
        """

        data, socket = request

        with lock:
            # Limit bandwidth
            if self.rate:
                time.sleep(len(data) / self.rate)
            self.buffer -= len(data)

            # Packet loss
            if random.random() < self.loss_rate:
                drop = RDTSegment.parse(data[8:])
                print(client_address, bytes_to_addr(data[:8]), f" packet loss, {drop.log_raw_info()}")
                return

            # Packet corrupt
            corrupt = RDTSegment.parse(data[8:])
            data = bytearray(data)
            is_corrupted = False
            for i in range(8, len(data)):
                if random.random() > self.corrupt_rate_byte:
                    is_corrupted = True
                    data[i] = data[i] ^ random.randint(0, 255)
            if is_corrupted and corrupt:
                print(client_address, bytes_to_addr(data[:8]), f" corrupt pkt, {corrupt.log_raw_info()}")
            data = bytes(data)

        # Transmission delay
        time.sleep(self.delay * random.random())

        to = bytes_to_addr(data[:8])
        print(client_address, to)  # observe tht traffic
        socket.sendto(addr_to_bytes(client_address) + data[8:], to)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-rate', dest='rate',
                        help='Limit bandwidth, unit Byte/s, default is None, None means no rate limit',
                        type=int, default=RATE)
    parser.add_argument('-buf', dest='buf_size', help='Router buffer size, unit byte', type=int, default=BUF_SIZE)
    parser.add_argument('-loss', dest='loss_rate', help='Segment loss rate', type=float, default=LOSS_RATE)
    parser.add_argument('-delay', dest='delay', help='Link delay, unit byte/s', type=int, default=DELAY)
    parser.add_argument('-corrupt', dest='corrupt_rate', help='Segment corrupt rate', type=float, default=CORRUPT_RATE)

    args = parser.parse_args()

    with Server((NETWORK_ADDR, NETWORK_PORT), rate=args.rate, buf_size=args.buf_size,
                loss_rate=args.loss_rate, delay=args.delay, corrupt_rate=args.corrupt_rate) as server:
        server.serve_forever()
