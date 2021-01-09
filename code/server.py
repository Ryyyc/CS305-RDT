#!/usr/bin/env python3
import argparse
import time
import threading
from rdt import RDTSocket

SERVER_ADDR = '127.0.0.1'
SERVER_PORT = 9999


class Echo(threading.Thread):
    def __init__(self, conn, address):
        threading.Thread.__init__(self)
        self.conn = conn
        self.address = address

    def run(self):
        start = time.perf_counter()
        while True:
            data = self.conn.recv(2048)
            if data:
                self.conn.send(data)
            else:
                break
        self.conn.close()
        end = time.perf_counter()
        print(f'connection finished in {end - start}s')


def server_test(debug_model: bool):
    server = RDTSocket(debug=debug_model)
    server.bind((SERVER_ADDR, SERVER_PORT))
    try:
        while True:
            conn, client_addr = server.accept()
            Echo(conn, client_addr).start()
    except KeyboardInterrupt as k:
        print(k)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-debug', dest='debug_model', help='Debug model, 1 means active, 0 means closed',
                        type=int, default=1)
    args = parser.parse_args()

    server_test(debug_model=bool(args.debug_model))
