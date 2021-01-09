#!/usr/bin/env python3
import time
from rdt import RDTSocket
from utils import unit_convert
import argparse

SERVER_ADDR = '127.0.0.1'
SERVER_PORT = 9999


def client_test(file_path, debug):
    client = RDTSocket(debug=debug)
    if client.connect((SERVER_ADDR, SERVER_PORT)):
        data_count = 0
        count = 20

        f = open(file_path, 'rb')
        data_send = f.read()

        start = time.perf_counter()
        for i in range(count):  # send data for count times
            data_count += len(data_send)
            client.send(data_send)

        data_send = data_send * count

        mid = time.perf_counter()
        print('---------------------------------------------')
        print(f'client send OK, data size: {unit_convert(len(data_send))}, send time cost: {mid - start} s')
        print('---------------------------------------------')
        echo = bytearray()

        while True:
            reply = client.recv(2048)
            echo.extend(reply)
            assert echo == data_send[:len(echo)], 'Inconsistent echo content'
            if len(echo) == len(data_send):
                break

        end = time.perf_counter()

        print('=============================================')
        print(f'client recv OK, data size: {unit_convert(len(echo))} bytes, recv time cost: {end - mid} s')
        print(f'Total time cost: {end - start}')
        print('=============================================')
        client.close()
        print('client main thread stop.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-file', dest='file_path', help='File that need to be transferred by the client',
                        type=str, default='../LICENSE')
    parser.add_argument('-debug', dest='debug_model', help='Debug model, 1 means active, 0 means closed',
                        type=int, default=1)

    args = parser.parse_args()

    client_test(args.file_path, args.debug_model)
