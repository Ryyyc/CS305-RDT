from socket import inet_aton, inet_ntoa


def unit_convert(value):
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = 1024.0
    for i in range(len(units)):
        if (value / size) < 1:
            return "%.2f%s" % (value, units[i])
        value = value / size


def bytes_to_addr(data_bytes):
    return inet_ntoa(data_bytes[:4]), int.from_bytes(data_bytes[4:8], 'big')


def addr_to_bytes(addr):
    return inet_aton(addr[0]) + addr[1].to_bytes(4, 'big')
