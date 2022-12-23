native_size_map = {
    "int": 4,
    "uint": 4,
    "float": 4,
    "long": 8,
    "ulong": 8,
    "double": 8,
}

def next_power_of_2(v):
    v = v - 1
    v |= v >> 1
    v |= v >> 2
    v |= v >> 4
    v |= v >> 8
    v |= v >> 16
    v = v + 1
    return int(v)
