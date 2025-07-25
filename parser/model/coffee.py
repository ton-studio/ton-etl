from pytoniq_core import Slice, Address, begin_cell, Cell

TON = Address("EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c")


def read_coffee_asset(cell: Slice) -> Address:
    kind = cell.load_uint(2)
    if kind == 0b00:
        return TON
    elif kind == 0b01:
        wc = cell.load_uint(8)
        hash_part = cell.load_bytes(32)  # 256 bits
        return Address((wc, hash_part))
    elif kind == 0b10:
        raise Exception("Extra currency assets are not supported")
    else:
        raise Exception(f"Unsupported asset type: {kind}")


def write_coffee_asset(address: Address) -> Cell:
    builder = begin_cell()
    if address == TON:
        builder.store_uint(0, 2)
    else:
        builder.store_uint(1, 2)
        builder.store_uint(address.wc, 8)
        builder.store_bytes(address.hash_part)
    return builder.end_cell()
