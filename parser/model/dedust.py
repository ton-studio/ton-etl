from pytoniq_core import Slice, Address, begin_cell, Cell

TON = Address("EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c")

def read_dedust_asset(cell: Slice):
    kind = cell.load_uint(4)
    if kind == 0:
        return TON
    else:
        wc = cell.load_int(8)
        account_id = cell.load_bytes(32) # 256 bits
        return Address((wc, account_id))
    
def write_dedust_asset(address: Address) -> Cell:
    builder = begin_cell()
    if address == TON:
        builder.store_uint(0, 4)
    else:
        builder.store_uint(1, 4)
        builder.store_int(address.wc, 8)
        builder.store_bytes(address.hash_part)
    return builder.end_cell()