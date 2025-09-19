from loguru import logger
from db import DB
from pytvm.tvm_emulator.tvm_emulator import TvmEmulator
from parsers.accounts.emulator import EmulatorParser
from model.extra_nft_sale import ExtraNFTSale


"""
Parser of NFT sale contracts not supported by the NOT indexer
"""

SALES_CODE_HASHES = ["a5WmQYucnSNZBF0edVm41UmuDlBvJMqrWPowyPsf64Y="]  # nft_sale_getgems_v4


class NFTSalesParser(EmulatorParser):
    def __init__(self, emulator_path):
        super().__init__(emulator_path)

    def predicate(self, obj) -> bool:
        return super().predicate(obj) and obj["code_hash"] in SALES_CODE_HASHES

    def _do_parse(self, obj, db: DB, emulator: TvmEmulator):
        try:
            if obj["code_hash"] == "a5WmQYucnSNZBF0edVm41UmuDlBvJMqrWPowyPsf64Y=":
                asset = "TON"
                (
                    is_complete,
                    created_at,
                    marketplace_address,
                    nft_address,
                    nft_owner_address,
                    full_price,
                    fee_address,
                    fee_percent,
                    royalty_address,
                    royalty_percent,
                    sold_at,
                    sold_query_id,
                    jetton_price_dict,
                ) = self._execute_method(emulator, "get_fix_price_data_v4", [], db, obj)

                if jetton_price_dict and not full_price:
                    for value in jetton_price_dict.begin_parse().load_hashmap(256).values():
                        full_price = value.load_coins()
                        asset = value.load_address()
                        break

        except Exception as e:
            logger.warning(f"Failed to parse NFT sale contract {obj['account']}: {e}")
            return

        sale = ExtraNFTSale(
            address=obj["account"],
            is_complete=is_complete,
            created_at=created_at,
            marketplace_address=marketplace_address,
            nft_address=nft_address,
            nft_owner_address=nft_owner_address,
            full_price=full_price,
            asset=asset,
            marketplace_fee_address=fee_address,
            marketplace_fee=int(full_price * fee_percent / 100000),
            royalty_address=royalty_address,
            royalty_amount=int(full_price * royalty_percent / 100000),
            last_transaction_lt=obj["last_trans_lt"],
            last_tx_now=obj["timestamp"],
            code_hash=obj["code_hash"],
            data_hash=obj["data_hash"],
        )

        logger.info(f"New NFT sale contract discovered: {obj['account']} {sale}")
        db.serialize(sale)
