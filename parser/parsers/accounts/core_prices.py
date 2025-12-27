from model.parser import Parser, TOPIC_ACCOUNT_STATES
from loguru import logger
from db import DB
from pytoniq_core import Cell, Address
from parsers.accounts.emulator import EmulatorParser
from pytvm.tvm_emulator.tvm_emulator import TvmEmulator


"""
Discovers prices for the core assets, i.e.:
* TON itself, based on TON/USDT pool reserves
* stTON and tsTON
"""
class CorePrices(Parser):
    """
    account - address to look updates for
    asset - item for core prices table
    """
    def __init__(self, account, asset, update_interval=60):
        self.account = account
        self.asset = asset
        self.latest_update = None
        self.latest_price = None
        self.update_interval = update_interval
    
    def topics(self):
        return [TOPIC_ACCOUNT_STATES]

    def predicate(self, obj) -> bool:
        return obj.get("account", None) == self.account
    
    """
    Handle price updates and store in the DB
    """
    def update_price(self, price, obj, db: DB):
        timestamp = obj.get('timestamp')
        if self.latest_update is not None:
            if timestamp < self.latest_update + self.update_interval:
                logger.debug(f"Price timestamp for {self.asset} is before latest update + update interval, skipping")
                return

            if price == self.latest_price:
                logger.info(f"Price for asset {self.asset} is the same: {price}")
                return

        self.latest_update = timestamp
        self.latest_price = price
        db.insert_core_price(self.asset, price, obj)
    

"""
We will use TON/USDT pool to discover TON price based on the current reserves
"""
class CorePricesUSDT(CorePrices):
    def __init__(self, update_interval=60):
        super().__init__(account=Parser.uf2raw('EQD8TJ8xEWB1SpnRE4d89YO3jl0W0EiBnNS4IBaHaUmdfizE'), 
                         asset=Parser.uf2raw('EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs'), 
                         update_interval=update_interval)

    def handle_internal(self, obj, db: DB):
        cell = Cell.one_from_boc(Parser.require(obj.get('data_boc'))).begin_parse()

        cell = cell.load_ref().begin_parse()
        cell.load_coins() # proto fee
        cell.load_coins() # proto fee
        cell.load_address() # proto fee address
        reserve0 = cell.load_coins()
        reserve1 = cell.load_coins()
        
        logger.info(f"TON/USDT pool: {reserve0}, {reserve1}")
        if reserve1 > 0:
            self.update_price(1.0 * reserve0 / reserve1, obj, db)

"""
stTON (bemo) stores total TON staked and jetton supply in the account state in the first two values.
"""
class CorePricesLSDstTON(CorePrices):
    def __init__(self, update_interval=3600):
        super().__init__(account=Parser.uf2raw('EQDNhy-nxYFgUqzfUzImBEP67JqsyMIcyk2S5_RwNNEYku0k'), 
                         asset=Parser.uf2raw('EQDNhy-nxYFgUqzfUzImBEP67JqsyMIcyk2S5_RwNNEYku0k'), 
                         update_interval=update_interval)

    def handle_internal(self, obj, db: DB):
        cell = Cell.one_from_boc(Parser.require(obj.get('data_boc'))).begin_parse()

        jetton_total_supply = cell.load_coins()
        ton_total_supply = cell.load_coins()

        
        logger.info(f"stTON price: {jetton_total_supply}, {ton_total_supply}")
        if jetton_total_supply > 0:
            self.update_price(1.0 * ton_total_supply / jetton_total_supply, obj, db)


"""
tsTON (tonstakers) stores TON staked and jetton supply inside the account state
accoring to the smart contract source code (https://github.com/ton-blockchain/liquid-staking-contract/blob/main/contracts/pool_storage.func)
"""
class CorePricesLSDtsTON(CorePrices):
    def __init__(self, update_interval=3600):
        super().__init__(account=Parser.uf2raw('EQCkWxfyhAkim3g2DjKQQg8T5P4g-Q1-K_jErGcDJZ4i-vqR'), 
                         asset=Parser.uf2raw('EQC98_qAmNEptUtPc7W6xdHh_ZHrBUFpw5Ft_IzNU20QAJav'), 
                         update_interval=update_interval)

    def handle_internal(self, obj, db: DB):
        cell = Cell.one_from_boc(Parser.require(obj.get('data_boc'))).begin_parse()

        cell.skip_bits(9) # state + halted
        total_balance = cell.load_coins()
        cell = cell.load_ref().begin_parse()
        cell.load_address() # minter_address
        supply = cell.load_coins()

        logger.info(f"tsTON total balance: {total_balance}, supply: {supply}")

        
        logger.info(f"tsTON price: {total_balance}, {supply}")
        if supply > 0:
            self.update_price(1.0 * total_balance / supply, obj, db)

class CorePricesHipoTON(CorePrices, EmulatorParser):
    def __init__(self, emulator_path, update_interval=3600):
        EmulatorParser.__init__(self, emulator_path)
        CorePrices.__init__(self, account=Parser.uf2raw('EQCLyZHP4Xe8fpchQz76O-_RmUhaVc_9BAoGyJrwJrcbz2eZ'), 
                         asset=Parser.uf2raw('EQDPdq8xjAhytYqfGSX8KcFWIReCufsB9Wdg0pLlYSO_h76w'), 
                         update_interval=update_interval)
        
    def predicate(self, obj) -> bool:
        return EmulatorParser.predicate(self, obj) and CorePrices.predicate(self, obj)

    def _do_parse(self, obj, db: DB, emulator: TvmEmulator): 
        response = self._execute_method(emulator, 'get_treasury_state', [], db, obj)
        total_coins = int(response[0])
        total_tokens = int(response[1])
        if total_tokens > 0:
            self.update_price(1.0 * total_coins / total_tokens, obj, db)


class CorePricesStormTrade(CorePrices, EmulatorParser):
    def __init__(self, emulator_path, vault, lp_jetton, update_interval=3600):
        EmulatorParser.__init__(self, emulator_path)
        CorePrices.__init__(self, account=vault, asset=lp_jetton, update_interval=update_interval)
        
    def predicate(self, obj) -> bool:
        # checks that we are handling Storm vault
        return EmulatorParser.predicate(self, obj) and CorePrices.predicate(self, obj)

    def _do_parse(self, obj, db: DB, emulator: TvmEmulator): 
        result = self._execute_method(emulator, 'get_vault_data', [], db, obj)
        lp_total_supply = result[2]
        free_balance = result[3]
        
        if lp_total_supply > 0:
            self.update_price(1.0 * free_balance / lp_total_supply, obj, db)


class CorePricesEthena(CorePrices, EmulatorParser):
    def __init__(self, emulator_path, update_interval=3600):
        EmulatorParser.__init__(self, emulator_path)
        CorePrices.__init__(self, account=Parser.uf2raw('EQChGuD1u0e7KUWHH5FaYh_ygcLXhsdG2nSHPXHW8qqnpZXW'), 
                         asset=Parser.uf2raw('EQDQ5UUyPHrLcQJlPAczd_fjxn8SLrlNQwolBznxCdSlfQwr'), 
                         update_interval=update_interval)
        
    def predicate(self, obj) -> bool:
        return EmulatorParser.predicate(self, obj) and CorePrices.predicate(self, obj)

    def _do_parse(self, obj, db: DB, emulator: TvmEmulator): 
        total_assets, = self._execute_method(emulator, 'getTotalAssets', [], db, obj)
        total_shares, = self._execute_method(emulator, 'getTotalShares', [], db, obj)

        logger.info(f"tsUSDe vault total assets: {total_assets}, total shares: {total_shares}")

        if total_assets and total_shares:
            self.update_price(total_assets / total_shares, obj, db)
