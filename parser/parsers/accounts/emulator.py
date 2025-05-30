from model.parser import TOPIC_ACCOUNT_STATES, NonCriticalParserError, Parser
from db import DB
import time
import os
import base64
import asyncio
import json
from loguru import logger
from pytvm.tvm_emulator.tvm_emulator import TvmEmulator
from pytvm.engine import EmulatorEngineC
from pytoniq_core import Cell, Address, begin_cell, HashMap, Builder
from pytoniq import LiteClient


def create_lite_client():
    config_path = os.getenv("LITECLIENT_CONFIG_OVERRIDE", None)
    is_testnet_mode = int(os.getenv("TESTNET_MODE", "0"))
    if config_path is None:
        if is_testnet_mode:
            return LiteClient.from_testnet_config(ls_i=0, trust_level=2, timeout=30)
        else:
            return LiteClient.from_mainnet_config(ls_i=0, trust_level=2, timeout=30)
    else:
        logger.info(f"Using liteclient config from {config_path}")
        with open(config_path) as src:
            conf = json.loads(src.read())
            return LiteClient.from_config(conf)

"""
Utility class for lazy inittialization of config cell.
Config cell is required for proper emulator initialization.
"""
class ConfigCellHolder:
    def get(self):
        if getattr(self, '_CONFIG_CELL', None) is None:
            logger.info("Initing config cell from the blockchain")
            asyncio.run(self._get_cell())
            logger.info("Config cell initialized")
        return self._CONFIG_CELL
    
    async def _get_cell(self):
        client = create_lite_client()
        await client.connect()
        mc = await client.get_masterchain_info()
        logger.info(f"Got mc info: {mc}")
        cell = await client._get_config_cell(client.last_mc_block)
        self._CONFIG_CELL = cell
        await client.close()


CONFIGCELL = ConfigCellHolder()

"""
Any non zero exit codes from the emulator are considered non critical
and are ignored.
"""
class EmulatorException(NonCriticalParserError):

    def __init__(self, message, result = None):
        super().__init__(message)
        self.result = result
    
"""
Utility base class for all pytvm backed parsers. Handles 
states and prepares TvmEmulator for the child class.

It uses db cache for mc libraries:
CREATE TABLE parsed.mc_libraries (
	boc varchar NOT NULL,
	CONSTRAINT mc_libraries_pk PRIMARY KEY (boc)
);

"""
class EmulatorParser(Parser):
    def __init__(self, emulator_path):
        if emulator_path is not None:
            self.engine = EmulatorEngineC(emulator_path)
            self.engine.emulator_set_verbosity_level(0)
            logger.info(f"Emulator initialized {emulator_path}!")
        self.libs = None

    def topics(self):
        return [TOPIC_ACCOUNT_STATES]
    
    """
    Read cached mc libraries from DB
    TODO: move it to some singleton instance...
    """
    def prepare(self, db: DB):
        libs = db.get_mc_libraries()
        logger.info(f"Got {len(libs)} mc libraries")
        def value_serializer(src: Cell, dest: Builder):
            dest.store_ref(src)

        libs_map = {}
        for lib in libs:
            cell = Cell.one_from_boc(lib)
            libs_map[int(cell.hash.hex(), 16)] = cell

        hm = HashMap(256, value_serializer=value_serializer)
        hm.map = libs_map
        self.libs = hm.serialize()
        if not self.libs:
            # special case for empty libraries at the first run
            self.libs = Builder().store_uint(0, 2).end_cell()
    
    def predicate(self, obj) -> bool:
        return obj.get("data_boc", None) is not None and obj.get("code_boc", None) is not None
    
    def handle_internal(self, obj, db: DB):
        emulator = self._prepare_emulator(obj)
        self._do_parse(obj, db, emulator)

    async def get_lib(self, lib_hash):
        client = create_lite_client()
        await client.connect()
        lib = await client.get_libraries([lib_hash])
        await client.close()
        return lib.get(lib_hash.lower(), None)

    def _prepare_emulator(self, obj):
        assert self.libs is not None, "libs are not inited"
        emulator = TvmEmulator(obj.get('code_boc').encode("utf-8"), obj.get('data_boc').encode("utf-8"), verbosity_level=0, engine=self.engine)
        emulator.set_c7(address=obj.get('account'),
                        unixtime=int(time.time()),
                        balance=10, 
                        rand_seed_hex="0443c4e42c6ae4c9e62b584098bc73a699c654130260ae0c4a8a24605921c0be", 
                        config=CONFIGCELL.get())
        emulator.set_libraries(self.libs)
        emulator.set_gas_limit(10000000)

        return emulator
    
    def _execute_method(self, emulator, method, stack, db: DB, obj):
        result = emulator.run_get_method(method=method, stack=stack)
        if not result['success']:
            raise EmulatorException(f"Method {method} execution failed: {result}", result)
        if result['vm_exit_code'] == 9 and 'missing_library' in result and result['missing_library'] is not None:
            missing_library = result['missing_library']

            logger.warning(f"Got missing library {missing_library}: {result}")
            lib = asyncio.run(self.get_lib(missing_library))
            if not lib:
                logger.error(f"Failed to get library {missing_library}")
                raise EmulatorException(f"Library {missing_library} not found")
            db.insert_mc_library(base64.b64encode(lib.to_boc()).decode())
            self.prepare(db)
            emulator = self._prepare_emulator(obj)
            # retry execution with new instance of emulator
            return self._execute_method(emulator, method, stack, db, obj)
            
        if result['vm_exit_code'] != 0:
            raise EmulatorException(f"Method {method} execution failed with wrong exit code {result['vm_exit_code']}: {result}", result)
        # logger.info(f"Method {method} execution result: {result}")
        return result['stack']

    # Actual implementation
    def _do_parse(self, obj, db: DB, emulator: TvmEmulator):
        raise Exception("Not implemented")