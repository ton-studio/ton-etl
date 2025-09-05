from datetime import datetime
import pandas as pd
import sys
import os
from dataclasses import fields
from tqdm import tqdm
from loguru import logger
from pytoniq_core import Address

from parsers import generate_parsers
from parsers.message.swap_volume import USDT


if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Usage: python backfill.py <parser_name> <messages.csv> <tx_statuses.csv> <wallets.csv> <ton_prices.csv> <output.csv>")
        sys.exit(1)
    parser_name, messages_path, tx_path, wallets_path, ton_prices_path, output_path = sys.argv[1:]
    parsers = list(generate_parsers(set([sys.argv[1]])).values())
    assert len(parsers) == 1, f"Expected 1 parser, got {len(parsers)}, check parser name"
    parser = parsers[0][0]
    logger.info(f"Using parser {parser}")
    
    messages = pd.read_csv(messages_path)
    logger.info(f"Loaded {messages.shape[0]} messages")
    messages['body_boc'] = messages['body_boc'].apply(lambda x: x[2:] if x.startswith('0x') else x)
    

    class DBMock:
        def __init__(self):
            tx_statuses = pd.read_csv(tx_path)
            self.tx_statuses = {}
            for _, row in tx_statuses.iterrows():
                self.tx_statuses[row['hash']] = row['action_result_code'] == 0 and row['compute_exit_code'] == 0
            true_count = sum(1 for status in self.tx_statuses.values() if status)
            false_count = sum(1 for status in self.tx_statuses.values() if not status)
            logger.info(f"Loaded {true_count} successful and {false_count} failed transactions")

            self.wallets = {}
            for _, row in pd.read_csv(wallets_path).iterrows():
                self.wallets[row['jetton_wallet']] = row['jetton_master']
            logger.info(f"Loaded {len(self.wallets)} wallets")

            self.ton_prices = {}
            for _, row in pd.read_csv(ton_prices_path).iterrows():
                self.ton_prices[row['timestamp'].split(' ')[0]] = row['price']
            logger.info(f"Loaded {len(self.ton_prices)} ton prices")

            self.output = []

        def is_tx_successful(self, tx_hash):
            assert tx_hash in self.tx_statuses, f"Transaction {tx_hash} not found in tx_statuses"
            return self.tx_statuses[tx_hash]
        
        def get_latest_account_state(self, address):
            # no need to mock since Parser itself has a fallback to toncenter API
            return None
        
        def get_mc_libraries(self):
            return [] # no support for cache masterchain libraries
        
        def get_parent_message_body(self, msg_hash):
            # logger.info(f"Getting parent message body for {msg_hash}")
            source_tx = messages.query(f"msg_hash == '{msg_hash}' & direction == 'out'")
            assert source_tx.shape[0] != 0, f"Source tx not found for {msg_hash}"
            assert source_tx.shape[0] == 1, f"Multiple source tx found for {msg_hash}"
            source_tx_hash = source_tx.iloc[0]['tx_hash']
            parent_message = messages.query(f"tx_hash == '{source_tx_hash}' & direction == 'in'")
            assert parent_message.shape[0] != 0, f"Parent tx not found for {source_tx_hash}"
            assert parent_message.shape[0] == 1, f"Multiple parent tx found for {source_tx_hash}"
            return parent_message.iloc[0]['body_boc']
        
        def get_wallet_master(self, address):
            address = address.to_str(0).upper()
            assert address in self.wallets, f"Wallet {address} not found in wallets"
            return self.wallets[address]
        
        def get_core_price(self, asset, timestamp):
            assert asset == USDT, "Only USDT is supported"
            date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            if date not in self.ton_prices:
                return None
            assert date in self.ton_prices, f"Ton price not found for {date}"
            return self.ton_prices[date] / 1e3

        def serialize(self, event):
            # Store event for later processing and analysis
            for field in fields(event.__class__):
                if type(getattr(event, field.name)) == Address:
                    setattr(event, field.name, getattr(event, field.name).to_str(is_user_friendly=False).upper())
            self.output.append(event)

        def discover_dex_pool(self, event):
            pass

        def dump(self, output):
            logger.info(f"Dumping {len(self.output)} events")
            pd.DataFrame(self.output).to_csv(output, index=False)

    db = DBMock()
    parser.prepare(db)
    
    for _, row in tqdm(messages.iterrows()):
        parser.handle(row.to_dict(), db)

    db.dump(output_path)
