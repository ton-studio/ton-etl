#!/usr/bin/env python

import base64
from datetime import datetime
import decimal
import os
import json
import traceback
from typing import Dict
from topics import TOPIC_BLOCKS
from loguru import logger
from kafka import KafkaConsumer, KafkaProducer
from converters.messages import MessageConverter, MessageWithDataConverter
from converters.jetton_events import JettonEventsConverter
from converters.blocks import BlocksConverter
from converters.transactions import TransactionsConverter
from converters.account_states import AccountStatesConverter
from converters.jetton_metadata import JettonMetadataConverter
from converters.dex_trades import DexTradesConverter
from converters.dex_pools import DexPoolsConverter
from converters.balances_history import BalancesHistoryConverter



CONVERTERS = {
    "messages": MessageWithDataConverter() if os.environ.get("STREAMING_MESSAGE_FULL", "true") == "true" else MessageConverter(),
    "transactions": TransactionsConverter(),
    "jetton_events": JettonEventsConverter(),
    "blocks": BlocksConverter(),
    "account_states": AccountStatesConverter(),
    "jetton_metadata": JettonMetadataConverter(),
    "dex_trades": DexTradesConverter(),
    "dex_pools": DexPoolsConverter(),
    "balances_history": BalancesHistoryConverter(),
}

FIELDS_TO_REMOVE = ['__op', '__table', '__source_ts_ms', '__lsn']

PREFIX = "streaming_"

def prepare_output(obj):
    for k, v in obj.items():
        if isinstance(v, bytes):
            obj[k] = base64.b64encode(v).decode("utf-8")
        if isinstance(v, decimal.Decimal):
            obj[k] = str(v)
    return obj

class StreamWriter:
    def __init__(self):
        group_id = os.environ.get("KAFKA_GROUP_ID")

        self.consumer = KafkaConsumer(
                group_id=group_id,
                bootstrap_servers=os.environ.get("KAFKA_BROKER"),
                enable_auto_commit=False
                )
        
        self.producer = KafkaProducer(
                bootstrap_servers=os.environ.get("KAFKA_BROKER")
                )
        
        topics = set()
        for converter in CONVERTERS.values():
            for topic in converter.topics():
                topics.add(topic)

        topics = list(topics)
        logger.info(f"Subscribing to {topics}")
        self.consumer.subscribe(topics)

    def run(self):
        logger.info("Starting streaming")
        last_mc_block = 0
        total = 0

        for msg in self.consumer:
            try:
                topic = msg.topic
                for name, converter in CONVERTERS.items():
                    if topic in converter.topics():
                        obj = json.loads(msg.value.decode("utf-8"))
                        __op = obj.get('__op', None)
                        if not (__op == 'c' or __op == 'r' or (converter.updates_enabled and __op == 'u')): # ignore everything apart from new items (c - new item, r - initial snapshot)
                            continue
                        table = obj['__table']
                        for f in FIELDS_TO_REMOVE:
                            del obj[f]

                        # logger.info(f"Received message {obj} for {name}")
                        output = converter.convert(obj, table_name=table)
                        if not output:
                            continue
                        if type(output) != list:
                            output = [output]
                        output_topic = f"{PREFIX}{name}"
                        for item in output:
                            self.producer.send(output_topic, json.dumps(prepare_output(item)).encode("utf-8"), timestamp_ms=msg.timestamp)
                        total += 1
                # Will commit each time the new masterchain block is received
                if topic == TOPIC_BLOCKS:
                    if obj['workchain'] == -1:
                        mc_seqno = obj['seqno']
                        if mc_seqno > last_mc_block:
                            last_mc_block = mc_seqno
                            logger.info(f"New MC block: {mc_seqno}, committing, {total} items processed")
                            self.consumer.commit()
                            total = 0
                    
            except Exception as e:
                logger.error(f"Failed to process item {msg}: {e} {traceback.format_exc()}")
                raise



if __name__ == "__main__":
    StreamWriter().run()

