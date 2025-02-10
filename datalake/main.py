#!/usr/bin/env python

from datetime import datetime
import hashlib
import os
import time
import json
import traceback
from typing import Dict
from loguru import logger
from kafka import KafkaConsumer
import boto3
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from converters.balances_history import BalancesHistoryConverter
from converters.messages import MessageConverter, MessageWithDataConverter
from converters.jetton_events import JettonEventsConverter
from converters.blocks import BlocksConverter
from converters.dex_trades import DexTradesConverter
from converters.transactions import TransactionsConverter
from converters.account_states import AccountStatesConverter
from converters.jetton_metadata import JettonMetadataConverter
from converters.dex_pools import DexPoolsConverter
from converters.nft_items import NFTItemsConverter
from converters.nft_transfers import NFTTransfersConverter
from converters.nft_sales import NFTSalesConverter
from converters.nft_metadata import NFTMetadataConverter

AVRO_TMP_BUFFER = "tmp_buffer.avro"
FLUSH_INTERVAL = 100

CONVERTERS = {
    "messages": MessageConverter(),
    "messages_with_data": MessageWithDataConverter(),
    "transactions": TransactionsConverter(),
    "jetton_events": JettonEventsConverter(),
    "blocks": BlocksConverter(),
    "account_states": AccountStatesConverter(),
    "jetton_metadata": JettonMetadataConverter(),
    "dex_trades": DexTradesConverter(),
    "dex_pools": DexPoolsConverter(),
    "balances_history": BalancesHistoryConverter(),
    "nft_items": NFTItemsConverter(),
    "nft_transfers": NFTTransfersConverter(),
    "nft_sales": NFTSalesConverter(),
    "nft_metadata": NFTMetadataConverter()
}

FIELDS_TO_REMOVE = ['__op', '__table', '__source_ts_ms', '__lsn']

class Partition:
    def __init__(self, partition, schema):
        self.partition = partition
        self.count = 0
        self.filename = f"{partition}.avro"
        self.last_event_ts = None
        if os.path.exists(self.filename):
            logger.info(f"Removing {self.filename}")
            os.remove(self.filename)
        self.writer = DataFileWriter(open(self.filename, "wb"), DatumWriter(), schema)
        self.file_size = 0

    def __del__(self):
        self.writer.close()
        os.remove(self.filename)

    def append(self, obj):
        self.writer.append(obj)
        self.count += 1
        self.last_event_ts = int(time.time())
        if self.total % FLUSH_INTERVAL == 0:
            self.writer.flush()
                
        self.file_size = os.path.getsize(self.filename)

    def flush_file(self, datalake):
        self.writer.close()
        with open(self.filename, "rb") as f:
            sha256 = hashlib.sha256(f.read()).hexdigest()[0:32]
        path = f"{datalake.datalake_s3_prefix}{datalake.converter.name()}/date={self.partition}/{sha256}.avro"
        self.file_size = os.path.getsize(self.filename)
    

PARTITION_MODE_ADDING_DATE = "adding_date"
PARTITION_MODE_OBJ_IMESTAMP = "obj_timestamp"

class DatalakeWriter:
    def __init__(self, partition_mode: str):
        self.partition_mode = partition_mode

        converter_name = os.environ.get("CONVERTER", "messages")
        assert converter_name in CONVERTERS, f"Converter {converter_name} not found"
        self.converter = CONVERTERS[converter_name]
        self.writer = DataFileWriter(open(AVRO_TMP_BUFFER, "wb"), DatumWriter(), self.converter.schema)

        group_id = os.environ.get("KAFKA_GROUP_ID")
        topics = os.environ.get("KAFKA_TOPICS", "ton.public.messages").split(",")

        self.max_file_size = int(os.environ.get("MAX_FILE_SIZE", '100000000'))
        self.log_interval = int(os.environ.get("LOG_INTERVAL", '10'))

        # We should commit after commit_interval
        self.commit_interval = int(os.environ.get("COMMIT_INTERVAL", '7200'))
        # But only if we have at least min_commit_size in the buffer
        self.min_commit_size = int(os.environ.get("MIN_COMMIT_SIZE", '1000000'))

        self.datalake_s3_bucket = os.environ.get("DATALAKE_S3_BUCKET")
        self.datalake_s3_prefix = os.environ.get("DATALAKE_S3_PREFIX")


        self.consumer = KafkaConsumer(
                group_id=group_id,
                bootstrap_servers=os.environ.get("KAFKA_BROKER"),
                auto_offset_reset=os.environ.get("KAFKA_OFFSET_RESET", 'earliest'),
                enable_auto_commit=False
                )

        logger.info(f"Subscribing to {topics}")
        self.consumer.subscribe(topics)

    def append(self, obj, partition):
        if obj is None:
            return
        if type(obj) == list:
            for item in obj:
                self.append(item, partition)
            return

        if self.partition_mode == PARTITION_MODE_ADDING_DATE:
            self.append_adding_date(obj)
        elif self.partition_mode == PARTITION_MODE_OBJ_IMESTAMP:
            self.append_obj_timestamp(obj, partition)
        else:
            raise ValueError(f"Unknown partition mode {self.partition_mode}")

    def append_adding_date(self, obj):
        self.writer.append(obj)
        self.total += 1
        if self.total % FLUSH_INTERVAL == 0:
            self.writer.flush()
        self.file_size = os.path.getsize(AVRO_TMP_BUFFER)
        if self.file_size > self.max_file_size or (time.time() - self.last_commit > self.commit_interval):
            logger.info(f"Reached max file size {self.file_size}, {time.time() - self.last_commit:0.1f}s since last commit, flushing file")
            self.writer.flush()
            self.writer.close()
            with open(AVRO_TMP_BUFFER, "rb") as f:
                sha256 = hashlib.sha256(f.read()).hexdigest()[0:32]
            partition = datetime.now().strftime('%Y%m%d')
            path = f"{self.datalake_s3_prefix}{self.converter.name()}/adding_date={partition}/{sha256}.avro"
            logger.info(f"Going to flush file, total size is {self.file_size}B, {time.time() - self.last_commit:0.1f}s since last commit, {self.total} items to {path}")

            self.s3.upload_file(AVRO_TMP_BUFFER, self.datalake_s3_bucket, path)
            self.writer = DataFileWriter(open(AVRO_TMP_BUFFER, "wb"), DatumWriter(), self.converter.schema)
            now = time.time()
            self.last_commit = now
            logger.info(f"{1.0 * self.total / (now - self.last):0.2f} Kafka messages per second")
            self.last = now
            self.total = 0
            self.consumer.commit()

    def append_obj_timestamp(self, obj, partition):
        raise NotImplementedError("Not implemented yet")


    def run(self):
        self.last = time.time()
        self.last_commit = time.time()
        self.total = 0
        self.s3 = boto3.client('s3')

        for msg in self.consumer:
            try:
                self.total += 1
                obj = json.loads(msg.value.decode("utf-8"))
                __op = obj.get('__op', None)
                if not (__op == 'c' or __op == 'r' or (self.converter.updates_enabled and __op == 'u')): # ignore everything apart from new items (c - new item, r - initial snapshot)
                    continue


                local_partition = self.converter.partition(obj)
                table = obj['__table']
                for f in FIELDS_TO_REMOVE:
                    del obj[f]

                if self.converter.strict:
                    self.append(self.converter.convert(obj, table_name=table), local_partition)
                else:
                    try:
                        self.append(self.converter.convert(obj, table_name=table), local_partition)
                    except Exception as e:
                        logger.error(f"Failed to convert item {obj}: {e} {traceback.format_exc()}")
                        continue
                    
            except Exception as e:
                logger.error(f"Failted to process item {msg}: {e} {traceback.format_exc()}")
                raise



if __name__ == "__main__":
    if os.path.exists(AVRO_TMP_BUFFER):
        logger.info(f"Removing {AVRO_TMP_BUFFER}")
        os.remove(AVRO_TMP_BUFFER)

    DatalakeWriter(os.environ.get("PARTITION_MODE", PARTITION_MODE_ADDING_DATE)).run()

