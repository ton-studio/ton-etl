#!/usr/bin/env python

import base64
from datetime import datetime
import decimal
import os
import json
import signal
import traceback
from typing import Dict
from topics import TOPIC_BLOCKS
from loguru import logger
from confluent_kafka import Consumer, Producer, KafkaError
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


class GracefulShutdown:
    shutdown = False

    @classmethod
    def install(cls):
        signal.signal(signal.SIGTERM, cls._handle)
        signal.signal(signal.SIGINT, cls._handle)

    @classmethod
    def _handle(cls, signum, _frame):
        logger.info(f"Received signal {signum}, scheduling graceful shutdown")
        cls.shutdown = True


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
        bootstrap = os.environ.get("KAFKA_BROKER")

        self.consumer = Consumer({
            'group.id': group_id,
            'bootstrap.servers': bootstrap,
            'enable.auto.commit': False,
        })

        self.producer = Producer({
            'bootstrap.servers': bootstrap,
        })

        topics = set()
        for converter in CONVERTERS.values():
            for topic in converter.topics():
                topics.add(topic)

        topics = list(topics)
        logger.info(f"Subscribing to {topics}")
        self.consumer.subscribe(topics)

    @staticmethod
    def _on_delivery(err, msg):
        # Surface async produce errors to logs — without this, failed deliveries are only
        # known to librdkafka's internal logger and never reach Python.
        if err is not None:
            logger.error(f"Producer delivery failed for topic {msg.topic()}: {err}")

    def _produce(self, topic, value, ts_ms):
        # Drop timestamp when source has none (msg.timestamp() returns -1) — producing with
        # timestamp=-1 results in invalid downstream record metadata.
        kwargs = {'timestamp': ts_ms} if ts_ms and ts_ms > 0 else {}
        try:
            self.producer.produce(topic, value, on_delivery=self._on_delivery, **kwargs)
        except BufferError:
            # librdkafka internal queue full — drain delivery callbacks to free space and retry once
            logger.warning(f"Producer queue full, draining and retrying produce to {topic}")
            self.producer.poll(1.0)
            self.producer.produce(topic, value, on_delivery=self._on_delivery, **kwargs)

    def _flush_and_commit(self, total):
        # Always flush producer BEFORE committing consumer offset — otherwise we could
        # mark input as consumed while the converted output is still buffered in librdkafka
        # and would be lost on pod crash.
        self.producer.flush()
        self.consumer.commit(asynchronous=False)
        logger.info(f"Committed: {total} items processed since last commit")

    def run(self):
        logger.info("Starting streaming")
        last_mc_block = 0
        total = 0

        while not GracefulShutdown.shutdown:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                # Idle = queue drained, no in-flight events for any pending MC block —
                # safe to commit. Drains lag when upstream (indexer) is quiet.
                if total > 0:
                    self._flush_and_commit(total)
                    total = 0
                # Allow producer to process delivery callbacks
                self.producer.poll(0)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                topic = msg.topic()
                _ts_type, ts_ms = msg.timestamp()
                for name, converter in CONVERTERS.items():
                    if topic in converter.topics():
                        obj = json.loads(msg.value().decode("utf-8"))
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
                            self._produce(output_topic, json.dumps(prepare_output(item)).encode("utf-8"), ts_ms)
                        # Trigger delivery callbacks + handle backpressure
                        self.producer.poll(0)
                        total += 1
                # Will commit each time the new masterchain block is received
                if topic == TOPIC_BLOCKS:
                    if obj['workchain'] == -1:
                        mc_seqno = obj['seqno']
                        if mc_seqno > last_mc_block:
                            last_mc_block = mc_seqno
                            logger.info(f"New MC block: {mc_seqno}, committing, {total} items processed")
                            self._flush_and_commit(total)
                            total = 0

            except Exception as e:
                logger.error(f"Failed to process item {msg}: {e} {traceback.format_exc()}")
                raise

        logger.info("Shutdown: final producer flush + consumer commit")
        try:
            self._flush_and_commit(total)
        except Exception as e:
            logger.error(f"Error during final flush/commit: {e}")
        finally:
            self.consumer.close()
        logger.info("Shutdown complete")



if __name__ == "__main__":
    GracefulShutdown.install()
    StreamWriter().run()
