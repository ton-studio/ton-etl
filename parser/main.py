#!/usr/bin/env python
import psycopg2
import os
import time
import json
import traceback
from loguru import logger
from db import DB
from confluent_kafka import Consumer, KafkaError
from model.parser import Parser
from parsers import generate_parsers


def process_msg(obj, topic, parsers_map, db):
    if obj.get('__op') == 'd':
        return 0
    handled = 0
    for parser in parsers_map.get(topic, []):
        if parser.handle(obj, db):
            handled = 1
    return handled


if __name__ == "__main__":
    group_id = os.environ.get("KAFKA_GROUP_ID")
    commit_batch_size = int(os.environ.get("COMMIT_BATCH_SIZE", "100"))
    topics = os.environ.get("KAFKA_TOPICS", "ton.public.latest_account_states,ton.public.messages,ton.public.nft_transfers")
    log_interval = int(os.environ.get("LOG_INTERVAL", '10'))
    # during initial processing we can be in the situation where we process a lot of messages without any DB updates
    max_processed_items = int(os.environ.get("MAX_PROCESSED_ITEMS", '1000000'))
    # idle commit timer — flush partial batch when upstream is quiet, so consumer lag drains on indexer stop
    commit_interval = int(os.environ.get("COMMIT_INTERVAL", '600'))
    supported_parsers = os.environ.get("SUPPORTED_PARSERS", "*")
    # to avoid race conditions we will process messages with maturity greater than MIN_MATURITY_SECONDS
    min_maturity = int(os.environ.get("MIN_MATURITY_SECONDS", "0")) * 1000
    save_dex_pool_history = int(os.environ.get("DEX_POOL_HISTORY", "0"))

    db = DB(Parser.USE_MESSAGE_CONTENT, dex_pool_history=save_dex_pool_history, run_migrations=os.environ.get("RUN_MIGRATIONS", "0") == "1")
    db.acquire()

    PARSERS = generate_parsers(None if supported_parsers == '*' else set(supported_parsers.split(",")))
    for parser_list in PARSERS.values():
        for parser in parser_list:
            parser.prepare(db)

    # PROCESS_* one-shot reprocessing modes — iterate DB rows directly, no Kafka subscribe/commit
    process_mode_generator = None
    if os.environ.get("PROCESS_ONE_HASH"):
        process_mode_generator = db.get_messages_for_processing(os.environ.get("PROCESS_ONE_HASH"))
    elif os.environ.get("PROCESS_ONE_HASH_STATE"):
        process_mode_generator = db.get_account_state_for_processing(os.environ.get("PROCESS_ONE_HASH_STATE").upper())
    elif os.environ.get("PROCESS_JETTONS_ONE_TRACE_ID"):
        process_mode_generator = db.get_jetton_transfers_for_processing(os.environ.get("PROCESS_JETTONS_ONE_TRACE_ID"))

    last = time.time()
    total = 0
    kafka_batch = 0
    successful = 0

    if process_mode_generator is not None:
        logger.info("Running in PROCESS_* one-shot mode (DB iterator, no Kafka)")
        for msg in process_mode_generator:
            try:
                total += 1
                kafka_batch += 1
                obj = json.loads(msg.value.decode("utf-8"))
                successful += process_msg(obj, msg.topic, PARSERS, db)
                now = time.time()
                if now - last > log_interval:
                    logger.info(f"{1.0 * total / (now - last):0.2f} messages per second ({total} processed), {100.0 * successful / total:0.2f}% handled")
                    last = now
                    successful = 0
                    total = 0
            except Exception as e:
                logger.error(f"Failed to process item {msg}: {e} {traceback.format_exc()}")
                raise
            # Per-batch DB commit so a long reprocess does not hold one giant transaction
            if db.updated >= commit_batch_size:
                db.release()
                db.acquire()
                kafka_batch = 0
        db.release()
        logger.info("PROCESS_* mode complete")
    else:
        consumer = Consumer({
            'group.id': group_id,
            'bootstrap.servers': os.environ.get("KAFKA_BROKER"),
            'auto.offset.reset': os.environ.get("KAFKA_OFFSET_RESET", 'earliest'),
            'enable.auto.commit': False,
            'max.poll.interval.ms': int(os.environ.get("KAFKA_MAX_POLL_INTERVAL_MS", '300000')),
        })
        topic_list = topics.split(",")
        logger.info(f"Subscribing to {topic_list}")
        consumer.subscribe(topic_list)

        last_commit_at = time.time()

        while True:
            # Idle commit check — fires even when poll returns None, so lag drains when upstream is quiet.
            # Time-based trigger uses kafka_batch (not db.updated) so it also drains streams where
            # most messages are filtered out and never produce DB writes.
            should_commit = (
                db.updated >= commit_batch_size
                or (kafka_batch > 0 and time.time() - last_commit_at > commit_interval)
                or (db.updated == 0 and kafka_batch > max_processed_items)
            )
            if should_commit:
                logger.info(f"Committing: {db.updated} DB updates, {kafka_batch} items, {time.time() - last_commit_at:0.1f}s since last commit")
                db.release()
                consumer.commit(asynchronous=False)
                db.acquire()
                kafka_batch = 0
                last_commit_at = time.time()

            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                _ts_type, ts_ms = msg.timestamp()
                if ts_ms and min_maturity and ts_ms > time.time() * 1000 - min_maturity:
                    wait_interval_ms = ts_ms - time.time() * 1000 + min_maturity + 100
                    logger.info(f"Waiting for {wait_interval_ms / 1000:0.1f} s before processing next message, timestamp {ts_ms}, partition {msg.partition()}")
                    time.sleep(wait_interval_ms / 1000)

                total += 1
                kafka_batch += 1
                obj = json.loads(msg.value().decode("utf-8"))
                successful += process_msg(obj, msg.topic(), PARSERS, db)
                now = time.time()
                if now - last > log_interval:
                    logger.info(f"{1.0 * total / (now - last):0.2f} Kafka messages per second ({total} processed), {100.0 * successful / total:0.2f}% handled")
                    last = now
                    successful = 0
                    total = 0
            except Exception as e:
                logger.error(f"Failed to process item {msg}: {e} {traceback.format_exc()}")
                raise
