#!/usr/bin/env python
import psycopg2
import os
import time
import json
import traceback
from loguru import logger
from db import DB
from confluent_kafka import Consumer, KafkaError, OFFSET_END, TopicPartition
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


def process_cache_event(obj, topic, cache_parsers_map, db):
    op = obj.get('__op')
    if op not in ('c', 'u'):
        return
    for parser in cache_parsers_map.get(topic, []):
        try:
            parser.on_cache_event(obj, db)
        except Exception as e:
            logger.error(f"Failed to apply cache event on {parser.__class__.__name__}: {e}")


def unique_parsers(parsers_dict):
    seen = set()
    for parser_list in parsers_dict.values():
        for parser in parser_list:
            if id(parser) not in seen:
                seen.add(id(parser))
                yield parser


def build_cache_topics_map(parsers_dict):
    cache_map = {}
    for parser in unique_parsers(parsers_dict):
        for t in parser.cache_topics():
            cache_map.setdefault(t, []).append(parser)
    return cache_map


if __name__ == "__main__":
    group_id = os.environ.get("KAFKA_GROUP_ID")
    commit_batch_size = int(os.environ.get("COMMIT_BATCH_SIZE", "100"))
    topics = os.environ.get("KAFKA_TOPICS", "ton.public.latest_account_states,ton.public.messages,ton.public.nft_transfers")
    log_interval = int(os.environ.get("LOG_INTERVAL", '10'))
    # during initial processing we can be in the situation where we process a lot of messages without any DB updates
    max_processed_items = int(os.environ.get("MAX_PROCESSED_ITEMS", '1000000'))
    # idle commit timer — flush partial batch when upstream is quiet, so consumer lag drains on indexer stop
    commit_interval = int(os.environ.get("COMMIT_INTERVAL", '600'))
    # timer fallback for parser cache reload — safety net against missed Kafka cache events
    cache_reload_interval = int(os.environ.get("PARSER_CACHE_RELOAD_SEC", '3600'))
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
        broker = os.environ.get("KAFKA_BROKER")
        consumer = Consumer({
            'group.id': group_id,
            'bootstrap.servers': broker,
            'auto.offset.reset': os.environ.get("KAFKA_OFFSET_RESET", 'earliest'),
            'enable.auto.commit': False,
            'max.poll.interval.ms': int(os.environ.get("KAFKA_MAX_POLL_INTERVAL_MS", '300000')),
        })
        cache_parsers_map = build_cache_topics_map(PARSERS)
        cache_topics_set = set(cache_parsers_map.keys())
        event_topics = list(set(topics.split(",")))
        logger.info(f"Subscribing (event) to {event_topics}; cache topics (broadcast): {sorted(cache_topics_set) or 'none'}")
        consumer.subscribe(event_topics)

        # Cache consumer — broadcast pattern via assign() to all partitions of all cache topics.
        # Every pod in a replica set sees every cache event, independent of partition assignment
        # of the main consumer group. No group coordination for cache — offsets are not persisted,
        # startup begins at OFFSET_END (prepare() snapshot covers historical state), timer reload
        # is the safety net for anything missed while the pod was down.
        cache_consumer = None
        if cache_topics_set:
            cache_consumer = Consumer({
                'group.id': f"cache-broadcast-{os.environ.get('HOSTNAME', 'unknown')}",
                'bootstrap.servers': broker,
                'enable.auto.commit': False,
            })
            all_tps = []
            for topic in cache_topics_set:
                md = cache_consumer.list_topics(topic, timeout=10).topics.get(topic)
                if md is None or md.error is not None:
                    logger.warning(f"Cache topic {topic} metadata unavailable, skipping")
                    continue
                for partition in md.partitions.keys():
                    all_tps.append(TopicPartition(topic, partition, OFFSET_END))
            if all_tps:
                cache_consumer.assign(all_tps)
                logger.info(f"Cache consumer assigned to {len(all_tps)} partitions across {sorted(cache_topics_set)}")
            else:
                cache_consumer = None

        last_commit_at = time.time()
        # Start timer such that first tick fires quickly — closes gap between prepare() snapshot
        # and consumer subscribe (any cache events landed in that window are picked up).
        last_cache_reload_at = time.time() - cache_reload_interval + 60

        while True:
            # Timer-based cache reload — safety net against missed Kafka events / cold-start dead loops.
            # Runs independently of predicate, so cache-only-predicate parsers cannot get stuck at cache=0.
            if time.time() - last_cache_reload_at > cache_reload_interval:
                for parser in unique_parsers(PARSERS):
                    try:
                        parser.reload_cache(db)
                    except Exception as e:
                        logger.error(f"reload_cache failed on {parser.__class__.__name__}: {e}")
                last_cache_reload_at = time.time()
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

            # Drain any pending cache events first (non-blocking) so they land ahead of
            # the state events that depend on them. Cache events are rare, so this loop
            # exits quickly in normal operation.
            if cache_consumer is not None:
                while True:
                    cache_msg = cache_consumer.poll(timeout=0)
                    if cache_msg is None:
                        break
                    if cache_msg.error():
                        if cache_msg.error().code() != KafkaError._PARTITION_EOF:
                            logger.error(f"Cache consumer error: {cache_msg.error()}")
                        break
                    try:
                        cache_obj = json.loads(cache_msg.value().decode("utf-8"))
                        process_cache_event(cache_obj, cache_msg.topic(), cache_parsers_map, db)
                    except Exception as e:
                        logger.error(f"Failed to process cache item {cache_msg.topic()}:{cache_msg.partition()}:{cache_msg.offset()} {cache_msg.value()}: {e} {traceback.format_exc()}")

            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                total += 1
                kafka_batch += 1
                obj = json.loads(msg.value().decode("utf-8"))
                _ts_type, ts_ms = msg.timestamp()
                if ts_ms and min_maturity and ts_ms > time.time() * 1000 - min_maturity:
                    wait_interval_ms = ts_ms - time.time() * 1000 + min_maturity + 100
                    logger.info(f"Waiting for {wait_interval_ms / 1000:0.1f} s before processing next message, timestamp {ts_ms}, partition {msg.partition()}")
                    time.sleep(wait_interval_ms / 1000)
                successful += process_msg(obj, msg.topic(), PARSERS, db)
                now = time.time()
                if now - last > log_interval:
                    logger.info(f"{1.0 * total / (now - last):0.2f} Kafka messages per second ({total} processed), {100.0 * successful / total:0.2f}% handled")
                    last = now
                    successful = 0
                    total = 0
            except Exception as e:
                logger.error(f"Failed to process item {msg.topic()}:{msg.partition()}:{msg.offset()} {msg.value()}: {e} {traceback.format_exc()}")
                raise
