"""Kafka -> Collector -> stores.

Point OpenLineage emitters at Kafka (``transport.type=kafka``) and run one
collector per few partitions. Offsets are committed only after a batch is
durably written, giving at-least-once delivery; run merging and idempotent
upserts make replays safe. Malformed events go to a dead-letter topic.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time

from agent_authority.collector.pipeline import Collector, InvalidEvent
from agent_authority.collector.policy import CapturePolicy

log = logging.getLogger("agent_authority.collector")


def main() -> None:
    from confluent_kafka import Consumer, Producer

    from agent_authority.storage.composite import build_prod_store_from_env

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    brokers = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "openlineage.events")
    dlq = os.getenv("KAFKA_DLQ_TOPIC", "openlineage.events.dlq")
    batch_max = int(os.getenv("COLLECTOR_BATCH_MAX", "5000"))
    batch_ms = int(os.getenv("COLLECTOR_BATCH_MS", "1000"))

    store = build_prod_store_from_env()
    collector = Collector(store, capture_policy=CapturePolicy.load(os.getenv("AUTHORITY_POLICY")))
    consumer = Consumer({
        "bootstrap.servers": brokers,
        "group.id": os.getenv("KAFKA_GROUP", "authority-collector"),
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": brokers})
    consumer.subscribe([topic])

    running = True

    def stop(*_: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    log.info("collecting from %s/%s", brokers, topic)

    while running:
        msgs = consumer.consume(num_messages=batch_max, timeout=batch_ms / 1000)
        if not msgs:
            continue
        t0 = time.perf_counter()
        bad = 0
        for m in msgs:
            if m.error():
                log.warning("kafka error: %s", m.error())
                continue
            try:
                collector.process(json.loads(m.value()))
            except (InvalidEvent, json.JSONDecodeError) as exc:
                bad += 1
                producer.produce(dlq, m.value(), headers={"error": str(exc)[:500]})
        batch = collector.flush()  # raises on store failure -> no commit -> redelivery
        consumer.commit(asynchronous=False)
        producer.poll(0)
        log.info("batch: %d msgs (%d to DLQ), %d runs, %d new facets, %.0f ms, cache hit %.1f%%",
                 len(msgs), bad, len(batch.runs), len(batch.facets), (time.perf_counter() - t0) * 1000,
                 collector.facet_cache.hit_rate * 100)

    producer.flush(5)
    consumer.close()


if __name__ == "__main__":
    main()
